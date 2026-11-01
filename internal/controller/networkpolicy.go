package controller

import (
	"context"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/instancemanager"
)

// operatorPodLabels are the labels of the Pods of the operator, from
// config/manager/manager.yaml.
var operatorPodLabels = map[string]string{
	"control-plane":          "controller-manager",
	"app.kubernetes.io/name": "redis-operator",
}

// reconcileNetworkPolicy creates the NetworkPolicy of the instance when
// spec.networkPolicy.allowFrom is set, and deletes it when it is not. A
// policy isolates a Pod for every port at once, so it lets in all that the
// instance needs, not only the clients.
func (r *RedisInstanceReconciler) reconcileNetworkPolicy(ctx context.Context, instance *redisv1.RedisInstance) error {
	policy := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: instance.Name, Namespace: instance.Namespace}}
	if len(instance.Spec.NetworkPolicy.AllowFrom) == 0 {
		meta.RemoveStatusCondition(&instance.Status.Conditions, redisv1.ConditionNetworkPolicyReady)
		return client.IgnoreNotFound(r.Delete(ctx, policy))
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, policy, func() error {
		policy.Spec = networkPolicySpec(instance, r.OperatorNamespace)
		return ctrl.SetControllerReference(instance, policy, r.Scheme)
	})
	if err != nil {
		return err
	}
	var policies networkingv1.NetworkPolicyList
	if err := r.List(ctx, &policies, client.InNamespace(instance.Namespace)); err != nil {
		return err
	}
	r.setNetworkPolicyCondition(instance, foreignPolicies(instance, policies.Items))
	return nil
}

// foreignPolicies returns the NetworkPolicies, other than the one of the
// operator, that let traffic in to some instance. They still apply: every
// policy that selects a Pod adds to what may reach it, so they may let in
// more than allowFrom.
func foreignPolicies(instance *redisv1.RedisInstance, policies []networkingv1.NetworkPolicy) []string {
	var names []string
	for _, policy := range policies {
		if policy.Name == instance.Name && metav1.IsControlledBy(&policy, instance) {
			continue
		}
		// An empty policyTypes means Ingress, plus Egress with egress rules.
		// Without ingress rules a policy only isolates, like the one of the
		// operator, and lets nothing more in.
		if len(policy.Spec.PolicyTypes) > 0 && !slices.Contains(policy.Spec.PolicyTypes, networkingv1.PolicyTypeIngress) ||
			len(policy.Spec.Ingress) == 0 {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(&policy.Spec.PodSelector)
		if err != nil {
			continue // the API server accepts no such policy
		}
		for _, role := range []string{rolePrimary, roleReplica} {
			if selector.Matches(labels.Set(rolePodLabels(instance, role))) {
				names = append(names, policy.Name)
				break
			}
		}
	}
	slices.Sort(names)
	return names
}

// setNetworkPolicyCondition sets NetworkPolicyReady, and says in an Event
// once per change which policies to remove.
func (r *RedisInstanceReconciler) setNetworkPolicyCondition(instance *redisv1.RedisInstance, foreign []string) {
	condition := metav1.Condition{
		Type: redisv1.ConditionNetworkPolicyReady, Status: metav1.ConditionTrue, ObservedGeneration: instance.Generation,
		Reason: "Applied", Message: "Only the NetworkPolicy of the operator lets traffic in to the instances",
	}
	if len(foreign) > 0 {
		condition.Status, condition.Reason = metav1.ConditionFalse, "ForeignPolicy"
		condition.Message = fmt.Sprintf("The NetworkPolicies %s also select the instances and still apply, so more than "+
			"spec.networkPolicy.allowFrom may reach them; remove them", strings.Join(foreign, ", "))
		previous := meta.FindStatusCondition(instance.Status.Conditions, redisv1.ConditionNetworkPolicyReady)
		if previous == nil || previous.Message != condition.Message {
			r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, "ForeignNetworkPolicy", "Isolate", condition.Message)
		}
	}
	meta.SetStatusCondition(&instance.Status.Conditions, condition)
}

// instancesWithNetworkPolicy maps a NetworkPolicy to the instances of its
// namespace that have one of the operator, since it may select their Pods.
func (r *RedisInstanceReconciler) instancesWithNetworkPolicy(ctx context.Context, obj client.Object) []reconcile.Request {
	var instances redisv1.RedisInstanceList
	if err := r.List(ctx, &instances, client.InNamespace(obj.GetNamespace())); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list RedisInstances for a NetworkPolicy event", "networkPolicy", obj.GetName())
		return nil
	}
	var requests []reconcile.Request
	for _, instance := range instances.Items {
		if len(instance.Spec.NetworkPolicy.AllowFrom) > 0 {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&instance)})
		}
	}
	return requests
}

// networkPolicySpec lets in the clients of allowFrom and the other instances
// on the port of Redis, the operator on the API of the Instance Manager,
// anyone on its probes, which come from the kubelet on the node and change
// nothing, and in a shard the nodes of the cluster on the cluster bus.
func networkPolicySpec(instance *redisv1.RedisInstance, operatorNamespace string) networkingv1.NetworkPolicySpec {
	// TCP like the default of the API server: CreateOrUpdate must see no change.
	port := func(number int) []networkingv1.NetworkPolicyPort {
		p, tcp := intstr.FromInt(number), corev1.ProtocolTCP
		return []networkingv1.NetworkPolicyPort{{Protocol: &tcp, Port: &p}}
	}
	instances := metav1.LabelSelector{MatchLabels: instanceSelector(instance)}
	spec := networkingv1.NetworkPolicySpec{
		PodSelector: instances,
		PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
		Ingress: []networkingv1.NetworkPolicyIngressRule{
			{Ports: port(redisPort), From: instance.Spec.NetworkPolicy.AllowFrom},
			{Ports: port(redisPort), From: []networkingv1.NetworkPolicyPeer{{PodSelector: &instances}}},
			{Ports: port(instancemanager.StatusPort), From: []networkingv1.NetworkPolicyPeer{{
				NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": operatorNamespace}},
				PodSelector:       &metav1.LabelSelector{MatchLabels: operatorPodLabels},
			}}},
			{Ports: port(instancemanager.ProbePort)},
		},
	}
	// The nodes of a Redis Cluster talk on the cluster bus, every one with
	// every other one, across the shards.
	if cluster := clusterOf(instance); cluster != "" {
		nodes := metav1.LabelSelector{MatchLabels: map[string]string{labelCluster: cluster}}
		spec.Ingress = append(spec.Ingress, networkingv1.NetworkPolicyIngressRule{
			Ports: port(clusterBusPort), From: []networkingv1.NetworkPolicyPeer{{PodSelector: &nodes}},
		})
	}
	return spec
}
