package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

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
		return client.IgnoreNotFound(r.Delete(ctx, policy))
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, policy, func() error {
		policy.Spec = networkPolicySpec(instance, r.OperatorNamespace)
		return ctrl.SetControllerReference(instance, policy, r.Scheme)
	})
	return err
}

// networkPolicySpec lets in the clients of allowFrom and the other instances
// on the port of Redis, the operator on the API of the Instance Manager, and
// anyone on its probes, which come from the kubelet on the node and change
// nothing.
func networkPolicySpec(instance *redisv1.RedisInstance, operatorNamespace string) networkingv1.NetworkPolicySpec {
	// TCP like the default of the API server: CreateOrUpdate must see no change.
	port := func(number int) []networkingv1.NetworkPolicyPort {
		p, tcp := intstr.FromInt(number), corev1.ProtocolTCP
		return []networkingv1.NetworkPolicyPort{{Protocol: &tcp, Port: &p}}
	}
	instances := metav1.LabelSelector{MatchLabels: instanceSelector(instance)}
	return networkingv1.NetworkPolicySpec{
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
}
