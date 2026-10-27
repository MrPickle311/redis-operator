package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

// --- Placement ---

// podAffinity keeps the instances on different nodes, so one failing node
// takes at most one of them and a replica is left to take over.
func podAffinity(instance *redisv1.RedisInstance) *corev1.Affinity {
	term := corev1.PodAffinityTerm{
		LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{labelInstance: instance.Name}},
		TopologyKey:   corev1.LabelHostname,
	}
	antiAffinity := &corev1.PodAntiAffinity{}
	if instance.Spec.Affinity.PodAntiAffinityType == redisv1.PodAntiAffinityPreferred {
		antiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = []corev1.WeightedPodAffinityTerm{
			{Weight: 100, PodAffinityTerm: term},
		}
	} else {
		antiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = []corev1.PodAffinityTerm{term}
	}
	return &corev1.Affinity{PodAntiAffinity: antiAffinity}
}

// --- PodDisruptionBudgets ---

// pdbName returns the name of the PodDisruptionBudget of the Pods in role.
func pdbName(instance *redisv1.RedisInstance, role string) string {
	return fmt.Sprintf("%s-%s", instance.Name, role)
}

// reconcilePDBs keeps voluntary disruptions, like a drain, from taking down
// more than one replica, or the primary before a switchover moved it. They
// follow the role label, so a switchover moves them along. With one instance
// there is no replica to switch over to, and a budget would block a drain
// for good, so there is none.
//
// Both budgets are minAvailable counts: maxUnavailable, like a percentage,
// needs the owner of the Pods to have a scale subresource, which a
// RedisInstance does not, and then allows no disruption at all.
func (r *RedisInstanceReconciler) reconcilePDBs(ctx context.Context, instance *redisv1.RedisInstance) error {
	for _, b := range []struct {
		role         string
		minAvailable int32
	}{
		{rolePrimary, 1},
		{roleReplica, instance.Spec.Instances - 2}, // every replica but one
	} {
		pdb := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: pdbName(instance, b.role), Namespace: instance.Namespace}}
		if instance.Spec.Instances < 2 {
			if err := client.IgnoreNotFound(r.Delete(ctx, pdb)); err != nil {
				return err
			}
			continue
		}
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
			selector := instanceSelector(instance)
			selector[labelRole] = b.role
			pdb.Spec.Selector = &metav1.LabelSelector{MatchLabels: selector}
			minAvailable := intstr.FromInt32(b.minAvailable)
			pdb.Spec.MinAvailable, pdb.Spec.MaxUnavailable = &minAvailable, nil
			// A Pod that is not Ready serves nothing; it must not block a drain.
			alwaysAllow := policyv1.AlwaysAllow
			pdb.Spec.UnhealthyPodEvictionPolicy = &alwaysAllow
			return ctrl.SetControllerReference(instance, pdb, r.Scheme)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// --- Cordoned nodes ---

// reconcileCordon switches over when the node of the primary is cordoned,
// e.g. before a drain, which the PodDisruptionBudget of the primary holds
// back until then. The target is a Ready replica on a node that takes Pods.
func (r *RedisInstanceReconciler) reconcileCordon(ctx context.Context, instance *redisv1.RedisInstance) error {
	if instance.Status.CurrentPrimary == "" || instance.Status.TargetPrimary != "" ||
		meta.IsStatusConditionTrue(instance.Status.Conditions, redisv1.ConditionFailover) {
		return nil
	}
	if _, requested := pendingRequest(instance, redisv1.AnnotationPromote); requested {
		return nil
	}
	node, cordoned := r.cordonedNode(ctx, instance, instance.Status.CurrentPrimary)
	if !cordoned {
		return nil
	}

	var candidates []candidate
	for _, c := range r.readyReplicas(ctx, instance) {
		if _, onCordoned := r.cordonedNode(ctx, instance, instancePodName(instance, c.ordinal)); !onCordoned {
			candidates = append(candidates, c)
		}
	}
	best, ok := bestCandidate(candidates)
	if !ok {
		logf.FromContext(ctx).Info("No replica on a node that takes Pods can take over from the primary", "node", node)
		return nil
	}
	target := instancePodName(instance, best)
	r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, "PrimaryNodeCordoned", "Switchover",
		"Switching over to %s: node %s of the primary is cordoned", target, node)
	return r.startSwitchover(ctx, instance, target)
}

// cordonedNode returns the node of the Pod and whether it takes no new Pods.
func (r *RedisInstanceReconciler) cordonedNode(ctx context.Context, instance *redisv1.RedisInstance, pod string) (string, bool) {
	var p corev1.Pod
	if err := r.Get(ctx, types.NamespacedName{Name: pod, Namespace: instance.Namespace}, &p); err != nil || p.Spec.NodeName == "" {
		return "", false
	}
	var node corev1.Node
	if err := r.Get(ctx, types.NamespacedName{Name: p.Spec.NodeName}, &node); err != nil {
		return p.Spec.NodeName, false
	}
	return node.Name, node.Spec.Unschedulable
}

// instancesOnCordonedNode maps an event of a node to every RedisInstance when
// the node is cordoned: cordons are rare, and the reconcile looks at the node
// of each primary itself.
func (r *RedisInstanceReconciler) instancesOnCordonedNode(ctx context.Context, obj client.Object) []reconcile.Request {
	if node, ok := obj.(*corev1.Node); !ok || !node.Spec.Unschedulable {
		return nil
	}
	var instances redisv1.RedisInstanceList
	if err := r.List(ctx, &instances); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list RedisInstances for a cordoned node", "node", obj.GetName())
		return nil
	}
	requests := make([]reconcile.Request, 0, len(instances.Items))
	for _, instance := range instances.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&instance)})
	}
	return requests
}
