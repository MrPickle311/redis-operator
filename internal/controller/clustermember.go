package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/instancemanager"
)

// labelCluster is set on the Pods of a shard of a RedisCluster; the seed
// Service of the cluster selects them by it.
const labelCluster = "redis.operator.com/cluster"

// kindRedisCluster is the kind of a RedisCluster in references to it.
const kindRedisCluster = "RedisCluster"

// clusterBusPort is where the nodes of a Redis Cluster talk to each other.
const clusterBusPort = 16379

// clusterOf returns the RedisCluster the instance is a shard of, or "" for a
// RedisInstance of its own. A shard runs Redis in cluster mode: Redis Cluster
// elects the masters, so the logic of the primary of the operator is off.
func clusterOf(instance *redisv1.RedisInstance) string {
	owner := metav1.GetControllerOf(instance)
	if owner == nil || owner.Kind != kindRedisCluster || owner.APIVersion != redisv1.GroupVersion.String() {
		return ""
	}
	return owner.Name
}

// observeShardPrimary records in status.currentPrimary the node of the shard
// that serves slots: Redis Cluster elects it, and the role label and the
// PodDisruptionBudgets follow it. A node that does not answer changes nothing,
// as a master that is down stays the master until a replica takes over. Two
// nodes with slots, an old master back before it learns of the failover,
// change nothing either.
func (r *RedisInstanceReconciler) observeShardPrimary(ctx context.Context, instance *redisv1.RedisInstance) error {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(instance.Namespace), client.MatchingLabels(instanceSelector(instance))); err != nil {
		return err
	}
	var masters []string
	for _, pod := range pods.Items {
		if pod.Status.PodIP == "" || !pod.DeletionTimestamp.IsZero() {
			continue
		}
		status, err := r.InstanceManager.Status(ctx, instancemanager.PodFQDN(pod.Name, instance.Name, instance.Namespace))
		if err == nil && status.Cluster != nil && len(status.Cluster.Slots) > 0 {
			masters = append(masters, pod.Name)
		}
	}
	if len(masters) == 1 {
		instance.Status.CurrentPrimary = masters[0]
	}
	return nil
}

// startClusterFailover makes target, a replica of the shard, take over from
// its master with CLUSTER FAILOVER, which waits until target has every write.
// status.targetPrimary holds target until the shard sees it as its master,
// see finishClusterFailover. The masters elect target, and they give one vote
// per epoch and then none for the same shard for 2 × cluster-node-timeout:
// failovers of two shards at once lose votes, so one shard at a time goes.
func (r *RedisInstanceReconciler) startClusterFailover(ctx context.Context, instance *redisv1.RedisInstance, target string) error {
	var instances redisv1.RedisInstanceList
	if err := r.List(ctx, &instances, client.InNamespace(instance.Namespace)); err != nil {
		return err
	}
	for _, other := range instances.Items {
		if other.Name != instance.Name && clusterOf(&other) == clusterOf(instance) && other.Status.TargetPrimary != "" {
			return nil // the next reconcile tries again
		}
	}
	host := instancemanager.PodFQDN(target, instance.Name, instance.Namespace)
	if _, err := r.InstanceManager.Cluster(ctx, host, "failover", instancemanager.ClusterRequest{}); err != nil {
		return err
	}
	instance.Status.TargetPrimary = target
	r.setSwitchoverCondition(instance, metav1.ConditionTrue, "InProgress",
		fmt.Sprintf("Failing over from %s to %s", instance.Status.CurrentPrimary, target))
	logf.FromContext(ctx).Info("Started cluster failover", "from", instance.Status.CurrentPrimary, "to", target)
	r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, "Switchover", "Switchover",
		"Failing over from %s to %s", instance.Status.CurrentPrimary, target)
	return r.Status().Update(ctx, instance)
}

// finishClusterFailover ends the failover in status.targetPrimary once target
// is the master, or gives up after switchoverTimeout: Redis drops a manual
// failover that did not finish in 5 seconds.
func (r *RedisInstanceReconciler) finishClusterFailover(instance *redisv1.RedisInstance) {
	target := instance.Status.TargetPrimary
	if target == "" {
		return
	}
	if instance.Status.CurrentPrimary == target {
		instance.Status.TargetPrimary = ""
		r.setSwitchoverCondition(instance, metav1.ConditionFalse, "Completed", "The master moved to "+target)
		r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, "SwitchoverCompleted", "Switchover",
			"Moved the master to %s", target)
		return
	}
	if started := meta.FindStatusCondition(instance.Status.Conditions, redisv1.ConditionSwitchover); started == nil ||
		time.Since(started.LastTransitionTime.Time) > switchoverTimeout {
		instance.Status.TargetPrimary = ""
		message := fmt.Sprintf("%s did not take over within %s", target, switchoverTimeout)
		r.setSwitchoverCondition(instance, metav1.ConditionFalse, "Failed", message)
		r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, "SwitchoverFailed", "Switchover", message)
	}
}

// reconcileShardSwitchover moves the master to the Pod in AnnotationPromote.
func (r *RedisInstanceReconciler) reconcileShardSwitchover(ctx context.Context, instance *redisv1.RedisInstance) error {
	target, requested := pendingRequest(instance, redisv1.AnnotationPromote)
	if !requested || instance.Status.TargetPrimary != "" {
		return nil
	}
	if reason := r.switchoverRejection(ctx, instance, target); reason != "" {
		markHandled(instance, redisv1.AnnotationPromote)
		r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, "SwitchoverRejected", "Switchover",
			"Cannot switch over to %s: %s", target, reason)
		return nil
	}
	// Handled only once started: it may wait for another shard.
	if err := r.startClusterFailover(ctx, instance, target); err != nil || instance.Status.TargetPrimary == "" {
		return err
	}
	markHandled(instance, redisv1.AnnotationPromote)
	return nil
}

// reconcileShardScaleDown moves the master off an instance that a scale-down
// removes, to the remaining replica with the most of the dataset; then
// cleanupExcessPods deletes it, and the RedisCluster makes the other nodes
// forget it.
func (r *RedisInstanceReconciler) reconcileShardScaleDown(ctx context.Context, instance *redisv1.RedisInstance) error {
	if instance.Status.TargetPrimary != "" || primaryOrdinal(instance) < instance.Spec.Instances {
		return nil
	}
	target, ok := r.failoverCandidate(ctx, instance)
	if !ok {
		return nil // no remaining replica is Ready yet
	}
	return r.startClusterFailover(ctx, instance, target)
}

// reconcileShard finishes the reconcile of a shard of a RedisCluster once its
// Pods exist. The RedisCluster builds the cluster; the shard moves its master
// with CLUSTER FAILOVER, for a switchover, a scale-down, a failing volume or
// a rolling update, keeps the configuration and the users of Redis and
// reports its state.
//
// ponytail: no switchover off a cordoned node; add it with CLUSTER FAILOVER
// like the rolling update.
func (r *RedisInstanceReconciler) reconcileShard(ctx context.Context, instance *redisv1.RedisInstance,
	oldStatus *redisv1.RedisInstanceStatus, readyCount int32, ready map[int32]bool, outdated []int32,
	healthy, memoryUnlimited bool) (ctrl.Result, error) {
	r.finishClusterFailover(instance)
	if err := r.reconcileShardSwitchover(ctx, instance); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileShardScaleDown(ctx, instance); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileVolumeFailures(ctx, instance, ready); err != nil {
		return ctrl.Result{}, err
	}
	if _, err := r.reconcileRollout(ctx, instance, outdated, healthy); err != nil {
		return ctrl.Result{}, err
	}
	pending := r.reconcileConfig(ctx, instance)
	pending = r.reconcileACLApplied(ctx, instance) || pending

	r.setMemoryLimitedCondition(instance, memoryUnlimited)
	meta.SetStatusCondition(&instance.Status.Conditions, readyCondition(instance, readyCount, len(outdated)))
	instance.Status.ReadyInstances = readyCount
	instance.Status.Phase = fmt.Sprintf("%d/%d ready", readyCount, instance.Spec.Instances)
	if !equality.Semantic.DeepEqual(oldStatus, &instance.Status) {
		if err := r.Status().Update(ctx, instance); err != nil {
			return ctrl.Result{}, err
		}
	}
	if pending || len(outdated) > 0 || instance.Status.TargetPrimary != "" {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	// Redis Cluster fails over on its own, without an event of Kubernetes.
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}
