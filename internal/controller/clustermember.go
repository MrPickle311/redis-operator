package controller

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

// labelCluster is set on the Pods of a shard of a RedisCluster; the seed
// Service of the cluster selects them by it.
const labelCluster = "redis.operator.com/cluster"

// clusterBusPort is where the nodes of a Redis Cluster talk to each other.
const clusterBusPort = 16379

// clusterOf returns the RedisCluster the instance is a shard of, or "" for a
// RedisInstance of its own. A shard runs Redis in cluster mode: Redis Cluster
// elects the masters, so the logic of the primary of the operator is off.
func clusterOf(instance *redisv1.RedisInstance) string {
	owner := metav1.GetControllerOf(instance)
	if owner == nil || owner.Kind != "RedisCluster" || owner.APIVersion != redisv1.GroupVersion.String() {
		return ""
	}
	return owner.Name
}

// reconcileShard finishes the reconcile of a shard of a RedisCluster once its
// Pods exist. The RedisCluster builds the cluster; the shard only keeps the
// configuration and the users of Redis and reports its state.
//
// ponytail: no rolling update and no replacement of a lost volume yet; they
// need CLUSTER FAILOVER instead of the switchover of the operator.
func (r *RedisInstanceReconciler) reconcileShard(ctx context.Context, instance *redisv1.RedisInstance,
	oldStatus *redisv1.RedisInstanceStatus, readyCount int32, outdated int, memoryUnlimited bool) (ctrl.Result, error) {
	pending := r.reconcileConfig(ctx, instance)
	pending = r.reconcileACLApplied(ctx, instance) || pending

	r.setMemoryLimitedCondition(instance, memoryUnlimited)
	meta.SetStatusCondition(&instance.Status.Conditions, readyCondition(instance, readyCount, outdated))
	instance.Status.ReadyInstances = readyCount
	instance.Status.Phase = fmt.Sprintf("%d/%d ready", readyCount, instance.Spec.Instances)
	if !equality.Semantic.DeepEqual(oldStatus, &instance.Status) {
		if err := r.Status().Update(ctx, instance); err != nil {
			return ctrl.Result{}, err
		}
	}
	if pending {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}
