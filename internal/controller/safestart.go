package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/instancemanager"
)

// safeStartPollInterval is how often the operator checks a starting primary,
// which becomes Ready only once approved, so no Pod event announces it.
const safeStartPollInterval = 2 * time.Second

// reconcileSafeStart decides about a restarted primary, which lets no replica
// sync until approved: an empty or old dataset would make every replica drop
// its data in a full resync. It approves the primary with /promote, or
// promotes a replica instead. It returns how long to wait before it checks
// again, or 0.
func (r *RedisInstanceReconciler) reconcileSafeStart(ctx context.Context, instance *redisv1.RedisInstance) (time.Duration, error) {
	if instance.Status.CurrentPrimary == "" || instance.Status.TargetPrimary != "" ||
		meta.IsStatusConditionTrue(instance.Status.Conditions, redisv1.ConditionFailover) {
		return 0, nil
	}
	var pod corev1.Pod
	if err := r.Get(ctx, types.NamespacedName{Name: instance.Status.CurrentPrimary, Namespace: instance.Namespace}, &pod); err != nil ||
		isPodReady(&pod) || !pod.DeletionTimestamp.IsZero() || pod.Status.Phase != corev1.PodRunning {
		// Gone, which failover handles; approved already; or not started:
		// turning Running triggers a reconcile.
		return 0, nil
	}
	host := podFQDN(instance, primaryOrdinal(instance))
	callCtx, cancel := context.WithTimeout(ctx, failoverCallTimeout)
	primary, err := r.InstanceManager.Status(callCtx, host)
	cancel()
	if err != nil || primary.Role != "master" || !primary.AwaitingApproval {
		return safeStartPollInterval, nil // still starting
	}

	replicas := r.readyReplicas(ctx, instance)
	reason := safeStartRejection(primary, replicas, instance.Spec.Redis.Persistence)
	if reason == "" {
		if _, err := r.InstanceManager.Promote(ctx, host, instance.Status.PrimaryEpoch); err != nil {
			return 0, err
		}
		logf.FromContext(ctx).Info("Approved the dataset of the restarted primary", "pod", instance.Status.CurrentPrimary)
		return 0, nil
	}
	best, _ := bestCandidate(replicas) // a rejection always names a replica
	if err := r.startFailover(ctx, instance, instancePodName(instance, best), "the restarted primary "+reason); err != nil {
		return 0, err
	}
	return 0, r.continueFailover(ctx, instance)
}

// safeStartRejection explains why a restarted primary must not keep its role,
// or returns "" when it may.
//
// After a restart Redis takes a new replication ID, so the offsets of the
// replicas tell nothing then; only the size and the origin of the dataset do.
func safeStartRejection(primary instancemanager.InstanceStatus, replicas []candidate, persistence redisv1.Persistence) string {
	if len(replicas) == 0 {
		return "" // nothing better to promote
	}
	continues, replicaKeys := true, int64(0)
	for _, r := range replicas {
		continues = continues && r.status.ReplID == primary.ReplID && r.status.Offset <= primary.Offset
		replicaKeys = max(replicaKeys, r.status.Keys)
	}
	switch {
	case continues:
		return "" // same replication history, nothing missing: a partial resync
	case replicaKeys == 0:
		return "" // nothing to lose
	case primary.Keys == 0:
		return fmt.Sprintf("started with an empty dataset, while a replica holds %d keys", replicaKeys)
	case persistence == redisv1.PersistenceAOF:
		return "" // it loaded its AOF: at most about a second behind, like a replica
	}
	return fmt.Sprintf("restarted with persistence %s, so its dataset may be older than the replicas'", persistence)
}
