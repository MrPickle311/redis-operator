package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/instancemanager"
)

// failoverCallTimeout bounds a call to an Instance Manager during a failover:
// the old primary is often the one that does not answer.
const failoverCallTimeout = 2 * time.Second

// noCandidateRetry is how often the operator looks again for a replica to
// promote when none can take over.
const noCandidateRetry = 5 * time.Second

// reconcileFailover promotes a replica once the primary has been unavailable
// for spec.failover.delay. The rest of the reconcile then repoints the other
// replicas. It returns how long to wait before it checks again, or 0.
//
// Like a switchover, every step can be repeated: after a crash, the next
// reconcile continues from status.targetPrimary.
func (r *RedisInstanceReconciler) reconcileFailover(ctx context.Context, instance *redisv1.RedisInstance) (time.Duration, error) {
	if instance.Status.CurrentPrimary == "" {
		return 0, nil // no primary yet
	}
	if meta.IsStatusConditionTrue(instance.Status.Conditions, redisv1.ConditionFailover) {
		return 0, r.continueFailover(ctx, instance)
	}

	unavailable := r.primaryUnavailability(ctx, instance)
	r.setPrimaryAvailableCondition(instance, unavailable)
	if unavailable == "" {
		return 0, nil
	}
	// The condition turned False when the primary became unavailable.
	since := meta.FindStatusCondition(instance.Status.Conditions, redisv1.ConditionPrimaryAvailable).LastTransitionTime
	if wait := instance.Spec.Failover.Delay.Duration - time.Since(since.Time); wait > 0 {
		return wait, nil
	}

	target, ok := r.failoverCandidate(ctx, instance)
	if !ok {
		logf.FromContext(ctx).Info("No replica can take over from the unavailable primary", "primary", instance.Status.CurrentPrimary)
		return noCandidateRetry, nil
	}
	if err := r.startFailover(ctx, instance, target, unavailable); err != nil {
		return 0, err
	}
	return 0, r.continueFailover(ctx, instance)
}

// primaryUnavailability explains why the primary cannot take writes, or
// returns "" when it can.
func (r *RedisInstanceReconciler) primaryUnavailability(ctx context.Context, instance *redisv1.RedisInstance) string {
	name := instance.Status.CurrentPrimary
	var pod corev1.Pod
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: instance.Namespace}, &pod); err != nil {
		return fmt.Sprintf("Pod %s does not exist", name)
	}
	switch {
	case !pod.DeletionTimestamp.IsZero():
		return fmt.Sprintf("Pod %s is being deleted", name)
	case !isPodReady(&pod):
		return fmt.Sprintf("Pod %s is not Ready", name)
	}
	return ""
}

// failoverCandidate returns the Ready replica with the most of the dataset.
func (r *RedisInstanceReconciler) failoverCandidate(ctx context.Context, instance *redisv1.RedisInstance) (string, bool) {
	primary := primaryOrdinal(instance)
	var candidates []candidate
	for ordinal := range instance.Spec.Instances {
		if ordinal == primary {
			continue
		}
		var pod corev1.Pod
		name := instancePodName(instance, ordinal)
		// Ready means the replica finished its first sync, see /readyz.
		if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: instance.Namespace}, &pod); err != nil ||
			!isPodReady(&pod) || !pod.DeletionTimestamp.IsZero() {
			continue
		}
		callCtx, cancel := context.WithTimeout(ctx, failoverCallTimeout)
		status, err := r.InstanceManager.Status(callCtx, podFQDN(instance, ordinal))
		cancel()
		if err != nil || status.Role != "slave" {
			continue
		}
		candidates = append(candidates, candidate{ordinal: ordinal, status: status})
	}
	best, ok := bestCandidate(candidates)
	return instancePodName(instance, best), ok
}

// candidate is a replica that may become the primary.
type candidate struct {
	ordinal int32
	status  instancemanager.InstanceStatus
}

// bestCandidate picks the replica that holds the most writes of the primary:
// the highest replication offset, then the lowest ordinal, so the choice is
// the same on every reconcile.
//
// ponytail: compares offsets across replication IDs; every Ready replica
// follows the same primary, so they share one. Track the replication ID of
// the primary in the status if replicas ever diverge.
func bestCandidate(candidates []candidate) (int32, bool) {
	if len(candidates) == 0 {
		return 0, false
	}
	best := candidates[0]
	for _, currentCandidate := range candidates[1:] {
		if currentCandidate.status.Offset > best.status.Offset ||
			(currentCandidate.status.Offset == best.status.Offset && currentCandidate.ordinal < best.ordinal) {
			best = currentCandidate
		}
	}
	return best.ordinal, true
}

// startFailover records the target and takes a new epoch, before any command,
// like startSwitchover. It takes over from a switchover in progress, whose
// primary is the one that failed.
func (r *RedisInstanceReconciler) startFailover(ctx context.Context, instance *redisv1.RedisInstance, target, reason string) error {
	if instance.Status.TargetPrimary != "" {
		r.setSwitchoverCondition(instance, metav1.ConditionFalse, "Interrupted", "The primary failed during the switchover")
	}
	from := instance.Status.CurrentPrimary
	instance.Status.TargetPrimary = target
	instance.Status.PrimaryEpoch++
	r.setFailoverCondition(instance, metav1.ConditionTrue, "InProgress",
		fmt.Sprintf("Promoting %s: %s", target, reason))
	if err := r.Status().Update(ctx, instance); err != nil {
		return err
	}
	r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, "FailoverStarted", "Failover",
		"Promoting %s, as %s for %s", target, reason, instance.Spec.Failover.Delay.Duration)
	logf.FromContext(ctx).Info("Started failover", "from", from, "to", target, "epoch", instance.Status.PrimaryEpoch)
	return nil
}

// continueFailover fences the old primary if it answers, promotes the target
// and records it as the primary. Unlike a switchover it does not wait for the
// target to catch up: the writes the old primary did not replicate are lost.
func (r *RedisInstanceReconciler) continueFailover(ctx context.Context, instance *redisv1.RedisInstance) error {
	epoch := instance.Status.PrimaryEpoch
	from, to := instance.Status.CurrentPrimary, instance.Status.TargetPrimary
	targetOrdinal, err := ordinalFromPodName(to, instance.Name)
	if err != nil {
		return err // chosen from the instances when the failover started
	}

	// Best effort: an old primary that still runs stops taking writes. If it
	// cannot be reached, it fences itself, see the Instance Manager.
	fenceCtx, cancel := context.WithTimeout(ctx, failoverCallTimeout)
	_ = r.InstanceManager.Fence(fenceCtx, podFQDN(instance, primaryOrdinal(instance)), epoch)
	cancel()

	if _, err := r.InstanceManager.Promote(ctx, podFQDN(instance, targetOrdinal), epoch); err != nil {
		return err
	}
	instance.Status.CurrentPrimary = to
	instance.Status.TargetPrimary = ""
	r.setFailoverCondition(instance, metav1.ConditionFalse, "Completed", fmt.Sprintf("Promoted %s in place of %s", to, from))
	if err := r.Status().Update(ctx, instance); err != nil {
		return err
	}
	r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, "FailoverCompleted", "Failover",
		"Promoted %s in place of %s", to, from)
	return nil
}

func (r *RedisInstanceReconciler) setFailoverCondition(instance *redisv1.RedisInstance, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
		Type:               redisv1.ConditionFailover,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: instance.Generation,
	})
}

// setPrimaryAvailableCondition sets PrimaryAvailable. Its lastTransitionTime
// is when the primary became unavailable, so the delay survives a restart of
// the operator.
func (r *RedisInstanceReconciler) setPrimaryAvailableCondition(instance *redisv1.RedisInstance, unavailable string) {
	condition := metav1.Condition{
		Type:               redisv1.ConditionPrimaryAvailable,
		Status:             metav1.ConditionTrue,
		Reason:             "Ready",
		Message:            "The primary " + instance.Status.CurrentPrimary + " is Ready",
		ObservedGeneration: instance.Generation,
	}
	if unavailable != "" {
		condition.Status, condition.Reason, condition.Message = metav1.ConditionFalse, "Unavailable", unavailable
	}
	meta.SetStatusCondition(&instance.Status.Conditions, condition)
}
