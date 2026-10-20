package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/instancemanager"
)

// switchoverTimeout is how long the fenced primary may hold back writes while
// the target catches up. After it, the switchover fails and writes resume.
const switchoverTimeout = time.Minute

// switchoverPollInterval is how often a switchover checks the replication offsets.
const switchoverPollInterval = time.Second

// reconcileSwitchover moves the primary to the Pod in AnnotationPromote. It
// returns a non-zero Result while it waits for the target to catch up.
//
// Every step can be repeated: after a crash or a conflict, the next reconcile
// continues from status.targetPrimary.
func (r *RedisInstanceReconciler) reconcileSwitchover(ctx context.Context, instance *redisv1.RedisInstance) (ctrl.Result, error) {
	if instance.Status.TargetPrimary == "" {
		target, requested := instance.Annotations[redisv1.AnnotationPromote]
		if !requested {
			return ctrl.Result{}, nil
		}
		if reason := r.switchoverRejection(ctx, instance, target); reason != "" {
			r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, "SwitchoverRejected", "Switchover",
				"Cannot switch over to %s: %s", target, reason)
			return ctrl.Result{}, r.removePromoteAnnotation(ctx, instance)
		}
		if err := r.startSwitchover(ctx, instance, target); err != nil {
			return ctrl.Result{}, err
		}
	}

	targetOrdinal, err := ordinalFromPodName(instance.Status.TargetPrimary, instance.Name)
	if err != nil {
		return ctrl.Result{}, err // validated when the switchover started
	}
	epoch := instance.Status.PrimaryEpoch
	primaryHost := podFQDN(instance, primaryOrdinal(instance))
	targetHost := podFQDN(instance, targetOrdinal)

	target, err := r.InstanceManager.Status(ctx, targetHost)
	if err != nil {
		return ctrl.Result{}, err
	}
	if target.Role == "master" {
		// Promoted by an earlier pass that stopped before writing the status.
		return ctrl.Result{}, r.finishSwitchover(ctx, instance)
	}
	if started := meta.FindStatusCondition(instance.Status.Conditions, redisv1.ConditionSwitchover); started != nil &&
		time.Since(started.LastTransitionTime.Time) > switchoverTimeout {
		return ctrl.Result{}, r.abortSwitchover(ctx, instance, primaryHost)
	}

	// Fenced on every pass: it is idempotent and restarts the fence timeout.
	if err := r.InstanceManager.Fence(ctx, primaryHost, epoch); err != nil {
		return ctrl.Result{}, err
	}
	primary, err := r.InstanceManager.Status(ctx, primaryHost)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !caughtUp(primary, target) {
		return ctrl.Result{RequeueAfter: switchoverPollInterval}, nil
	}
	if _, err := r.InstanceManager.Promote(ctx, targetHost, epoch); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.finishSwitchover(ctx, instance)
}

// switchoverRejection explains why the primary cannot move to target, or
// returns "" when it can.
func (r *RedisInstanceReconciler) switchoverRejection(ctx context.Context, instance *redisv1.RedisInstance, target string) string {
	if instance.Status.CurrentPrimary == "" {
		return "the instance has no primary yet"
	}
	if target == instance.Status.CurrentPrimary {
		return "it is already the primary"
	}
	if ordinal, err := ordinalFromPodName(target, instance.Name); err != nil || ordinal >= instance.Spec.Instances {
		return "it is not an instance of this RedisInstance"
	}
	var pod corev1.Pod
	if err := r.Get(ctx, types.NamespacedName{Name: target, Namespace: instance.Namespace}, &pod); err != nil || !isPodReady(&pod) {
		return "the Pod is not Ready"
	}
	return ""
}

// startSwitchover records the target and takes a new epoch. It writes the
// status before any Instance Manager sees the epoch, and the update fails on a
// stale resourceVersion, so two operator leaders cannot both take it.
func (r *RedisInstanceReconciler) startSwitchover(ctx context.Context, instance *redisv1.RedisInstance, target string) error {
	instance.Status.TargetPrimary = target
	instance.Status.PrimaryEpoch++
	r.setSwitchoverCondition(instance, metav1.ConditionTrue, "InProgress",
		fmt.Sprintf("Moving the primary from %s to %s", instance.Status.CurrentPrimary, target))
	if err := r.Status().Update(ctx, instance); err != nil {
		return err
	}
	logf.FromContext(ctx).Info("Started switchover", "from", instance.Status.CurrentPrimary, "to", target,
		"epoch", instance.Status.PrimaryEpoch)
	return nil
}

// finishSwitchover makes the target the primary. The replication loop of the
// same reconcile then demotes the old primary, which also lifts its fence.
func (r *RedisInstanceReconciler) finishSwitchover(ctx context.Context, instance *redisv1.RedisInstance) error {
	from, to := instance.Status.CurrentPrimary, instance.Status.TargetPrimary
	instance.Status.CurrentPrimary = to
	instance.Status.TargetPrimary = ""
	r.setSwitchoverCondition(instance, metav1.ConditionFalse, "Completed", "The primary moved to "+to)
	if err := r.Status().Update(ctx, instance); err != nil {
		return err
	}
	r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, "SwitchoverCompleted", "Switchover",
		"Moved the primary from %s to %s", from, to)
	return r.removePromoteAnnotation(ctx, instance)
}

// abortSwitchover gives up when the target does not catch up in time and lets
// the current primary take writes again.
func (r *RedisInstanceReconciler) abortSwitchover(ctx context.Context, instance *redisv1.RedisInstance, primaryHost string) error {
	// Promoting the primary keeps its role and only lifts the fence.
	if _, err := r.InstanceManager.Promote(ctx, primaryHost, instance.Status.PrimaryEpoch); err != nil {
		return err
	}
	target := instance.Status.TargetPrimary
	instance.Status.TargetPrimary = ""
	message := fmt.Sprintf("%s did not catch up with the primary within %s", target, switchoverTimeout)
	r.setSwitchoverCondition(instance, metav1.ConditionFalse, "Failed", message)
	if err := r.Status().Update(ctx, instance); err != nil {
		return err
	}
	r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, "SwitchoverFailed", "Switchover", message)
	return r.removePromoteAnnotation(ctx, instance)
}

func (r *RedisInstanceReconciler) setSwitchoverCondition(instance *redisv1.RedisInstance, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
		Type:               redisv1.ConditionSwitchover,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: instance.Generation,
	})
}

// removePromoteAnnotation marks the request as handled, so it is not served twice.
func (r *RedisInstanceReconciler) removePromoteAnnotation(ctx context.Context, instance *redisv1.RedisInstance) error {
	if _, ok := instance.Annotations[redisv1.AnnotationPromote]; !ok {
		return nil
	}
	patch := client.MergeFrom(instance.DeepCopy())
	delete(instance.Annotations, redisv1.AnnotationPromote)
	return r.Patch(ctx, instance, patch)
}

// caughtUp reports whether the target holds every write of the fenced
// primary, or was already promoted by an earlier pass.
func caughtUp(primary, target instancemanager.InstanceStatus) bool {
	if target.Role == "master" {
		return true
	}
	return target.ReplID == primary.ReplID && target.Offset >= primary.Offset
}
