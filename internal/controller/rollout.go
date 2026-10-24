package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

// annotationSpecHash records on a Pod the hash of the PodSpec it was built
// from. A Pod whose hash differs from the one of the current spec is outdated.
const annotationSpecHash = "redis.operator.com/spec-hash"

// specHash returns a short hash of spec. It is stable: encoding/json writes
// struct fields in order and map keys sorted.
func specHash(spec *corev1.PodSpec) string {
	data, err := json.Marshal(spec)
	if err != nil {
		panic(err) // a PodSpec always marshals
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:8])
}

// rolloutRetryDelay is how long the rolling update waits after a failed
// switchover before it fences the primary again.
const rolloutRetryDelay = 5 * time.Minute

// isOutdated reports whether existing was built from another spec than desired.
// A Pod created before the hash existed counts as outdated too.
func isOutdated(existing, desired *corev1.Pod) bool {
	return existing.Annotations[annotationSpecHash] != desired.Annotations[annotationSpecHash]
}

// reconcileRollout replaces the outdated Pods one at a time: the replicas
// first, then the primary after a switchover to an updated replica. It acts
// only while every instance is Ready, so at most one instance is down.
//
// It returns how long to wait before retrying a failed switchover, or 0.
func (r *RedisInstanceReconciler) reconcileRollout(ctx context.Context, instance *redisv1.RedisInstance,
	outdated []int32, allReady bool) (time.Duration, error) {
	if len(outdated) == 0 || !allReady || instance.Status.TargetPrimary != "" {
		return 0, nil
	}
	if _, requested := pendingRequest(instance, redisv1.AnnotationPromote); requested {
		return 0, nil // the switchover asked by the user goes first
	}

	primary := primaryOrdinal(instance)
	for _, ordinal := range outdated {
		if ordinal != primary {
			return 0, r.replacePod(ctx, instance, ordinal)
		}
	}

	// Only the primary is left.
	if instance.Spec.Instances == 1 {
		// No replica can take over: clients wait until the new Pod is up.
		return 0, r.replacePod(ctx, instance, primary)
	}
	if last := meta.FindStatusCondition(instance.Status.Conditions, redisv1.ConditionSwitchover); last != nil && last.Reason == "Failed" {
		if wait := rolloutRetryDelay - time.Since(last.LastTransitionTime.Time); wait > 0 {
			return wait, nil
		}
	}
	// Every replica is updated and Ready, so the lowest one takes over. The
	// old primary becomes an outdated replica and is replaced next.
	target := int32(0)
	if target == primary {
		target = 1
	}
	targetName := instancePodName(instance, target)
	if reason := r.switchoverRejection(ctx, instance, targetName); reason != "" {
		return 0, nil // e.g. no primary recorded yet; the next reconcile tries again
	}
	r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, "RollingUpdate", "Switchover",
		"Switching over to %s to update the primary", targetName)
	return 0, r.startSwitchover(ctx, instance, targetName)
}

// replacePod deletes the Pod of ordinal. The next reconcile creates it again
// from the current spec, on the same volume.
func (r *RedisInstanceReconciler) replacePod(ctx context.Context, instance *redisv1.RedisInstance, ordinal int32) error {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      instancePodName(instance, ordinal),
		Namespace: instance.Namespace,
	}}
	r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, "RollingUpdate", "DeletePod",
		"Replacing outdated Pod %s", pod.Name)
	return client.IgnoreNotFound(r.Delete(ctx, pod))
}
