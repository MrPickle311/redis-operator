package controller

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

// hasMemoryLimit reports whether the redis container of the Pod has a memory limit.
func hasMemoryLimit(pod *corev1.Pod) bool {
	for _, container := range pod.Spec.Containers {
		if container.Name == appName {
			_, ok := container.Resources.Limits[corev1.ResourceMemory]
			return ok
		}
	}
	return false
}

// setMemoryLimitedCondition sets the MemoryLimited condition and returns true
// when it changed. Without a memory limit Redis runs without maxmemory and
// grows until it is OOMKilled, so the condition turning False also emits a
// Warning Event, once rather than on every reconcile.
func (r *RedisInstanceReconciler) setMemoryLimitedCondition(instance *redisv1.RedisInstance, unlimited bool) bool {
	condition := metav1.Condition{
		Type:               redisv1.ConditionMemoryLimited,
		Status:             metav1.ConditionTrue,
		Reason:             "MemoryLimitSet",
		Message:            "maxmemory is derived from the memory limit",
		ObservedGeneration: instance.Generation,
	}
	if unlimited {
		condition.Status = metav1.ConditionFalse
		condition.Reason = "MemoryLimitMissing"
		condition.Message = "The redis container has no memory limit, so Redis runs without maxmemory " +
			"and may be OOMKilled; set spec.resources.limits.memory"

		if !meta.IsStatusConditionFalse(instance.Status.Conditions, redisv1.ConditionMemoryLimited) {
			r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, condition.Reason, "Reconcile", condition.Message)
		}
	}
	return meta.SetStatusCondition(&instance.Status.Conditions, condition)
}
