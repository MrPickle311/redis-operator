package controller

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/instancemanager"
)

// reconcileConfig sends the configuration to every running Pod and sets the
// condition ParametersApplied from the options Redis refused. It returns true
// while a Pod could not be reached, so the reconcile is repeated.
func (r *RedisInstanceReconciler) reconcileConfig(ctx context.Context, instance *redisv1.RedisInstance) bool {
	logger := logf.FromContext(ctx)
	req := instancemanager.ConfigFor(instance)
	rejected := map[string]string{}
	answered, pending := false, false

	for ordinal := range instance.Spec.Instances {
		var pod corev1.Pod
		name := instancePodName(instance, ordinal)
		if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: instance.Namespace}, &pod); err != nil ||
			pod.Status.Phase != corev1.PodRunning || !pod.DeletionTimestamp.IsZero() {
			continue
		}
		resp, err := r.InstanceManager.Config(ctx, podFQDN(instance, ordinal), req)
		if err != nil {
			pending = true
			logger.V(1).Info("Could not configure Redis yet", "pod", name, "reason", err.Error())
			continue
		}
		answered = true
		maps.Copy(rejected, resp.Rejected)
		if resp.Changed {
			logger.Info("Configured Redis", "pod", name)
		}
	}
	if answered {
		r.setParametersCondition(instance, rejected)
	}
	return pending
}

// setParametersCondition sets ParametersApplied. Turning False also emits a
// Warning Event, once rather than on every reconcile.
func (r *RedisInstanceReconciler) setParametersCondition(instance *redisv1.RedisInstance, rejected map[string]string) {
	condition := metav1.Condition{
		Type:               redisv1.ConditionParametersApplied,
		Status:             metav1.ConditionTrue,
		Reason:             "Applied",
		Message:            "Every running instance has the configuration of spec.redis",
		ObservedGeneration: instance.Generation,
	}
	if len(rejected) > 0 {
		var refusals []string
		for _, name := range slices.Sorted(maps.Keys(rejected)) {
			refusals = append(refusals, fmt.Sprintf("%s: %s", name, rejected[name]))
		}
		condition.Status = metav1.ConditionFalse
		condition.Reason = "Rejected"
		condition.Message = "Redis refused " + strings.Join(refusals, "; ")

		if !meta.IsStatusConditionFalse(instance.Status.Conditions, redisv1.ConditionParametersApplied) {
			r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, "ParametersRejected", "Configure", condition.Message)
		}
	}
	meta.SetStatusCondition(&instance.Status.Conditions, condition)
}
