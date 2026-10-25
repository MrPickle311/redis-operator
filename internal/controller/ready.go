package controller

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

// readyCondition sums up the instance in one condition. When False, the
// Reason tells why; StorageNotReady and ParametersNotApplied need a human,
// every other Reason passes on its own.
func readyCondition(instance *redisv1.RedisInstance, readyCount int32, outdated int) metav1.Condition {
	_, promoteRequested := pendingRequest(instance, redisv1.AnnotationPromote)
	instances := instance.Spec.Instances

	status, reason, message := metav1.ConditionFalse, "", ""
	switch {
	case !meta.IsStatusConditionTrue(instance.Status.Conditions, redisv1.ConditionStorageReady):
		reason, message = "StorageNotReady", "A volume is not Ready, see the condition StorageReady"
	case meta.IsStatusConditionFalse(instance.Status.Conditions, redisv1.ConditionParametersApplied):
		reason, message = "ParametersNotApplied", "Redis refused some options, see the condition ParametersApplied"
	case instance.Status.TargetPrimary != "" || promoteRequested:
		reason, message = "Switchover", "The primary is moving to another instance"
	case outdated > 0:
		reason, message = "RollingUpdate", fmt.Sprintf("%d of %d Pods run an outdated spec", outdated, instances)
	case readyCount < instances:
		reason, message = "InstancesNotReady", fmt.Sprintf("%d of %d instances are Ready", readyCount, instances)
	default:
		status, reason, message = metav1.ConditionTrue, "Ready", fmt.Sprintf("All %d instances are Ready and up to date", instances)
	}
	return metav1.Condition{
		Type:               redisv1.ConditionReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: instance.Generation,
	}
}
