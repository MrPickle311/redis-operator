package controller

import (
	"fmt"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

const redisPort = 6379

// Labels set on every Pod. Services select Pods by them, and users can rely on
// them in their own NetworkPolicies, so they are part of the public contract.
const (
	labelName     = "app.kubernetes.io/name"
	labelInstance = "app.kubernetes.io/instance"
	labelRole     = "redis.operator.com/role"

	rolePrimary = "primary"
	roleReplica = "replica"
	appName     = "redis"
)

// The initContainer copies the Instance Manager binary into the controllerVolume
// emptyDir; the redis container then runs it from managerPath.
const (
	controllerVolume = "controller"
	controllerDir    = "/controller"
	managerPath      = controllerDir + "/manager"
)

// instancePodName returns the name of the Pod for the given ordinal.
func instancePodName(instance *redisv1.RedisInstance, ordinal int32) string {
	return fmt.Sprintf("%s-%d", instance.Name, ordinal)
}

// headlessServiceName returns the name of the headless Service that gives
// every Pod a stable DNS record.
func headlessServiceName(instance *redisv1.RedisInstance) string {
	return instance.Name + "-hl"
}

// podFQDN returns the stable DNS name of the instance with the given ordinal.
// Unlike the Pod IP, it does not change when the Pod is recreated.
func podFQDN(instance *redisv1.RedisInstance, ordinal int32) string {
	return fmt.Sprintf("%s.%s.%s.svc",
		instancePodName(instance, ordinal), headlessServiceName(instance), instance.Namespace)
}

// pvcName returns the PVC name used by the instance with the given ordinal.
// The second value is true when the PVC comes from spec.storage.existingClaims.
func pvcName(instance *redisv1.RedisInstance, ordinal int32) (string, bool) {
	for _, c := range instance.Spec.Storage.ExistingClaims {
		if c.InstanceOrdinal == ordinal {
			return c.ClaimName, true
		}
	}
	return fmt.Sprintf("%s-%d-data", instance.Name, ordinal), false
}

// podRole returns the role of the instance with the given ordinal.
// For now ordinal 0 is always the primary; failover (step 4) will take the
// primary from status.currentPrimary instead.
func podRole(ordinal int32) string {
	if ordinal == 0 {
		return rolePrimary
	}
	return roleReplica
}

// instanceSelector matches every Pod of the RedisInstance.
func instanceSelector(instance *redisv1.RedisInstance) map[string]string {
	return map[string]string{labelInstance: instance.Name}
}
