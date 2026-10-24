package controller

import (
	"fmt"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/instancemanager"
)

const redisPort = 6379

// Labels set on every Pod. Services select Pods by them, and users can rely on
// them in their own NetworkPolicies, so they are part of the public contract.
const (
	labelName = "app.kubernetes.io/name"
	// labelInstance is the operator's own: Argo CD with label tracking takes
	// any object with app.kubernetes.io/instance for one of its own.
	labelInstance = "redis.operator.com/instance"
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

// Volumes with the Secrets the Instance Manager reads.
const (
	certificatesVolume = "certificates"  // TLS of the Instance Manager API
	internalAuthVolume = "internal-auth" // passwords of the internal users
	apiAccessVolume    = "kube-api-access"
)

// instancePodName returns the name of the Pod for the given ordinal.
func instancePodName(instance *redisv1.RedisInstance, ordinal int32) string {
	return fmt.Sprintf("%s-%d", instance.Name, ordinal)
}

// headlessServiceName returns the name of the headless Service that gives
// every Pod a stable DNS record.
func headlessServiceName(instance *redisv1.RedisInstance) string {
	return instancemanager.HeadlessServiceName(instance.Name)
}

// podFQDN returns the stable DNS name of the instance with the given ordinal.
func podFQDN(instance *redisv1.RedisInstance, ordinal int32) string {
	return instancemanager.PodFQDN(instancePodName(instance, ordinal), instance.Name, instance.Namespace)
}

// instanceAccountName returns the name of the ServiceAccount, Role and
// RoleBinding of the Instance Managers.
func instanceAccountName(instance *redisv1.RedisInstance) string {
	return instance.Name + "-instance"
}

// serverSecretName returns the name of the Secret with the TLS certificate
// of the Instance Manager API.
func serverSecretName(instance *redisv1.RedisInstance) string {
	return instance.Name + "-im-tls"
}

// internalAuthSecretName returns the name of the Secret with the passwords of
// the internal Redis users.
func internalAuthSecretName(instance *redisv1.RedisInstance) string {
	return instance.Name + "-internal-auth"
}

// serverCertDNSName returns the wildcard name that matches podFQDN of every
// ordinal, so one certificate serves the whole instance.
func serverCertDNSName(instance *redisv1.RedisInstance) string {
	return fmt.Sprintf("*.%s.%s.svc", headlessServiceName(instance), instance.Namespace)
}

// pvcName returns the PVC the spec chooses for the instance with the given
// ordinal. The second value is true when it comes from
// spec.storage.existingClaims. It is read only when the instance is bound to
// a volume; after that status.volumes decides.
func pvcName(instance *redisv1.RedisInstance, ordinal int32) (string, bool) {
	for _, c := range instance.Spec.Storage.ExistingClaims {
		if c.InstanceOrdinal == ordinal {
			return c.ClaimName, true
		}
	}
	return generatedPVCName(instance, ordinal), false
}

// generatedPVCName returns the name of the PVC the operator generates for the
// instance with the given ordinal from spec.storage.volumeClaimTemplate.
func generatedPVCName(instance *redisv1.RedisInstance, ordinal int32) string {
	return fmt.Sprintf("%s-%d-data", instance.Name, ordinal)
}

// primaryOrdinal returns the ordinal of the current primary: the Pod in
// status.currentPrimary, or 0 for a new instance that has none yet.
func primaryOrdinal(instance *redisv1.RedisInstance) int32 {
	ordinal, err := ordinalFromPodName(instance.Status.CurrentPrimary, instance.Name)
	if err != nil {
		return 0
	}
	return ordinal
}

// podRole returns the role of the instance with the given ordinal.
func podRole(instance *redisv1.RedisInstance, ordinal int32) string {
	if ordinal == primaryOrdinal(instance) {
		return rolePrimary
	}
	return roleReplica
}

// instanceSelector matches every Pod of the RedisInstance.
func instanceSelector(instance *redisv1.RedisInstance) map[string]string {
	return map[string]string{labelInstance: instance.Name}
}
