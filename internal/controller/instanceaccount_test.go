package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

var _ = Describe("Instance Manager account", func() {
	const name = "account-test"
	accountKey := objectKey(name + "-instance")

	BeforeEach(func() {
		instance := newRedisInstance(name, 1, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()})
		Expect(k8sClient.Create(ctx, instance)).To(Succeed())
		DeferCleanup(k8sClient.Delete, ctx, instance)
		Expect(reconcileOnce(objectKey(name))).To(Succeed())
	})

	It("lets the Instance Managers only read their own RedisInstance", func() {
		role := &rbacv1.Role{}
		Expect(k8sClient.Get(ctx, accountKey, role)).To(Succeed())
		Expect(role.Rules).To(Equal([]rbacv1.PolicyRule{{
			APIGroups:     []string{"redis.operator.com"},
			Resources:     []string{"redisinstances"},
			ResourceNames: []string{name},
			Verbs:         []string{"get"},
		}}))

		binding := &rbacv1.RoleBinding{}
		Expect(k8sClient.Get(ctx, accountKey, binding)).To(Succeed())
		Expect(binding.RoleRef.Name).To(Equal(accountKey.Name))
		Expect(binding.Subjects).To(ConsistOf(HaveField("Name", accountKey.Name)))

		for _, object := range []client.Object{&corev1.ServiceAccount{}, role, binding} {
			Expect(k8sClient.Get(ctx, accountKey, object)).To(Succeed())
			Expect(metav1.GetControllerOf(object)).To(HaveField("Name", name), "%T", object)
		}
	})

	It("gives the token to the redis container only", func() {
		pod := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, objectKey(name+"-0"), pod)).To(Succeed())
		Expect(pod.Spec.ServiceAccountName).To(Equal(accountKey.Name))

		tokenMount := HaveField("MountPath", "/var/run/secrets/kubernetes.io/serviceaccount")
		Expect(pod.Spec.Containers[0].VolumeMounts).To(ContainElement(tokenMount))
		Expect(pod.Spec.InitContainers[0].VolumeMounts).NotTo(ContainElement(tokenMount))
	})

	It("tells the Instance Manager which Pod it runs in", func() {
		pod := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, objectKey(name+"-0"), pod)).To(Succeed())

		env := pod.Spec.Containers[0].Env
		for name, field := range map[string]string{"POD_NAME": "metadata.name", "POD_NAMESPACE": "metadata.namespace"} {
			Expect(env).To(ContainElement(And(HaveField("Name", name), HaveField("ValueFrom.FieldRef.FieldPath", field))))
		}
	})
})
