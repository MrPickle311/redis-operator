package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

var _ = Describe("Internal Redis users", func() {
	Context("when reconciling a RedisInstance", func() {
		const name = "auth-test"
		secretKey := objectKey(name + "-internal-auth")

		BeforeEach(func() {
			instance := newRedisInstance(name, 1, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()})
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			DeferCleanup(k8sClient.Delete, ctx, instance)
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
		})

		It("generates a different password for each internal user", func() {
			secret := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, secretKey, secret)).To(Succeed())

			Expect(metav1.GetControllerOf(secret)).To(HaveField("Name", name))
			operator, replication := secret.Data["operator-password"], secret.Data["replication-password"]
			Expect(operator).To(HaveLen(26), "crypto/rand.Text gives 128 bits as 26 base32 characters")
			Expect(replication).To(HaveLen(26))
			Expect(operator).NotTo(Equal(replication))
		})

		It("keeps the passwords the instances started with and fills in a missing one", func() {
			secret := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, secretKey, secret)).To(Succeed())
			operator := secret.Data["operator-password"]
			delete(secret.Data, "replication-password")
			Expect(k8sClient.Update(ctx, secret)).To(Succeed())

			Expect(reconcileOnce(objectKey(name))).To(Succeed())

			Expect(k8sClient.Get(ctx, secretKey, secret)).To(Succeed())
			Expect(secret.Data).To(HaveKeyWithValue("operator-password", operator))
			Expect(secret.Data["replication-password"]).NotTo(BeEmpty())
		})

		It("mounts the passwords read-only and leaves the default user off", func() {
			pod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, objectKey(name+"-0"), pod)).To(Succeed())

			Expect(pod.Spec.Volumes).To(ContainElement(HaveField("Secret.SecretName", secretKey.Name)))
			redis := pod.Spec.Containers[0]
			Expect(redis.VolumeMounts).To(ContainElement(corev1.VolumeMount{
				Name: "internal-auth", MountPath: "/internal-auth", ReadOnly: true,
			}))
			Expect(redis.Command).NotTo(ContainElement("--default-user"))
		})
	})

	Context("with spec.acl.defaultUser", func() {
		It("defaults to disabled", func() {
			instance := newRedisInstance("acl-default-test", 1, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()})
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			DeferCleanup(k8sClient.Delete, ctx, instance)

			Expect(instance.Spec.ACL.DefaultUser).To(Equal(redisv1.DefaultUserDisabled))
		})

		It("rejects an unknown mode", func() {
			instance := newRedisInstance("acl-invalid-test", 1, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()})
			instance.Spec.ACL.DefaultUser = "on"

			err := k8sClient.Create(ctx, instance)
			Expect(errors.IsInvalid(err)).To(BeTrue(), "expected an Invalid error, got: %v", err)
		})

		It("starts the Instance Manager with the default user when enabled", func() {
			const name = "acl-enabled-test"
			instance := newRedisInstance(name, 1, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()})
			instance.Spec.ACL.DefaultUser = redisv1.DefaultUserEnabled
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			DeferCleanup(k8sClient.Delete, ctx, instance)
			Expect(reconcileOnce(objectKey(name))).To(Succeed())

			pod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, objectKey(name+"-0"), pod)).To(Succeed())
			Expect(pod.Spec.Containers[0].Command).To(ContainElement("--default-user"))
		})
	})
})
