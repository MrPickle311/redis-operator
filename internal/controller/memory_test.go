package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/tools/events"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

var _ = Describe("Memory", func() {
	newInstance := func(name string) *redisv1.RedisInstance {
		return newRedisInstance(name, 1, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()})
	}

	Context("with spec.redis", func() {
		It("defaults to noeviction at 75% of the limit", func() {
			instance := newInstance("memory-default-test")
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			DeferCleanup(k8sClient.Delete, ctx, instance)

			Expect(instance.Spec.Redis.MaxmemoryPolicy).To(Equal("noeviction"))
			Expect(instance.Spec.Redis.MaxmemoryRatio).To(BeEquivalentTo(75))
		})

		It("accepts every eviction policy of Redis", func() {
			for _, policy := range []string{
				"noeviction", "allkeys-lru", "allkeys-lfu", "allkeys-random",
				"volatile-lru", "volatile-lfu", "volatile-random", "volatile-ttl",
			} {
				instance := newInstance("memory-policy-" + policy)
				instance.Spec.Redis.MaxmemoryPolicy = policy
				Expect(k8sClient.Create(ctx, instance)).To(Succeed(), "policy %s", policy)
				DeferCleanup(k8sClient.Delete, ctx, instance)
			}
		})

		DescribeTable("rejects invalid values",
			func(mutate func(*redisv1.RedisSpec)) {
				instance := newInstance("memory-invalid-test")
				mutate(&instance.Spec.Redis)

				err := k8sClient.Create(ctx, instance)
				Expect(errors.IsInvalid(err)).To(BeTrue(), "expected an Invalid error, got: %v", err)
			},
			Entry("ratio below 10%", func(r *redisv1.RedisSpec) { r.MaxmemoryRatio = 5 }),
			Entry("ratio above 95%", func(r *redisv1.RedisSpec) { r.MaxmemoryRatio = 100 }),
			Entry("unknown policy", func(r *redisv1.RedisSpec) { r.MaxmemoryPolicy = "lru" }),
		)
	})

	It("passes the resources and the maxmemory settings to the redis container", func() {
		const name = "memory-settings-test"
		instance := newInstance(name)
		instance.Spec.Resources = memoryLimit("128Mi")
		instance.Spec.Redis = redisv1.RedisSpec{MaxmemoryPolicy: "allkeys-lru", MaxmemoryRatio: 80}
		Expect(k8sClient.Create(ctx, instance)).To(Succeed())
		DeferCleanup(k8sClient.Delete, ctx, instance)
		Expect(reconcileOnce(objectKey(name))).To(Succeed())

		pod := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, objectKey(name+"-0"), pod)).To(Succeed())
		redis := pod.Spec.Containers[0]
		Expect(redis.Resources.Limits.Memory().String()).To(Equal("128Mi"))
		Expect(redis.Command).To(ContainElements("--maxmemory-ratio=80", "--maxmemory-policy=allkeys-lru"))
	})

	DescribeTable("reports the memory limit in the MemoryLimited condition",
		func(name string, resources corev1.ResourceRequirements, want bool) {
			instance := newInstance(name)
			instance.Spec.Resources = resources
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			DeferCleanup(k8sClient.Delete, ctx, instance)

			recorder := events.NewFakeRecorder(10)
			Expect(reconcileWith(recorder, objectKey(name))).To(Succeed())
			// The status is written once the primary has an IP, which envtest never assigns.
			setPodIP(name + "-0")
			Expect(reconcileWith(recorder, objectKey(name))).To(Succeed())
			Expect(reconcileWith(recorder, objectKey(name))).To(Succeed())

			Expect(k8sClient.Get(ctx, objectKey(name), instance)).To(Succeed())
			Expect(meta.IsStatusConditionTrue(instance.Status.Conditions, redisv1.ConditionMemoryLimited)).To(Equal(want))
			if want {
				Expect(recorder.Events).To(BeEmpty())
			} else {
				Expect(recorder.Events).To(HaveLen(1), "the Event must be emitted once, not on every reconcile")
				Expect(<-recorder.Events).To(HavePrefix("Warning MemoryLimitMissing"))
			}
		},
		Entry("with a limit", "memory-limited-test", memoryLimit("128Mi"), true),
		Entry("without a limit", "memory-unlimited-test", corev1.ResourceRequirements{}, false),
	)
})

func memoryLimit(limit string) corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(limit)},
	}
}

// setPodIP sets the IP of the Pod, as the kubelet would.
func setPodIP(name string) {
	pod := &corev1.Pod{}
	Expect(k8sClient.Get(ctx, objectKey(name), pod)).To(Succeed())
	pod.Status.PodIP = "10.0.0.1"
	Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
}
