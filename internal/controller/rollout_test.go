package controller

import (
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

func TestSpecHash(t *testing.T) {
	build := func(mutate func(*redisv1.RedisInstance)) string {
		instance := testInstance.DeepCopy()
		instance.Spec.Image = "redis:7.2"
		mutate(instance)
		return buildRedisPod(instance, 0, "operator:v1", "data-cache-0").Annotations[annotationSpecHash]
	}
	base := build(func(*redisv1.RedisInstance) {})

	if again := build(func(*redisv1.RedisInstance) {}); again != base {
		t.Fatalf("hash of the same spec changed: %s, then %s", base, again)
	}
	for name, mutate := range map[string]func(*redisv1.RedisInstance){
		"image": func(i *redisv1.RedisInstance) { i.Spec.Image = "redis:7.4" },
		"memory limit": func(i *redisv1.RedisInstance) {
			i.Spec.Resources.Limits = map[corev1.ResourceName]resource.Quantity{"memory": resource.MustParse("1Gi")}
		},
		"policy": func(i *redisv1.RedisInstance) { i.Spec.Redis.MaxmemoryPolicy = "allkeys-lru" },
	} {
		if build(mutate) == base {
			t.Errorf("changing the %s did not change the hash", name)
		}
	}
	// The role label moves with every switchover; it must not restart the Pod.
	if got := build(func(i *redisv1.RedisInstance) { i.Status.CurrentPrimary = "cache-1" }); got != base {
		t.Error("a change of the primary changed the hash")
	}
}

// envtest runs no kubelet, so the tests mark Pods Ready themselves, and no
// Instance Manager, so a switchover is checked only up to its start.
var _ = Describe("RedisInstance rolling update", func() {
	Context("when the image of three instances changes", Ordered, func() {
		const name = "roll"

		BeforeAll(func() {
			Expect(k8sClient.Create(ctx, newRedisInstance(name, 3, redisv1.StorageSpec{
				VolumeClaimTemplate: volumeClaimTemplate(),
			}))).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			markReady(name+"-0", name+"-1", name+"-2")
			// Without Pod IPs the reconciler never records the primary itself.
			instance := getInstance(name)
			instance.Status.CurrentPrimary = name + "-0"
			Expect(k8sClient.Status().Update(ctx, instance)).To(Succeed())
		})

		AfterAll(func() { deleteInstance(name) })

		It("leaves up-to-date Pods alone", func() {
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			Expect(existingPodNames(name, 3)).To(ConsistOf(name+"-0", name+"-1", name+"-2"))
		})

		It("replaces one replica at a time, and only while every instance is Ready", func() {
			setImage(name, "redis:7.4")

			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			Expect(existingPodNames(name, 3)).To(ConsistOf(name+"-0", name+"-2"), "the first replica goes first")

			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			Expect(podImage(name + "-1")).To(Equal("redis:7.4"))
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			Expect(existingPodNames(name, 3)).To(HaveLen(3), "a Pod went down while the new one was not Ready")

			markReady(name + "-1")
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			Expect(existingPodNames(name, 3)).To(ConsistOf(name+"-0", name+"-1"))
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			markReady(name + "-2")
			// From here on the reconciler would start a switchover, which
			// needs an Instance Manager: the tests call the rollout directly.
		})

		It("waits for a switchover the user asked for", func() {
			annotate(name, redisv1.AnnotationPromote, name+"-2")
			retry, err := rolloutReconciler(nil).reconcileRollout(ctx, getInstance(name), []int32{0}, true)
			Expect(err).NotTo(HaveOccurred())
			Expect(retry).To(BeZero())
			Expect(getInstance(name).Status.TargetPrimary).To(BeEmpty())
			removeAnnotation(name, redisv1.AnnotationPromote)
		})

		It("waits after a failed switchover before it fences the primary again", func() {
			instance := getInstance(name)
			meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
				Type: redisv1.ConditionSwitchover, Status: metav1.ConditionFalse, Reason: "Failed", Message: "test",
			})
			retry, err := rolloutReconciler(nil).reconcileRollout(ctx, instance, []int32{0}, true)
			Expect(err).NotTo(HaveOccurred())
			Expect(retry).To(BeNumerically("~", rolloutRetryDelay, time.Minute))
			Expect(instance.Status.TargetPrimary).To(BeEmpty())
		})

		It("then switches over to the lowest replica, with a new epoch", func() {
			recorder := events.NewFakeRecorder(10)
			instance := getInstance(name)
			epoch := instance.Status.PrimaryEpoch
			retry, err := rolloutReconciler(recorder).reconcileRollout(ctx, instance, []int32{0}, true)
			Expect(err).NotTo(HaveOccurred())
			Expect(retry).To(BeZero())

			instance = getInstance(name)
			Expect(instance.Status.TargetPrimary).To(Equal(name + "-1"))
			Expect(instance.Status.PrimaryEpoch).To(Equal(epoch + 1))
			Expect(drain(recorder)).To(ContainElement(ContainSubstring("Switching over to " + name + "-1")))
			Expect(podImage(name+"-0")).To(Equal("redis:7.2"), "the primary was deleted before the switchover")
		})
	})

	Context("when the volume of a running replica is going away", Ordered, func() {
		const name = "roll-storage"

		BeforeAll(func() {
			Expect(k8sClient.Create(ctx, newRedisInstance(name, 3, redisv1.StorageSpec{
				VolumeClaimTemplate: volumeClaimTemplate(),
			}))).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			markReady(name+"-0", name+"-1", name+"-2")
		})

		AfterAll(func() {
			deletePVCNow(generatedPVCName(getInstance(name), 2))
			deleteInstance(name)
		})

		It("neither replaces that replica nor takes down another one", func() {
			// The pvc-protection finalizer keeps the PVC Terminating.
			Expect(k8sClient.Delete(ctx, newPVC(generatedPVCName(getInstance(name), 2)))).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			Expect(volume(getInstance(name), 2).State).To(Equal(redisv1.VolumeTerminating))

			setImage(name, "redis:7.4")
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			Expect(existingPodNames(name, 3)).To(HaveLen(3), "a Pod was replaced while a volume is not Ready")
		})
	})

	Context("when the image of a single instance changes", Ordered, func() {
		const name = "roll-single"

		BeforeAll(func() {
			Expect(k8sClient.Create(ctx, newRedisInstance(name, 1, redisv1.StorageSpec{
				VolumeClaimTemplate: volumeClaimTemplate(),
			}))).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			markReady(name + "-0")
		})

		AfterAll(func() { deleteInstance(name) })

		It("restarts the primary, as there is no replica to take over", func() {
			setImage(name, "redis:7.4")
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			Expect(podExistsNow(name + "-0")).To(BeFalse())

			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			Expect(podImage(name + "-0")).To(Equal("redis:7.4"))
		})
	})
})

func rolloutReconciler(recorder events.EventRecorder) *RedisInstanceReconciler {
	if recorder == nil {
		recorder = &events.FakeRecorder{}
	}
	return &RedisInstanceReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: recorder}
}

// markReady makes the Pods look started and Ready, as a kubelet would.
func markReady(names ...string) {
	for _, name := range names {
		pod := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, objectKey(name), pod)).To(Succeed())
		pod.Status.Phase = corev1.PodRunning
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}
}

func existingPodNames(instance string, instances int32) []string {
	var names []string
	for ordinal := range instances {
		if name := fmt.Sprintf("%s-%d", instance, ordinal); podExistsNow(name) {
			names = append(names, name)
		}
	}
	return names
}

func podImage(name string) string {
	pod := &corev1.Pod{}
	Expect(k8sClient.Get(ctx, objectKey(name), pod)).To(Succeed())
	return pod.Spec.Containers[0].Image
}

func setImage(name, image string) {
	instance := getInstance(name)
	instance.Spec.Image = image
	Expect(k8sClient.Update(ctx, instance)).To(Succeed())
}

func removeAnnotation(name, key string) {
	instance := getInstance(name)
	delete(instance.Annotations, key)
	Expect(k8sClient.Update(ctx, instance)).To(Succeed())
}
