package controller

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

func TestReadyCondition(t *testing.T) {
	storage := func(status metav1.ConditionStatus) []metav1.Condition {
		return []metav1.Condition{{Type: redisv1.ConditionStorageReady, Status: status}}
	}
	tests := []struct {
		name       string
		conditions []metav1.Condition
		target     string
		promote    bool
		ready      int32
		outdated   int
		want       string
	}{
		{"healthy", storage(metav1.ConditionTrue), "", false, 3, 0, "Ready"},
		{"a replica is starting", storage(metav1.ConditionTrue), "", false, 2, 0, "InstancesNotReady"},
		{"Pods are outdated", storage(metav1.ConditionTrue), "", false, 2, 1, "RollingUpdate"},
		{"a switchover runs", storage(metav1.ConditionTrue), "cache-1", false, 3, 1, "Switchover"},
		{"a switchover is requested", storage(metav1.ConditionTrue), "", true, 3, 0, "Switchover"},
		// Only this one needs a human, so it wins over every other reason.
		{"a volume is not Ready", storage(metav1.ConditionFalse), "cache-1", true, 1, 2, "StorageNotReady"},
		{"volumes not checked yet", nil, "", false, 3, 0, "StorageNotReady"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instance := &redisv1.RedisInstance{
				ObjectMeta: metav1.ObjectMeta{Generation: 7},
				Spec:       redisv1.RedisInstanceSpec{Instances: 3},
				Status:     redisv1.RedisInstanceStatus{Conditions: tt.conditions, TargetPrimary: tt.target},
			}
			if tt.promote {
				instance.Annotations = map[string]string{redisv1.AnnotationPromote: "cache-1"}
			}
			got := readyCondition(instance, tt.ready, tt.outdated)
			if got.Reason != tt.want {
				t.Errorf("Reason = %s, want %s", got.Reason, tt.want)
			}
			if wantTrue := tt.want == "Ready"; (got.Status == metav1.ConditionTrue) != wantTrue {
				t.Errorf("Status = %s with Reason %s", got.Status, got.Reason)
			}
			if got.ObservedGeneration != 7 {
				t.Errorf("ObservedGeneration = %d, want 7", got.ObservedGeneration)
			}
		})
	}
}

var _ = Describe("RedisInstance Ready condition", Ordered, func() {
	const name = "ready"

	readyReason := func() string {
		condition := meta.FindStatusCondition(getInstance(name).Status.Conditions, redisv1.ConditionReady)
		Expect(condition).NotTo(BeNil())
		Expect(condition.ObservedGeneration).To(Equal(getInstance(name).Generation))
		return condition.Reason
	}

	BeforeAll(func() {
		Expect(k8sClient.Create(ctx, newRedisInstance(name, 2, redisv1.StorageSpec{
			VolumeClaimTemplate: volumeClaimTemplate(),
		}))).To(Succeed())
		Expect(reconcileOnce(objectKey(name))).To(Succeed())
	})

	AfterAll(func() {
		deletePVCNow(generatedPVCName(getInstance(name), 1))
		deleteInstance(name)
	})

	It("is False while the Pods start", func() {
		Expect(reconcileOnce(objectKey(name))).To(Succeed())
		Expect(readyReason()).To(Equal("InstancesNotReady"))
	})

	It("is True once every instance is Ready", func() {
		markReady(name+"-0", name+"-1")
		Expect(reconcileOnce(objectKey(name))).To(Succeed())
		Expect(readyReason()).To(Equal("Ready"))
		Expect(meta.IsStatusConditionTrue(getInstance(name).Status.Conditions, redisv1.ConditionReady)).To(BeTrue())
	})

	It("is False with RollingUpdate after a change of the spec, for the new generation", func() {
		setImage(name, "redis:7.4")
		Expect(reconcileOnce(objectKey(name))).To(Succeed())
		Expect(readyReason()).To(Equal("RollingUpdate"))
	})

	It("reports a volume going away, even before the outdated Pods", func() {
		Expect(k8sClient.Delete(ctx, newPVC(generatedPVCName(getInstance(name), 1)))).To(Succeed())
		Expect(reconcileOnce(objectKey(name))).To(Succeed())
		Expect(readyReason()).To(Equal("StorageNotReady"))
	})
})
