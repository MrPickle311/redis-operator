package controller

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/instancemanager"
)

func TestBestCandidate(t *testing.T) {
	replica := func(ordinal int32, offset int64) candidate {
		return candidate{ordinal: ordinal, status: instancemanager.InstanceStatus{Role: "slave", ReplID: "a", Offset: offset}}
	}
	tests := []struct {
		name       string
		candidates []candidate
		want       int32
		wantOK     bool
	}{
		{"no replica can take over", nil, 0, false},
		{"the one with the most writes", []candidate{replica(1, 100), replica(2, 150)}, 2, true},
		{"a tie goes to the lowest ordinal", []candidate{replica(2, 150), replica(1, 150)}, 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := bestCandidate(tt.candidates)
			if got != tt.want || ok != tt.wantOK {
				t.Errorf("bestCandidate() = %d, %v; want %d, %v", got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestSoonest(t *testing.T) {
	if got := soonest(0, 5*time.Second, 2*time.Second); got != 2*time.Second {
		t.Errorf("soonest() = %s, want the shortest positive duration", got)
	}
	if got := soonest(0, 0); got != 0 {
		t.Errorf("soonest() = %s, want 0 when nothing waits", got)
	}
}

// envtest runs no Instance Manager, so these specs stop before a replica is
// asked for its status: no replica is Ready.
var _ = Describe("RedisInstance failover", Ordered, func() {
	const name = "failover"

	primaryAvailable := func() *metav1.Condition {
		return meta.FindStatusCondition(getInstance(name).Status.Conditions, redisv1.ConditionPrimaryAvailable)
	}

	BeforeAll(func() {
		Expect(k8sClient.Create(ctx, newRedisInstance(name, 2, redisv1.StorageSpec{
			VolumeClaimTemplate: volumeClaimTemplate(),
		}))).To(Succeed())
		Expect(reconcileOnce(objectKey(name))).To(Succeed())
		instance := getInstance(name)
		instance.Status.CurrentPrimary = name + "-0"
		Expect(k8sClient.Status().Update(ctx, instance)).To(Succeed())
	})

	AfterAll(func() { deleteInstance(name) })

	It("reports a primary that is not Ready", func() {
		Expect(reconcileOnce(objectKey(name))).To(Succeed())
		condition := primaryAvailable()
		Expect(condition.Status).To(Equal(metav1.ConditionFalse))
		Expect(condition.Message).To(Equal("Pod " + name + "-0 is not Ready"))
		Expect(getInstance(name).Status.Conditions).To(ContainElement(SatisfyAll(
			HaveField("Type", redisv1.ConditionReady), HaveField("Reason", "PrimaryUnavailable"))))
	})

	It("keeps the time the primary became unavailable over later reconciles", func() {
		since := primaryAvailable().LastTransitionTime
		time.Sleep(1100 * time.Millisecond) // conditions keep seconds
		Expect(reconcileOnce(objectKey(name))).To(Succeed())
		Expect(primaryAvailable().LastTransitionTime).To(Equal(since))
	})

	It("does not fail over without a Ready replica, even after the delay", func() {
		instance := getInstance(name)
		meta.FindStatusCondition(instance.Status.Conditions, redisv1.ConditionPrimaryAvailable).LastTransitionTime =
			metav1.NewTime(time.Now().Add(-time.Hour))
		Expect(k8sClient.Status().Update(ctx, instance)).To(Succeed())

		Expect(reconcileOnce(objectKey(name))).To(Succeed())
		instance = getInstance(name)
		Expect(instance.Status.CurrentPrimary).To(Equal(name + "-0"))
		Expect(instance.Status.TargetPrimary).To(BeEmpty())
		Expect(meta.FindStatusCondition(instance.Status.Conditions, redisv1.ConditionFailover)).To(BeNil())
	})

	It("reports the primary as available again once it is Ready", func() {
		markReady(name + "-0")
		Expect(reconcileOnce(objectKey(name))).To(Succeed())
		Expect(primaryAvailable().Status).To(Equal(metav1.ConditionTrue))
	})
})
