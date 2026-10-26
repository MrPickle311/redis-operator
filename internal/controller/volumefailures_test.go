package controller

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

func TestVolumeFailureAction(t *testing.T) {
	allReady := map[int32]bool{0: true, 1: true, 2: true}
	instanceWith := func(volumes ...redisv1.VolumeStatus) *redisv1.RedisInstance {
		return &redisv1.RedisInstance{
			ObjectMeta: metav1.ObjectMeta{Name: "cache"},
			Spec:       redisv1.RedisInstanceSpec{Instances: 3},
			Status:     redisv1.RedisInstanceStatus{CurrentPrimary: "cache-0", Volumes: volumes},
		}
	}
	vol := func(ordinal int32, state redisv1.VolumeState, external bool) redisv1.VolumeStatus {
		return redisv1.VolumeStatus{Ordinal: ordinal, State: state, External: external}
	}
	tests := []struct {
		name        string
		instance    *redisv1.RedisInstance
		ready       map[int32]bool
		wantOrdinal int32
		want        volumeAction
	}{
		{"every volume is Ready", instanceWith(vol(0, redisv1.VolumeReady, false), vol(1, redisv1.VolumeReady, false)), allReady, 0, noVolumeAction},
		{"the volume of the primary is going", instanceWith(vol(0, redisv1.VolumeTerminating, false)), allReady, 0, switchOverPrimary},
		{"the volume of the primary is lost", instanceWith(vol(0, redisv1.VolumeLost, true)), allReady, 0, switchOverPrimary},
		{"a primary that is down takes a failover instead", instanceWith(vol(0, redisv1.VolumeLost, false)), map[int32]bool{1: true, 2: true}, 0, noVolumeAction},
		{"a generated replica volume is going", instanceWith(vol(1, redisv1.VolumeTerminating, false)), allReady, 1, deleteReplicaPod},
		{"a generated replica volume is lost", instanceWith(vol(2, redisv1.VolumeLost, false)), allReady, 2, recreateReplica},
		{"an external replica volume belongs to the user", instanceWith(vol(1, redisv1.VolumeLost, true)), allReady, 0, noVolumeAction},
		{"one instance at a time", instanceWith(vol(1, redisv1.VolumeTerminating, false)), map[int32]bool{0: true, 1: true}, 0, noVolumeAction},
		{"the lowest ordinal first", instanceWith(vol(2, redisv1.VolumeLost, false), vol(1, redisv1.VolumeTerminating, false)), allReady, 1, deleteReplicaPod},
		{"Missing is handled when the volume is bound", instanceWith(vol(1, redisv1.VolumeMissing, false)), allReady, 0, noVolumeAction},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ordinal, action := volumeFailureAction(tt.instance, tt.ready)
			if action != tt.want || (action != noVolumeAction && ordinal != tt.wantOrdinal) {
				t.Errorf("volumeFailureAction() = %d, %d; want %d, %d", ordinal, action, tt.wantOrdinal, tt.want)
			}
		})
	}
}

// envtest runs no Instance Manager, so only the replicas are covered here; the
// switchover for the primary is in the e2e scenario of volumes.
var _ = Describe("RedisInstance volume failures", func() {
	setUp := func(name string) {
		Expect(k8sClient.Create(ctx, newRedisInstance(name, 3, redisv1.StorageSpec{
			VolumeClaimTemplate: volumeClaimTemplate(),
		}))).To(Succeed())
		Expect(reconcileOnce(objectKey(name))).To(Succeed())
		markReady(name+"-0", name+"-1", name+"-2")
		instance := getInstance(name)
		instance.Status.CurrentPrimary = name + "-0"
		Expect(k8sClient.Status().Update(ctx, instance)).To(Succeed())
	}

	Context("when the generated volume of a replica is being deleted", Ordered, func() {
		const name = "volfail-term"
		BeforeAll(func() { setUp(name) })
		AfterAll(func() { deletePVCNow(name + "-2-data"); deleteInstance(name) })

		It("deletes the replica, so a new volume can follow", func() {
			// The pvc-protection finalizer keeps the PVC Terminating.
			Expect(k8sClient.Delete(ctx, newPVC(name+"-2-data"))).To(Succeed())
			recorder := events.NewFakeRecorder(20)
			Expect(reconcileWith(recorder, objectKey(name))).To(Succeed())

			Expect(podExistsNow(name + "-2")).To(BeFalse())
			Expect(existingPodNames(name, 3)).To(ConsistOf(name+"-0", name+"-1"))
			Expect(drain(recorder)).To(ContainElement(ContainSubstring("ReplicaVolumeFailing")))
		})
	})

	Context("when the generated volume of a replica lost its PersistentVolume", Ordered, func() {
		const name = "volfail-lost"
		BeforeAll(func() { setUp(name) })
		AfterAll(func() { deletePVCNow(name + "-1-data"); deleteInstance(name) })

		It("recreates the replica on a new generated volume", func() {
			pvc := &corev1.PersistentVolumeClaim{}
			Expect(k8sClient.Get(ctx, objectKey(name+"-1-data"), pvc)).To(Succeed())
			oldUID := pvc.UID
			pvc.Status.Phase = corev1.ClaimLost
			Expect(k8sClient.Status().Update(ctx, pvc)).To(Succeed())

			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			Expect(podExistsNow(name + "-1")).To(BeFalse())
			Expect(pvcDeleted(name + "-1-data")).To(BeTrue())
			Expect(volume(getInstance(name), 1).UID).NotTo(Equal(oldUID), "the binding still points at the lost volume")
		})
	})
})
