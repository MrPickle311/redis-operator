package controller

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

// Each Context uses its own RedisInstance and PVC names: envtest has no
// garbage collector, so objects of one Context would leak into the next.
var _ = Describe("RedisInstance volumes", func() {
	Context("when binding instances to volumes", Ordered, func() {
		const name = "vol-bind"
		external := name + "-restored"

		BeforeAll(func() {
			Expect(k8sClient.Create(ctx, newPVC(external))).To(Succeed())
			instance := newRedisInstance(name, 2, redisv1.StorageSpec{
				VolumeClaimTemplate: volumeClaimTemplate(),
				ExistingClaims:      []redisv1.ExistingClaim{{InstanceOrdinal: 1, ClaimName: external}},
			})
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
		})

		AfterAll(func() { deleteInstance(name); deletePVCNow(external) })

		It("records the PVC and its UID for every instance", func() {
			instance := getInstance(name)
			Expect(instance.Status.Volumes).To(HaveLen(2))

			generated := volume(instance, 0)
			Expect(generated.ClaimName).To(Equal(name + "-0-data"))
			Expect(generated.External).To(BeFalse())
			Expect(generated.UID).To(Equal(pvcUID(name + "-0-data")))
			Expect(generated.State).To(Equal(redisv1.VolumeReady))

			ext := volume(instance, 1)
			Expect(ext.ClaimName).To(Equal(external))
			Expect(ext.External).To(BeTrue())
			Expect(ext.UID).To(Equal(pvcUID(external)))
			Expect(ext.State).To(Equal(redisv1.VolumeReady))

			Expect(meta.IsStatusConditionTrue(instance.Status.Conditions, redisv1.ConditionStorageReady)).To(BeTrue())
		})

		It("mounts the bound PVC in every Pod", func() {
			Expect(podClaim(name + "-0")).To(Equal(name + "-0-data"))
			Expect(podClaim(name + "-1")).To(Equal(external))
		})

		It("rebuilds the same bindings when status.volumes is lost", func() {
			before := getInstance(name).Status.Volumes

			instance := getInstance(name)
			instance.Status.Volumes = nil
			Expect(k8sClient.Status().Update(ctx, instance)).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())

			Expect(getInstance(name).Status.Volumes).To(Equal(before))
		})
	})

	Context("when the external PVC of an instance does not exist yet", Ordered, func() {
		const name = "vol-ext-missing"
		external := name + "-claim"

		BeforeAll(func() {
			instance := newRedisInstance(name, 1, redisv1.StorageSpec{
				ExistingClaims: []redisv1.ExistingClaim{{InstanceOrdinal: 0, ClaimName: external}},
			})
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
		})

		AfterAll(func() { deleteInstance(name); deletePVCNow(external) })

		It("creates no Pod and reports StorageReady=False", func() {
			Expect(podExistsNow(name + "-0")).To(BeFalse())
			instance := getInstance(name)
			Expect(volume(instance, 0).State).To(Equal(redisv1.VolumeMissing))
			Expect(storageMessage(instance)).To(ContainSubstring(external + " not found"))
		})

		It("starts the instance once the PVC appears", func() {
			Expect(k8sClient.Create(ctx, newPVC(external))).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())

			Expect(podClaim(name + "-0")).To(Equal(external))
			Expect(meta.IsStatusConditionTrue(getInstance(name).Status.Conditions, redisv1.ConditionStorageReady)).To(BeTrue())
		})

		It("is woken up by events of the external PVC it waits for", func() {
			r := &RedisInstanceReconciler{Client: k8sClient}
			requests := r.instancesUsingPVC(ctx, newPVC(external))
			Expect(requests).To(ConsistOf(HaveField("NamespacedName", objectKey(name))))
			Expect(r.instancesUsingPVC(ctx, newPVC("unrelated-pvc"))).To(BeEmpty())
		})
	})

	Context("when a PVC with the generated name already exists", Ordered, func() {
		const name = "vol-squatter"

		BeforeAll(func() {
			Expect(k8sClient.Create(ctx, newPVC(name+"-0-data"))).To(Succeed())
			Expect(k8sClient.Create(ctx, newRedisInstance(name, 1,
				redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()}))).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
		})

		AfterAll(func() { deleteInstance(name); deletePVCNow(name + "-0-data") })

		It("does not adopt it, because it may hold any data", func() {
			Expect(podExistsNow(name + "-0")).To(BeFalse())
			instance := getInstance(name)
			Expect(volume(instance, 0).State).To(Equal(redisv1.VolumeMissing))
			Expect(volume(instance, 0).UID).To(BeEmpty())
			Expect(storageMessage(instance)).To(ContainSubstring("not created by this RedisInstance"))
		})
	})

	Context("when the PVC of a replica is deleted", Ordered, func() {
		const name = "vol-replica-lost"
		claim := name + "-1-data"
		var oldUID types.UID

		BeforeAll(func() {
			Expect(k8sClient.Create(ctx, newRedisInstance(name, 2,
				redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()}))).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			oldUID = pvcUID(claim)
		})

		AfterAll(func() { deleteInstance(name) })

		It("reports Terminating while the Pod still uses it", func() {
			Expect(k8sClient.Delete(ctx, newPVC(claim))).To(Succeed()) // pvc-protection keeps it
			Expect(reconcileOnce(objectKey(name))).To(Succeed())

			Expect(volume(getInstance(name), 1).State).To(Equal(redisv1.VolumeTerminating))
			Expect(podExistsNow(name + "-1")).To(BeTrue())
		})

		It("creates a new empty PVC once the Pod is gone, and a Pod on it", func() {
			deletePodNow(name + "-1")
			deletePVCNow(claim)

			recorder := events.NewFakeRecorder(20)
			Expect(reconcileWith(recorder, objectKey(name))).To(Succeed())

			By("forgetting the old volume first, in the status, before any new PVC exists")
			vol := volume(getInstance(name), 1)
			Expect(vol.UID).To(BeEmpty())
			Expect(vol.State).To(Equal(redisv1.VolumeMissing))
			Expect(pvcDeleted(claim)).To(BeTrue())

			Expect(reconcileWith(recorder, objectKey(name))).To(Succeed())
			vol = volume(getInstance(name), 1)
			Expect(vol.State).To(Equal(redisv1.VolumeReady))
			Expect(vol.UID).NotTo(BeEmpty())
			Expect(vol.UID).NotTo(Equal(oldUID))
			Expect(pvcUID(claim)).To(Equal(vol.UID))
			Expect(podClaim(name + "-1")).To(Equal(claim))
			Expect(drain(recorder)).To(ContainElement(ContainSubstring("VolumeRecreated")))
		})
	})

	Context("when the PVC of the primary is deleted", Ordered, func() {
		const name = "vol-primary-lost"
		claim := name + "-0-data"

		BeforeAll(func() {
			Expect(k8sClient.Create(ctx, newRedisInstance(name, 2,
				redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()}))).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())

			deletePodNow(name + "-0")
			deletePVCNow(claim)
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
		})

		AfterAll(func() { deleteInstance(name) })

		It("never creates an empty primary", func() {
			Expect(pvcDeleted(claim)).To(BeTrue(), "a new PVC was generated for the primary")
			Expect(podExistsNow(name + "-0")).To(BeFalse())

			instance := getInstance(name)
			Expect(volume(instance, 0).State).To(Equal(redisv1.VolumeMissing))
			Expect(storageMessage(instance)).To(ContainSubstring("primary"))
		})
	})

	Context("when an external PVC is replaced by one with the same name", Ordered, func() {
		const name = "vol-ext-replaced"
		external := name + "-claim"

		BeforeAll(func() {
			Expect(k8sClient.Create(ctx, newPVC(external))).To(Succeed())
			Expect(k8sClient.Create(ctx, newRedisInstance(name, 2, redisv1.StorageSpec{
				VolumeClaimTemplate: volumeClaimTemplate(),
				ExistingClaims:      []redisv1.ExistingClaim{{InstanceOrdinal: 1, ClaimName: external}},
			}))).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())

			By("deleting the Pod and putting another PVC under the same name")
			deletePodNow(name + "-1")
			deletePVCNow(external)
			Expect(k8sClient.Create(ctx, newPVC(external))).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
		})

		AfterAll(func() { deleteInstance(name); deletePVCNow(external) })

		It("does not mount the new PVC", func() {
			Expect(podExistsNow(name + "-1")).To(BeFalse())
			instance := getInstance(name)
			Expect(volume(instance, 1).State).To(Equal(redisv1.VolumeMissing))
			Expect(storageMessage(instance)).To(ContainSubstring("created again"))
		})

		It("moves the replica to a generated volume on recreate and leaves the external PVC alone", func() {
			annotate(name, redisv1.AnnotationRecreate, name+"-1")
			recorder := events.NewFakeRecorder(20)
			Expect(reconcileWith(recorder, objectKey(name))).To(Succeed())

			instance := getInstance(name)
			Expect(instance.Status.HandledRequests).To(HaveKeyWithValue(redisv1.AnnotationRecreate, name+"-1"))
			vol := volume(instance, 1)
			Expect(vol.ClaimName).To(Equal(name + "-1-data"))
			Expect(vol.External).To(BeFalse())
			Expect(vol.State).To(Equal(redisv1.VolumeReady))
			Expect(podClaim(name + "-1")).To(Equal(name + "-1-data"))
			Expect(pvcDeleted(external)).To(BeFalse(), "the operator deleted an external PVC")
			Expect(drain(recorder)).To(ContainElement(ContainSubstring("InstanceRecreated")))
		})
	})

	Context("when recreating a replica with a generated volume", Ordered, func() {
		const name = "vol-recreate"
		claim := name + "-1-data"
		var oldUID types.UID

		BeforeAll(func() {
			Expect(k8sClient.Create(ctx, newRedisInstance(name, 2,
				redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()}))).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			oldUID = pvcUID(claim)

			annotate(name, redisv1.AnnotationRecreate, name+"-1")
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
		})

		AfterAll(func() { deleteInstance(name) })

		It("waits for the old PVC to be deleted", func() {
			Expect(pvcDeleted(claim)).To(BeTrue())
			Expect(podExistsNow(name + "-1")).To(BeFalse())
			Expect(volume(getInstance(name), 1).State).To(Equal(redisv1.VolumeTerminating))
		})

		It("then generates a new PVC and starts the Pod on it", func() {
			deletePVCNow(claim) // what the PVC protection controller does once no Pod uses it
			Expect(reconcileOnce(objectKey(name))).To(Succeed())

			vol := volume(getInstance(name), 1)
			Expect(vol.State).To(Equal(redisv1.VolumeReady))
			Expect(vol.UID).NotTo(Equal(oldUID))
			Expect(podClaim(name + "-1")).To(Equal(claim))
		})

		It("does not recreate again while the annotation stays, as in git", func() {
			uid := volume(getInstance(name), 1).UID
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			Expect(podExistsNow(name + "-1")).To(BeTrue())
			Expect(volume(getInstance(name), 1).UID).To(Equal(uid))
		})

		It("recreates again for a new value of the same Pod", func() {
			annotate(name, redisv1.AnnotationRecreate, name+"-1@2")
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			Expect(podExistsNow(name + "-1")).To(BeFalse())
			Expect(getInstance(name).Status.HandledRequests).To(HaveKeyWithValue(redisv1.AnnotationRecreate, name+"-1@2"))
			deletePVCNow(claim)
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
		})

		It("forgets the request once the annotation is removed", func() {
			removeAnnotation(name, redisv1.AnnotationRecreate)
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			Expect(getInstance(name).Status.HandledRequests).NotTo(HaveKey(redisv1.AnnotationRecreate))
		})
	})

	Context("when the recreate annotation cannot be served", Ordered, func() {
		const name = "vol-recreate-rejected"

		BeforeAll(func() {
			Expect(k8sClient.Create(ctx, newRedisInstance(name, 2,
				redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()}))).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
		})

		AfterAll(func() { deleteInstance(name) })

		DescribeTable("rejects it once and keeps the instance",
			func(target, wantReason string) {
				before := getInstance(name).Status.Volumes
				annotate(name, redisv1.AnnotationRecreate, target)

				recorder := events.NewFakeRecorder(20)
				Expect(reconcileWith(recorder, objectKey(name))).To(Succeed())

				Expect(drain(recorder)).To(ContainElement(SatisfyAll(
					ContainSubstring("RecreateRejected"), ContainSubstring(wantReason))))
				instance := getInstance(name)
				Expect(instance.Status.HandledRequests).To(HaveKeyWithValue(redisv1.AnnotationRecreate, target))
				Expect(instance.Status.Volumes).To(Equal(before))
				Expect(podExistsNow(name + "-0")).To(BeTrue())
			},
			Entry("the primary", name+"-0", "primary"),
			Entry("a Pod of no instance", name+"-7", "not an instance"),
		)
	})

	Context("when scaling with an external PVC reserved for a later instance", Ordered, func() {
		const name = "vol-scale"
		spare := name + "-spare"
		var spareUID types.UID

		BeforeAll(func() {
			Expect(k8sClient.Create(ctx, newPVC(spare))).To(Succeed())
			Expect(k8sClient.Create(ctx, newRedisInstance(name, 2, redisv1.StorageSpec{
				VolumeClaimTemplate: volumeClaimTemplate(),
				ExistingClaims:      []redisv1.ExistingClaim{{InstanceOrdinal: 2, ClaimName: spare}},
			}))).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			spareUID = pvcUID(spare)
		})

		AfterAll(func() { deleteInstance(name); deletePVCNow(spare) })

		It("does not bind the reserved PVC before the instance exists", func() {
			Expect(getInstance(name).Status.Volumes).To(HaveLen(2))
		})

		It("mounts the reserved PVC when scaling up", func() {
			scale(name, 3)
			Expect(podClaim(name + "-2")).To(Equal(spare))
			Expect(volume(getInstance(name), 2).UID).To(Equal(spareUID))
		})

		It("detaches it on scale-down, removes the generated volumes and keeps the external one", func() {
			scale(name, 1)

			instance := getInstance(name)
			Expect(instance.Status.Volumes).To(HaveLen(2), "only instance 0 and the external volume of 2 stay bound")
			Expect(volume(instance, 2).State).To(Equal(redisv1.VolumeDetached))
			Expect(pvcDeleted(spare)).To(BeFalse(), "the operator deleted an external PVC")
			Expect(pvcDeleted(name + "-1-data")).To(BeTrue())
			Expect(meta.IsStatusConditionTrue(instance.Status.Conditions, redisv1.ConditionStorageReady)).To(BeTrue(),
				"a detached volume must not make the instance unhealthy")
		})

		It("mounts the same PVC again when scaling up, and a new generated one for instance 1", func() {
			deletePVCNow(name + "-1-data") // released by the PVC protection controller in a real cluster
			scale(name, 3)

			Expect(podClaim(name + "-2")).To(Equal(spare))
			instance := getInstance(name)
			Expect(volume(instance, 2).UID).To(Equal(spareUID))
			Expect(volume(instance, 2).State).To(Equal(redisv1.VolumeReady))
			Expect(volume(instance, 1).State).To(Equal(redisv1.VolumeReady))
		})
	})

	Context("validating changes of spec.storage", Ordered, func() {
		const name = "vol-cel"

		BeforeAll(func() {
			Expect(k8sClient.Create(ctx, newRedisInstance(name, 2, redisv1.StorageSpec{
				VolumeClaimTemplate: volumeClaimTemplate(),
				ExistingClaims:      []redisv1.ExistingClaim{{InstanceOrdinal: 1, ClaimName: "a"}},
			}))).To(Succeed())
		})

		AfterAll(func() { deleteInstance(name) })

		DescribeTable("rejects every change of existingClaims",
			func(claims []redisv1.ExistingClaim) {
				instance := getInstance(name)
				instance.Spec.Storage.ExistingClaims = claims
				err := k8sClient.Update(ctx, instance)
				Expect(errors.IsInvalid(err)).To(BeTrue(), "expected an Invalid error, got: %v", err)
				Expect(err.Error()).To(ContainSubstring("existingClaims is immutable"))
			},
			Entry("another claim name", []redisv1.ExistingClaim{{InstanceOrdinal: 1, ClaimName: "b"}}),
			Entry("another ordinal", []redisv1.ExistingClaim{{InstanceOrdinal: 0, ClaimName: "a"}}),
			Entry("an added claim", []redisv1.ExistingClaim{{InstanceOrdinal: 1, ClaimName: "a"}, {InstanceOrdinal: 2, ClaimName: "c"}}),
			Entry("no claims at all", nil),
		)

		It("still allows scaling down below an external ordinal", func() {
			instance := getInstance(name)
			instance.Spec.Instances = 1
			Expect(k8sClient.Update(ctx, instance)).To(Succeed())
		})

		It("rejects scaling up beyond the claims when there is no template", func() {
			const noTemplate = "vol-cel-no-template"
			Expect(k8sClient.Create(ctx, newRedisInstance(noTemplate, 1,
				redisv1.StorageSpec{ExistingClaims: existingClaims(0)}))).To(Succeed())
			DeferCleanup(deleteInstance, noTemplate)

			instance := getInstance(noTemplate)
			instance.Spec.Instances = 2
			err := k8sClient.Update(ctx, instance)
			Expect(errors.IsInvalid(err)).To(BeTrue(), "expected an Invalid error, got: %v", err)
		})

		It("does not allow adding existingClaims to an instance created without them", func() {
			const without = "vol-cel-without"
			Expect(k8sClient.Create(ctx, newRedisInstance(without, 1,
				redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()}))).To(Succeed())
			DeferCleanup(deleteInstance, without)

			instance := getInstance(without)
			instance.Spec.Storage.ExistingClaims = existingClaims(0)
			Expect(errors.IsInvalid(k8sClient.Update(ctx, instance))).To(BeTrue())
		})
	})
})

func getInstance(name string) *redisv1.RedisInstance {
	instance := &redisv1.RedisInstance{}
	Expect(k8sClient.Get(ctx, objectKey(name), instance)).To(Succeed())
	return instance
}

// deleteInstance deletes the RedisInstance. envtest has no garbage
// collector, so its Pods and PVCs stay; every Context uses its own names.
func deleteInstance(name string) {
	Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, newRedisInstance(name, 1, redisv1.StorageSpec{})))).To(Succeed())
}

// volume returns the binding of the given ordinal, failing when there is none.
func volume(instance *redisv1.RedisInstance, ordinal int32) redisv1.VolumeStatus {
	for _, v := range instance.Status.Volumes {
		if v.Ordinal == ordinal {
			return v
		}
	}
	Fail(fmt.Sprintf("no volume bound for instance %d", ordinal))
	return redisv1.VolumeStatus{}
}

func storageMessage(instance *redisv1.RedisInstance) string {
	condition := meta.FindStatusCondition(instance.Status.Conditions, redisv1.ConditionStorageReady)
	Expect(condition).NotTo(BeNil())
	Expect(condition.Status).To(Equal(metav1.ConditionFalse))
	return condition.Message
}

func pvcUID(name string) types.UID {
	pvc := &corev1.PersistentVolumeClaim{}
	Expect(k8sClient.Get(ctx, objectKey(name), pvc)).To(Succeed())
	return pvc.UID
}

func podClaim(name string) string {
	pod := &corev1.Pod{}
	Expect(k8sClient.Get(ctx, objectKey(name), pod)).To(Succeed())
	return dataClaimName(pod)
}

func podExistsNow(name string) bool {
	err := k8sClient.Get(ctx, objectKey(name), &corev1.Pod{})
	if errors.IsNotFound(err) {
		return false
	}
	Expect(err).NotTo(HaveOccurred())
	return true
}

// deletePodNow deletes a Pod. envtest schedules no Pod on a node, so the API
// server removes it at once instead of waiting for a kubelet.
func deletePodNow(name string) {
	Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace}}))).To(Succeed())
}

// deletePVCNow deletes a PVC and removes its pvc-protection finalizer, which
// in a real cluster a controller removes once no Pod uses the PVC.
func deletePVCNow(name string) {
	pvc := &corev1.PersistentVolumeClaim{}
	if err := k8sClient.Get(ctx, objectKey(name), pvc); errors.IsNotFound(err) {
		return
	}
	Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, pvc))).To(Succeed())
	Expect(k8sClient.Get(ctx, objectKey(name), pvc)).To(Succeed())
	pvc.Finalizers = nil
	Expect(client.IgnoreNotFound(k8sClient.Update(ctx, pvc))).To(Succeed())
	Eventually(func() bool { return errors.IsNotFound(k8sClient.Get(ctx, objectKey(name), pvc)) }).Should(BeTrue())
}

func annotate(name, key, value string) {
	instance := getInstance(name)
	if instance.Annotations == nil {
		instance.Annotations = map[string]string{}
	}
	instance.Annotations[key] = value
	Expect(k8sClient.Update(ctx, instance)).To(Succeed())
}

func scale(name string, instances int32) {
	instance := getInstance(name)
	instance.Spec.Instances = instances
	Expect(k8sClient.Update(ctx, instance)).To(Succeed())
	Expect(reconcileOnce(objectKey(name))).To(Succeed())
}

// drain returns the Events recorded so far.
func drain(recorder *events.FakeRecorder) []string {
	var got []string
	for {
		select {
		case e := <-recorder.Events:
			got = append(got, e)
		default:
			return got
		}
	}
}
