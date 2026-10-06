/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

const testNamespace = "default"

var _ = Describe("RedisInstance controller", func() {
	Context("when reconciling a RedisInstance", func() {
		const name = "reconcile-test"

		BeforeEach(func() {
			By("creating a RedisInstance with a primary and one replica")
			instance := newRedisInstance(name, 2, redisv1.StorageSpec{
				VolumeClaimTemplate: volumeClaimTemplate("1Gi"),
			})
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())

			By("running a single reconcile")
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
		})

		AfterEach(func() {
			// envtest has no garbage collector: Pods and Services owned by the
			// RedisInstance stay behind and are reused by the next reconcile.
			instance := &redisv1.RedisInstance{}
			Expect(k8sClient.Get(ctx, objectKey(name), instance)).To(Succeed())
			Expect(k8sClient.Delete(ctx, instance)).To(Succeed())
		})

		It("creates a headless Service that publishes not-ready Pods", func() {
			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, objectKey(name+"-hl"), svc)).To(Succeed())

			Expect(svc.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
			Expect(svc.Spec.PublishNotReadyAddresses).To(BeTrue())
		})

		It("gives the Pod a stable DNS name through hostname and subdomain", func() {
			pod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, objectKey(name+"-0"), pod)).To(Succeed())

			Expect(pod.Spec.Hostname).To(Equal(name + "-0"))
			Expect(pod.Spec.Subdomain).To(Equal(name + "-hl"))
		})

		It("labels the first Pod as primary and the others as replicas", func() {
			for pod, role := range map[string]string{name + "-0": rolePrimary, name + "-1": roleReplica} {
				Expect(podLabels(objectKey(pod))).To(HaveKeyWithValue(labelRole, role), "Pod %s", pod)
			}
		})

		It("owns the PVCs it creates, so they are deleted with the RedisInstance", func() {
			pvc := &corev1.PersistentVolumeClaim{}
			Expect(k8sClient.Get(ctx, objectKey(name+"-0-data"), pvc)).To(Succeed())
			Expect(metav1.GetControllerOf(pvc)).NotTo(BeNil())
			Expect(metav1.GetControllerOf(pvc).Name).To(Equal(name))
		})

		DescribeTable("creates a client Service per access mode",
			func(suffix string, wantSelector map[string]string) {
				svc := &corev1.Service{}
				Expect(k8sClient.Get(ctx, objectKey(name+suffix), svc)).To(Succeed())

				Expect(svc.Spec.Type).To(Equal(corev1.ServiceTypeClusterIP))
				Expect(svc.Spec.Selector).To(Equal(wantSelector))
			},
			Entry("-rw sends reads and writes to the primary", "-rw",
				map[string]string{labelInstance: name, labelRole: rolePrimary}),
			Entry("-ro sends reads to replicas only", "-ro",
				map[string]string{labelInstance: name, labelRole: roleReplica}),
			Entry("-r sends reads to every instance", "-r",
				map[string]string{labelInstance: name}),
		)
	})

	Context("with storage.existingClaims", func() {
		const name = "existing-claims-test"

		BeforeEach(func() {
			instance := newRedisInstance(name, 1, redisv1.StorageSpec{ExistingClaims: existingClaims(0)})
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
		})

		AfterEach(func() {
			Expect(k8sClient.Delete(ctx, newRedisInstance(name, 1, redisv1.StorageSpec{}))).To(Succeed())
		})

		It("mounts the external PVC and does not create one of its own", func() {
			pod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, objectKey(name+"-0"), pod)).To(Succeed())
			Expect(pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName).To(Equal("pvc-0"))

			Expect(pvcDeleted(name+"-0-data")).To(BeTrue(), "operator created a PVC although an external one was given")
		})
	})

	// Ordered with BeforeAll: envtest has no garbage collector, so Pods and PVCs
	// from a second run of the setup would collide with the first one.
	Context("when scaling down", Ordered, func() {
		const name = "scale-down-test"

		BeforeAll(func() {
			By("pre-creating a PVC for ordinal 2 that the operator does not own")
			Expect(k8sClient.Create(ctx, newPVC(name+"-2-data"))).To(Succeed())

			By("creating 3 instances")
			instance := newRedisInstance(name, 3, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate("1Gi")})
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())

			By("scaling down to 1 instance")
			Expect(k8sClient.Get(ctx, objectKey(name), instance)).To(Succeed())
			instance.Spec.Instances = 1
			Expect(k8sClient.Update(ctx, instance)).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
		})

		AfterAll(func() {
			Expect(k8sClient.Delete(ctx, newRedisInstance(name, 1, redisv1.StorageSpec{}))).To(Succeed())
		})

		It("deletes the Pods of removed instances", func() {
			for _, pod := range []string{name + "-1", name + "-2"} {
				err := k8sClient.Get(ctx, objectKey(pod), &corev1.Pod{})
				Expect(errors.IsNotFound(err)).To(BeTrue(), "Pod %s still exists", pod)
			}
		})

		It("deletes the PVC it created for a removed instance", func() {
			Expect(pvcDeleted(name + "-1-data")).To(BeTrue())
		})

		It("keeps a PVC it does not own, even when the name matches", func() {
			Expect(pvcDeleted(name + "-2-data")).To(BeFalse())
		})
	})

	DescribeTable("validating spec.storage with CEL rules",
		func(storage redisv1.StorageSpec, accepted bool) {
			instance := newRedisInstance("cel-test", 2, storage)
			err := k8sClient.Create(ctx, instance)

			if accepted {
				Expect(err).NotTo(HaveOccurred())
				Expect(k8sClient.Delete(ctx, instance)).To(Succeed())
				return
			}
			Expect(errors.IsInvalid(err)).To(BeTrue(), "expected an Invalid error, got: %v", err)
		},
		Entry("rejects a missing volumeClaimTemplate when existingClaims do not cover every instance",
			redisv1.StorageSpec{ExistingClaims: existingClaims(0)},
			false),
		Entry("rejects an existingClaims ordinal outside of instances",
			redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate("1Gi"), ExistingClaims: existingClaims(5)},
			false),
		Entry("accepts existingClaims covering every instance without a volumeClaimTemplate",
			redisv1.StorageSpec{ExistingClaims: existingClaims(0, 1)},
			true),
	)
})

// pvcDeleted reports whether the PVC is gone or being deleted. envtest runs no
// controller that removes the pvc-protection finalizer, so a deleted PVC may linger.
func pvcDeleted(name string) bool {
	pvc := &corev1.PersistentVolumeClaim{}
	err := k8sClient.Get(ctx, objectKey(name), pvc)
	if errors.IsNotFound(err) {
		return true
	}
	Expect(err).NotTo(HaveOccurred())
	return pvc.DeletionTimestamp != nil
}

// newPVC returns a PVC that is not owned by any RedisInstance.
func newPVC(name string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}
}

func podLabels(key types.NamespacedName) map[string]string {
	pod := &corev1.Pod{}
	Expect(k8sClient.Get(ctx, key, pod)).To(Succeed())
	return pod.Labels
}

// reconcileOnce runs the reconciler for the given RedisInstance exactly once.
func reconcileOnce(key types.NamespacedName) error {
	r := &RedisInstanceReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
	return err
}

func objectKey(name string) types.NamespacedName {
	return types.NamespacedName{Name: name, Namespace: testNamespace}
}

func newRedisInstance(name string, instances int32, storage redisv1.StorageSpec) *redisv1.RedisInstance {
	return &redisv1.RedisInstance{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: redisv1.RedisInstanceSpec{
			Instances: instances,
			Image:     "redis:7.2",
			Storage:   storage,
		},
	}
}

func volumeClaimTemplate(size string) *redisv1.VolumeClaimTemplateSpec {
	return &redisv1.VolumeClaimTemplateSpec{Size: resource.MustParse(size)}
}

// existingClaims binds each given ordinal to a PVC named pvc-<ordinal>.
func existingClaims(ordinals ...int32) []redisv1.ExistingClaim {
	claims := make([]redisv1.ExistingClaim, 0, len(ordinals))
	for _, o := range ordinals {
		claims = append(claims, redisv1.ExistingClaim{InstanceOrdinal: o, ClaimName: fmt.Sprintf("pvc-%d", o)})
	}
	return claims
}
