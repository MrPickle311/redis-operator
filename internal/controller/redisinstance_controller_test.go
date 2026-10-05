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
	"context"
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
	ctx := context.Background()

	Context("when reconciling a RedisInstance", func() {
		const name = "reconcile-test"

		BeforeEach(func() {
			By("creating a RedisInstance with one instance")
			instance := newRedisInstance(name, 1, redisv1.StorageSpec{
				VolumeClaimTemplate: volumeClaimTemplate("1Gi"),
			})
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())

			By("running a single reconcile")
			Expect(reconcileOnce(ctx, objectKey(name))).To(Succeed())
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

// reconcileOnce runs the reconciler for the given RedisInstance exactly once.
func reconcileOnce(ctx context.Context, key types.NamespacedName) error {
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
