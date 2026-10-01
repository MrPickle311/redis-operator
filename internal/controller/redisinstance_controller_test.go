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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

var _ = Describe("RedisInstance Controller", func() {
	Context("When reconciling a resource", func() {
		const (
			resourceName      = "test-resource"
			resourceNamespace = "default"
		)

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: resourceNamespace,
		}
		redisInstance := &redisv1.RedisInstance{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind RedisInstance")
			err := k8sClient.Get(ctx, typeNamespacedName, redisInstance)
			if err != nil && errors.IsNotFound(err) {
				ri := &redisv1.RedisInstance{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: resourceNamespace,
					},
					Spec: redisv1.RedisInstanceSpec{
						Instances: 1,
						Image:     "redis:7.2",
						Storage: redisv1.StorageSpec{
							VolumeClaimTemplate: &redisv1.VolumeClaimTemplateSpec{
								Size: resource.MustParse("1Gi"),
							},
						},
					},
				}
				Expect(k8sClient.Create(ctx, ri)).To(Succeed())
			}
		})

		AfterEach(func() {
			ri := &redisv1.RedisInstance{}
			err := k8sClient.Get(ctx, typeNamespacedName, ri)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance RedisInstance")
			Expect(k8sClient.Delete(ctx, ri)).To(Succeed())
		})

		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &RedisInstanceReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("When validating the spec with CEL rules", func() {
		ctx := context.Background()

		newInstance := func(name string, storage redisv1.StorageSpec) *redisv1.RedisInstance {
			return &redisv1.RedisInstance{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
				Spec: redisv1.RedisInstanceSpec{
					Instances: 2,
					Image:     "redis:7.2",
					Storage:   storage,
				},
			}
		}

		It("rejects a spec without volumeClaimTemplate when existingClaims do not cover all instances", func() {
			ri := newInstance("cel-missing-template", redisv1.StorageSpec{
				ExistingClaims: []redisv1.ExistingClaim{{InstanceOrdinal: 0, ClaimName: "pvc-0"}},
			})
			err := k8sClient.Create(ctx, ri)
			Expect(errors.IsInvalid(err)).To(BeTrue(), "expected Invalid error, got: %v", err)
		})

		It("rejects existingClaims with an ordinal outside of instances", func() {
			ri := newInstance("cel-ordinal-out-of-range", redisv1.StorageSpec{
				VolumeClaimTemplate: &redisv1.VolumeClaimTemplateSpec{Size: resource.MustParse("1Gi")},
				ExistingClaims:      []redisv1.ExistingClaim{{InstanceOrdinal: 5, ClaimName: "pvc-5"}},
			})
			err := k8sClient.Create(ctx, ri)
			Expect(errors.IsInvalid(err)).To(BeTrue(), "expected Invalid error, got: %v", err)
		})

		It("accepts existingClaims covering every instance without volumeClaimTemplate", func() {
			ri := newInstance("cel-all-external", redisv1.StorageSpec{
				ExistingClaims: []redisv1.ExistingClaim{
					{InstanceOrdinal: 0, ClaimName: "pvc-0"},
					{InstanceOrdinal: 1, ClaimName: "pvc-1"},
				},
			})
			Expect(k8sClient.Create(ctx, ri)).To(Succeed())
			Expect(k8sClient.Delete(ctx, ri)).To(Succeed())
		})
	})
})
