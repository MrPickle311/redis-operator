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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

func newRedisCluster(name string) *redisv1.RedisCluster {
	return &redisv1.RedisCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: redisv1.RedisClusterSpec{
			Image:   "redis:7.2",
			Storage: redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()},
		},
	}
}

var _ = Describe("RedisCluster API", func() {
	expectInvalid := func(err error, message string) {
		Expect(errors.IsInvalid(err)).To(BeTrue(), "expected an Invalid error, got: %v", err)
		Expect(err.Error()).To(ContainSubstring(message))
	}

	It("defaults to 3 shards of 2 instances", func() {
		cluster := newRedisCluster("rc-defaults")
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		DeferCleanup(k8sClient.Delete, ctx, cluster)

		var got redisv1.RedisCluster
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: cluster.Name, Namespace: testNamespace}, &got)).To(Succeed())
		Expect(got.Spec.Shards).To(Equal(int32(3)))
		Expect(got.Spec.InstancesPerShard).To(Equal(int32(2)))
		Expect(got.Spec.TLS.TLSEnabled()).To(BeTrue())
	})

	It("rejects fewer than 3 shards, which could never fail over", func() {
		cluster := newRedisCluster("rc-two")
		cluster.Spec.Shards = 2
		expectInvalid(k8sClient.Create(ctx, cluster), "spec.shards")
	})

	It("rejects a change of shards", func() {
		cluster := newRedisCluster("rc-immutable")
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		DeferCleanup(k8sClient.Delete, ctx, cluster)

		cluster.Spec.Shards = 4
		expectInvalid(k8sClient.Update(ctx, cluster), "shards is immutable")
	})

	It("allows a change of instancesPerShard", func() {
		cluster := newRedisCluster("rc-replicas")
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		DeferCleanup(k8sClient.Delete, ctx, cluster)

		cluster.Spec.InstancesPerShard = 3
		Expect(k8sClient.Update(ctx, cluster)).To(Succeed())
	})

	DescribeTable("rejects storage other than a volumeClaimTemplate",
		func(storage redisv1.StorageSpec) {
			cluster := newRedisCluster("rc-storage")
			cluster.Spec.Storage = storage
			expectInvalid(k8sClient.Create(ctx, cluster), "needs storage.volumeClaimTemplate and takes no storage.existingClaims")
		},
		Entry("no template", redisv1.StorageSpec{}),
		Entry("existing claims", redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate(), ExistingClaims: existingClaims(0)}),
	)
})
