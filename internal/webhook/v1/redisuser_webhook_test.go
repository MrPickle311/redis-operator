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

package v1

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/instancemanager"
)

func newUser(name, target, username string) *redisv1.RedisUser {
	return &redisv1.RedisUser{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: redisv1.RedisUserSpec{
			TargetRef:      redisv1.TargetRef{Kind: "RedisInstance", Name: target},
			Username:       username,
			Authentication: redisv1.RedisUserAuthentication{Generate: true},
			ACL:            redisv1.RedisUserACL{Rules: []redisv1.ACLRule{"~*", "+@read"}},
		},
	}
}

func newInstance(name string) *redisv1.RedisInstance {
	return &redisv1.RedisInstance{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: redisv1.RedisInstanceSpec{
			Instances: 1, Image: "redis:7.2",
			Storage: redisv1.StorageSpec{VolumeClaimTemplate: &redisv1.VolumeClaimTemplateSpec{Size: resource.MustParse("1Gi")}},
		},
	}
}

var _ = Describe("RedisUser Webhook", func() {
	validator := func(objects ...client.Object) *RedisUserValidator {
		return &RedisUserValidator{client: fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(objects...).Build()}
	}

	It("only warns when the RedisInstance does not exist yet", func() {
		warnings, err := validator().ValidateCreate(ctx, newUser("app", "cache", ""))
		Expect(err).NotTo(HaveOccurred())
		Expect(warnings).To(ConsistOf(ContainSubstring("RedisInstance cache does not exist yet")))
	})

	It("admits a username nobody else has", func() {
		v := validator(newInstance("cache"), newUser("other", "cache", "other"), newUser("app", "sessions", "app"))
		Expect(v.ValidateCreate(ctx, newUser("app", "cache", ""))).To(BeEmpty())
	})

	It("turns away a username another RedisUser of the instance has", func() {
		v := validator(newInstance("cache"), newUser("first", "cache", "app"))
		_, err := v.ValidateCreate(ctx, newUser("second", "cache", "app"))
		Expect(err).To(MatchError(ContainSubstring("RedisUser first also claims the username app; neither would be applied")))
	})

	It("checks a user of a RedisCluster against the users of every shard", func() {
		clusterUser := func(name string) *redisv1.RedisUser {
			user := newUser(name, "store", "app")
			user.Spec.TargetRef.Kind = "RedisCluster"
			return user
		}
		warnings, err := validator().ValidateCreate(ctx, clusterUser("first"))
		Expect(err).NotTo(HaveOccurred())
		Expect(warnings).To(ConsistOf(ContainSubstring("RedisCluster store does not exist yet")))

		cluster := &redisv1.RedisCluster{ObjectMeta: metav1.ObjectMeta{Name: "store", Namespace: "default", UID: "store-uid"}}
		shard := newInstance("store-1")
		shard.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: redisv1.GroupVersion.String(), Kind: "RedisCluster", Name: "store", UID: cluster.UID, Controller: ptr.To(true),
		}}
		_, err = validator(cluster, newInstance("store-0"), shard, clusterUser("first")).ValidateCreate(ctx, clusterUser("second"))
		Expect(err).To(MatchError(ContainSubstring("RedisUser first also claims the username app")))
	})

	It("turns away a username of the external ACL of the instance", func() {
		acl := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: instancemanager.ACLSecretName("cache"), Namespace: "default"},
			Data:       map[string][]byte{"external.acl": []byte("user app on #0123 ~* +@all\n")},
		}
		_, err := validator(newInstance("cache"), acl).ValidateCreate(ctx, newUser("app", "cache", ""))
		Expect(err).To(MatchError(ContainSubstring("spec.acl.externalSecretRef of cache also defines the username app")))
	})

	It("checks an update only when the username or the target changes", func() {
		v := validator(newInstance("cache"), newUser("first", "cache", "app"))
		old := newUser("second", "cache", "app")
		changedRules := newUser("second", "cache", "app")
		changedRules.Spec.ACL.Rules = []redisv1.ACLRule{"~*", "+@all"}
		Expect(v.ValidateUpdate(ctx, old, changedRules)).To(BeEmpty(), "a conflict that came later blocks no other fix")

		_, err := v.ValidateUpdate(ctx, newUser("second", "cache", "other"), old)
		Expect(err).To(MatchError(ContainSubstring("also claims the username app")))
	})

	It("is called by the API server, which refuses the RedisUser", func() {
		Expect(k8sClient.Create(ctx, newInstance("envtest"))).To(Succeed())
		Expect(k8sClient.Create(ctx, newUser("first", "envtest", "app"))).To(Succeed())
		Eventually(func() error {
			return k8sClient.Create(ctx, newUser("second", "envtest", "app"))
		}).Should(MatchError(ContainSubstring("spec.username: Forbidden: RedisUser first also claims the username app")))
	})
})
