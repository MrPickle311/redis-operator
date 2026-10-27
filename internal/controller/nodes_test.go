package controller

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/events"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

func TestPodAffinity(t *testing.T) {
	instance := &redisv1.RedisInstance{ObjectMeta: metav1.ObjectMeta{Name: "cache"}}
	want := corev1.PodAffinityTerm{
		LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"redis.operator.com/instance": "cache"}},
		TopologyKey:   "kubernetes.io/hostname",
	}

	instance.Spec.Affinity.PodAntiAffinityType = redisv1.PodAntiAffinityRequired
	required := podAffinity(instance).PodAntiAffinity
	if len(required.RequiredDuringSchedulingIgnoredDuringExecution) != 1 ||
		required.RequiredDuringSchedulingIgnoredDuringExecution[0].TopologyKey != want.TopologyKey ||
		len(required.PreferredDuringSchedulingIgnoredDuringExecution) != 0 {
		t.Errorf("required: %+v, want one required term per node", required)
	}

	instance.Spec.Affinity.PodAntiAffinityType = redisv1.PodAntiAffinityPreferred
	preferred := podAffinity(instance).PodAntiAffinity
	if len(preferred.PreferredDuringSchedulingIgnoredDuringExecution) != 1 ||
		len(preferred.RequiredDuringSchedulingIgnoredDuringExecution) != 0 {
		t.Errorf("preferred: %+v, want one preferred term", preferred)
	}
}

var _ = Describe("RedisInstance placement", func() {
	getPDB := func(name string) (*policyv1.PodDisruptionBudget, error) {
		pdb := &policyv1.PodDisruptionBudget{}
		return pdb, k8sClient.Get(ctx, objectKey(name), pdb)
	}

	It("defaults to keeping the instances on different nodes", func() {
		instance := newRedisInstance("affinity-default", 1, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()})
		Expect(k8sClient.Create(ctx, instance)).To(Succeed())
		DeferCleanup(func() { deleteInstance("affinity-default") })
		Expect(instance.Spec.Affinity.PodAntiAffinityType).To(Equal(redisv1.PodAntiAffinityRequired))
	})

	Context("with three instances", Ordered, func() {
		const name = "pdb"
		BeforeAll(func() {
			Expect(k8sClient.Create(ctx, newRedisInstance(name, 3, redisv1.StorageSpec{
				VolumeClaimTemplate: volumeClaimTemplate(),
			}))).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
		})
		AfterAll(func() { deleteInstance(name) })

		It("lets a drain take the primary only after a switchover, and one replica at a time", func() {
			primary, err := getPDB(name + "-primary")
			Expect(err).NotTo(HaveOccurred())
			Expect(primary.Spec.Selector.MatchLabels).To(Equal(map[string]string{
				"redis.operator.com/instance": name, "redis.operator.com/role": "primary"}))
			Expect(primary.Spec.MinAvailable).To(HaveValue(Equal(intstr.FromInt32(1))))
			Expect(primary.Spec.UnhealthyPodEvictionPolicy).To(HaveValue(Equal(policyv1.AlwaysAllow)))

			replicas, err := getPDB(name + "-replica")
			Expect(err).NotTo(HaveOccurred())
			Expect(replicas.Spec.Selector.MatchLabels).To(HaveKeyWithValue("redis.operator.com/role", "replica"))
			// Every replica but one: of two, one may go.
			Expect(replicas.Spec.MinAvailable).To(HaveValue(Equal(intstr.FromInt32(1))))
			Expect(replicas.Spec.MaxUnavailable).To(BeNil(), "maxUnavailable needs a scale subresource, which a RedisInstance has not")
		})

		It("removes the budgets with the last replica, so a drain is not blocked for good", func() {
			scale(name, 1)
			for _, pdb := range []string{name + "-primary", name + "-replica"} {
				_, err := getPDB(pdb)
				Expect(errors.IsNotFound(err)).To(BeTrue(), "%s is still there", pdb)
			}
		})
	})

	It("reconciles every RedisInstance when a node is cordoned, and none for other node events", func() {
		Expect(k8sClient.Create(ctx, newRedisInstance("cordon-map", 1, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()}))).To(Succeed())
		DeferCleanup(func() { deleteInstance("cordon-map") })
		r := &RedisInstanceReconciler{Client: k8sClient, Recorder: &events.FakeRecorder{}}

		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}
		Expect(r.instancesOnCordonedNode(ctx, node)).To(BeEmpty())
		node.Spec.Unschedulable = true
		Expect(r.instancesOnCordonedNode(ctx, node)).To(ContainElement(HaveField("NamespacedName", objectKey("cordon-map"))))
	})
})
