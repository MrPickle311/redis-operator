package controller

import (
	"slices"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/events"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

// testOperatorNamespace is where the tests pretend the operator runs.
const testOperatorNamespace = "redis-operator-system"

var _ = Describe("NetworkPolicy", Ordered, func() {
	const name = "np-db"
	backend := networkingv1.NetworkPolicyPeer{NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"team": "backend"}}}

	policy := func() (*networkingv1.NetworkPolicy, error) {
		p := &networkingv1.NetworkPolicy{}
		return p, k8sClient.Get(ctx, objectKey(name), p)
	}

	BeforeAll(func() {
		instance := newRedisInstance(name, 2, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()})
		instance.Spec.NetworkPolicy.AllowFrom = []networkingv1.NetworkPolicyPeer{backend}
		Expect(k8sClient.Create(ctx, instance)).To(Succeed())
		DeferCleanup(k8sClient.Delete, ctx, instance)
		Expect(reconcileOnce(objectKey(name))).To(Succeed())
	})

	It("lets in the clients of allowFrom, the instances, the operator and the probes", func() {
		p, err := policy()
		Expect(err).NotTo(HaveOccurred())
		Expect(metav1.GetControllerOf(p).Name).To(Equal(name))
		instances := metav1.LabelSelector{MatchLabels: map[string]string{labelInstance: name}}
		Expect(p.Spec.PodSelector).To(Equal(instances))
		Expect(p.Spec.PolicyTypes).To(Equal([]networkingv1.PolicyType{networkingv1.PolicyTypeIngress}), "no egress: the IM needs DNS and the API server")

		port := func(number int) []networkingv1.NetworkPolicyPort {
			p, tcp := intstr.FromInt(number), corev1.ProtocolTCP
			return []networkingv1.NetworkPolicyPort{{Protocol: &tcp, Port: &p}}
		}
		Expect(p.Spec.Ingress).To(Equal([]networkingv1.NetworkPolicyIngressRule{
			{Ports: port(6379), From: []networkingv1.NetworkPolicyPeer{backend}},
			{Ports: port(6379), From: []networkingv1.NetworkPolicyPeer{{PodSelector: &instances}}},
			{Ports: port(8000), From: []networkingv1.NetworkPolicyPeer{{
				NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": testOperatorNamespace}},
				PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
					"control-plane": "controller-manager", "app.kubernetes.io/name": "redis-operator"}},
			}}},
			{Ports: port(8001)},
		}))
	})

	It("does not update the policy when nothing changed", func() {
		before, err := policy()
		Expect(err).NotTo(HaveOccurred())
		Expect(reconcileOnce(objectKey(name))).To(Succeed())
		after, err := policy()
		Expect(err).NotTo(HaveOccurred())
		Expect(after.ResourceVersion).To(Equal(before.ResourceVersion))
	})

	It("warns once about a policy of the user that selects the instances, until it is gone", func() {
		condition := func() *metav1.Condition {
			instance := &redisv1.RedisInstance{}
			Expect(k8sClient.Get(ctx, objectKey(name), instance)).To(Succeed())
			return meta.FindStatusCondition(instance.Status.Conditions, redisv1.ConditionNetworkPolicyReady)
		}
		Expect(condition().Status).To(Equal(metav1.ConditionTrue))

		foreign := &networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "allow-all-to-primary", Namespace: testNamespace},
			Spec: networkingv1.NetworkPolicySpec{
				PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{labelRole: rolePrimary}},
				Ingress:     []networkingv1.NetworkPolicyIngressRule{{}},
			},
		}
		Expect(k8sClient.Create(ctx, foreign)).To(Succeed())
		recorder := events.NewFakeRecorder(10)
		Expect(reconcileWith(recorder, objectKey(name))).To(Succeed())
		Expect(reconcileWith(recorder, objectKey(name))).To(Succeed())

		Expect(condition().Reason).To(Equal("ForeignPolicy"))
		Expect(condition().Message).To(ContainSubstring("allow-all-to-primary"))
		Expect(recorder.Events).To(HaveLen(1), "one Event, not one per reconcile")
		Expect(<-recorder.Events).To(ContainSubstring("ForeignNetworkPolicy"))

		Expect(k8sClient.Delete(ctx, foreign)).To(Succeed())
		Expect(reconcileOnce(objectKey(name))).To(Succeed())
		Expect(condition().Status).To(Equal(metav1.ConditionTrue))
	})

	It("deletes the policy once allowFrom is gone", func() {
		instance := &redisv1.RedisInstance{}
		Expect(k8sClient.Get(ctx, objectKey(name), instance)).To(Succeed())
		instance.Spec.NetworkPolicy.AllowFrom = nil
		Expect(k8sClient.Update(ctx, instance)).To(Succeed())
		Expect(reconcileOnce(objectKey(name))).To(Succeed())

		_, err := policy()
		Expect(errors.IsNotFound(err)).To(BeTrue(), "the policy is still there: %v", err)
	})
})

func TestForeignPolicies(t *testing.T) {
	instance := &redisv1.RedisInstance{ObjectMeta: metav1.ObjectMeta{Name: "cache", UID: "cache-uid"}}
	isController := true
	policy := func(name string, selector map[string]string, types ...networkingv1.PolicyType) networkingv1.NetworkPolicy {
		return networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: networkingv1.NetworkPolicySpec{PodSelector: metav1.LabelSelector{MatchLabels: selector}, PolicyTypes: types,
				Ingress: []networkingv1.NetworkPolicyIngressRule{{}}},
		}
	}
	denyAll := policy("deny-all", nil)
	denyAll.Spec.Ingress = nil
	own := policy("cache", map[string]string{labelInstance: "cache"})
	own.OwnerReferences = []metav1.OwnerReference{{UID: "cache-uid", Controller: &isController}}

	got := foreignPolicies(instance, []networkingv1.NetworkPolicy{
		own,
		policy("everything", nil),
		policy("replicas", map[string]string{labelRole: roleReplica}),
		policy("other-db", map[string]string{labelInstance: "other"}),
		policy("egress-only", nil, networkingv1.PolicyTypeEgress),
		denyAll,
		policy("cache", map[string]string{labelInstance: "cache"}), // the name, but not owned
	})
	want := []string{"cache", "everything", "replicas"}
	if !slices.Equal(got, want) {
		t.Errorf("foreignPolicies() = %v, want %v", got, want)
	}
}
