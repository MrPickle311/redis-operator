package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

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
