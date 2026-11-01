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
	"crypto/x509"
	"encoding/pem"
	"reflect"
	"slices"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/instancemanager"
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

func reconcileCluster(name string) error {
	r := &RedisClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: objectKey(name)})
	return err
}

func getCluster(name string) *redisv1.RedisCluster {
	var cluster redisv1.RedisCluster
	Expect(k8sClient.Get(ctx, objectKey(name), &cluster)).To(Succeed())
	return &cluster
}

var _ = Describe("RedisCluster controller", Ordered, func() {
	// envtest has no garbage collector: the shards go with the test.
	const name = "rc"

	BeforeAll(func() {
		cluster := newRedisCluster(name)
		cluster.Spec.InstancesPerShard = 2
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		Expect(reconcileCluster(name)).To(Succeed())
	})

	AfterAll(func() {
		for _, shard := range []string{"rc-0", "rc-1", "rc-2"} {
			deleteInstance(shard)
		}
		Expect(k8sClient.Delete(ctx, getCluster(name))).To(Succeed())
	})

	It("creates a RedisInstance per shard, owned by the cluster, with its spec", func() {
		cluster := getCluster(name)
		for _, shard := range []string{"rc-0", "rc-1", "rc-2"} {
			instance := getInstance(shard)
			Expect(clusterOf(instance)).To(Equal(name))
			Expect(instance.Spec.Instances).To(Equal(int32(2)))
			Expect(instance.Spec.Image).To(Equal(cluster.Spec.Image))
			Expect(instance.Spec.TLS.TLSEnabled()).To(BeTrue())
		}
	})

	It("creates the seed Service over the nodes of every shard and the CA of the cluster", func() {
		var seed corev1.Service
		Expect(k8sClient.Get(ctx, objectKey(name), &seed)).To(Succeed())
		Expect(seed.Spec.Selector).To(Equal(map[string]string{labelCluster: name}))
		var ca corev1.Secret
		Expect(k8sClient.Get(ctx, objectKey("rc-ca"), &ca)).To(Succeed())
		Expect(ca.Data).To(HaveKey(caKeyKey))
	})

	It("reports the slots uncovered while no node answers", func() {
		cluster := getCluster(name)
		covered := meta.FindStatusCondition(cluster.Status.Conditions, redisv1.ConditionSlotsCovered)
		Expect(covered).NotTo(BeNil())
		Expect(covered.Message).To(Equal("16384 of 16384 slots have no master"))
		Expect(meta.IsStatusConditionFalse(cluster.Status.Conditions, redisv1.ConditionReady)).To(BeTrue())
		Expect(cluster.Status.Shards).To(HaveLen(3))
	})

	It("passes a change of instancesPerShard on to every shard", func() {
		cluster := getCluster(name)
		cluster.Spec.InstancesPerShard = 3
		Expect(k8sClient.Update(ctx, cluster)).To(Succeed())
		Expect(reconcileCluster(name)).To(Succeed())
		Expect(getInstance("rc-1").Spec.Instances).To(Equal(int32(3)))
	})

	It("runs a shard as a node of the cluster, with a certificate from the CA of the cluster", func() {
		Expect(reconcileOnce(objectKey("rc-0"))).To(Succeed())

		pod := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, objectKey("rc-0-0"), pod)).To(Succeed())
		Expect(pod.Labels).To(HaveKeyWithValue(labelCluster, name))
		Expect(pod.Spec.Containers[0].Command).To(ContainElement("--cluster"))
		Expect(pod.Spec.Containers[0].Ports).To(ContainElement(HaveField("ContainerPort", int32(clusterBusPort))))

		Expect(k8sClient.Get(ctx, objectKey("rc-0-rw"), &corev1.Service{})).NotTo(Succeed(), "a shard has no client Services")
		Expect(k8sClient.Get(ctx, objectKey("rc-0-hl"), &corev1.Service{})).To(Succeed())

		var tlsSecret corev1.Secret
		Expect(k8sClient.Get(ctx, objectKey("rc-0-server-tls"), &tlsSecret)).To(Succeed())
		block, _ := pem.Decode(tlsSecret.Data[corev1.TLSCertKey])
		cert, err := x509.ParseCertificate(block.Bytes)
		Expect(err).NotTo(HaveOccurred())
		Expect(cert.DNSNames).To(ContainElements("rc.default.svc", "*.rc-0-hl.default.svc"))
		var ca corev1.Secret
		Expect(k8sClient.Get(ctx, objectKey("rc-ca"), &ca)).To(Succeed())
		Expect(tlsSecret.Data[caCertKey]).To(Equal(ca.Data[caCertKey]))
	})

	Context("with a RedisUser of the cluster", Ordered, func() {
		shards := []string{"rc-0", "rc-1", "rc-2"}
		reconcileUser := func() {
			r := &RedisUserReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: &events.FakeRecorder{}}
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: objectKey("rc-app")})
			Expect(err).NotTo(HaveOccurred())
		}
		userReason := func() string {
			var user redisv1.RedisUser
			Expect(k8sClient.Get(ctx, objectKey("rc-app"), &user)).To(Succeed())
			return meta.FindStatusCondition(user.Status.Conditions, "Ready").Reason
		}
		acl := func(shard string) string {
			var secret corev1.Secret
			Expect(k8sClient.Get(ctx, objectKey(instancemanager.ACLSecretName(shard)), &secret)).To(Succeed())
			return string(secret.Data[instancemanager.ACLSecretKey])
		}
		// reconcileShards reconciles every shard and records what their
		// Instance Managers would report: every node applied <shard>-acl.
		reconcileShards := func() {
			for _, shard := range shards {
				Expect(reconcileOnce(objectKey(shard))).To(Succeed())
				instance := getInstance(shard)
				instance.Status.ACL = &redisv1.ACLStatus{Digest: instancemanager.ACLDigest([]byte(acl(shard))), Applied: true}
				Expect(k8sClient.Status().Update(ctx, instance)).To(Succeed())
			}
		}

		It("gives the user to every shard", func() {
			Expect(k8sClient.Create(ctx, &redisv1.RedisUser{
				ObjectMeta: metav1.ObjectMeta{Name: "rc-app", Namespace: testNamespace},
				Spec: redisv1.RedisUserSpec{
					TargetRef:      redisv1.TargetRef{Kind: "RedisCluster", Name: name},
					Username:       "app",
					Authentication: redisv1.RedisUserAuthentication{Generate: true},
					ACL:            redisv1.RedisUserACL{Rules: []redisv1.ACLRule{"~app:*", "+@read"}},
				},
			})).To(Succeed())
			reconcileUser()
			Expect(userReason()).To(Equal("Pending"))

			reconcileShards()
			for _, shard := range shards {
				Expect(acl(shard)).To(ContainSubstring("user app "), "shard %s", shard)
			}
			reconcileUser()
			Expect(userReason()).To(Equal("Applied"))
		})

		It("keeps a deleted user until no shard has it any more", func() {
			var user redisv1.RedisUser
			Expect(k8sClient.Get(ctx, objectKey("rc-app"), &user)).To(Succeed())
			Expect(k8sClient.Delete(ctx, &user)).To(Succeed())
			reconcileUser()
			Expect(k8sClient.Get(ctx, objectKey("rc-app"), &user)).To(Succeed(), "released before the shards dropped the user")

			reconcileShards()
			reconcileUser()
			Expect(errors.IsNotFound(k8sClient.Get(ctx, objectKey("rc-app"), &user))).To(BeTrue())
		})
	})

	It("lets the nodes of the cluster in on the cluster bus of a shard", func() {
		cluster := getCluster(name)
		cluster.Spec.NetworkPolicy.AllowFrom = []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{}}}
		Expect(k8sClient.Update(ctx, cluster)).To(Succeed())
		Expect(reconcileCluster(name)).To(Succeed())
		Expect(reconcileOnce(objectKey("rc-1"))).To(Succeed())

		var policy networkingv1.NetworkPolicy
		Expect(k8sClient.Get(ctx, objectKey("rc-1"), &policy)).To(Succeed())
		bus, tcp := intstr.FromInt(clusterBusPort), corev1.ProtocolTCP
		Expect(policy.Spec.Ingress).To(ContainElement(networkingv1.NetworkPolicyIngressRule{
			Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcp, Port: &bus}},
			From:  []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{labelCluster: name}}}},
		}))
	})

	It("never adopts a RedisInstance of its own", func() {
		Expect(k8sClient.Create(ctx, newRedisInstance("other-0", 1, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()}))).To(Succeed())
		DeferCleanup(deleteInstance, "other-0")
		cluster := newRedisCluster("other")
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		DeferCleanup(k8sClient.Delete, ctx, cluster)

		Expect(reconcileCluster("other")).To(MatchError(ContainSubstring("RedisInstance other-0 exists and is not a shard of RedisCluster other")))
		Expect(clusterOf(getInstance("other-0"))).To(BeEmpty())
	})
})

// clusterNodeStatus returns the status of node id that knows the nodes known,
// or only itself for nil.
func clusterNodeStatus(id string, known []string, master string, slots ...string) *instancemanager.ClusterStatus {
	if known == nil {
		known = []string{id}
	}
	return &instancemanager.ClusterStatus{NodeID: id, Nodes: known, MasterID: master, Slots: slots, State: "fail"}
}

func TestPlanCluster(t *testing.T) {
	// Two shards of two nodes: a-0, a-1 in shard 0, b-0, b-1 in shard 1.
	nodes := func(statuses ...*instancemanager.ClusterStatus) []clusterNode {
		var nodes []clusterNode
		for i, status := range statuses {
			pod := []string{"a-0", "a-1", "b-0", "b-1"}[i]
			nodes = append(nodes, clusterNode{shard: int32(i / 2), pod: pod, host: pod + ".svc", status: status})
		}
		return nodes
	}
	all := []string{"a0", "a1", "b0", "b1"}
	withOld := append(slices.Clone(all), "old")
	tests := []struct {
		name     string
		nodes    []clusterNode
		assigned map[int32]bool
		want     []clusterAction
	}{
		{
			name:  "waits for every node",
			nodes: nodes(clusterNodeStatus("a0", nil, ""), nil, clusterNodeStatus("b0", nil, ""), clusterNodeStatus("b1", nil, "")),
		},
		{
			name:  "the first node meets the others",
			nodes: nodes(clusterNodeStatus("a0", nil, ""), clusterNodeStatus("a1", nil, ""), clusterNodeStatus("b0", all, ""), clusterNodeStatus("b1", nil, "")),
			want: []clusterAction{
				{"a-0.svc", "meet", instancemanager.ClusterRequest{Host: "a-1.svc"}},
				{"a-0.svc", "meet", instancemanager.ClusterRequest{Host: "b-0.svc"}},
				{"a-0.svc", "meet", instancemanager.ClusterRequest{Host: "b-1.svc"}},
			},
		},
		{
			name:  "the first node of every shard takes its slots",
			nodes: nodes(clusterNodeStatus("a0", all, ""), clusterNodeStatus("a1", all, ""), clusterNodeStatus("b0", all, ""), clusterNodeStatus("b1", all, "")),
			want: []clusterAction{
				{"a-0.svc", "addslots", instancemanager.ClusterRequest{Slots: "0-8191"}},
				{"b-0.svc", "addslots", instancemanager.ClusterRequest{Slots: "8192-16383"}},
			},
		},
		{
			name: "the other nodes replicate the master of their shard, wherever it is",
			nodes: nodes(clusterNodeStatus("a0", all, ""), clusterNodeStatus("a1", all, "", "0-8191"),
				clusterNodeStatus("b0", all, "", "8192-16383"), clusterNodeStatus("b1", all, "b0")),
			want: []clusterAction{
				{"a-0.svc", "replicate", instancemanager.ClusterRequest{NodeID: "a1"}},
			},
		},
		{
			name:     "gives no slots again to a shard that had them",
			nodes:    nodes(clusterNodeStatus("a0", all, ""), clusterNodeStatus("a1", all, ""), clusterNodeStatus("b0", all, "", "8192-16383"), clusterNodeStatus("b1", all, "b0")),
			assigned: map[int32]bool{0: true, 1: true},
		},
		{
			name: "every node forgets an ID no node has, e.g. of a lost volume",
			nodes: nodes(clusterNodeStatus("a0", withOld, "", "0-8191"), clusterNodeStatus("a1", all, "a0"),
				clusterNodeStatus("b0", withOld, "", "8192-16383"), clusterNodeStatus("b1", all, "b0")),
			want: []clusterAction{
				{"a-0.svc", "forget", instancemanager.ClusterRequest{NodeID: "old"}},
				{"b-0.svc", "forget", instancemanager.ClusterRequest{NodeID: "old"}},
			},
		},
		{
			name: "keeps an old master, which holds the slots until its replica takes over",
			nodes: nodes(clusterNodeStatus("a0", withOld, ""), clusterNodeStatus("a1", withOld, "old"),
				clusterNodeStatus("b0", withOld, "", "8192-16383"), clusterNodeStatus("b1", withOld, "b0")),
			assigned: map[int32]bool{0: true, 1: true},
		},
		{
			name: "keeps the ID of a node whose Pod is down",
			nodes: nodes(clusterNodeStatus("a0", withOld, "", "0-8191"), clusterNodeStatus("a1", withOld, "a0"),
				clusterNodeStatus("b0", withOld, "", "8192-16383")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := planCluster(tt.nodes, 2, 2, tt.assigned); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("planCluster() =\n%+v\nwant\n%+v", got, tt.want)
			}
		})
	}
}

func TestSlotRangeCoversEverySlotOnce(t *testing.T) {
	var ranges []string
	for shard := range int32(5) {
		ranges = append(ranges, slotRange(shard, 5))
	}
	if ranges[0] != "0-3275" || ranges[4] != "13107-16383" || countSlots(ranges) != clusterSlots {
		t.Errorf("slotRange() = %v", ranges)
	}
	if countSlots([]string{"0-5460", "12000"}) != 5462 {
		t.Error("countSlots() miscounts a single slot")
	}
}
