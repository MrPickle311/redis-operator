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
	"fmt"
	"reflect"
	"slices"
	"strconv"
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

	It("allows a change of shards, but not while slots move", func() {
		cluster := newRedisCluster("rc-reshard")
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		DeferCleanup(k8sClient.Delete, ctx, cluster)

		cluster.Spec.Shards = 4
		Expect(k8sClient.Update(ctx, cluster)).To(Succeed())
		cluster.Status.SlotMigration = &redisv1.SlotMigration{SlotsToMove: 4096}
		Expect(k8sClient.Status().Update(ctx, cluster)).To(Succeed())

		cluster.Spec.Shards = 5
		expectInvalid(k8sClient.Update(ctx, cluster), "spec.shards cannot change while slots move between shards")
		cluster.Spec.Shards, cluster.Spec.InstancesPerShard = 4, 3
		Expect(k8sClient.Update(ctx, cluster)).To(Succeed(), "the other fields still change")
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

var _ = Describe("RedisCluster changing shards", func() {
	It("keeps a removed shard while its nodes do not answer, and lists it in the status", func() {
		cluster := newRedisCluster("rcd")
		cluster.Spec.Shards = 4
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		DeferCleanup(func() {
			for _, shard := range []string{"rcd-0", "rcd-1", "rcd-2", "rcd-3"} {
				deleteInstance(shard)
			}
			Expect(k8sClient.Delete(ctx, getCluster("rcd"))).To(Succeed())
		})
		Expect(reconcileCluster("rcd")).To(Succeed())

		cluster = getCluster("rcd")
		cluster.Spec.Shards = 3
		Expect(k8sClient.Update(ctx, cluster)).To(Succeed())
		Expect(reconcileCluster("rcd")).To(Succeed())
		Expect(getInstance("rcd-3").DeletionTimestamp).To(BeNil(), "its slots may still be on it")
		Expect(getCluster("rcd").Status.Shards).To(HaveLen(4))
	})

	It("starts no failover of a shard while slots move", func() {
		cluster := newRedisCluster("rcf")
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		DeferCleanup(func() {
			for _, shard := range []string{"rcf-0", "rcf-1", "rcf-2"} {
				deleteInstance(shard)
			}
			Expect(k8sClient.Delete(ctx, getCluster("rcf"))).To(Succeed())
		})
		Expect(reconcileCluster("rcf")).To(Succeed())
		cluster = getCluster("rcf")
		cluster.Status.SlotMigration = &redisv1.SlotMigration{SlotsToMove: 10}
		Expect(k8sClient.Status().Update(ctx, cluster)).To(Succeed())

		// Without an Instance Manager: reaching it would panic.
		r := &RedisInstanceReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: &events.FakeRecorder{}}
		shard := getInstance("rcf-0")
		Expect(r.startClusterFailover(ctx, shard, "rcf-0-1", "test")).To(Succeed())
		Expect(getInstance("rcf-0").Status.TargetPrimary).To(BeEmpty())
	})
})

func reconcileCluster(name string) error {
	r := &RedisClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: events.NewFakeRecorder(100)}
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
		var auth corev1.Secret
		Expect(k8sClient.Get(ctx, objectKey("rc-internal-auth"), &auth)).To(Succeed())
		Expect(metav1.GetControllerOf(&auth).Name).To(Equal(name))
		Expect(auth.Data).To(HaveKey(instancemanager.OperatorPasswordKey))
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
		Expect(pod.Spec.Volumes).To(ContainElement(HaveField("VolumeSource.Secret.SecretName", "rc-internal-auth")),
			"every shard logs in to the others for MIGRATE with the same password")

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

	It("lets the nodes of the cluster in on the cluster bus and, for MIGRATE, on the port of Redis of a shard", func() {
		cluster := getCluster(name)
		cluster.Spec.NetworkPolicy.AllowFrom = []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{}}}
		Expect(k8sClient.Update(ctx, cluster)).To(Succeed())
		Expect(reconcileCluster(name)).To(Succeed())
		Expect(reconcileOnce(objectKey("rc-1"))).To(Succeed())

		var policy networkingv1.NetworkPolicy
		Expect(k8sClient.Get(ctx, objectKey("rc-1"), &policy)).To(Succeed())
		bus, redis, tcp := intstr.FromInt(clusterBusPort), intstr.FromInt(redisPort), corev1.ProtocolTCP
		Expect(policy.Spec.Ingress).To(ContainElement(networkingv1.NetworkPolicyIngressRule{
			Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcp, Port: &bus}, {Protocol: &tcp, Port: &redis}},
			From:  []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{labelCluster: name}}}},
		}))
	})

	It("removes a node of a smaller shard only once it is not the master", func() {
		instance := getInstance("rc-0")
		instance.Status.CurrentPrimary = "rc-0-2"
		Expect(k8sClient.Status().Update(ctx, instance)).To(Succeed())
		cluster := getCluster(name)
		cluster.Spec.InstancesPerShard = 2
		Expect(k8sClient.Update(ctx, cluster)).To(Succeed())
		Expect(reconcileCluster(name)).To(Succeed())

		recorder := events.NewFakeRecorder(20)
		Expect(reconcileWith(recorder, objectKey("rc-0"))).To(Succeed())
		Expect(existingPodNames("rc-0", 3)).To(ConsistOf("rc-0-0", "rc-0-1", "rc-0-2"), "the master went before a failover")
		Expect(recorder.Events).NotTo(Receive(ContainSubstring("ScaleDownBlocked")), "a shard fails over on its own")

		// What observeShardPrimary records once a remaining replica took over.
		instance = getInstance("rc-0")
		instance.Status.CurrentPrimary = "rc-0-1"
		Expect(k8sClient.Status().Update(ctx, instance)).To(Succeed())
		Expect(reconcileOnce(objectKey("rc-0"))).To(Succeed())
		Expect(existingPodNames("rc-0", 3)).To(ConsistOf("rc-0-0", "rc-0-1"))
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
		nodes := make([]clusterNode, 0, len(statuses))
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
			name: "a shard added later takes no slots the others serve, its first node is the master",
			nodes: nodes(clusterNodeStatus("a0", all, "", "0-16383"), clusterNodeStatus("a1", all, "a0"),
				clusterNodeStatus("b0", all, ""), clusterNodeStatus("b1", all, "")),
			assigned: map[int32]bool{0: true},
			want: []clusterAction{
				{"b-1.svc", "replicate", instancemanager.ClusterRequest{NodeID: "b0"}},
			},
		},
		{
			name: "meets the others again when a node cannot reach one that runs, e.g. after every Pod restarted",
			nodes: func() []clusterNode {
				n := nodes(clusterNodeStatus("a0", all, "", "0-8191"), clusterNodeStatus("a1", all, "a0"),
					clusterNodeStatus("b0", all, "", "8192-16383"), clusterNodeStatus("b1", all, "b0"))
				n[1].status.Unreachable = []string{"a0", "b0"}
				return n
			}(),
			assigned: map[int32]bool{0: true, 1: true},
			want: []clusterAction{
				{"a-0.svc", "meet", instancemanager.ClusterRequest{Host: "a-1.svc"}},
				{"a-0.svc", "meet", instancemanager.ClusterRequest{Host: "b-0.svc"}},
				{"a-0.svc", "meet", instancemanager.ClusterRequest{Host: "b-1.svc"}},
			},
		},
		{
			name: "a replica replicates its master again when it still uses the old address of the master",
			nodes: func() []clusterNode {
				n := nodes(clusterNodeStatus("a0", all, "", "0-8191"), clusterNodeStatus("a1", all, "a0"),
					clusterNodeStatus("b0", all, "", "8192-16383"), clusterNodeStatus("b1", all, "b0"))
				n[3].status.StaleMaster = true
				return n
			}(),
			assigned: map[int32]bool{0: true, 1: true},
			want: []clusterAction{
				{"b-1.svc", "replicate", instancemanager.ClusterRequest{NodeID: "b0"}},
			},
		},
		{
			name: "forgets, not meets, a node that cannot be reached because it is gone",
			nodes: func() []clusterNode {
				n := nodes(clusterNodeStatus("a0", withOld, "", "0-8191"), clusterNodeStatus("a1", all, "a0"),
					clusterNodeStatus("b0", withOld, "", "8192-16383"), clusterNodeStatus("b1", all, "b0"))
				n[0].status.Unreachable = []string{"old"}
				return n
			}(),
			assigned: map[int32]bool{0: true, 1: true},
			want: []clusterAction{
				{"a-0.svc", "forget", instancemanager.ClusterRequest{NodeID: "old"}},
				{"b-0.svc", "forget", instancemanager.ClusterRequest{NodeID: "old"}},
			},
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
	ranges := make([]string, 0, 5)
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

func TestNextSlotMove(t *testing.T) {
	// One node per shard: s0, s1, ... with the slots and flags given.
	nodes := func(statuses ...*instancemanager.ClusterStatus) []clusterNode {
		nodes := make([]clusterNode, 0, len(statuses))
		for i, status := range statuses {
			pod := fmt.Sprintf("s%d", i)
			nodes = append(nodes, clusterNode{shard: int32(i), pod: pod, host: pod + ".svc", status: status})
		}
		return nodes
	}
	flags := func(status *instancemanager.ClusterStatus, migrating, importing map[int]string) *instancemanager.ClusterStatus {
		status.Migrating, status.Importing = migrating, importing
		return status
	}
	three := []string{slotRange(0, 3), slotRange(1, 3), slotRange(2, 3)}
	four := []string{slotRange(0, 4), slotRange(1, 4), slotRange(2, 4), slotRange(3, 4)}
	tests := []struct {
		name     string
		nodes    []clusterNode
		shards   int32
		assigned map[int32]bool
		want     string // "slot from→to", the pods
	}{
		{
			name:   "moves nothing when every shard has its share",
			nodes:  nodes(clusterNodeStatus("n0", nil, "", three[0]), clusterNodeStatus("n1", nil, "", three[1]), clusterNodeStatus("n2", nil, "", three[2])),
			shards: 3,
		},
		{
			name: "a new shard gets the highest slot of the first overfull shard",
			nodes: nodes(clusterNodeStatus("n0", nil, "", three[0]), clusterNodeStatus("n1", nil, "", three[1]),
				clusterNodeStatus("n2", nil, "", three[2]), clusterNodeStatus("n3", nil, "")),
			shards: 4,
			want:   "5460 s0→s3",
		},
		{
			name: "the first overfull shard keeps its share",
			nodes: nodes(clusterNodeStatus("n0", nil, "", "0-4095"), clusterNodeStatus("n1", nil, "", three[1]),
				clusterNodeStatus("n2", nil, "", three[2]), clusterNodeStatus("n3", nil, "", "4096-5460")),
			shards: 4,
			want:   "10921 s1→s3",
		},
		{
			name: "a removed shard gives its slots to the first underfull one",
			nodes: nodes(clusterNodeStatus("n0", nil, "", four[0]), clusterNodeStatus("n1", nil, "", four[1]),
				clusterNodeStatus("n2", nil, "", four[2]), clusterNodeStatus("n3", nil, "", four[3])),
			shards: 3,
			want:   "16383 s3→s0",
		},
		{
			name: "a slot on the way goes first",
			nodes: nodes(flags(clusterNodeStatus("n0", nil, "", three[0]), map[int]string{100: "n3"}, nil), clusterNodeStatus("n1", nil, "", three[1]),
				clusterNodeStatus("n2", nil, "", three[2]), flags(clusterNodeStatus("n3", nil, ""), nil, map[int]string{100: "n0"})),
			shards: 4,
			want:   "100 s0→s3",
		},
		{
			name: "a slot on the way goes on from the master that took over the source",
			nodes: nodes(clusterNodeStatus("n0", nil, "", three[0]), clusterNodeStatus("n1", nil, "", three[1]),
				clusterNodeStatus("n2", nil, "", three[2]), flags(clusterNodeStatus("n3", nil, ""), nil, map[int]string{100: "gone"})),
			shards: 4,
			want:   "100 s0→s3",
		},
		{
			name: "a slot on the way goes to the master that took over the target",
			nodes: append(nodes(flags(clusterNodeStatus("n0", nil, "", three[0]), map[int]string{100: "n3"}, nil), clusterNodeStatus("n1", nil, "", three[1]),
				clusterNodeStatus("n2", nil, "", three[2]), clusterNodeStatus("n3", nil, "n4")),
				clusterNode{shard: 3, pod: "s3b", host: "s3b.svc", status: clusterNodeStatus("n4", nil, "", "200")}),
			shards:   4,
			assigned: map[int32]bool{3: true},
			want:     "100 s0→s3b",
		},
		{
			name: "the source lets go of a slot the target serves already",
			nodes: nodes(flags(clusterNodeStatus("n0", nil, "", "0-99", "101-5460"), map[int]string{100: "n3"}, nil), clusterNodeStatus("n1", nil, "", three[1]),
				clusterNodeStatus("n2", nil, "", three[2]), clusterNodeStatus("n3", nil, "", "100")),
			shards:   4,
			assigned: map[int32]bool{3: true},
			want:     "100 s0→s3",
		},
		{
			name: "stops at a flag naming a node no shard has",
			nodes: nodes(flags(clusterNodeStatus("n0", nil, "", three[0]), map[int]string{100: "gone"}, nil), clusterNodeStatus("n1", nil, "", three[1]),
				clusterNodeStatus("n2", nil, "", three[2]), clusterNodeStatus("n3", nil, "")),
			shards: 4,
		},
		{
			name:   "moves nothing while a slot has no master",
			nodes:  nodes(clusterNodeStatus("n0", nil, "", three[0]), clusterNodeStatus("n1", nil, "", three[1]), clusterNodeStatus("n2", nil, ""), clusterNodeStatus("n3", nil, "")),
			shards: 4,
		},
		{
			name:   "moves nothing while a node does not answer",
			nodes:  nodes(clusterNodeStatus("n0", nil, "", three[0]), clusterNodeStatus("n1", nil, "", three[1]), clusterNodeStatus("n2", nil, "", three[2]), nil),
			shards: 4,
		},
		{
			name: "gives nothing to a shard that lost the slots it had",
			nodes: nodes(clusterNodeStatus("n0", nil, "", three[0]), clusterNodeStatus("n1", nil, "", three[1]),
				clusterNodeStatus("n2", nil, "", three[2]), clusterNodeStatus("n3", nil, "")),
			shards:   4,
			assigned: map[int32]bool{3: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ""
			if move, ok := nextSlotMove(tt.nodes, tt.shards, tt.assigned); ok {
				got = fmt.Sprintf("%d %s→%s", move.slot, move.from.pod, move.to.pod)
			}
			if got != tt.want {
				t.Errorf("nextSlotMove() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSlotMovesAreMinimal(t *testing.T) {
	// Applies nextSlotMove until nothing moves: 3 → 4 → 3 shards.
	reshard := func(owners []int32, shards int32) ([]int32, int) {
		moves := 0
		for {
			count := max(shards, slices.Max(owners)+1)
			nodes := make([]clusterNode, 0, count)
			for shard := range count {
				var slots []string
				for s := 0; s < clusterSlots; s++ {
					if owners[s] == shard {
						first := s
						for s+1 < clusterSlots && owners[s+1] == shard {
							s++
						}
						slots = append(slots, fmt.Sprintf("%d-%d", first, s))
					}
				}
				nodes = append(nodes, clusterNode{shard: shard, pod: strconv.Itoa(int(shard)),
					status: &instancemanager.ClusterStatus{NodeID: strconv.Itoa(int(shard)), Slots: slots}})
			}
			move, ok := nextSlotMove(nodes, shards, nil)
			if left := slotsToMove(nodes, shards); left != 4096-moves {
				t.Fatalf("slotsToMove() = %d after %d moves, want %d", left, moves, 4096-moves)
			}
			if !ok {
				return owners, moves
			}
			owners[move.slot] = move.to.shard
			moves++
		}
	}
	owners := make([]int32, clusterSlots)
	for s := range owners {
		owners[s] = int32(s * 3 / clusterSlots)
	}
	before := slices.Clone(owners)
	owners, moves := reshard(owners, 4)
	if moves != 4096 {
		t.Errorf("3 → 4 moved %d slots, want 4096", moves)
	}
	for s := range owners {
		if owners[s] != before[s] && owners[s] != 3 {
			t.Errorf("3 → 4 moved slot %d between old shards", s)
		}
	}
	if _, moves = reshard(owners, 3); moves != 4096 {
		t.Errorf("4 → 3 moved %d slots, want 4096", moves)
	}
}
