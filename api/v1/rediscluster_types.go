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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ConditionSlotsCovered is True when masters serve all 16384 slots. When
// False, its message tells how many have no master: keys in them fail with
// CLUSTERDOWN.
const ConditionSlotsCovered = "SlotsCovered"

// RedisClusterSpec defines the desired state of RedisCluster: a Redis
// Cluster of Shards, each a RedisInstance <name>-<n> of InstancesPerShard
// nodes, a master and its replicas. Redis Cluster fails over by itself.
// +kubebuilder:validation:XValidation:rule="has(self.storage.volumeClaimTemplate) && !has(self.storage.existingClaims)",message="a RedisCluster needs storage.volumeClaimTemplate and takes no storage.existingClaims"
type RedisClusterSpec struct {
	// Shards is the number of masters the 16384 slots are spread over. A
	// replica takes over from a failed master only with the votes of most
	// masters, so a cluster below 3 could never fail over.
	// +kubebuilder:validation:Minimum=3
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="shards is immutable: changing it needs moving slots between shards"
	// +kubebuilder:default=3
	// +optional
	Shards int32 `json:"shards,omitempty"`

	// InstancesPerShard is the master of a shard and its replicas. With 1 a
	// shard has no replica to take over.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=2
	// +optional
	InstancesPerShard int32 `json:"instancesPerShard,omitempty"`

	Image   string      `json:"image"`
	Storage StorageSpec `json:"storage"`

	// ACL configures the users of Redis, the same on every shard.
	// +kubebuilder:default={}
	// +optional
	ACL ACLSpec `json:"acl,omitzero"`

	// Resources of the redis container of every node.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitzero"`

	// Redis configures redis-server on every node.
	// +kubebuilder:default={}
	// +optional
	Redis RedisSpec `json:"redis,omitzero"`

	// Affinity keeps the nodes of a shard on different nodes of Kubernetes.
	// +kubebuilder:default={}
	// +optional
	Affinity AffinitySpec `json:"affinity,omitzero"`

	// TLS encrypts the connections of the clients, of the replication and of
	// the cluster bus. One CA, <name>-ca, signs the certificates of every
	// shard; a ServerSecretRef must be valid for every node.
	// +kubebuilder:default={}
	// +optional
	TLS TLSSpec `json:"tls,omitzero"`

	// NetworkPolicy lets the operator isolate the nodes on the network.
	// +kubebuilder:default={}
	// +optional
	NetworkPolicy NetworkPolicySpec `json:"networkPolicy,omitzero"`
}

// RedisClusterStatus defines the observed state of RedisCluster.
type RedisClusterStatus struct {
	// ClusterState is cluster_state of CLUSTER INFO: ok, or fail when some
	// slot has no master.
	// +optional
	ClusterState string `json:"clusterState,omitempty"`

	// Shards tells which node is the master of every shard.
	// +listType=map
	// +listMapKey=name
	// +optional
	Shards []ShardStatus `json:"shards,omitempty"`

	// Conditions of the cluster: Ready and SlotsCovered.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// ShardStatus is one shard of a RedisCluster.
type ShardStatus struct {
	// Name of the RedisInstance of the shard.
	Name string `json:"name"`
	// Primary is the Pod of the master.
	// +optional
	Primary string `json:"primary,omitempty"`
	// Slots the master serves, e.g. "0-5460".
	// +optional
	Slots string `json:"slots,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Shards",type=integer,JSONPath=".spec.shards"
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=".status.clusterState"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].reason"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// RedisCluster is the Schema for the redisclusters API
type RedisCluster struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of RedisCluster
	// +required
	Spec RedisClusterSpec `json:"spec"`

	// status defines the observed state of RedisCluster
	// +optional
	Status RedisClusterStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// RedisClusterList contains a list of RedisCluster
type RedisClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []RedisCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &RedisCluster{}, &RedisClusterList{})
		return nil
	})
}
