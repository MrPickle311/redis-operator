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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// ConditionMemoryLimited is True when every redis container has a memory
// limit, so maxmemory is set. When False, Redis grows until it is OOMKilled.
const ConditionMemoryLimited = "MemoryLimited"

// ConditionSwitchover is True while the primary moves to another Pod. When
// False, its Reason tells how the last switchover ended: Completed or Failed.
const ConditionSwitchover = "Switchover"

// AnnotationPromote asks for a planned switchover to the named Pod:
//
//	kubectl annotate redisinstance cache redis.operator.com/promote=cache-1
//
// The operator handles every value once and records it in
// status.handledRequests, so the annotation may stay, e.g. in git. To ask
// again for the same Pod, change the value after an @: cache-1@2.
const AnnotationPromote = "redis.operator.com/promote"

// ConditionStorageReady is True when the volume of every instance is Ready,
// see RedisInstanceStatus.Volumes. When False, its message lists the instances
// whose volume is not.
const ConditionStorageReady = "StorageReady"

// ConditionParametersApplied is True when every running instance has the
// configuration of spec.redis. When False, its message lists the options
// Redis refused, e.g. a wrong value of a parameter.
const ConditionParametersApplied = "ParametersApplied"

// ConditionPrimaryAvailable is True while the primary Pod is Ready. When
// False, its lastTransitionTime is when the primary became unavailable: a
// replica takes over after spec.failover.delay.
const ConditionPrimaryAvailable = "PrimaryAvailable"

// ConditionFailover is True while a replica takes over from a failed primary.
// When False, its Reason tells how the last failover ended.
const ConditionFailover = "Failover"

// ConditionReady is True when every instance is Ready, up to date and on a
// Ready volume, and no switchover is running. It sums up the instance for
// tools that read one condition, like the health check of Argo CD.
const ConditionReady = "Ready"

// AnnotationRecreate asks to recreate the named replica on a new generated
// volume, e.g. after its PVC was lost:
//
//	kubectl annotate redisinstance cache redis.operator.com/recreate=cache-2
//
// It is the only way to change the volume of an instance once it is bound.
// Like AnnotationPromote, every value is handled once: cache-2@2 asks again.
const AnnotationRecreate = "redis.operator.com/recreate"

// RedisInstanceSpec defines the desired state of RedisInstance
// +kubebuilder:validation:XValidation:rule="has(self.storage.volumeClaimTemplate) || (has(self.storage.existingClaims) && self.storage.existingClaims.filter(c, c.instanceOrdinal < self.instances).size() == self.instances)",message="storage.volumeClaimTemplate is required unless storage.existingClaims cover every instance"
type RedisInstanceSpec struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	Instances int32       `json:"instances,omitempty"`
	Image     string      `json:"image"`
	Storage   StorageSpec `json:"storage"`

	// ACL configures the users of Redis.
	// +kubebuilder:default={}
	// +optional
	ACL ACLSpec `json:"acl,omitzero"`

	// Resources of the redis container. Set limits.memory: maxmemory is
	// derived from it, see redis.maxmemoryRatio.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitzero"`

	// Redis configures redis-server.
	// +kubebuilder:default={}
	// +optional
	Redis RedisSpec `json:"redis,omitzero"`

	// Failover configures what happens when the primary is not available.
	// +kubebuilder:default={}
	// +optional
	Failover FailoverSpec `json:"failover,omitzero"`

	// Affinity places the instances on the nodes.
	// +kubebuilder:default={}
	// +optional
	Affinity AffinitySpec `json:"affinity,omitzero"`
}

// AffinitySpec places the instances on the nodes.
type AffinitySpec struct {
	// PodAntiAffinityType keeps the instances on different nodes, so a node
	// that fails takes at most one of them. required leaves an instance
	// Pending without a free node; preferred places it anyway, e.g. on a
	// small cluster for development.
	// +kubebuilder:validation:Enum=required;preferred
	// +kubebuilder:default=required
	// +optional
	PodAntiAffinityType PodAntiAffinityType `json:"podAntiAffinityType,omitempty"`
}

// PodAntiAffinityType tells how strictly the instances avoid each other.
type PodAntiAffinityType string

const (
	PodAntiAffinityRequired  PodAntiAffinityType = "required"
	PodAntiAffinityPreferred PodAntiAffinityType = "preferred"
)

// FailoverSpec configures what happens when the primary is not available.
type FailoverSpec struct {
	// Delay is how long the primary may be unavailable before a replica
	// takes over. A primary that cannot read its RedisInstance fences itself
	// after half of it, so it takes no more writes by then.
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('4s')",message="delay must be at least 4s"
	// +kubebuilder:default="30s"
	// +optional
	Delay metav1.Duration `json:"delay,omitzero"`

	// FenceOnAPILoss makes a primary that cannot read its RedisInstance for
	// Delay/2 hold back writes, as a replica may be promoted meanwhile. With
	// false, writes go on while the API server is down, at the risk of two
	// primaries taking writes during a network partition.
	// +kubebuilder:default=true
	// +optional
	FenceOnAPILoss *bool `json:"fenceOnAPILoss,omitempty"`
}

type RedisSpec struct {
	// Persistence is how the dataset survives a restart of Redis.
	// +kubebuilder:default=aof
	// +optional
	Persistence Persistence `json:"persistence,omitempty"`

	// Startup holds the options redis-server reads only when it starts.
	// Changing any of them replaces every Pod, one at a time: a rolling
	// update, with a switchover for the primary.
	// +kubebuilder:default={}
	// +optional
	Startup RedisStartupSpec `json:"startup,omitzero"`

	// MaxmemoryPolicy is what Redis does when it reaches maxmemory. The
	// default noeviction rejects writes instead of silently deleting keys.
	// +kubebuilder:validation:Enum=noeviction;allkeys-lru;allkeys-lfu;allkeys-random;volatile-lru;volatile-lfu;volatile-random;volatile-ttl
	// +kubebuilder:default=noeviction
	// +optional
	MaxmemoryPolicy string `json:"maxmemoryPolicy,omitempty"`

	// MaxmemoryRatio is the percent of the memory limit that becomes
	// maxmemory. The rest is left for fork copy-on-write, replication and
	// client buffers, and fragmentation.
	// +kubebuilder:validation:Minimum=10
	// +kubebuilder:validation:Maximum=95
	// +kubebuilder:default=75
	// +optional
	MaxmemoryRatio int32 `json:"maxmemoryRatio,omitempty"`

	// Parameters are further options of redis.conf, set on every instance
	// with CONFIG SET, without a restart, e.g. {"hz": "20"}.
	// +kubebuilder:validation:MaxProperties=64
	// +kubebuilder:validation:XValidation:rule="self.all(k, k.matches('^[a-z][a-z0-9-]{0,63}$'))",message="parameter names are lowercase options of redis.conf"
	// +kubebuilder:validation:XValidation:rule="!self.exists(k, k.matches('^(bind|port|unixsocket.*|protected-mode|dir|dbfilename|append(only|dirname|filename)|save|(replica|slave)of|master(user|auth)|replica-(announce-.*|read-only)|slave-read-only|user|aclfile|requirepass|maxmemory|maxmemory-policy|databases|io-threads.*|include|rename-command|loadmodule|daemonize|supervised|pidfile|logfile|enable-.*-(command|configs)|tls-.*|cluster-.*)$'))",message="parameters must not set options the operator manages: ports, files, replication, users, memory, persistence, TLS, cluster and commands that run code; databases and io-threads go to spec.redis.startup"
	// +optional
	Parameters map[string]ParameterValue `json:"parameters,omitempty"`
}

// RedisStartupSpec holds the options of redis.conf that Redis cannot change
// while it runs: CONFIG SET answers "can't set immutable config".
type RedisStartupSpec struct {
	// Databases is the number of logical databases, selected with SELECT.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=16
	// +optional
	Databases int32 `json:"databases,omitempty"`

	// IOThreads is the number of threads that serve clients; 1 is the main
	// thread alone.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=128
	// +kubebuilder:default=1
	// +optional
	IOThreads int32 `json:"ioThreads,omitempty"`
}

// Persistence is how Redis keeps its dataset on the volume.
// +kubebuilder:validation:Enum=aof;rdb;none
type Persistence string

const (
	// PersistenceAOF logs every write and fsyncs it every second, plus RDB
	// snapshots: a crash loses at most about a second of writes.
	PersistenceAOF Persistence = "aof"
	// PersistenceRDB takes only RDB snapshots: a crash loses the writes since
	// the last one, which can be minutes.
	PersistenceRDB Persistence = "rdb"
	// PersistenceNone keeps nothing, for a cache.
	PersistenceNone Persistence = "none"
)

// ParameterValue is the value of an option of redis.conf. It is one line, so
// it can never add another option.
// +kubebuilder:validation:MaxLength=1024
// +kubebuilder:validation:Pattern=`^[^\r\n]*$`
type ParameterValue string

// DefaultUserMode tells whether the default user of Redis is enabled.
// +kubebuilder:validation:Enum=disabled;enabled
type DefaultUserMode string

const (
	DefaultUserDisabled DefaultUserMode = "disabled"
	DefaultUserEnabled  DefaultUserMode = "enabled"
)

type ACLSpec struct {
	// DefaultUser enables the default user of Redis, which needs no password
	// and may run every command. Meant for development only.
	// +kubebuilder:default=disabled
	// +optional
	DefaultUser DefaultUserMode `json:"defaultUser,omitempty"`
}

// StorageSpec chooses the volumes when the RedisInstance is created. After
// that the operator alone manages them through status.volumes, so the volume
// of a running instance can never be switched by an edit of the spec.
// +kubebuilder:validation:XValidation:rule="has(self.existingClaims) == has(oldSelf.existingClaims) && (!has(self.existingClaims) || self.existingClaims == oldSelf.existingClaims)",message="storage.existingClaims is immutable; use the redis.operator.com/recreate annotation to move a replica to a new volume"
type StorageSpec struct {
	// VolumeClaimTemplate is used to generate a PVC for every instance
	// not listed in ExistingClaims. Generated PVCs are owned by the operator.
	// +optional
	VolumeClaimTemplate *VolumeClaimTemplateSpec `json:"volumeClaimTemplate,omitempty"`
	// ExistingClaims binds instances to PVCs created outside of the operator.
	// The operator never deletes these PVCs. An ordinal at or above instances
	// reserves the PVC for scaling up.
	// +optional
	// +listType=map
	// +listMapKey=instanceOrdinal
	// +kubebuilder:validation:MaxItems=64
	ExistingClaims []ExistingClaim `json:"existingClaims,omitempty"`

	// RecreateOnNodeLoss moves a replica whose generated volume is local to a
	// node that is gone, or tainted node.kubernetes.io/out-of-service, to a
	// new generated volume, where it syncs from the primary. Without it such a
	// replica waits for its node. External volumes and the primary are never
	// moved: a failover takes over from a primary on a lost node first.
	// +optional
	RecreateOnNodeLoss bool `json:"recreateOnNodeLoss,omitempty"`
}

type VolumeClaimTemplateSpec struct {
	Size resource.Quantity `json:"size"`

	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="storageClassName is immutable"
	StorageClassName *string `json:"storageClassName,omitempty"`
}

// ExistingClaim binds one instance to an existing PVC.
type ExistingClaim struct {
	// +kubebuilder:validation:Minimum=0
	InstanceOrdinal int32 `json:"instanceOrdinal"`

	// +kubebuilder:validation:MinLength=1
	ClaimName string `json:"claimName"`
}

// RedisInstanceStatus defines the observed state of RedisInstance.
type RedisInstanceStatus struct {
	ReadyInstances int32  `json:"readyInstances,omitempty"`
	Phase          string `json:"phase,omitempty"`
	CurrentPrimary string `json:"currentPrimary,omitempty"`

	// PrimaryEpoch grows with every change of the primary. The Instance
	// Managers reject commands sent for an older epoch, e.g. by a former
	// operator leader.
	// +optional
	PrimaryEpoch int64 `json:"primaryEpoch,omitempty"`
	// Conditions of the instance, e.g. MemoryLimited.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// TargetPrimary is the Pod a switchover in progress moves the primary to.
	// +optional
	TargetPrimary string `json:"targetPrimary,omitempty"`

	// Volumes binds every instance to its PVC. A binding is made once, when
	// the instance first gets a volume, and stays until the instance is
	// removed or recreated, so it is the memory of the operator: the spec
	// only provides the initial choice.
	// +listType=map
	// +listMapKey=ordinal
	// +optional
	Volumes []VolumeStatus `json:"volumes,omitempty"`

	// HandledRequests holds, per request annotation, the value the operator
	// last handled, so a request kept in the annotation is not served again.
	// +optional
	HandledRequests map[string]string `json:"handledRequests,omitempty"`
}

// VolumeState is the observed state of the volume of one instance.
// +kubebuilder:validation:Enum=Ready;Terminating;Missing;Lost;Detached
type VolumeState string

const (
	// VolumeReady: the PVC exists, is the bound one and is not being deleted.
	VolumeReady VolumeState = "Ready"
	// VolumeTerminating: the PVC is being deleted, but a Pod still uses it.
	VolumeTerminating VolumeState = "Terminating"
	// VolumeMissing: the PVC does not exist, or a PVC with its name is not
	// the bound one (another UID). The instance gets no Pod.
	VolumeMissing VolumeState = "Missing"
	// VolumeLost: the PVC lost its PersistentVolume.
	VolumeLost VolumeState = "Lost"
	// VolumeDetached: an external PVC of an instance removed by a scale-down.
	// It is mounted again when the instance comes back.
	VolumeDetached VolumeState = "Detached"
)

// VolumeStatus is the volume bound to one instance.
type VolumeStatus struct {
	Ordinal int32 `json:"ordinal"`
	// ClaimName is the PVC of the instance.
	ClaimName string `json:"claimName"`
	// UID of the PVC when it was bound. A PVC with the same name and another
	// UID was deleted and created again: it holds other data.
	// +optional
	UID types.UID `json:"uid,omitempty"`
	// External is true for a PVC from spec.storage.existingClaims, which the
	// operator never deletes.
	// +optional
	External bool `json:"external,omitempty"`
	// +optional
	State VolumeState `json:"state,omitempty"`
	// Message explains a State other than Ready.
	// +optional
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].reason"
// +kubebuilder:printcolumn:name="Primary",type=string,JSONPath=".status.currentPrimary"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// RedisInstance is the Schema for the redisinstances API
type RedisInstance struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of RedisInstance
	// +required
	Spec RedisInstanceSpec `json:"spec"`

	// status defines the observed state of RedisInstance
	// +optional
	Status RedisInstanceStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// RedisInstanceList contains a list of RedisInstance
type RedisInstanceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []RedisInstance `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &RedisInstance{}, &RedisInstanceList{})
		return nil
	})
}
