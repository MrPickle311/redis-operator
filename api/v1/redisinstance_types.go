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
// The operator removes it once the switchover has completed or failed.
const AnnotationPromote = "redis.operator.com/promote"

// RedisInstanceSpec defines the desired state of RedisInstance
// +kubebuilder:validation:XValidation:rule="!has(self.storage.existingClaims) || self.storage.existingClaims.all(c, c.instanceOrdinal < self.instances)",message="storage.existingClaims[].instanceOrdinal must be lower than instances"
// +kubebuilder:validation:XValidation:rule="has(self.storage.volumeClaimTemplate) || (has(self.storage.existingClaims) && size(self.storage.existingClaims) >= self.instances)",message="storage.volumeClaimTemplate is required unless storage.existingClaims cover every instance"
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
}

type RedisSpec struct {
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
}

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

type StorageSpec struct {
	// VolumeClaimTemplate is used to generate a PVC for every instance
	// not listed in ExistingClaims. Generated PVCs are owned by the operator.
	// +optional
	VolumeClaimTemplate *VolumeClaimTemplateSpec `json:"volumeClaimTemplate,omitempty"`
	// ExistingClaims binds instances to PVCs created outside of the operator.
	// The operator never deletes these PVCs.
	// +optional
	// +listType=map
	// +listMapKey=instanceOrdinal
	// +kubebuilder:validation:MaxItems=64
	ExistingClaims []ExistingClaim `json:"existingClaims,omitempty"`
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
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

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
