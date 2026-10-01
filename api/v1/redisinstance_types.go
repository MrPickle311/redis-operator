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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// RedisInstanceSpec defines the desired state of RedisInstance
// +kubebuilder:validation:XValidation:rule="!has(self.storage.existingClaims) || self.storage.existingClaims.all(c, c.instanceOrdinal < self.instances)",message="storage.existingClaims[].instanceOrdinal must be lower than instances"
// +kubebuilder:validation:XValidation:rule="has(self.storage.volumeClaimTemplate) || (has(self.storage.existingClaims) && size(self.storage.existingClaims) >= self.instances)",message="storage.volumeClaimTemplate is required unless storage.existingClaims cover every instance"
type RedisInstanceSpec struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	Instances int32       `json:"instances,omitempty"`
	Image     string      `json:"image"`
	Storage   StorageSpec `json:"storage"`
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
