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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// RedisInstanceSpec defines the desired state of RedisInstance
type RedisInstanceSpec struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	Instances int32       `json:"instances,omitempty"`
	Image     string      `json:"image"`
	Storage   StorageSpec `json:"storage"`
}

type StorageSpec struct {
	Size             string  `json:"size"`
	StorageClassName *string `json:"storageClassName,omitempty"`
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
