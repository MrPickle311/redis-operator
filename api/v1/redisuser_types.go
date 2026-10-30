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

// RedisUserSpec defines a user of Redis for one application: its password
// and what it may do.
type RedisUserSpec struct {
	// TargetRef names the database, in the same namespace, the user belongs to.
	// +required
	TargetRef TargetRef `json:"targetRef"`

	// Username is the name the application logs in with. It defaults to the
	// name of the RedisUser.
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`
	// +kubebuilder:validation:XValidation:rule="self != 'default'",message="default is the user spec.acl.defaultUser of the RedisInstance controls"
	// +optional
	Username string `json:"username,omitempty"`

	// Authentication tells where the password comes from.
	// +required
	Authentication RedisUserAuthentication `json:"authentication"`

	// ACL is what the user may do.
	// +required
	ACL RedisUserACL `json:"acl"`
}

// TargetRef names an object in the same namespace.
type TargetRef struct {
	// Kind is the kind of the database; RedisCluster comes with step 10.
	// +kubebuilder:validation:Enum=RedisInstance
	// +kubebuilder:default=RedisInstance
	// +optional
	Kind string `json:"kind,omitempty"`

	// Name is the name of the database.
	// +kubebuilder:validation:MinLength=1
	// +required
	Name string `json:"name"`
}

// RedisUserAuthentication tells where the password of a user comes from:
// a Secret of the user, e.g. synced from Vault, or one the operator generates.
// Redis gets only a hash of it.
// +kubebuilder:validation:XValidation:rule="has(self.passwordSecretRef) != (has(self.generate) && self.generate)",message="set exactly one of passwordSecretRef and generate"
type RedisUserAuthentication struct {
	// PasswordSecretRef selects the password in a Secret of the user.
	// +optional
	PasswordSecretRef *corev1.SecretKeySelector `json:"passwordSecretRef,omitempty"`

	// Generate makes the operator create the Secret <name>-redis-user, with
	// the keys username and password, for the application to mount.
	// +optional
	Generate bool `json:"generate,omitempty"`
}

// RedisUserACL is what a user may do.
type RedisUserACL struct {
	// Rules are ACL rules of Redis, one per item, e.g. "~orders:*", "+@read",
	// "-@dangerous". The password and whether the user is on come from the
	// operator, so rules may not set them.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:XValidation:rule="self.all(r, !(r.lowerAscii() in ['nopass', 'resetpass', 'reset', 'on', 'off']))",message="nopass, resetpass, reset, on and off are set by the operator"
	// +required
	Rules []ACLRule `json:"rules"`
}

// ACLRule is one ACL rule of Redis: one word, and no password.
// +kubebuilder:validation:MaxLength=256
// +kubebuilder:validation:Pattern=`^[^\s<>#!][^\s]*$`
type ACLRule string

// RedisUserStatus defines the observed state of RedisUser.
type RedisUserStatus struct {
	// Username is the name the user logs in with.
	// +optional
	Username string `json:"username,omitempty"`

	// Conditions: Ready is True once every running instance of the database
	// has the user as specified.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=".spec.targetRef.name"
// +kubebuilder:printcolumn:name="Username",type=string,JSONPath=".status.username"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].reason"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// RedisUser is a user of Redis for one application.
type RedisUser struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of RedisUser
	// +required
	Spec RedisUserSpec `json:"spec"`

	// status defines the observed state of RedisUser
	// +optional
	Status RedisUserStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// RedisUserList contains a list of RedisUser
type RedisUserList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []RedisUser `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &RedisUser{}, &RedisUserList{})
		return nil
	})
}
