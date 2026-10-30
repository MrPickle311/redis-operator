package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/instancemanager"
)

// serviceAccountDir is where the kubelet puts the ServiceAccount token, and
// where client-go looks for it.
const serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// ensureInstanceAccount lets the Instance Managers of the instance read their
// own RedisInstance and nothing else: they learn the current primary from its
// status.
func (r *RedisInstanceReconciler) ensureInstanceAccount(ctx context.Context, instance *redisv1.RedisInstance) error {
	name := instanceAccountName(instance)
	objectMeta := metav1.ObjectMeta{Name: name, Namespace: instance.Namespace}
	account := &corev1.ServiceAccount{ObjectMeta: objectMeta}
	role := &rbacv1.Role{ObjectMeta: objectMeta}
	binding := &rbacv1.RoleBinding{ObjectMeta: objectMeta}

	for _, o := range []struct {
		object client.Object
		mutate func()
	}{
		{account, func() {}},
		{role, func() {
			role.Rules = []rbacv1.PolicyRule{{
				APIGroups:     []string{redisv1.GroupVersion.Group},
				Resources:     []string{"redisinstances"},
				ResourceNames: []string{instance.Name},
				Verbs:         []string{"get"},
			}, {
				// The users of RedisUsers, as hashes; see the aclSync of the
				// Instance Manager.
				APIGroups:     []string{""},
				Resources:     []string{"secrets"},
				ResourceNames: []string{instancemanager.ACLSecretName(instance.Name)},
				Verbs:         []string{"get"},
			}}
		}},
		{binding, func() {
			binding.RoleRef = rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name}
			binding.Subjects = []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: name, Namespace: instance.Namespace}}
		}},
	} {
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, o.object, func() error {
			o.mutate()
			return ctrl.SetControllerReference(instance, o.object, r.Scheme)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// apiAccessVolumeSource holds what automountServiceAccountToken would mount.
// Mounted only in the redis container, it keeps the token away from the init
// container.
func apiAccessVolumeSource() corev1.VolumeSource {
	return corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{
		{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token"}},
		{ConfigMap: &corev1.ConfigMapProjection{
			LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"},
			Items:                []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}},
		}},
		{DownwardAPI: &corev1.DownwardAPIProjection{Items: []corev1.DownwardAPIVolumeFile{{
			Path: "namespace", FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
		}}}},
	}}}
}

// fieldEnv returns an environment variable with a field of the Pod.
func fieldEnv(name, fieldPath string) corev1.EnvVar {
	return corev1.EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{
		FieldRef: &corev1.ObjectFieldSelector{FieldPath: fieldPath},
	}}
}
