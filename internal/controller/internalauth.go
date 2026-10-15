package controller

import (
	"context"
	"crypto/rand"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/instancemanager"
)

// ensureInternalAuthSecret returns the Secret with the passwords of the
// internal users, generating each password once. Running instances keep using
// the passwords they started with, so existing ones are never replaced.
func (r *RedisInstanceReconciler) ensureInternalAuthSecret(ctx context.Context, instance *redisv1.RedisInstance) (*corev1.Secret, error) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: internalAuthSecretName(instance), Namespace: instance.Namespace}}

	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}
		for _, key := range []string{instancemanager.OperatorPasswordKey, instancemanager.ReplicationPasswordKey} {
			if len(secret.Data[key]) == 0 {
				secret.Data[key] = []byte(rand.Text())
			}
		}
		return ctrl.SetControllerReference(instance, secret, r.Scheme)
	})
	if result != controllerutil.OperationResultNone {
		logf.FromContext(ctx).Info("Reconciled internal users Secret", "name", secret.Name, "operation", result)
	}
	return secret, err
}
