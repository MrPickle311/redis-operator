package controller

import (
	"context"
	"crypto/rand"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/instancemanager"
)

// ensureInternalAuthSecret creates the Secret with the passwords of the
// internal users. A shard uses the one of its RedisCluster, which creates it:
// MIGRATE logs in to another shard as the internal user.
func (r *RedisInstanceReconciler) ensureInternalAuthSecret(ctx context.Context, instance *redisv1.RedisInstance) error {
	if cluster := clusterOf(instance); cluster != "" {
		var secret corev1.Secret
		err := r.Get(ctx, types.NamespacedName{Name: internalAuthSecretName(instance), Namespace: instance.Namespace}, &secret)
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("waiting for the RedisCluster %s to create the Secret %s", cluster, internalAuthSecretName(instance))
		}
		return err
	}
	return ensureInternalAuth(ctx, r.Client, r.Scheme, instance, internalAuthSecretName(instance))
}

// ensureInternalAuth creates the Secret name, owned by owner, generating each
// password once. Running instances keep using the passwords they started
// with, so existing ones are never replaced.
func ensureInternalAuth(ctx context.Context, c client.Client, scheme *runtime.Scheme, owner client.Object, name string) error {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: owner.GetNamespace()}}

	result, err := controllerutil.CreateOrUpdate(ctx, c, secret, func() error {
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}
		for _, key := range []string{instancemanager.OperatorPasswordKey, instancemanager.ReplicationPasswordKey} {
			if len(secret.Data[key]) == 0 {
				secret.Data[key] = []byte(rand.Text())
			}
		}
		return ctrl.SetControllerReference(owner, secret, scheme)
	})
	if result != controllerutil.OperationResultNone {
		logf.FromContext(ctx).Info("Reconciled internal users Secret", "name", secret.Name, "operation", result)
	}
	return err
}
