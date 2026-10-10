package controller

import (
	"bytes"
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/pki"
)

const (
	// CASecretName is the Secret in the operator namespace that holds the CA
	// of the Instance Manager API. Deleting it and restarting the operator
	// rotates the CA: every instance then gets a certificate from the new one.
	CASecretName = "redis-operator-ca"

	// caCertKey is the Secret key with the CA certificate, as in cert-manager.
	caCertKey = "ca.crt"

	caValidity = 10 * 365 * 24 * time.Hour

	// serverCertValidity is renewed after 2/3 of it, leaving a month for a
	// reconcile to happen; the periodic resync of every object comes each 10h.
	serverCertValidity = 90 * 24 * time.Hour
)

// EnsureCA returns the CA stored in the CA Secret, creating it on first start.
func EnsureCA(ctx context.Context, c client.Client, namespace string) (pki.KeyPair, error) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: CASecretName, Namespace: namespace}}
	err := c.Get(ctx, client.ObjectKeyFromObject(secret), secret)
	if apierrors.IsNotFound(err) {
		err = createCASecret(ctx, c, secret)
	}
	if err != nil {
		return pki.KeyPair{}, err
	}
	return pki.KeyPair{CertPEM: secret.Data[corev1.TLSCertKey], KeyPEM: secret.Data[corev1.TLSPrivateKeyKey]}, nil
}

func createCASecret(ctx context.Context, c client.Client, secret *corev1.Secret) error {
	ca, err := pki.NewCA("redis-operator", caValidity)
	if err != nil {
		return err
	}
	secret.Type = corev1.SecretTypeTLS
	secret.Data = map[string][]byte{corev1.TLSCertKey: ca.CertPEM, corev1.TLSPrivateKeyKey: ca.KeyPEM}

	err = c.Create(ctx, secret)
	if apierrors.IsAlreadyExists(err) {
		// Another operator replica created it first: use that CA.
		return c.Get(ctx, client.ObjectKeyFromObject(secret), secret)
	}
	return err
}

// ensureServerSecret keeps the TLS Secret of the Instance Manager API signed
// by the current CA and renews the certificate before it expires.
func (r *RedisInstanceReconciler) ensureServerSecret(ctx context.Context, instance *redisv1.RedisInstance) error {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: serverSecretName(instance), Namespace: instance.Namespace}}

	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		if err := ctrl.SetControllerReference(instance, secret, r.Scheme); err != nil {
			return err
		}
		if !r.needsNewServerCert(secret) {
			return nil
		}

		cert, err := pki.IssueServerCert(r.CA, []string{serverCertDNSName(instance)}, serverCertValidity)
		if err != nil {
			return err
		}
		secret.Type = corev1.SecretTypeTLS
		secret.Data = map[string][]byte{
			corev1.TLSCertKey:       cert.CertPEM,
			corev1.TLSPrivateKeyKey: cert.KeyPEM,
			caCertKey:               r.CA.CertPEM,
		}
		return nil
	})
	if result != controllerutil.OperationResultNone {
		logf.FromContext(ctx).Info("Reconciled Instance Manager TLS Secret", "name", secret.Name, "operation", result)
	}
	return err
}

// needsNewServerCert reports whether the Secret has no certificate yet, one
// from a previous CA, or one in the last third of its lifetime.
func (r *RedisInstanceReconciler) needsNewServerCert(secret *corev1.Secret) bool {
	if !bytes.Equal(secret.Data[caCertKey], r.CA.CertPEM) {
		return true
	}
	renew, err := pki.NeedsRenewal(secret.Data[corev1.TLSCertKey], time.Now())
	return renew || err != nil
}
