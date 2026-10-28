package controller

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/pki"
)

// The TLS of Redis has a CA per instance, apart from the CA of the operator
// that secures its API to the Instance Managers: a client trusts only its
// own database, and the CA of one can be replaced without touching others.
const (
	// instanceCAValidity is long, as clients trust the CA: replacing it means
	// handing every client a new ca.crt.
	instanceCAValidity = 10 * 365 * 24 * time.Hour
	// caExpiryWarning is how long before the CA expires TLSReady warns.
	// ponytail: a warning only; rotating the CA, with a bundle of the old and
	// the new one for the clients meanwhile, comes later.
	caExpiryWarning = 365 * 24 * time.Hour

	caKeyKey = "ca.key"
)

// instanceCASecretName returns the Secret with the CA of the TLS of Redis.
func instanceCASecretName(instance *redisv1.RedisInstance) string {
	return instance.Name + "-ca"
}

// redisTLSSecretName returns the Secret the Pods mount for the TLS of Redis.
// The operator manages it from either source of certificates, so a Pod never
// mounts a certificate the operator has not checked.
func redisTLSSecretName(instance *redisv1.RedisInstance) string {
	return instance.Name + "-server-tls"
}

// redisDNSNames returns the names the certificate of Redis must cover: the
// client Services, short and qualified, and every Pod through the headless
// Service, which replicas use to reach the primary.
//
// ponytail: assumes the cluster domain cluster.local; a flag of the operator
// if a cluster uses another one.
func redisDNSNames(instance *redisv1.RedisInstance) []string {
	var names []string
	for _, suffix := range []string{"rw", "ro", "r"} {
		service := instance.Name + "-" + suffix
		names = append(names,
			service,
			service+"."+instance.Namespace,
			service+"."+instance.Namespace+".svc",
			service+"."+instance.Namespace+".svc.cluster.local",
		)
	}
	return append(names, "*."+headlessServiceName(instance)+"."+instance.Namespace+".svc")
}

// reconcileRedisTLS keeps the certificate of Redis issued by the CA of the
// instance, when TLS is on and the user brings no certificate of their own.
func (r *RedisInstanceReconciler) reconcileRedisTLS(ctx context.Context, instance *redisv1.RedisInstance) error {
	if !instance.Spec.TLS.TLSEnabled() {
		meta.RemoveStatusCondition(&instance.Status.Conditions, redisv1.ConditionTLSReady)
		return nil
	}
	if instance.Spec.TLS.ServerSecretRef != nil {
		return nil // a certificate of the user, step 5.6
	}
	ca, err := r.ensureInstanceCA(ctx, instance)
	if err != nil {
		return err
	}
	if err := r.ensureRedisServerSecret(ctx, instance, ca); err != nil {
		return err
	}
	r.setTLSReadyCondition(instance, ca)
	return nil
}

// ensureInstanceCA returns the CA of the instance, creating it once. It is
// never replaced on its own: clients trust it.
func (r *RedisInstanceReconciler) ensureInstanceCA(ctx context.Context, instance *redisv1.RedisInstance) (pki.KeyPair, error) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: instanceCASecretName(instance), Namespace: instance.Namespace}}
	err := r.Get(ctx, client.ObjectKeyFromObject(secret), secret)
	if apierrors.IsNotFound(err) {
		ca, err := pki.NewCA(fmt.Sprintf("%s/%s Redis CA", instance.Namespace, instance.Name), instanceCAValidity)
		if err != nil {
			return pki.KeyPair{}, err
		}
		secret.Data = map[string][]byte{caCertKey: ca.CertPEM, caKeyKey: ca.KeyPEM}
		if err := ctrl.SetControllerReference(instance, secret, r.Scheme); err != nil {
			return pki.KeyPair{}, err
		}
		logf.FromContext(ctx).Info("Creating the CA of the TLS of Redis", "name", secret.Name)
		if err := r.Create(ctx, secret); err != nil {
			return pki.KeyPair{}, err
		}
		return ca, nil
	}
	if err != nil {
		return pki.KeyPair{}, err
	}
	return pki.KeyPair{CertPEM: secret.Data[caCertKey], KeyPEM: secret.Data[caKeyKey]}, nil
}

// ensureRedisServerSecret keeps the certificate of Redis signed by ca, for
// the names of the instance, and renews it after two thirds of its lifetime.
func (r *RedisInstanceReconciler) ensureRedisServerSecret(ctx context.Context, instance *redisv1.RedisInstance, ca pki.KeyPair) error {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: redisTLSSecretName(instance), Namespace: instance.Namespace}}
	names := redisDNSNames(instance)
	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		if err := ctrl.SetControllerReference(instance, secret, r.Scheme); err != nil {
			return err
		}
		if !needsNewRedisCert(secret, ca, names, time.Now()) {
			return nil
		}
		cert, err := pki.IssueServerCert(ca, names, serverCertValidity)
		if err != nil {
			return err
		}
		secret.Type = corev1.SecretTypeTLS
		secret.Data = map[string][]byte{
			corev1.TLSCertKey:       cert.CertPEM,
			corev1.TLSPrivateKeyKey: cert.KeyPEM,
			caCertKey:               ca.CertPEM,
		}
		return nil
	})
	if result != controllerutil.OperationResultNone {
		logf.FromContext(ctx).Info("Reconciled the TLS Secret of Redis", "name", secret.Name, "operation", result)
	}
	return err
}

// needsNewRedisCert reports whether the Secret has no certificate yet, one
// from another CA, for other names, or in the last third of its lifetime.
func needsNewRedisCert(secret *corev1.Secret, ca pki.KeyPair, names []string, now time.Time) bool {
	if !bytes.Equal(secret.Data[caCertKey], ca.CertPEM) {
		return true
	}
	certNames, err := pki.DNSNames(secret.Data[corev1.TLSCertKey])
	if err != nil || !slices.Equal(certNames, names) {
		return true
	}
	renew, err := pki.NeedsRenewal(secret.Data[corev1.TLSCertKey], now)
	return renew || err != nil
}

// setTLSReadyCondition sets TLSReady, warning once when the CA expires soon.
func (r *RedisInstanceReconciler) setTLSReadyCondition(instance *redisv1.RedisInstance, ca pki.KeyPair) {
	condition := metav1.Condition{
		Type:               redisv1.ConditionTLSReady,
		Status:             metav1.ConditionTrue,
		Reason:             "Issued",
		Message:            "The certificate of Redis is issued by the CA in " + instanceCASecretName(instance),
		ObservedGeneration: instance.Generation,
	}
	if expires, err := pki.NotAfter(ca.CertPEM); err == nil && time.Until(expires) < caExpiryWarning {
		condition.Reason = "CAExpiresSoon"
		condition.Message = fmt.Sprintf("The CA in %s expires on %s; clients need a new ca.crt before then",
			instanceCASecretName(instance), expires.Format(time.DateOnly))
		if previous := meta.FindStatusCondition(instance.Status.Conditions, redisv1.ConditionTLSReady); previous == nil || previous.Reason != condition.Reason {
			r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, "CAExpiresSoon", "TLS", condition.Message)
		}
	}
	meta.SetStatusCondition(&instance.Status.Conditions, condition)
}
