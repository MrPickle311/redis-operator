package controller

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/instancemanager"
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

// redisTLSVolume holds the certificate of Redis in the Pod.
const redisTLSVolume = "redis-tls"

// mountRedisTLS mounts the TLS Secret of Redis into the redis container. It
// is always the Secret the operator manages, see redisTLSSecretName.
func mountRedisTLS(pod *corev1.Pod, instance *redisv1.RedisInstance) {
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name:         redisTLSVolume,
		VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: redisTLSSecretName(instance)}},
	})
	redis := &pod.Spec.Containers[0]
	redis.VolumeMounts = append(redis.VolumeMounts, corev1.VolumeMount{
		Name: redisTLSVolume, MountPath: instancemanager.TLSDir, ReadOnly: true,
	})
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

// serverCert is the certificate Redis serves, with the CA that issued it,
// which clients and replicas verify it with.
type serverCert struct {
	cert, key, ca []byte
}

// reconcileRedisTLS keeps the TLS Secret the Pods mount up to date: the
// certificate of Redis, from the CA of the instance or from the Secret of the
// user, the certificate replicas show the primary, and the CAs Redis trusts.
// A certificate that fails the checks never reaches the Secret: TLSReady
// turns False and Redis keeps serving the one it has.
func (r *RedisInstanceReconciler) reconcileRedisTLS(ctx context.Context, instance *redisv1.RedisInstance) error {
	if !instance.Spec.TLS.TLSEnabled() {
		meta.RemoveStatusCondition(&instance.Status.Conditions, redisv1.ConditionTLSReady)
		return nil
	}
	// Always: it also signs the certificate replicas show the primary.
	ca, err := r.ensureInstanceCA(ctx, instance)
	if err != nil {
		return err
	}
	server, problem, err := r.serverCertificate(ctx, instance, ca)
	if err != nil {
		return err
	}
	clientCA, clientProblem, err := r.clientCA(ctx, instance)
	if err != nil {
		return err
	}
	if problem == "" {
		problem = clientProblem
	}
	if problem != "" {
		r.setTLSNotReady(instance, problem)
		return nil
	}
	if err := r.ensureRedisTLSSecret(ctx, instance, ca, server, clientCA); err != nil {
		return err
	}
	r.setTLSReadyCondition(instance, ca, server)
	return nil
}

// serverCertificate returns the certificate Redis is to serve, or explains
// why the certificate of the user cannot be served.
func (r *RedisInstanceReconciler) serverCertificate(ctx context.Context, instance *redisv1.RedisInstance, ca pki.KeyPair) (serverCert, string, error) {
	if ref := instance.Spec.TLS.ServerSecretRef; ref != nil {
		var secret corev1.Secret
		err := r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: instance.Namespace}, &secret)
		if apierrors.IsNotFound(err) {
			return serverCert{}, fmt.Sprintf("Secret %s of spec.tls.serverSecretRef does not exist", ref.Name), nil
		}
		if err != nil {
			return serverCert{}, "", err
		}
		user := serverCert{
			cert: secret.Data[corev1.TLSCertKey],
			key:  secret.Data[corev1.TLSPrivateKeyKey],
			ca:   secret.Data[caCertKey],
		}
		if problem := validateUserCert(user, requiredDNSNames(instance)); problem != "" {
			return serverCert{}, fmt.Sprintf("Secret %s: %s", ref.Name, problem), nil
		}
		return user, "", nil
	}

	// From the CA of the instance: keep the current one while it is good.
	var current corev1.Secret
	err := r.Get(ctx, types.NamespacedName{Name: redisTLSSecretName(instance), Namespace: instance.Namespace}, &current)
	if err != nil && !apierrors.IsNotFound(err) {
		return serverCert{}, "", err
	}
	if !needsNewRedisCert(&current, ca, redisDNSNames(instance), time.Now()) {
		return serverCert{
				cert: current.Data[corev1.TLSCertKey],
				key:  current.Data[corev1.TLSPrivateKeyKey],
				ca:   ca.CertPEM,
			},
			"", nil
	}
	cert, err := pki.IssueServerCert(ca, redisDNSNames(instance), serverCertValidity)
	if err != nil {
		return serverCert{}, "", err
	}
	return serverCert{cert: cert.CertPEM, key: cert.KeyPEM, ca: ca.CertPEM}, "", nil
}

// requiredDNSNames returns the names a certificate of the user must cover:
// the client Services, qualified, and every Pod, which replicas connect to.
func requiredDNSNames(instance *redisv1.RedisInstance) []string {
	var names []string
	for _, suffix := range []string{"rw", "ro", "r"} {
		names = append(names, instance.Name+"-"+suffix+"."+instance.Namespace+".svc")
	}
	for ordinal := range instance.Spec.Instances {
		names = append(names, podFQDN(instance, ordinal))
	}
	return names
}

// validateUserCert explains what is wrong with a certificate of the user, or
// returns "" when Redis may serve it.
func validateUserCert(user serverCert, names []string) string {
	if len(user.cert) == 0 || len(user.key) == 0 {
		return "it has no tls.crt or no tls.key"
	}
	if _, err := tls.X509KeyPair(user.cert, user.key); err != nil {
		return "tls.key does not belong to tls.crt"
	}
	if len(user.ca) == 0 {
		return "it has no ca.crt, which replicas need to verify the primary"
	}
	var missing []string
	for _, name := range names {
		err := pki.Verify(user.cert, user.ca, name, x509.ExtKeyUsageServerAuth)
		switch {
		case errors.As(err, &x509.HostnameError{}):
			missing = append(missing, name)
		case err != nil:
			return "tls.crt does not verify against ca.crt: " + err.Error()
		}
	}
	if len(missing) > 0 {
		return "tls.crt does not cover " + strings.Join(missing, ", ")
	}
	return ""
}

// clientCA returns the CA of the client certificates when clients must show
// one, or explains why it cannot be read.
func (r *RedisInstanceReconciler) clientCA(ctx context.Context, instance *redisv1.RedisInstance) ([]byte, string, error) {
	ref := instance.Spec.TLS.ClientCASecretRef
	if instance.Spec.TLS.ClientAuth == redisv1.TLSClientAuthNone || instance.Spec.TLS.ClientAuth == "" || ref == nil {
		return nil, "", nil
	}
	var secret corev1.Secret
	err := r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: instance.Namespace}, &secret)
	if apierrors.IsNotFound(err) {
		return nil, fmt.Sprintf("Secret %s of spec.tls.clientCASecretRef does not exist", ref.Name), nil
	}
	if err != nil {
		return nil, "", err
	}
	if !x509.NewCertPool().AppendCertsFromPEM(secret.Data[ref.Key]) {
		return nil, fmt.Sprintf("Secret %s has no CA certificate in %s", ref.Name, ref.Key), nil
	}
	return secret.Data[ref.Key], "", nil
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

// ensureRedisTLSSecret writes the TLS Secret the Pods mount:
//   - tls.crt, tls.key: the certificate Redis serves;
//   - ca.crt: its CA, which clients and replicas verify it with;
//   - replication.crt, replication.key: the certificate a replica shows the
//     primary, from the CA of the instance, so it passes tls-auth-clients;
//   - trusted.crt: every CA Redis trusts: of the server, of the instance and,
//     with client certificates, of the clients.
func (r *RedisInstanceReconciler) ensureRedisTLSSecret(ctx context.Context, instance *redisv1.RedisInstance, ca pki.KeyPair, server serverCert, clientCA []byte) error {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: redisTLSSecretName(instance), Namespace: instance.Namespace}}
	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		if err := ctrl.SetControllerReference(instance, secret, r.Scheme); err != nil {
			return err
		}
		replicationCert, replicationKey := secret.Data[instancemanager.TLSReplicationCert], secret.Data[instancemanager.TLSReplicationKey]
		if needsNewReplicationCert(replicationCert, ca, time.Now()) {
			cert, err := pki.IssueClientCert(ca, instancemanager.ReplicationUser, serverCertValidity)
			if err != nil {
				return err
			}
			replicationCert, replicationKey = cert.CertPEM, cert.KeyPEM
		}
		trusted := slices.Clone(server.ca)
		if !bytes.Equal(server.ca, ca.CertPEM) {
			trusted = append(trusted, ca.CertPEM...)
		}
		trusted = append(trusted, clientCA...)

		secret.Type = corev1.SecretTypeTLS
		secret.Data = map[string][]byte{
			corev1.TLSCertKey:                  server.cert,
			corev1.TLSPrivateKeyKey:            server.key,
			caCertKey:                          server.ca,
			instancemanager.TLSReplicationCert: replicationCert,
			instancemanager.TLSReplicationKey:  replicationKey,
			instancemanager.TLSTrustedCAs:      trusted,
		}
		return nil
	})
	if result != controllerutil.OperationResultNone {
		logf.FromContext(ctx).Info("Reconciled the TLS Secret of Redis", "name", secret.Name, "operation", result)
	}
	return err
}

// needsNewReplicationCert reports whether the certificate replicas show the
// primary is missing, not from the CA of the instance, or due for renewal.
func needsNewReplicationCert(cert []byte, ca pki.KeyPair, now time.Time) bool {
	if pki.Verify(cert, ca.CertPEM, "", x509.ExtKeyUsageClientAuth) != nil {
		return true
	}
	renew, err := pki.NeedsRenewal(cert, now)
	return renew || err != nil
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

// setTLSReadyCondition sets TLSReady to True. For a certificate from the CA
// of the instance it warns once when that CA expires soon.
func (r *RedisInstanceReconciler) setTLSReadyCondition(instance *redisv1.RedisInstance, ca pki.KeyPair, server serverCert) {
	condition := metav1.Condition{
		Type:               redisv1.ConditionTLSReady,
		Status:             metav1.ConditionTrue,
		Reason:             "Issued",
		Message:            "The certificate of Redis is issued by the CA in " + instanceCASecretName(instance),
		ObservedGeneration: instance.Generation,
	}
	if ref := instance.Spec.TLS.ServerSecretRef; ref != nil {
		condition.Reason = "Validated"
		condition.Message = fmt.Sprintf("The certificate of Redis comes from Secret %s", ref.Name)
		if expires, err := pki.NotAfter(server.cert); err == nil {
			condition.Message += ", valid until " + expires.Format(time.DateOnly)
		}
	} else if expires, err := pki.NotAfter(ca.CertPEM); err == nil && time.Until(expires) < caExpiryWarning {
		condition.Reason = "CAExpiresSoon"
		condition.Message = fmt.Sprintf("The CA in %s expires on %s; clients need a new ca.crt before then",
			instanceCASecretName(instance), expires.Format(time.DateOnly))
		if previous := meta.FindStatusCondition(instance.Status.Conditions, redisv1.ConditionTLSReady); previous == nil || previous.Reason != condition.Reason {
			r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, "CAExpiresSoon", "TLS", condition.Message)
		}
	}
	meta.SetStatusCondition(&instance.Status.Conditions, condition)
}

// setTLSNotReady sets TLSReady to False with the problem, and emits a
// Warning Event once, when the problem appears.
func (r *RedisInstanceReconciler) setTLSNotReady(instance *redisv1.RedisInstance, problem string) {
	condition := metav1.Condition{
		Type:               redisv1.ConditionTLSReady,
		Status:             metav1.ConditionFalse,
		Reason:             "InvalidCertificate",
		Message:            problem + "; Redis keeps the certificate it serves",
		ObservedGeneration: instance.Generation,
	}
	if previous := meta.FindStatusCondition(instance.Status.Conditions, redisv1.ConditionTLSReady); previous == nil || previous.Message != condition.Message {
		r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, "InvalidCertificate", "TLS", condition.Message)
	}
	meta.SetStatusCondition(&instance.Status.Conditions, condition)
}

// instancesUsingSecret maps an event of a Secret of the user to the
// RedisInstances that take a certificate, a client CA or the password of a
// RedisUser from it. Those
// Secrets are not owned, so without it a renewal by cert-manager would go
// unnoticed until the next reconcile.
func (r *RedisInstanceReconciler) instancesUsingSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	var instances redisv1.RedisInstanceList
	if err := r.List(ctx, &instances, client.InNamespace(obj.GetNamespace())); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list RedisInstances for a Secret event", "secret", obj.GetName())
		return nil
	}
	var requests []reconcile.Request
	for _, instance := range instances.Items {
		spec := instance.Spec.TLS
		if (spec.ServerSecretRef != nil && spec.ServerSecretRef.Name == obj.GetName()) ||
			(spec.ClientCASecretRef != nil && spec.ClientCASecretRef.Name == obj.GetName()) {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&instance)})
		}
	}
	// The password of a RedisUser, which changes the hash in <instance>-acl.
	var users redisv1.RedisUserList
	if err := r.List(ctx, &users, client.InNamespace(obj.GetNamespace())); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list RedisUsers for a Secret event", "secret", obj.GetName())
		return requests
	}
	for _, user := range users.Items {
		if usesPasswordSecret(&user, obj.GetName()) {
			requests = append(requests, instanceOfUser(ctx, &user)...)
		}
	}
	return requests
}
