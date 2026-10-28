package controller

import (
	"crypto/x509"
	"encoding/pem"
	"slices"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/pki"
)

func TestRedisDNSNames(t *testing.T) {
	names := redisDNSNames(&redisv1.RedisInstance{ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "prod"}})
	for _, want := range []string{
		"cache-rw", "cache-rw.prod", "cache-rw.prod.svc", "cache-rw.prod.svc.cluster.local",
		"cache-ro.prod.svc", "cache-r.prod.svc.cluster.local",
		"*.cache-hl.prod.svc", // the Pods, which replicas connect to
	} {
		if !slices.Contains(names, want) {
			t.Errorf("redisDNSNames() lacks %s: %v", want, names)
		}
	}
}

func TestNeedsNewRedisCert(t *testing.T) {
	ca, _ := pki.NewCA("ca", time.Hour*24*365)
	otherCA, _ := pki.NewCA("other", time.Hour*24*365)
	names := []string{"cache-rw", "*.cache-hl.prod.svc"}
	secretWith := func(signer pki.KeyPair, names []string) *corev1.Secret {
		cert, err := pki.IssueServerCert(signer, names, 90*24*time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		return &corev1.Secret{Data: map[string][]byte{"tls.crt": cert.CertPEM, "ca.crt": signer.CertPEM}}
	}
	now := time.Now()
	tests := []struct {
		name   string
		secret *corev1.Secret
		now    time.Time
		want   bool
	}{
		{"no certificate yet", &corev1.Secret{}, now, true},
		{"a current certificate", secretWith(ca, names), now, false},
		{"from another CA", secretWith(otherCA, names), now, true},
		{"for other names", secretWith(ca, names[:1]), now, true},
		{"in the last third of its lifetime", secretWith(ca, names), now.Add(61 * 24 * time.Hour), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := needsNewRedisCert(tt.secret, ca, names, tt.now); got != tt.want {
				t.Errorf("needsNewRedisCert() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTLSReadyWarnsOnceWhenTheCAExpiresSoon(t *testing.T) {
	recorder := events.NewFakeRecorder(10)
	r := &RedisInstanceReconciler{Recorder: recorder}
	instance := &redisv1.RedisInstance{ObjectMeta: metav1.ObjectMeta{Name: "cache"}}

	longCA, _ := pki.NewCA("ca", instanceCAValidity)
	r.setTLSReadyCondition(instance, longCA, serverCert{})
	if c := meta.FindStatusCondition(instance.Status.Conditions, redisv1.ConditionTLSReady); c.Reason != "Issued" {
		t.Errorf("Reason = %s for a CA of ten years, want Issued", c.Reason)
	}

	shortCA, _ := pki.NewCA("ca", 30*24*time.Hour)
	r.setTLSReadyCondition(instance, shortCA, serverCert{})
	r.setTLSReadyCondition(instance, shortCA, serverCert{})
	c := meta.FindStatusCondition(instance.Status.Conditions, redisv1.ConditionTLSReady)
	if c.Status != metav1.ConditionTrue || c.Reason != "CAExpiresSoon" {
		t.Errorf("condition = %s %s, want True CAExpiresSoon: TLS still works", c.Status, c.Reason)
	}
	if len(recorder.Events) != 1 || !strings.Contains(<-recorder.Events, "CAExpiresSoon") {
		t.Error("want one CAExpiresSoon Event, not one per reconcile")
	}
}

var _ = Describe("TLS of Redis", func() {
	getSecret := func(name string) (*corev1.Secret, error) {
		secret := &corev1.Secret{}
		return secret, k8sClient.Get(ctx, objectKey(name), secret)
	}
	parse := func(data []byte) *x509.Certificate {
		block, _ := pem.Decode(data)
		Expect(block).NotTo(BeNil())
		cert, err := x509.ParseCertificate(block.Bytes)
		Expect(err).NotTo(HaveOccurred())
		return cert
	}

	It("is on by default, without client certificates", func() {
		instance := newRedisInstance("tls-default", 1, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()})
		Expect(k8sClient.Create(ctx, instance)).To(Succeed())
		DeferCleanup(func() { deleteInstance("tls-default") })
		Expect(instance.Spec.TLS.TLSEnabled()).To(BeTrue())
		Expect(instance.Spec.TLS.Enabled).To(HaveValue(BeTrue()))
		Expect(instance.Spec.TLS.ClientAuth).To(Equal(redisv1.TLSClientAuthNone))
	})

	Context("with the CA of the instance", Ordered, func() {
		const name = "tls-internal"
		BeforeAll(func() {
			Expect(k8sClient.Create(ctx, newRedisInstance(name, 1, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()}))).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
		})
		AfterAll(func() { deleteInstance(name) })

		It("issues a certificate for the Services and the Pods, which clients verify with ca.crt", func() {
			caSecret, err := getSecret(name + "-ca")
			Expect(err).NotTo(HaveOccurred())
			Expect(caSecret.Data).To(HaveKey("ca.key"))
			tlsSecret, err := getSecret(name + "-server-tls")
			Expect(err).NotTo(HaveOccurred())
			Expect(tlsSecret.Type).To(Equal(corev1.SecretTypeTLS))
			Expect(tlsSecret.Data["ca.crt"]).To(Equal(caSecret.Data["ca.crt"]))
			Expect(tlsSecret.Data).NotTo(HaveKey("ca.key"), "the key of the CA must stay in its own Secret")
			for _, secret := range []*corev1.Secret{caSecret, tlsSecret} {
				Expect(metav1.IsControlledBy(secret, getInstance(name))).To(BeTrue())
			}

			roots := x509.NewCertPool()
			roots.AddCert(parse(caSecret.Data["ca.crt"]))
			for _, host := range []string{name + "-rw", name + "-ro." + testNamespace + ".svc", name + "-0." + name + "-hl." + testNamespace + ".svc"} {
				_, err := parse(tlsSecret.Data["tls.crt"]).Verify(x509.VerifyOptions{
					Roots: roots, DNSName: host, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
				})
				Expect(err).NotTo(HaveOccurred(), "a client of %s would not trust the certificate", host)
			}
			Expect(meta.IsStatusConditionTrue(getInstance(name).Status.Conditions, redisv1.ConditionTLSReady)).To(BeTrue())
		})

		It("renews the certificate with the same CA, so clients need no new ca.crt", func() {
			caSecret, _ := getSecret(name + "-ca")
			tlsSecret, _ := getSecret(name + "-server-tls")
			oldCert := tlsSecret.Data["tls.crt"]
			other, _ := pki.NewCA("foreign", time.Hour)
			tlsSecret.Data["ca.crt"] = other.CertPEM // as a stale certificate looks
			Expect(k8sClient.Update(ctx, tlsSecret)).To(Succeed())

			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			tlsSecret, _ = getSecret(name + "-server-tls")
			Expect(tlsSecret.Data["tls.crt"]).NotTo(Equal(oldCert))
			Expect(tlsSecret.Data["ca.crt"]).To(Equal(caSecret.Data["ca.crt"]))
			caAfter, _ := getSecret(name + "-ca")
			Expect(caAfter.Data).To(Equal(caSecret.Data), "the CA changed")
		})
	})

	It("issues nothing with TLS off", func() {
		instance := newRedisInstance("tls-off", 1, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()})
		instance.Spec.TLS = redisv1.TLSSpec{Enabled: new(false)}
		Expect(k8sClient.Create(ctx, instance)).To(Succeed())
		DeferCleanup(deleteInstance, "tls-off")
		Expect(reconcileOnce(objectKey("tls-off"))).To(Succeed())
		for _, secret := range []string{"tls-off-ca", "tls-off-server-tls"} {
			_, err := getSecret(secret)
			Expect(errors.IsNotFound(err)).To(BeTrue(), "%s exists", secret)
		}
	})

	Context("with a certificate of the user", Ordered, func() {
		const name = "tls-byo"
		var userCA pki.KeyPair
		userSecret := func(names []string, signer pki.KeyPair) *corev1.Secret {
			cert, err := pki.IssueServerCert(signer, names, 90*24*time.Hour)
			Expect(err).NotTo(HaveOccurred())
			return &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "user-cert", Namespace: testNamespace},
				Type:       corev1.SecretTypeTLS,
				Data:       map[string][]byte{"tls.crt": cert.CertPEM, "tls.key": cert.KeyPEM, "ca.crt": signer.CertPEM},
			}
		}
		allNames := []string{
			name + "-rw." + testNamespace + ".svc", name + "-ro." + testNamespace + ".svc", name + "-r." + testNamespace + ".svc",
			"*." + name + "-hl." + testNamespace + ".svc",
		}
		tlsReady := func() *metav1.Condition {
			return meta.FindStatusCondition(getInstance(name).Status.Conditions, redisv1.ConditionTLSReady)
		}

		BeforeAll(func() {
			var err error
			userCA, err = pki.NewCA("user CA", 365*24*time.Hour)
			Expect(err).NotTo(HaveOccurred())
			instance := newRedisInstance(name, 2, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()})
			instance.Spec.TLS = redisv1.TLSSpec{ServerSecretRef: &corev1.LocalObjectReference{Name: "user-cert"}}
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
		})
		AfterAll(func() {
			_ = k8sClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "user-cert", Namespace: testNamespace}})
			deleteInstance(name)
		})

		It("waits for the Secret, without a certificate of its own for Redis", func() {
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			_, err := getSecret(name + "-server-tls")
			Expect(errors.IsNotFound(err)).To(BeTrue())
			Expect(tlsReady().Status).To(Equal(metav1.ConditionFalse))
			Expect(tlsReady().Message).To(ContainSubstring("does not exist"))
			Expect(getInstance(name).Status.Conditions).To(ContainElement(SatisfyAll(
				HaveField("Type", redisv1.ConditionReady), HaveField("Reason", "TLSNotReady"))))
		})

		It("serves a certificate that covers every name, with a certificate for the replicas from the CA of the instance", func() {
			Expect(k8sClient.Create(ctx, userSecret(allNames, userCA))).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())

			managed, err := getSecret(name + "-server-tls")
			Expect(err).NotTo(HaveOccurred())
			user, _ := getSecret("user-cert")
			Expect(managed.Data["tls.crt"]).To(Equal(user.Data["tls.crt"]))
			Expect(managed.Data["ca.crt"]).To(Equal(userCA.CertPEM), "clients verify Redis with the CA of the user")
			instanceCA, _ := getSecret(name + "-ca")
			Expect(pki.Verify(managed.Data["replication.crt"], instanceCA.Data["ca.crt"], "", x509.ExtKeyUsageClientAuth)).To(Succeed())
			Expect(managed.Data["trusted.crt"]).To(Equal(append(slices.Clone(userCA.CertPEM), instanceCA.Data["ca.crt"]...)))
			Expect(tlsReady().Reason).To(Equal("Validated"))
		})

		It("keeps serving the last good certificate when the user brings one that misses a name", func() {
			before, _ := getSecret(name + "-server-tls")
			user, _ := getSecret("user-cert")
			bad := userSecret(allNames[:3], userCA) // no Pods: replicas could not verify the primary
			user.Data = bad.Data
			Expect(k8sClient.Update(ctx, user)).To(Succeed())

			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			after, _ := getSecret(name + "-server-tls")
			Expect(after.Data).To(Equal(before.Data), "a certificate that fails the checks reached the Pods")
			Expect(tlsReady().Status).To(Equal(metav1.ConditionFalse))
			Expect(tlsReady().Message).To(ContainSubstring(name + "-0." + name + "-hl." + testNamespace + ".svc"))
		})

		It("takes a renewed certificate once it passes the checks", func() {
			user, _ := getSecret("user-cert")
			user.Data = userSecret(allNames, userCA).Data
			Expect(k8sClient.Update(ctx, user)).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			managed, _ := getSecret(name + "-server-tls")
			Expect(managed.Data["tls.crt"]).To(Equal(user.Data["tls.crt"]))
			Expect(tlsReady().Status).To(Equal(metav1.ConditionTrue))
		})

		It("is reconciled when the Secret of the user changes", func() {
			r := &RedisInstanceReconciler{Client: k8sClient}
			user, _ := getSecret("user-cert")
			Expect(r.instancesUsingSecret(ctx, user)).To(ConsistOf(HaveField("NamespacedName", objectKey(name))))
			other := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: testNamespace}}
			Expect(r.instancesUsingSecret(ctx, other)).To(BeEmpty())
		})
	})

	Context("with client certificates", Ordered, func() {
		const name = "tls-clients"
		var clientCA pki.KeyPair
		BeforeAll(func() {
			var err error
			clientCA, err = pki.NewCA("clients CA", 365*24*time.Hour)
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "clients-ca", Namespace: testNamespace},
				Data:       map[string][]byte{"ca.crt": clientCA.CertPEM},
			})).To(Succeed())
			instance := newRedisInstance(name, 2, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()})
			instance.Spec.TLS = redisv1.TLSSpec{ClientAuth: redisv1.TLSClientAuthRequired, ClientCASecretRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "clients-ca"}, Key: "ca.crt"}}
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
		})
		AfterAll(func() {
			_ = k8sClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "clients-ca", Namespace: testNamespace}})
			deleteInstance(name)
		})

		It("trusts the client CA beside the CA of the instance, and asks every client for a certificate", func() {
			managed, err := getSecret(name + "-server-tls")
			Expect(err).NotTo(HaveOccurred())
			instanceCA, _ := getSecret(name + "-ca")
			Expect(managed.Data["trusted.crt"]).To(Equal(append(slices.Clone(instanceCA.Data["ca.crt"]), clientCA.CertPEM...)))
			Expect(managed.Data["ca.crt"]).To(Equal(instanceCA.Data["ca.crt"]), "clients still verify Redis with the CA of the instance")

			pod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, objectKey(name+"-0"), pod)).To(Succeed())
			Expect(pod.Spec.Containers[0].Command).To(ContainElement("--tls-client-auth=required"))
		})
	})

	It("mounts the TLS Secret of Redis and starts Redis with --tls, unless TLS is off", func() {
		for _, c := range []struct {
			name string
			tls  redisv1.TLSSpec
			want bool
		}{{"tls-pod-on", redisv1.TLSSpec{}, true}, {"tls-pod-off", redisv1.TLSSpec{Enabled: new(false)}, false}} {
			instance := newRedisInstance(c.name, 1, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()})
			instance.Spec.TLS = c.tls
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			DeferCleanup(deleteInstance, c.name)
			Expect(reconcileOnce(objectKey(c.name))).To(Succeed())

			pod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, objectKey(c.name+"-0"), pod)).To(Succeed())
			redis := pod.Spec.Containers[0]
			mounted := false
			for _, m := range redis.VolumeMounts {
				mounted = mounted || (m.MountPath == "/tls" && m.ReadOnly)
			}
			Expect(mounted).To(Equal(c.want), "%s: /tls mounted", c.name)
			Expect(slices.Contains(redis.Command, "--tls")).To(Equal(c.want), "%s: --tls", c.name)
			if c.want {
				Expect(pod.Spec.Volumes).To(ContainElement(HaveField("VolumeSource.Secret.SecretName", c.name+"-server-tls")))
			}
		}
	})

	It("keeps spec.tls.enabled as created, as replicas cannot sync across the modes", func() {
		instance := newRedisInstance("tls-immutable", 1, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()})
		Expect(k8sClient.Create(ctx, instance)).To(Succeed())
		DeferCleanup(deleteInstance, "tls-immutable")
		instance.Spec.TLS.Enabled = new(false)
		err := k8sClient.Update(ctx, instance)
		Expect(errors.IsInvalid(err)).To(BeTrue(), "TLS was switched off: %v", err)
		Expect(err.Error()).To(ContainSubstring("cannot change"))
	})

	DescribeTable("validating spec.tls with CEL rules",
		func(tls redisv1.TLSSpec, wantError string) {
			instance := newRedisInstance("cel-tls", 1, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()})
			instance.Spec.TLS = tls
			err := k8sClient.Create(ctx, instance)
			if wantError == "" {
				Expect(err).NotTo(HaveOccurred())
				Expect(k8sClient.Delete(ctx, instance)).To(Succeed())
				return
			}
			Expect(errors.IsInvalid(err)).To(BeTrue(), "expected an Invalid error, got: %v", err)
			Expect(err.Error()).To(ContainSubstring(wantError))
		},
		Entry("accepts client certificates checked against a CA", redisv1.TLSSpec{
			ClientAuth: redisv1.TLSClientAuthRequired, ClientCASecretRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "clients-ca"}, Key: "ca.crt"}}, ""),
		Entry("rejects client certificates without a CA", redisv1.TLSSpec{ClientAuth: redisv1.TLSClientAuthOptional}, "clientCASecretRef"),
		Entry("rejects a certificate with TLS off", redisv1.TLSSpec{
			Enabled: new(false), ServerSecretRef: &corev1.LocalObjectReference{Name: "mine"}}, "need TLS enabled"),
	)
})

func TestValidateUserCert(t *testing.T) {
	ca, _ := pki.NewCA("user CA", 365*24*time.Hour)
	other, _ := pki.NewCA("other CA", 365*24*time.Hour)
	names := []string{"cache-rw.prod.svc", "cache-0.cache-hl.prod.svc"}
	issue := func(signer pki.KeyPair, names []string, validity time.Duration) pki.KeyPair {
		cert, err := pki.IssueServerCert(signer, names, validity)
		if err != nil {
			t.Fatal(err)
		}
		return cert
	}
	good := issue(ca, []string{"cache-rw.prod.svc", "*.cache-hl.prod.svc"}, 90*24*time.Hour)
	another := issue(ca, names, 90*24*time.Hour)
	tests := []struct {
		name string
		user serverCert
		want string // a part of the problem, "" when valid
	}{
		{"covers every name, the Pods by a wildcard", serverCert{good.CertPEM, good.KeyPEM, ca.CertPEM}, ""},
		{"no key", serverCert{good.CertPEM, nil, ca.CertPEM}, "no tls.key"},
		{"the key of another certificate", serverCert{good.CertPEM, another.KeyPEM, ca.CertPEM}, "does not belong"},
		{"no CA", serverCert{good.CertPEM, good.KeyPEM, nil}, "no ca.crt"},
		{"from another CA", serverCert{good.CertPEM, good.KeyPEM, other.CertPEM}, "does not verify"},
		{"misses the Pods", func() serverCert {
			c := issue(ca, names[:1], 90*24*time.Hour)
			return serverCert{c.CertPEM, c.KeyPEM, ca.CertPEM}
		}(), "does not cover cache-0.cache-hl.prod.svc"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validateUserCert(tt.user, names)
			if (got == "") != (tt.want == "") || !strings.Contains(got, tt.want) {
				t.Errorf("validateUserCert() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNeedsNewReplicationCert(t *testing.T) {
	ca, _ := pki.NewCA("ca", 365*24*time.Hour)
	other, _ := pki.NewCA("other", 365*24*time.Hour)
	cert, _ := pki.IssueClientCert(ca, "_replication", 90*24*time.Hour)
	foreign, _ := pki.IssueClientCert(other, "_replication", 90*24*time.Hour)
	server, _ := pki.IssueServerCert(ca, []string{"cache-rw"}, 90*24*time.Hour)
	now := time.Now()
	for name, tt := range map[string]struct {
		cert []byte
		now  time.Time
		want bool
	}{
		"none yet":                  {nil, now, true},
		"current":                   {cert.CertPEM, now, false},
		"from another CA":           {foreign.CertPEM, now, true},
		"a server certificate":      {server.CertPEM, now, true},
		"in the last third of life": {cert.CertPEM, now.Add(61 * 24 * time.Hour), true},
	} {
		if got := needsNewReplicationCert(tt.cert, ca, tt.now); got != tt.want {
			t.Errorf("%s: needsNewReplicationCert() = %v, want %v", name, got, tt.want)
		}
	}
}
