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
	r.setTLSReadyCondition(instance, longCA)
	if c := meta.FindStatusCondition(instance.Status.Conditions, redisv1.ConditionTLSReady); c.Reason != "Issued" {
		t.Errorf("Reason = %s for a CA of ten years, want Issued", c.Reason)
	}

	shortCA, _ := pki.NewCA("ca", 30*24*time.Hour)
	r.setTLSReadyCondition(instance, shortCA)
	r.setTLSReadyCondition(instance, shortCA)
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

	DescribeTable("issues nothing of its own",
		func(name string, tls redisv1.TLSSpec) {
			instance := newRedisInstance(name, 1, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()})
			instance.Spec.TLS = tls
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			DeferCleanup(func() { deleteInstance(name) })
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
			for _, secret := range []string{name + "-ca", name + "-server-tls"} {
				_, err := getSecret(secret)
				Expect(errors.IsNotFound(err)).To(BeTrue(), "%s exists", secret)
			}
		},
		Entry("with TLS off", "tls-off", redisv1.TLSSpec{Enabled: new(false)}),
		Entry("with a certificate of the user", "tls-byo", redisv1.TLSSpec{ServerSecretRef: &corev1.LocalObjectReference{Name: "mine"}}),
	)

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
