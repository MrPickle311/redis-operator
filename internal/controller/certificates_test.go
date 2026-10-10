package controller

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/pki"
)

// testCA signs the Instance Manager certificates in every reconcile of the
// suite; previousCA stands for a CA that has since been rotated.
var testCA, previousCA = mustCA(), mustCA()

var _ = Describe("Instance Manager certificates", func() {
	Describe("EnsureCA", func() {
		It("creates the CA Secret on first start and reuses it afterwards", func() {
			DeferCleanup(k8sClient.Delete, ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: CASecretName, Namespace: testNamespace},
			})

			created, err := EnsureCA(ctx, k8sClient, testNamespace)
			Expect(err).NotTo(HaveOccurred())

			secret := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, objectKey(CASecretName), secret)).To(Succeed())
			Expect(secret.Type).To(Equal(corev1.SecretTypeTLS))

			Expect(EnsureCA(ctx, k8sClient, testNamespace)).To(Equal(created), "a restart must not replace the CA")
		})
	})

	Context("when reconciling a RedisInstance", func() {
		const name = "tls-test"
		secretKey := objectKey(name + "-im-tls")

		BeforeEach(func() {
			instance := newRedisInstance(name, 2, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()})
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			DeferCleanup(k8sClient.Delete, ctx, instance)
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
		})

		It("issues a server certificate, owned by the RedisInstance, for every Pod of it", func() {
			secret := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, secretKey, secret)).To(Succeed())

			Expect(metav1.GetControllerOf(secret)).To(HaveField("Name", name))
			Expect(secret.Data).To(HaveKeyWithValue("ca.crt", testCA.CertPEM))
			for _, pod := range []string{name + "-0", name + "-1"} {
				Expect(verifyServerCert(secret, fmt.Sprintf("%s.%s-hl.%s.svc", pod, name, testNamespace))).
					To(Succeed(), "Pod %s", pod)
			}
		})

		It("mounts the certificate read-only into the redis container", func() {
			pod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, objectKey(name+"-0"), pod)).To(Succeed())

			Expect(pod.Spec.Volumes).To(ContainElement(HaveField("Secret.SecretName", secretKey.Name)))
			Expect(pod.Spec.Containers[0].VolumeMounts).To(ContainElement(corev1.VolumeMount{
				Name: "certificates", MountPath: "/certificates", ReadOnly: true,
			}))
		})

		It("keeps a valid certificate", func() {
			before := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, secretKey, before)).To(Succeed())

			Expect(reconcileOnce(objectKey(name))).To(Succeed())

			after := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, secretKey, after)).To(Succeed())
			Expect(after.Data).To(Equal(before.Data))
		})

		DescribeTable("replaces a stale certificate with one from the current CA",
			func(issuer *pki.KeyPair, validity time.Duration) {
				stale, err := pki.IssueServerCert(*issuer, []string{fmt.Sprintf("*.%s-hl.%s.svc", name, testNamespace)}, validity)
				Expect(err).NotTo(HaveOccurred())

				secret := &corev1.Secret{}
				Expect(k8sClient.Get(ctx, secretKey, secret)).To(Succeed())
				secret.Data = map[string][]byte{"tls.crt": stale.CertPEM, "tls.key": stale.KeyPEM, "ca.crt": issuer.CertPEM}
				Expect(k8sClient.Update(ctx, secret)).To(Succeed())

				Expect(reconcileOnce(objectKey(name))).To(Succeed())

				Expect(k8sClient.Get(ctx, secretKey, secret)).To(Succeed())
				Expect(secret.Data["tls.crt"]).NotTo(Equal(stale.CertPEM))
				Expect(secret.Data).To(HaveKeyWithValue("ca.crt", testCA.CertPEM))
				Expect(verifyServerCert(secret, fmt.Sprintf("%s-0.%s-hl.%s.svc", name, name, testNamespace))).To(Succeed())
			},
			// Pointers: Entry parameters are evaluated when the tree is built.
			Entry("when it is in the last third of its lifetime", &testCA, time.Millisecond),
			Entry("when a previous CA signed it", &previousCA, 90*24*time.Hour),
		)
	})
})

// verifyServerCert checks that the Secret holds a matching key pair whose
// certificate the Secret's own ca.crt trusts as the TLS server dnsName.
func verifyServerCert(secret *corev1.Secret, dnsName string) error {
	pair, err := tls.X509KeyPair(secret.Data["tls.crt"], secret.Data["tls.key"])
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(secret.Data["ca.crt"])
	_, err = pair.Leaf.Verify(x509.VerifyOptions{DNSName: dnsName, Roots: roots})
	return err
}

func mustCA() pki.KeyPair {
	ca, err := pki.NewCA("test-ca", time.Hour)
	if err != nil {
		panic(err)
	}
	return ca
}
