package v1

import (
	"crypto/tls"
	"crypto/x509"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/MrPickle311/redis-operator/internal/pki"
)

var _ = Describe("Webhook certificate", func() {
	var ca pki.KeyPair

	BeforeEach(func() {
		var err error
		ca, err = pki.NewCA("test", time.Hour)
		Expect(err).NotTo(HaveOccurred())
	})

	// handshake dials a server set up by ServeWithCA the way the API server
	// does: through the Service name, trusting only the CA bundle.
	handshake := func(serverName string, roots []byte) error {
		config := &tls.Config{}
		ServeWithCA(ca, "redis-operator-system")(config)
		listener, err := tls.Listen("tcp", "127.0.0.1:0", config)
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = listener.Close() }()
		go func() {
			if conn, err := listener.Accept(); err == nil {
				_ = conn.(*tls.Conn).Handshake()
				_ = conn.Close()
			}
		}()
		pool := x509.NewCertPool()
		Expect(pool.AppendCertsFromPEM(roots)).To(BeTrue())
		conn, err := tls.Dial("tcp", listener.Addr().String(), &tls.Config{RootCAs: pool, ServerName: serverName})
		if err == nil {
			_ = conn.Close()
		}
		return err
	}

	It("serves a certificate the API server trusts through the CA bundle", func() {
		Expect(handshake(ServiceName+".redis-operator-system.svc", ca.CertPEM)).To(Succeed())
	})

	It("serves no other name and no other CA", func() {
		Expect(handshake(ServiceName+".default.svc", ca.CertPEM)).NotTo(Succeed())
		other, err := pki.NewCA("other", time.Hour)
		Expect(err).NotTo(HaveOccurred())
		Expect(handshake(ServiceName+".redis-operator-system.svc", other.CertPEM)).NotTo(Succeed())
	})

	It("injects the CA bundle into every webhook, and updates nothing after", func() {
		c := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(&admissionregistrationv1.ValidatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{Name: ConfigurationName},
			Webhooks:   []admissionregistrationv1.ValidatingWebhook{{Name: "a"}, {Name: "b"}},
		}).Build()
		get := func() *admissionregistrationv1.ValidatingWebhookConfiguration {
			var configuration admissionregistrationv1.ValidatingWebhookConfiguration
			Expect(c.Get(ctx, client.ObjectKey{Name: ConfigurationName}, &configuration)).To(Succeed())
			return &configuration
		}

		Expect(InjectCABundle(ctx, c, ca)).To(Succeed())
		injected := get()
		for _, webhook := range injected.Webhooks {
			Expect(webhook.ClientConfig.CABundle).To(Equal(ca.CertPEM))
		}

		Expect(InjectCABundle(ctx, c, ca)).To(Succeed())
		Expect(get().ResourceVersion).To(Equal(injected.ResourceVersion))
	})
})
