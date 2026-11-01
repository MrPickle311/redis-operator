package v1

import (
	"bytes"
	"context"
	"crypto/tls"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/MrPickle311/redis-operator/internal/pki"
)

// The names kustomize gives the Service and the configuration of the webhook,
// with the prefix of config/default.
const (
	ServiceName       = "redis-operator-webhook-service"
	ConfigurationName = "redis-operator-validating-webhook-configuration"
)

// The API server reuses its connections, so a short validity costs little.
const servingCertValidity = 24 * time.Hour

// ServeWithCA makes the webhook server present a certificate from the CA of
// the operator, the one the API server trusts through InjectCABundle. A fresh
// certificate per handshake never expires and needs no Secret: issuing one
// takes well under a millisecond.
func ServeWithCA(ca pki.KeyPair, namespace string) func(*tls.Config) {
	dnsName := ServiceName + "." + namespace + ".svc"
	return func(config *tls.Config) {
		config.GetCertificate = func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			cert, err := pki.IssueServerCert(ca, []string{dnsName}, servingCertValidity)
			if err != nil {
				return nil, err
			}
			pair, err := tls.X509KeyPair(cert.CertPEM, cert.KeyPEM)
			return &pair, err
		}
	}
}

// +kubebuilder:rbac:groups=admissionregistration.k8s.io,resources=validatingwebhookconfigurations,verbs=get;update,resourceNames=redis-operator-validating-webhook-configuration

// InjectCABundle makes the API server trust the CA of the operator for the
// webhook. A new CA comes only with a restart (see controller.CASecretName),
// so once at the start is enough.
func InjectCABundle(ctx context.Context, c client.Client, ca pki.KeyPair) error {
	var configuration admissionregistrationv1.ValidatingWebhookConfiguration
	if err := c.Get(ctx, client.ObjectKey{Name: ConfigurationName}, &configuration); err != nil {
		return err
	}
	changed := false
	for i := range configuration.Webhooks {
		if !bytes.Equal(configuration.Webhooks[i].ClientConfig.CABundle, ca.CertPEM) {
			configuration.Webhooks[i].ClientConfig.CABundle = ca.CertPEM
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return c.Update(ctx, &configuration)
}
