package pki

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"
)

const (
	year = 365 * 24 * time.Hour
	// instanceDNS is the wildcard that covers every Pod of RedisInstance "cache" in namespace "prod".
	instanceDNS = "*.cache-hl.prod.svc"
)

// verify checks cert against the CA for the given usage and, if set, DNS name.
func verify(t *testing.T, ca, cert KeyPair, usage x509.ExtKeyUsage, dnsName string) error {
	t.Helper()
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca.CertPEM) {
		t.Fatal("CA certificate is not valid PEM")
	}
	_, err := parse(t, cert).Verify(x509.VerifyOptions{
		Roots:     roots,
		DNSName:   dnsName,
		KeyUsages: []x509.ExtKeyUsage{usage},
	})
	return err
}

func parse(t *testing.T, kp KeyPair) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(kp.CertPEM)
	if block == nil {
		t.Fatal("certificate is not PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

func mustCA(t *testing.T) KeyPair {
	t.Helper()
	ca, err := NewCA("test-ca", 10*year)
	if err != nil {
		t.Fatal(err)
	}
	return ca
}

func TestServerCertCoversEveryPodOfTheInstance(t *testing.T) {
	ca := mustCA(t)
	server, err := IssueServerCert(ca, []string{instanceDNS}, year)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		dnsName string
		valid   bool
	}{
		{"cache-0.cache-hl.prod.svc", true},
		{"cache-7.cache-hl.prod.svc", true},
		{"other-0.other-hl.prod.svc", false}, // another RedisInstance
		{"cache-0.cache-hl.dev.svc", false},  // same name, other namespace
	}
	for _, tt := range tests {
		t.Run(tt.dnsName, func(t *testing.T) {
			err := verify(t, ca, server, x509.ExtKeyUsageServerAuth, tt.dnsName)
			if (err == nil) != tt.valid {
				t.Errorf("verify(%s) error = %v, want valid = %v", tt.dnsName, err, tt.valid)
			}
		})
	}
}

// A server key lives inside a Redis Pod. Whoever takes over that Pod must not be
// able to use it to call the Instance Manager API of other instances.
func TestServerCertCannotAuthenticateAsClient(t *testing.T) {
	ca := mustCA(t)
	server, err := IssueServerCert(ca, []string{instanceDNS}, year)
	if err != nil {
		t.Fatal(err)
	}

	if err := verify(t, ca, server, x509.ExtKeyUsageClientAuth, ""); err == nil {
		t.Fatal("server certificate was accepted for client authentication")
	}
}

func TestClientCertAuthenticatesWithItsCommonName(t *testing.T) {
	ca := mustCA(t)
	client, err := IssueClientCert(ca, "redis-operator", year)
	if err != nil {
		t.Fatal(err)
	}

	if err := verify(t, ca, client, x509.ExtKeyUsageClientAuth, ""); err != nil {
		t.Fatalf("client certificate rejected: %v", err)
	}
	if err := verify(t, ca, client, x509.ExtKeyUsageServerAuth, ""); err == nil {
		t.Error("client certificate was accepted as a server certificate")
	}
	if cn := parse(t, client).Subject.CommonName; cn != "redis-operator" {
		t.Errorf("CommonName = %q, want redis-operator", cn)
	}
}

func TestCertFromAnotherCAIsRejected(t *testing.T) {
	ca, otherCA := mustCA(t), mustCA(t)
	client, err := IssueClientCert(otherCA, "redis-operator", year)
	if err != nil {
		t.Fatal(err)
	}

	if err := verify(t, ca, client, x509.ExtKeyUsageClientAuth, ""); err == nil {
		t.Fatal("certificate signed by another CA was accepted")
	}
}

func TestKeyPairsLoadIntoTLS(t *testing.T) {
	ca := mustCA(t)
	server, err := IssueServerCert(ca, []string{instanceDNS}, year)
	if err != nil {
		t.Fatal(err)
	}
	client, err := IssueClientCert(ca, "redis-operator", year)
	if err != nil {
		t.Fatal(err)
	}

	for name, kp := range map[string]KeyPair{"ca": ca, "server": server, "client": client} {
		if _, err := tls.X509KeyPair(kp.CertPEM, kp.KeyPEM); err != nil {
			t.Errorf("%s: certificate and key do not form a TLS key pair: %v", name, err)
		}
	}
}

func TestNeedsRenewalInTheLastThirdOfTheLifetime(t *testing.T) {
	ca := mustCA(t)
	server, err := IssueServerCert(ca, []string{instanceDNS}, 3*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	issued := time.Now()

	tests := []struct {
		name  string
		at    time.Time
		renew bool
	}{
		{"freshly issued", issued, false},
		{"just before two thirds", issued.Add(1*time.Hour + 50*time.Minute), false},
		{"just after two thirds", issued.Add(2*time.Hour + 10*time.Minute), true},
		{"expired", issued.Add(4 * time.Hour), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			renew, err := NeedsRenewal(server.CertPEM, tt.at)
			if err != nil {
				t.Fatal(err)
			}
			if renew != tt.renew {
				t.Errorf("NeedsRenewal() = %v, want %v", renew, tt.renew)
			}
		})
	}
}

func TestNeedsRenewalRejectsGarbage(t *testing.T) {
	if _, err := NeedsRenewal([]byte("not a certificate"), time.Now()); err == nil {
		t.Fatal("NeedsRenewal() accepted invalid PEM")
	}
}
