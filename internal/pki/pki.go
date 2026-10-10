// Package pki is the internal certificate authority of the operator. It secures
// the API between the operator and the Instance Managers (mTLS) and is
// independent of the TLS that clients use to talk to Redis.
package pki

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"time"
)

// KeyPair is a certificate with its private key, both PEM-encoded, the way
// they are stored in a Secret (tls.crt and tls.key).
type KeyPair struct {
	CertPEM []byte
	KeyPEM  []byte
}

// NewCA creates a self-signed certificate authority.
func NewCA(commonName string, validity time.Duration) (KeyPair, error) {
	template := &x509.Certificate{
		Subject:               pkix.Name{CommonName: commonName},
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	return issue(template, validity, nil, nil)
}

// IssueServerCert issues a certificate that may only be used by a TLS server
// for the given DNS names.
func IssueServerCert(ca KeyPair, dnsNames []string, validity time.Duration) (KeyPair, error) {
	if len(dnsNames) == 0 {
		return KeyPair{}, errors.New("a server certificate needs at least one DNS name")
	}
	template := &x509.Certificate{
		Subject:     pkix.Name{CommonName: dnsNames[0]},
		DNSNames:    dnsNames,
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	return issueSigned(ca, template, validity)
}

// IssueClientCert issues a certificate that may only be used by a TLS client;
// the server identifies the client by its common name.
func IssueClientCert(ca KeyPair, commonName string, validity time.Duration) (KeyPair, error) {
	template := &x509.Certificate{
		Subject:     pkix.Name{CommonName: commonName},
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	return issueSigned(ca, template, validity)
}

// NeedsRenewal reports whether less than a third of the certificate's
// lifetime is left at the given time.
func NeedsRenewal(certPEM []byte, now time.Time) (bool, error) {
	cert, err := parseCert(certPEM)
	if err != nil {
		return false, err
	}
	lifetime := cert.NotAfter.Sub(cert.NotBefore)
	renewAt := cert.NotBefore.Add(lifetime * 2 / 3)
	return !now.Before(renewAt), nil
}

func issueSigned(ca KeyPair, template *x509.Certificate, validity time.Duration) (KeyPair, error) {
	caCert, err := parseCert(ca.CertPEM)
	if err != nil {
		return KeyPair{}, err
	}
	caKey, err := parseKey(ca.KeyPEM)
	if err != nil {
		return KeyPair{}, err
	}
	return issue(template, validity, caCert, caKey)
}

// issue generates a new key and signs the template with the parent; without a
// parent the certificate is self-signed.
func issue(template *x509.Certificate, validity time.Duration, parent *x509.Certificate, parentKey *ecdsa.PrivateKey) (KeyPair, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return KeyPair{}, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return KeyPair{}, err
	}

	now := time.Now()
	template.SerialNumber = serial
	template.NotBefore = now.Add(-time.Minute) // tolerate small clock skew between nodes
	template.NotAfter = now.Add(validity)

	if parent == nil {
		parent, parentKey = template, key
	}
	der, err := x509.CreateCertificate(rand.Reader, template, parent, &key.PublicKey, parentKey)
	if err != nil {
		return KeyPair{}, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return KeyPair{}, err
	}

	return KeyPair{
		CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	}, nil
}

func parseCert(certPEM []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, errors.New("no PEM certificate found")
	}
	return x509.ParseCertificate(block.Bytes)
}

func parseKey(keyPEM []byte) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode(keyPEM)
	if block == nil || block.Type != "EC PRIVATE KEY" {
		return nil, errors.New("no PEM EC private key found")
	}
	return x509.ParseECPrivateKey(block.Bytes)
}
