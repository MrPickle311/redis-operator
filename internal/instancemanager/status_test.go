package instancemanager

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/MrPickle311/redis-operator/internal/pki"
)

// The test server presents a certificate for localhost, as an instance does
// for its Pod DNS names.
const serverName = "localhost"

func TestStatusAPIAcceptsOnlyTheOperator(t *testing.T) {
	ca, otherCA := mustCA(t), mustCA(t)
	dir := t.TempDir()
	serverCert := writeServerCert(t, dir, ca)
	srv := startStatusServer(t, dir)

	tests := []struct {
		name       string
		clientCert *pki.KeyPair
		wantOK     bool
	}{
		{"the operator", mustClientCert(t, ca, OperatorCommonName), true},
		{"a client without a certificate", nil, false},
		{"another client of the same CA", mustClientCert(t, ca, "intruder"), false},
		{"an instance with its server certificate", &serverCert, false},
		{"the operator name signed by another CA", mustClientCert(t, otherCA, OperatorCommonName), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := newClient(t, ca, tt.clientCert).Get(srv.URL + "/status")
			if err == nil {
				_ = resp.Body.Close()
			}
			if gotOK := err == nil; gotOK != tt.wantOK {
				t.Errorf("GET /status succeeded = %v, want %v (error: %v)", gotOK, tt.wantOK, err)
			}
		})
	}
}

func TestStatusAPIServesARenewedCertificateWithoutRestart(t *testing.T) {
	ca := mustCA(t)
	dir := t.TempDir()
	writeServerCert(t, dir, ca)
	srv := startStatusServer(t, dir)
	client := newClient(t, ca, mustClientCert(t, ca, OperatorCommonName))
	client.Transport.(*http.Transport).DisableKeepAlives = true // a new handshake per request

	before := servedSerial(t, client, srv.URL)
	writeServerCert(t, dir, ca) // what the kubelet does when the operator renews the Secret
	after := servedSerial(t, client, srv.URL)

	if before == after {
		t.Errorf("server still presents certificate %s after renewal", before)
	}
}

func TestStatusIsUnavailableWithoutRedis(t *testing.T) {
	// Nothing listens on port 1, like with a dead redis-server.
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	defer func() { _ = rdb.Close() }()
	srv := httptest.NewServer(statusHandler(rdb))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/status")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("GET /status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
}

// startStatusServer serves 200 on every path with the TLS setup of the status
// API, reading the certificates from dir.
func startStatusServer(t *testing.T, dir string) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.TLS = serverTLSConfig(dir)
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

// newClient trusts servers of the CA and presents clientCert, if any.
func newClient(t *testing.T, ca pki.KeyPair, clientCert *pki.KeyPair) *http.Client {
	t.Helper()
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.CertPEM)
	config := &tls.Config{RootCAs: roots, ServerName: serverName}
	if clientCert != nil {
		pair, err := tls.X509KeyPair(clientCert.CertPEM, clientCert.KeyPEM)
		if err != nil {
			t.Fatal(err)
		}
		config.Certificates = []tls.Certificate{pair}
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: config}}
}

func servedSerial(t *testing.T, client *http.Client, url string) string {
	t.Helper()
	resp, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	return resp.TLS.PeerCertificates[0].SerialNumber.String()
}

// writeServerCert issues a server certificate and writes it to dir the way the
// operator's Secret is mounted.
func writeServerCert(t *testing.T, dir string, ca pki.KeyPair) pki.KeyPair {
	t.Helper()
	cert, err := pki.IssueServerCert(ca, []string{serverName}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	for file, data := range map[string][]byte{"tls.crt": cert.CertPEM, "tls.key": cert.KeyPEM, "ca.crt": ca.CertPEM} {
		if err := os.WriteFile(filepath.Join(dir, file), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return cert
}

func mustClientCert(t *testing.T, ca pki.KeyPair, commonName string) *pki.KeyPair {
	t.Helper()
	cert, err := pki.IssueClientCert(ca, commonName, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return &cert
}

func mustCA(t *testing.T) pki.KeyPair {
	t.Helper()
	ca, err := pki.NewCA("test-ca", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return ca
}
