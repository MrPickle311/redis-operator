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
	rdb := deadRedis(t)
	srv := httptest.NewServer(apiHandler(rdb, &epochGuard{path: filepath.Join(t.TempDir(), "epoch")}, &readiness{}))
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

func TestParseStatus(t *testing.T) {
	tests := []struct {
		name        string
		replication map[string]string
		want        InstanceStatus
		wantErr     bool
	}{
		{
			name:        "primary",
			replication: map[string]string{"role": "master", "master_replid": "abc", "master_repl_offset": "1500"},
			want:        InstanceStatus{Role: "master", ReplID: "abc", Offset: 1500},
		},
		{
			name: "replica",
			replication: map[string]string{
				"role": "slave", "master_host": "cache-0.cache-hl.prod.svc", "master_port": "6379",
				"master_replid": "abc", "master_repl_offset": "1490",
			},
			want: InstanceStatus{Role: "slave", MasterHost: "cache-0.cache-hl.prod.svc", MasterPort: 6379, ReplID: "abc", Offset: 1490},
		},
		{name: "bad port", replication: map[string]string{"role": "slave", "master_port": "x"}, wantErr: true},
		{name: "bad offset", replication: map[string]string{"role": "master", "master_repl_offset": "x"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseStatus(tt.replication)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseStatus() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("parseStatus() = %+v, want %+v", got, tt.want)
			}
		})
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

func TestCountKeys(t *testing.T) {
	keyspace := map[string]string{"db0": "keys=150,expires=0,avg_ttl=0", "db3": "keys=7,expires=2,avg_ttl=100"}
	if got := countKeys(keyspace); got != 157 {
		t.Errorf("countKeys() = %d, want the keys of every database", got)
	}
	if got := countKeys(nil); got != 0 {
		t.Errorf("countKeys() = %d for no databases, want 0", got)
	}
}
