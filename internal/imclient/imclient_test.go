package imclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/MrPickle311/redis-operator/internal/instancemanager"
	"github.com/MrPickle311/redis-operator/internal/pki"
)

var replica = instancemanager.InstanceStatus{Role: "slave", MasterHost: "cache-0.cache-hl.prod.svc", MasterPort: 6379}

func TestStatusAuthenticatesAsTheOperator(t *testing.T) {
	ca := mustCA(t)
	client := newTestClient(t, ca, startInstanceManager(t, ca, func(w http.ResponseWriter, r *http.Request) {
		// The server already verified the chain and the ClientAuth usage.
		if cn := r.TLS.PeerCertificates[0].Subject.CommonName; cn != instancemanager.OperatorCommonName {
			http.Error(w, "unexpected client "+cn, http.StatusForbidden)
			return
		}
		_ = json.NewEncoder(w).Encode(replica)
	}))

	got, err := client.Status(context.Background(), "localhost")
	if err != nil {
		t.Fatal(err)
	}
	if got != replica {
		t.Errorf("Status() = %+v, want %+v", got, replica)
	}
}

func TestStatusRejectsAServerFromAnotherCA(t *testing.T) {
	client := newTestClient(t, mustCA(t), startInstanceManager(t, mustCA(t), func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(replica)
	}))

	if _, err := client.Status(context.Background(), "localhost"); err == nil {
		t.Fatal("Status() trusted a server whose certificate comes from another CA")
	}
}

func TestStatusReturnsTheErrorOfTheInstanceManager(t *testing.T) {
	ca := mustCA(t)
	client := newTestClient(t, ca, startInstanceManager(t, ca, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "connection refused", http.StatusServiceUnavailable)
	}))

	_, err := client.Status(context.Background(), "localhost")
	if err == nil || !strings.Contains(err.Error(), "503 Service Unavailable: connection refused") {
		t.Fatalf("Status() error = %v, want the 503 with its message", err)
	}
}

func TestReplicaOfSendsTheEpochAndThePrimary(t *testing.T) {
	ca := mustCA(t)
	var got instancemanager.ReplicaOfRequest
	client := newTestClient(t, ca, startInstanceManager(t, ca, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/replicaof" {
			http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusNotFound)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		_ = json.NewEncoder(w).Encode(instancemanager.ReplicaOfResponse{Changed: true})
	}))

	changed, err := client.ReplicaOf(context.Background(), "localhost", 3, replica.MasterHost)
	if err != nil {
		t.Fatal(err)
	}
	want := instancemanager.ReplicaOfRequest{Epoch: 3, PrimaryHost: replica.MasterHost}
	if got != want || !changed {
		t.Errorf("ReplicaOf() sent %+v and returned changed=%v, want %+v and true", got, changed, want)
	}
}

// startInstanceManager serves handler over mTLS like an Instance Manager: with
// a server certificate for localhost and only clients from ca.
func startInstanceManager(t *testing.T, ca pki.KeyPair, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	cert, err := pki.IssueServerCert(ca, []string{"localhost"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(cert.CertPEM, cert.KeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	clientCAs := x509.NewCertPool()
	clientCAs.AppendCertsFromPEM(ca.CertPEM)

	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientCAs}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

// newTestClient returns a client of ca that calls srv instead of the status port.
func newTestClient(t *testing.T, ca pki.KeyPair, srv *httptest.Server) *Client {
	t.Helper()
	client, err := New(ca)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if client.port, err = strconv.Atoi(u.Port()); err != nil {
		t.Fatal(err)
	}
	return client
}

func mustCA(t *testing.T) pki.KeyPair {
	t.Helper()
	ca, err := pki.NewCA("test-ca", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return ca
}
