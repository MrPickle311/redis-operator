package instancemanager

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// StatusPort serves the API of the Instance Manager to the operator, over mTLS.
const StatusPort = 8000

// OperatorCommonName is the only client certificate name the API accepts.
const OperatorCommonName = "redis-operator"

// InstanceStatus is what GET /status returns: the replication state of Redis
// as INFO replication reports it.
type InstanceStatus struct {
	Role       string `json:"role"` // "master" or "slave"
	MasterHost string `json:"masterHost,omitempty"`
	MasterPort int    `json:"masterPort,omitempty"`
}

// statusHandler serves the API of the Instance Manager.
func statusHandler(rdb *redis.Client) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		status, err := readStatus(r.Context(), rdb)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(status)
	})
	return mux
}

func readStatus(ctx context.Context, rdb *redis.Client) (InstanceStatus, error) {
	info, err := rdb.InfoMap(ctx, "replication").Result()
	if err != nil {
		return InstanceStatus{}, err
	}
	replication := info["Replication"]

	status := InstanceStatus{Role: replication["role"], MasterHost: replication["master_host"]}
	if port, ok := replication["master_port"]; ok {
		if status.MasterPort, err = strconv.Atoi(port); err != nil {
			return InstanceStatus{}, err
		}
	}
	return status, nil
}

// serverTLSConfig accepts only clients with a certificate from the CA in dir
// and the OperatorCommonName. The files are read on every handshake, because
// the kubelet updates the mounted Secret in place when the operator renews it.
func serverTLSConfig(dir string) *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			return loadServerTLSConfig(dir)
		},
	}
}

func loadServerTLSConfig(dir string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key"))
	if err != nil {
		return nil, err
	}
	caPEM, err := os.ReadFile(filepath.Join(dir, "ca.crt"))
	if err != nil {
		return nil, err
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("no CA certificate found in ca.crt")
	}

	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		// Verification also requires the ClientAuth usage, so the server
		// certificate of another instance cannot be used to log in.
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  clientCAs,
		VerifyConnection: func(cs tls.ConnectionState) error {
			if cn := cs.PeerCertificates[0].Subject.CommonName; cn != OperatorCommonName {
				return fmt.Errorf("client %q is not the operator", cn)
			}
			return nil
		},
	}, nil
}
