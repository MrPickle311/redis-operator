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
	// ReplID and Offset place the dataset in the replication history: a
	// replica with the ReplID of its primary and the same Offset has every
	// write of the primary.
	ReplID string `json:"replID,omitempty"`
	Offset int64  `json:"offset"`
}

// apiHandler serves the API of the Instance Manager. epochPath is where it
// keeps the highest epoch it has accepted.
func apiHandler(rdb *redis.Client, epochPath string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		status, err := readStatus(r.Context(), rdb)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, status)
	})

	guard := &epochGuard{path: epochPath}
	mux.HandleFunc("POST /replicaof", func(w http.ResponseWriter, r *http.Request) {
		var req ReplicaOfRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Epoch < 1 || req.PrimaryHost == "" {
			http.Error(w, "expected a JSON body with epoch >= 1 and primaryHost", http.StatusBadRequest)
			return
		}
		runCommand(w, guard, req.Epoch, func() (bool, error) { return replicaOf(r.Context(), rdb, req.PrimaryHost) })
	})
	mux.HandleFunc("POST /fence", func(w http.ResponseWriter, r *http.Request) {
		if epoch, ok := decodeEpoch(w, r); ok {
			runCommand(w, guard, epoch, func() (bool, error) { return true, fence(r.Context(), rdb) })
		}
	})
	mux.HandleFunc("POST /promote", func(w http.ResponseWriter, r *http.Request) {
		if epoch, ok := decodeEpoch(w, r); ok {
			runCommand(w, guard, epoch, func() (bool, error) { return promote(r.Context(), rdb) })
		}
	})
	return mux
}

// decodeEpoch reads an EpochRequest. It answers 400 and returns false when
// the body is not one.
func decodeEpoch(w http.ResponseWriter, r *http.Request) (int64, bool) {
	var req EpochRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Epoch < 1 {
		http.Error(w, "expected a JSON body with epoch >= 1", http.StatusBadRequest)
		return 0, false
	}
	return req.Epoch, true
}

// runCommand runs command under the epoch guard and writes the outcome:
// 409 for a stale epoch, 503 when Redis fails.
func runCommand(w http.ResponseWriter, guard *epochGuard, epoch int64, command func() (bool, error)) {
	var resp CommandResponse
	err := guard.run(epoch, func() (err error) {
		resp.Changed, err = command()
		return err
	})
	switch {
	case errors.Is(err, errStaleEpoch):
		http.Error(w, err.Error(), http.StatusConflict)
	case err != nil:
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	default:
		writeJSON(w, resp)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func readStatus(ctx context.Context, rdb *redis.Client) (InstanceStatus, error) {
	info, err := rdb.InfoMap(ctx, "replication").Result()
	if err != nil {
		return InstanceStatus{}, err
	}
	return parseStatus(info["Replication"])
}

// parseStatus reads the fields of INFO replication the operator needs.
func parseStatus(replication map[string]string) (InstanceStatus, error) {
	status := InstanceStatus{
		Role:       replication["role"],
		MasterHost: replication["master_host"],
		ReplID:     replication["master_replid"],
	}
	var err error
	if port, ok := replication["master_port"]; ok {
		if status.MasterPort, err = strconv.Atoi(port); err != nil {
			return InstanceStatus{}, err
		}
	}
	if offset, ok := replication["master_repl_offset"]; ok {
		if status.Offset, err = strconv.ParseInt(offset, 10, 64); err != nil {
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
