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
	"slices"
	"strconv"
	"strings"
	"sync"

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
	// Keys is the number of keys in every database.
	Keys int64 `json:"keys"`
	// AwaitingApproval tells that this restarted primary lets no replica
	// sync until the operator approves its dataset with /promote.
	AwaitingApproval bool `json:"awaitingApproval,omitempty"`
	// ACLDigest identifies the content of <instance>-acl Redis has, see
	// ACLDigest; ACLRejected maps a user to the error Redis gave for it.
	ACLDigest   string            `json:"aclDigest,omitempty"`
	ACLRejected map[string]string `json:"aclRejected,omitempty"`
}

// apiHandler serves the API of the Instance Manager. guard runs its commands
// and keeps the highest epoch it has accepted; ready holds whether the
// operator approved this primary.
func apiHandler(rdb *redis.Client, guard *epochGuard, ready *readiness) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		status, err := readStatus(r.Context(), rdb)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		status.AwaitingApproval = status.Role == "master" && !ready.approved.Load()
		if acl := ready.acl.Load(); acl != nil {
			status.ACLDigest, status.ACLRejected = acl.Digest, acl.Rejected
		}
		writeJSON(w, status)
	})

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
			runCommand(w, guard, epoch, func() (bool, error) {
				changed, err := promote(r.Context(), rdb)
				if err == nil && !ready.approved.Swap(true) {
					changed = true // the approval of a restarted primary
				}
				return changed, err
			})
		}
	})

	// No epoch: the configuration does not depend on which Pod is the primary.
	var configMu sync.Mutex
	mux.HandleFunc("POST /config", func(w http.ResponseWriter, r *http.Request) {
		var req ConfigRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.MaxmemoryPolicy == "" || req.MaxmemoryRatio < 1 ||
			!slices.Contains([]string{PersistenceAOF, PersistenceRDB, PersistenceNone}, req.Persistence) {
			http.Error(w, "expected a JSON body with persistence, maxmemoryPolicy and maxmemoryRatio", http.StatusBadRequest)
			return
		}
		configMu.Lock()
		defer configMu.Unlock()
		limit, err := memoryLimit(cgroupMemoryMax)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		resp, err := applyConfig(r.Context(), rdb, req, limit, aofDir)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, resp)
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
	info, err := rdb.InfoMap(ctx, "replication", "keyspace").Result()
	if err != nil {
		return InstanceStatus{}, err
	}
	status, err := parseStatus(info["Replication"])
	if err != nil {
		return InstanceStatus{}, err
	}
	status.Keys = countKeys(info["Keyspace"])
	return status, nil
}

// countKeys sums the keys of every database in INFO keyspace, whose lines
// read "db0:keys=150,expires=0,avg_ttl=0".
func countKeys(keyspace map[string]string) int64 {
	var keys int64
	for _, db := range keyspace {
		for field := range strings.SplitSeq(db, ",") {
			if value, ok := strings.CutPrefix(field, "keys="); ok {
				n, _ := strconv.ParseInt(value, 10, 64)
				keys += n
			}
		}
	}
	return keys
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
