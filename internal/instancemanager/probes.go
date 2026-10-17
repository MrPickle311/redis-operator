package instancemanager

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"

	"github.com/redis/go-redis/v9"
)

// ProbePort is where the kubelet checks the Instance Manager. It serves only
// health endpoints, never anything that changes state, so it needs no TLS.
const ProbePort = 8001

// errNotSynced keeps a replica out of the Services until it holds the dataset
// of the primary, instead of serving an empty or stale one.
var errNotSynced = errors.New("replica has not finished its first sync with the primary")

// probeHandler serves the liveness and readiness probes of the Redis Pod.
func probeHandler(rdb *redis.Client) http.Handler {
	mux := http.NewServeMux()
	ready := &readiness{rdb: rdb}

	// Liveness: the Instance Manager answers, so the container is not stuck.
	// It does not check Redis: loading a big dataset must not restart the Pod.
	mux.HandleFunc("GET /healthz", func(http.ResponseWriter, *http.Request) {})

	// Readiness: Redis accepts commands and, on a replica, holds the dataset
	// of the primary, so Services may send clients here.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := ready.check(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
		}
	})

	return mux
}

// readiness remembers that the replica finished its first sync. It stays
// ready when the link to the primary drops later: during a primary outage the
// replicas keep serving reads, slightly stale, instead of all leaving -ro at once.
type readiness struct {
	rdb    *redis.Client
	synced atomic.Bool // the kubelet may probe concurrently
}

func (r *readiness) check(ctx context.Context) error {
	// PING fails with LOADING while Redis reads its dataset from disk.
	if err := r.rdb.Ping(ctx).Err(); err != nil {
		return err
	}
	if r.synced.Load() {
		return nil
	}
	info, err := r.rdb.InfoMap(ctx, "replication").Result()
	if err != nil {
		return err
	}
	replication := info["Replication"]
	if replication["role"] == "master" {
		return nil // not remembered: a demoted primary must sync before serving again
	}
	if !initialSyncDone(replication) {
		return errNotSynced
	}
	r.synced.Store(true)
	return nil
}

// initialSyncDone reports whether a replica is connected to its primary and
// has loaded the full dataset, according to its INFO replication fields.
func initialSyncDone(replication map[string]string) bool {
	return replication["master_link_status"] == "up" && replication["master_sync_in_progress"] == "0"
}
