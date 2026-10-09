package instancemanager

import (
	"net/http"

	"github.com/redis/go-redis/v9"
)

// ProbePort is where the kubelet checks the Instance Manager. It serves only
// health endpoints, never anything that changes state, so it needs no TLS.
const ProbePort = 8001

// probeHandler serves the liveness and readiness probes of the Redis Pod.
func probeHandler(rdb *redis.Client) http.Handler {
	mux := http.NewServeMux()

	// Liveness: the Instance Manager answers, so the container is not stuck.
	// It does not check Redis: loading a big dataset must not restart the Pod.
	mux.HandleFunc("GET /healthz", func(http.ResponseWriter, *http.Request) {})

	// Readiness: Redis accepts commands, so Services may send clients here.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := rdb.Ping(r.Context()).Err(); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
		}
	})

	return mux
}
