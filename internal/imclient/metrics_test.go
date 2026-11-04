package imclient

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestObserve(t *testing.T) {
	observe("store-1-0.store-1-hl.play.svc", "/cluster/meet", &http.Response{StatusCode: 200}, nil, time.Millisecond)
	observe("cache-2.cache-hl.play.svc", "/status", nil, errors.New("i/o timeout"), time.Second)
	if testutil.ToFloat64(requestsTotal.WithLabelValues("play", "store-1", "store-1-0", "/cluster/meet", "200")) != 1 ||
		testutil.ToFloat64(requestsTotal.WithLabelValues("play", "cache", "cache-2", "/status", "error")) != 1 {
		t.Error("calls are counted by the namespace, instance and Pod of the host, and their status")
	}
}
