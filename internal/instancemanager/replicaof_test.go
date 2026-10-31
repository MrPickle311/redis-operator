package instancemanager

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestEpochGuardRejectsAnOlderEpoch(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".operator", "epoch")
	guard := &epochGuard{path: path, uid: testUID}
	ran := 0
	command := func() error { ran++; return nil }

	for _, epoch := range []int64{2, 2, 3} {
		if err := guard.run(epoch, command); err != nil {
			t.Fatalf("run(%d) error = %v", epoch, err)
		}
	}
	if err := guard.run(2, command); !errors.Is(err, errStaleEpoch) {
		t.Fatalf("run(2) after epoch 3: error = %v, want errStaleEpoch", err)
	}
	if ran != 3 {
		t.Errorf("command ran %d times, want 3: never for the stale epoch", ran)
	}

	// A restarted Instance Manager reads the epoch back from the PVC.
	if err := (&epochGuard{path: path, uid: testUID}).run(2, command); !errors.Is(err, errStaleEpoch) {
		t.Errorf("run(2) after a restart: error = %v, want errStaleEpoch", err)
	}
}

// testUID is the UID of the RedisInstance of the tests.
const testUID = "db"

func TestEpochGuardIgnoresTheEpochOfAnotherInstance(t *testing.T) {
	path := filepath.Join(t.TempDir(), "epoch")
	if err := writeEpoch(path, 5, "restored-from"); err != nil {
		t.Fatal(err)
	}
	guard := &epochGuard{path: path, uid: testUID}
	if err := guard.run(1, func() error { return nil }); err != nil {
		t.Fatalf("run(1) on the data of another instance: error = %v, want it to run", err)
	}
	if err := guard.run(0, func() error { return nil }); !errors.Is(err, errStaleEpoch) {
		t.Errorf("run(0) after epoch 1: error = %v, want errStaleEpoch: the epoch is now of this instance", err)
	}
}

func TestEpochGuardWaitsForTheInstance(t *testing.T) {
	err := (&epochGuard{path: filepath.Join(t.TempDir(), "epoch")}).run(1, func() error { return nil })
	if !errors.Is(err, errInstanceUnknown) {
		t.Errorf("run() before setInstance: error = %v, want errInstanceUnknown", err)
	}
}

func TestEpochGuardDoesNotStoreTheEpochWhenTheFileIsCorrupt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "epoch")
	writeFile(t, filepath.Dir(path), "epoch", "garbage")

	if err := (&epochGuard{path: path, uid: testUID}).run(1, func() error { return nil }); err == nil {
		t.Fatal("run() accepted a command although the stored epoch cannot be read")
	}
}

func TestReplicaOfAPIRejectsBadRequests(t *testing.T) {
	path := filepath.Join(t.TempDir(), "epoch")
	if err := writeEpoch(path, 5, testUID); err != nil {
		t.Fatal(err)
	}
	rdb := deadRedis(t) // the rejected requests never reach Redis
	srv := httptest.NewServer(apiHandler(rdb, &epochGuard{path: path, uid: testUID}, &readiness{}))
	defer srv.Close()

	tests := []struct {
		name string
		body string
		want int
	}{
		{"not JSON", "replicaof", http.StatusBadRequest},
		{"no epoch", `{"primaryHost": "cache-0"}`, http.StatusBadRequest},
		{"no primary", `{"epoch": 5}`, http.StatusBadRequest},
		{"stale epoch", `{"epoch": 4, "primaryHost": "cache-0"}`, http.StatusConflict},
		{"Redis down", `{"epoch": 5, "primaryHost": "cache-0"}`, http.StatusServiceUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := http.Post(srv.URL+"/replicaof", "application/json", strings.NewReader(tt.body))
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != tt.want {
				t.Errorf("POST /replicaof = %d, want %d", resp.StatusCode, tt.want)
			}
		})
	}
}

func TestFenceAndPromoteAPIsRejectBadRequests(t *testing.T) {
	path := filepath.Join(t.TempDir(), "epoch")
	if err := writeEpoch(path, 5, testUID); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(apiHandler(deadRedis(t), &epochGuard{path: path, uid: testUID}, &readiness{}))
	defer srv.Close()

	for _, endpoint := range []string{"/fence", "/promote"} {
		for _, tt := range []struct {
			name string
			body string
			want int
		}{
			{"not JSON", "fence", http.StatusBadRequest},
			{"no epoch", `{}`, http.StatusBadRequest},
			{"stale epoch", `{"epoch": 4}`, http.StatusConflict},
			{"Redis down", `{"epoch": 5}`, http.StatusServiceUnavailable},
		} {
			t.Run(endpoint+" "+tt.name, func(t *testing.T) {
				resp, err := http.Post(srv.URL+endpoint, "application/json", strings.NewReader(tt.body))
				if err != nil {
					t.Fatal(err)
				}
				_ = resp.Body.Close()
				if resp.StatusCode != tt.want {
					t.Errorf("POST %s = %d, want %d", endpoint, resp.StatusCode, tt.want)
				}
			})
		}
	}
}

// deadRedis returns a client of a redis-server that is down: nothing listens on port 1.
func deadRedis(t *testing.T) *redis.Client {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}
