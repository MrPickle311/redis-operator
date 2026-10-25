package instancemanager

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

func TestConfigFor(t *testing.T) {
	instance := &redisv1.RedisInstance{Spec: redisv1.RedisInstanceSpec{Redis: redisv1.RedisSpec{
		Persistence:     redisv1.PersistenceRDB,
		MaxmemoryPolicy: "allkeys-lru",
		MaxmemoryRatio:  80,
		Parameters:      map[string]redisv1.ParameterValue{"hz": "20"},
	}}}
	want := ConfigRequest{
		Persistence: PersistenceRDB, MaxmemoryPolicy: "allkeys-lru", MaxmemoryRatio: 80,
		Parameters: map[string]string{"hz": "20"},
	}
	if got := ConfigFor(instance); !reflect.DeepEqual(got, want) {
		t.Errorf("ConfigFor() = %+v, want %+v", got, want)
	}
}

func TestAOFExistsOnlyWithAManifest(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "appendonlydir")
	if aofExists(dir) {
		t.Error("aofExists() = true without the directory")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if aofExists(dir) {
		t.Error("aofExists() = true for an empty directory, which Redis would load as an empty dataset")
	}
	if err := os.WriteFile(filepath.Join(dir, "appendonly.aof.manifest"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if !aofExists(dir) {
		t.Error("aofExists() = false with a manifest")
	}
}

func TestLiveOptions(t *testing.T) {
	req := ConfigRequest{
		Persistence:     PersistenceAOF,
		MaxmemoryPolicy: "allkeys-lru",
		MaxmemoryRatio:  75,
		Parameters:      map[string]string{"notify-keyspace-events": "Ex", "hz": "20"},
	}
	got := liveOptions(req, 1<<30)
	want := []option{
		{"maxmemory", "805306368"},
		{"maxmemory-policy", "allkeys-lru"},
		{"save", snapshotSchedule},
		{"hz", "20"}, // the parameters come last, sorted, so the order is stable
		{"notify-keyspace-events", "Ex"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("liveOptions() = %v, want %v", got, want)
	}

	for persistence, wantSave := range map[string]string{PersistenceRDB: snapshotSchedule, PersistenceNone: ""} {
		req.Persistence = persistence
		if save := liveOptions(req, 0)[2]; save != (option{"save", wantSave}) {
			t.Errorf("persistence %s: %v, want save %q", persistence, save, wantSave)
		}
	}
}

func TestConfigAPIRejectsBadRequests(t *testing.T) {
	srv := httptest.NewServer(apiHandler(deadRedis(t), filepath.Join(t.TempDir(), "epoch")))
	defer srv.Close()

	for _, tt := range []struct {
		name string
		body string
		want int
	}{
		{"not JSON", "config", http.StatusBadRequest},
		{"unknown persistence", `{"persistence": "disk", "maxmemoryPolicy": "noeviction", "maxmemoryRatio": 75}`, http.StatusBadRequest},
		{"no maxmemory policy", `{"persistence": "aof", "maxmemoryRatio": 75}`, http.StatusBadRequest},
		{"Redis down", `{"persistence": "aof", "maxmemoryPolicy": "noeviction", "maxmemoryRatio": 75}`, http.StatusServiceUnavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := http.Post(srv.URL+"/config", "application/json", strings.NewReader(tt.body))
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != tt.want {
				t.Errorf("POST /config = %d, want %d", resp.StatusCode, tt.want)
			}
		})
	}
}

// TestApplyConfigOnRedis needs a real Redis whose /data is REDIS_TEST_DATA,
// started with appendonly no:
//
//	docker run -d --rm -p 6390:6379 --user $(id -u) -v $DIR:/data redis:7.2
//	REDIS_TEST_ADDR=localhost:6390 REDIS_TEST_DATA=$DIR go test ./internal/instancemanager -run OnRedis
func TestApplyConfigOnRedis(t *testing.T) {
	addr, data := os.Getenv("REDIS_TEST_ADDR"), os.Getenv("REDIS_TEST_DATA")
	if addr == "" || data == "" {
		t.Skip("REDIS_TEST_ADDR and REDIS_TEST_DATA not set")
	}
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = rdb.Close() }()
	aof := filepath.Join(data, "appendonlydir")
	if err := rdb.Set(ctx, "key", "kept", 0).Err(); err != nil {
		t.Fatal(err)
	}

	apply := func(req ConfigRequest) ConfigResponse {
		t.Helper()
		resp, err := applyConfig(ctx, rdb, req, 1<<30, aof)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	want := func(name, value string) {
		t.Helper()
		if got, _ := getConfigValue(ctx, rdb, name); got != value {
			t.Errorf("%s = %q, want %q", name, got, value)
		}
	}
	req := ConfigRequest{
		Persistence:     PersistenceAOF,
		MaxmemoryPolicy: "allkeys-lru",
		MaxmemoryRatio:  75,
		Parameters:      map[string]string{"hz": "20", "notify-keyspace-events": "Ex", "databases": "32"},
	}

	t.Run("applies everything Redis accepts and reports the rest", func(t *testing.T) {
		resp := apply(req)
		if !resp.Changed {
			t.Error("Changed = false for a new configuration")
		}
		if _, ok := resp.Rejected["databases"]; !ok || len(resp.Rejected) != 1 {
			t.Errorf("Rejected = %v, want only databases, which Redis cannot change while it runs", resp.Rejected)
		}
		want("appendonly", "yes")
		want("maxmemory", "805306368")
		want("maxmemory-policy", "allkeys-lru")
		want("hz", "20")
	})

	t.Run("changes nothing the second time, though Redis rewrites a value", func(t *testing.T) {
		if resp := apply(req); resp.Changed {
			t.Error("Changed = true for the same configuration")
		}
	})

	t.Run("turns the AOF off without losing writes and removes its files", func(t *testing.T) {
		if err := rdb.Do(ctx, "BGREWRITEAOF").Err(); err != nil && !strings.Contains(err.Error(), "already in progress") {
			t.Fatal(err)
		}
		req.Persistence = PersistenceRDB
		if resp := apply(req); !resp.Changed {
			t.Error("Changed = false when the AOF went off")
		}
		want("appendonly", "no")
		if _, err := os.Stat(aof); !os.IsNotExist(err) {
			t.Errorf("the AOF directory is still there: %v", err)
		}
		if _, err := os.Stat(filepath.Join(data, "dump.rdb")); err != nil {
			t.Errorf("no snapshot holds the writes: %v", err)
		}
		if got, _ := rdb.Get(ctx, "key").Result(); got != "kept" {
			t.Errorf("key = %q, want kept", got)
		}
	})

	t.Run("takes no snapshots without persistence", func(t *testing.T) {
		req.Persistence = PersistenceNone
		apply(req)
		want("save", "")
	})
}

// TestConfigureOnStartMakesThePodReady needs a real Redis, like TestApplyConfigOnRedis.
func TestConfigureOnStartMakesThePodReady(t *testing.T) {
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("REDIS_TEST_ADDR not set")
	}
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = rdb.Close() }()
	ready := &readiness{rdb: rdb}

	if err := ready.check(ctx); err != errNotConfigured {
		t.Fatalf("check() = %v before the configuration, want errNotConfigured", err)
	}
	config := ConfigRequest{Persistence: PersistenceAOF, MaxmemoryPolicy: "volatile-lru", MaxmemoryRatio: 50}
	configureOnStart(ctx, rdb, config, 1<<30, ready)
	if err := ready.check(ctx); err != nil {
		t.Errorf("check() = %v after the configuration, want ready", err)
	}
	if got, _ := getConfigValue(ctx, rdb, "maxmemory-policy"); got != "volatile-lru" {
		t.Errorf("maxmemory-policy = %q, want the one of spec.redis", got)
	}
}
