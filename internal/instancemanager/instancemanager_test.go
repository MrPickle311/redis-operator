package instancemanager

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRunReturnsTheExitCodeOfTheProcess(t *testing.T) {
	err := Run(context.Background(), "sh", "-c", "exit 3")

	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 3 {
		t.Fatalf("Run() error = %v, want exit code 3", err)
	}
}

func TestRunStopsTheProcessCleanlyOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(200*time.Millisecond, cancel)

	// Like redis-server, the script exits with 0 only after it receives SIGTERM;
	// the default SIGKILL would make Run fail.
	start := time.Now()
	err := Run(ctx, "sh", "-c", "trap 'exit 0' TERM; while true; do sleep 0.1; done")

	if err != nil {
		t.Fatalf("Run() error = %v, want nil after a clean shutdown", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Run() took %v to stop the process", elapsed)
	}
}

func TestCopySelfWritesAnExecutableCopy(t *testing.T) {
	dest := filepath.Join(t.TempDir(), "manager")

	if err := CopySelf(dest); err != nil {
		t.Fatalf("CopySelf() error = %v", err)
	}

	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.Stat(self)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.Stat(dest)
	if err != nil {
		t.Fatalf("copy not found: %v", err)
	}
	if got.Size() != want.Size() {
		t.Errorf("copy size = %d, want %d", got.Size(), want.Size())
	}
	if got.Mode().Perm()&0o111 == 0 {
		t.Errorf("copy mode = %v, want executable", got.Mode())
	}
	if script, err := os.ReadFile(filepath.Join(filepath.Dir(dest), KeysScript)); err != nil || !bytes.Equal(script, keysScript) {
		t.Errorf("%s next to the copy: %v", KeysScript, err)
	}
}

// TestKeysScriptOnRedis needs a real Redis, like TestSyncACLOnRedis; one with
// --cluster-enabled yes and every slot also checks allow-cross-slot-keys.
func TestKeysScriptOnRedis(t *testing.T) {
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("REDIS_TEST_ADDR not set")
	}
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = rdb.Close() }()
	if err := rdb.FlushAll(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a", "b", "c"} {
		rdb.Set(ctx, "s:"+key, "x", 0)
	}
	rdb.RPush(ctx, "l", "x")
	rdb.Set(ctx, "big", strings.Repeat("x", 20000), 0)

	result, err := rdb.Eval(ctx, string(keysScript), nil).StringSlice()
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]bool{}
	for i := 0; i+1 < len(result); i += 2 {
		counts[result[i]] = true
	}
	for _, want := range []string{"string <1KB", "list <1KB", "string 10-100KB"} {
		if !counts[want] {
			t.Errorf("keys.lua = %v, want a count for %q", result, want)
		}
	}
}

func TestProbes(t *testing.T) {
	rdb := deadRedis(t)
	srv := httptest.NewServer(probeHandler(&readiness{rdb: rdb}))
	defer srv.Close()

	tests := []struct {
		path       string
		wantStatus int
	}{
		{"/healthz", http.StatusOK},                // the Instance Manager itself is alive
		{"/readyz", http.StatusServiceUnavailable}, // but Redis does not answer
		{"/promote", http.StatusNotFound},          // the probe port exposes nothing else
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			resp, err := http.Get(srv.URL + tt.path)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("GET %s = %d, want %d", tt.path, resp.StatusCode, tt.wantStatus)
			}
		})
	}
}
