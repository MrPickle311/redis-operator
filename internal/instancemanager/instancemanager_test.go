package instancemanager

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
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
}

func TestProbes(t *testing.T) {
	// Nothing listens on port 1, so every PING fails like with a dead redis-server.
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	defer func() { _ = rdb.Close() }()
	srv := httptest.NewServer(probeHandler(rdb))
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
