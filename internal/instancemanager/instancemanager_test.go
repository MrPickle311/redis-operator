package instancemanager

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
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
