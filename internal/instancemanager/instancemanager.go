package instancemanager

import (
	"context"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// shutdownTimeout is how long redis-server may take to stop after SIGTERM
// (e.g. to save the dataset) before it is killed.
const shutdownTimeout = 30 * time.Second

// Run starts the given command (redis-server) and waits until it exits.
// When ctx is cancelled, the process receives SIGTERM instead of the default
// SIGKILL, so redis-server can shut down cleanly.
func Run(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Cancel = func() error {
		return cmd.Process.Signal(syscall.SIGTERM)
	}
	cmd.WaitDelay = shutdownTimeout

	err := cmd.Run()
	if ctx.Err() != nil && cmd.ProcessState != nil && cmd.ProcessState.Success() {
		return nil // the shutdown we asked for
	}
	return err
}

// CopySelf copies the running binary to dest. The initContainer uses it to put
// the manager into a shared emptyDir, so the redis container can run it
// without the operator image.
func CopySelf(dest string) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}

	src, err := os.Open(self)
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()

	dst, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(dst, src); err != nil {
		_ = dst.Close()
		return err
	}
	return dst.Close()
}
