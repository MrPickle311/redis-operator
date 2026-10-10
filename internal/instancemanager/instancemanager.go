package instancemanager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// shutdownTimeout is how long redis-server may take to stop after SIGTERM
// (e.g. to save the dataset) before it is killed.
const shutdownTimeout = 30 * time.Second

// CertificatesDir is where the operator mounts the TLS Secret (tls.crt,
// tls.key, ca.crt) of the Instance Manager API.
const CertificatesDir = "/certificates"

// RunInstance runs redis-server and serves the probes until redis-server exits.
func RunInstance(ctx context.Context) error {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DialTimeout: time.Second})
	defer func() { _ = rdb.Close() }()

	probes := &http.Server{
		Addr:              ":" + strconv.Itoa(ProbePort),
		Handler:           probeHandler(rdb),
		ReadHeaderTimeout: 5 * time.Second,
	}
	status := &http.Server{
		Addr:              ":" + strconv.Itoa(StatusPort),
		Handler:           statusHandler(rdb),
		TLSConfig:         serverTLSConfig(CertificatesDir),
		ReadHeaderTimeout: 5 * time.Second,
	}
	for _, server := range []*http.Server{probes, status} {
		serve(server)
		defer func() { _ = server.Close() }()
	}

	return Run(ctx, "redis-server")
}

// serve runs the server in the background, over TLS when it has a TLSConfig.
func serve(server *http.Server) {
	go func() {
		var err error
		if server.TLSConfig != nil {
			err = server.ListenAndServeTLS("", "") // the certificates come from TLSConfig
		} else {
			err = server.ListenAndServe()
		}
		if !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(os.Stderr, "server on", server.Addr, "stopped:", err)
		}
	}()
}

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
