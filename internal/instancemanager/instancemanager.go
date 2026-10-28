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

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// shutdownTimeout is how long redis-server may take to stop after SIGTERM
// (e.g. to save the dataset) before it is killed.
const shutdownTimeout = 30 * time.Second

// CertificatesDir is where the operator mounts the TLS Secret (tls.crt,
// tls.key, ca.crt) of the Instance Manager API.
const CertificatesDir = "/certificates"

// Options are the settings the operator passes to "instance run".
type Options struct {
	// Instance is the name of the RedisInstance the Pod belongs to.
	Instance string
	// PodName and Namespace identify the Pod; they come from the Downward API.
	PodName   string
	Namespace string
	// DefaultUser enables the passwordless default user of Redis.
	DefaultUser bool
	// Databases and IOThreads are spec.redis.startup: Redis reads them only
	// at start, so they come as flags, and a change replaces the Pod.
	Databases int
	IOThreads int
	// TLS makes Redis take only TLS connections, with the certificate in TLSDir.
	TLS bool
	// TLSClientAuth is spec.tls.clientAuth: none, optional or required.
	TLSClientAuth string
}

// RunInstance runs redis-server and serves the probes until redis-server exits.
// It reads the current primary of the instance from c.
func RunInstance(ctx context.Context, c client.Reader, opts Options) error {
	auth, err := readInternalAuth(InternalAuthDir)
	if err != nil {
		return err
	}

	rdb := redis.NewClient(&redis.Options{
		Network:     "unix",
		Addr:        redisSocketPath,
		Username:    OperatorUser,
		Password:    auth.operatorPassword,
		DialTimeout: time.Second,
	})
	defer func() { _ = rdb.Close() }()

	ready := &readiness{rdb: rdb}
	// One guard for the commands of the operator and the ones the Instance
	// Manager takes on its own, so they run one at a time.
	guard := &epochGuard{path: epochPath}
	// Before waiting for the primary: the liveness probe must answer meanwhile.
	probes := &http.Server{
		Addr:              ":" + strconv.Itoa(ProbePort),
		Handler:           probeHandler(ready),
		ReadHeaderTimeout: 5 * time.Second,
	}
	status := &http.Server{
		Addr:              ":" + strconv.Itoa(StatusPort),
		Handler:           apiHandler(rdb, guard, ready),
		TLSConfig:         serverTLSConfig(CertificatesDir),
		ReadHeaderTimeout: 5 * time.Second,
	}
	for _, server := range []*http.Server{probes, status} {
		serve(server)
		defer func() { _ = server.Close() }()
	}

	// Safe start: a replica starts as a replica, so it never takes writes or
	// serves its stale dataset as a primary before the operator steps in.
	instance, err := waitForInstance(ctx, c, types.NamespacedName{Namespace: opts.Namespace, Name: opts.Instance})
	if err != nil {
		return err
	}
	var replicaOf string
	if primary := instance.Status.CurrentPrimary; primary != opts.PodName {
		replicaOf = PodFQDN(primary, opts.Instance, opts.Namespace)
	}
	// A replica needs no approval; a primary waits for one, see redisConfig.
	ready.approved.Store(replicaOf != "")

	limit, err := memoryLimit(cgroupMemoryMax)
	if err != nil {
		return err
	}
	config := ConfigFor(instance)
	conf := redisConfig(auth, opts, maxmemoryFor(limit, config.MaxmemoryRatio), config.MaxmemoryPolicy,
		replicaOf, aofExists(aofDir))
	// 0600: the file holds the passwords, like the mounted Secret it comes from.
	if err := os.WriteFile(redisConfPath, []byte(conf), 0o600); err != nil {
		return err
	}
	go configureOnStart(ctx, rdb, config, limit, ready)
	self := &selfFencing{
		client: c,
		key:    client.ObjectKeyFromObject(instance),
		pod:    opts.PodName,
		rdb:    rdb,
		guard:  guard,
		last:   instance, lastRead: time.Now(),
	}
	go self.run(ctx)
	if opts.TLS {
		go watchTLS(ctx, rdb)
	}
	return Run(ctx, "redis-server", redisConfPath)
}

// configureOnStart applies config once redis-server answers, and only then
// lets the Pod become Ready, so no client sees the defaults of Redis. Options
// Redis refuses are left to the operator, which reports them.
func configureOnStart(ctx context.Context, rdb *redis.Client, config ConfigRequest, limit int64, ready *readiness) {
	for {
		if _, err := applyConfig(ctx, rdb, config, limit, aofDir); err == nil {
			ready.configured.Store(true)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
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
