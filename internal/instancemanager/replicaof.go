package instancemanager

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
)

// redisPort is where redis-server listens, in every Pod.
const redisPort = 6379

// epochPath keeps the highest epoch the Instance Manager has accepted. It is
// on the PVC, so it survives restarts of the Pod.
const epochPath = "/data/.operator/epoch"

// ReplicaOfRequest is the body of POST /replicaof.
type ReplicaOfRequest struct {
	// Epoch is the status.primaryEpoch the operator acted on.
	Epoch int64 `json:"epoch"`
	// PrimaryHost is the Pod DNS name of the primary to replicate from.
	PrimaryHost string `json:"primaryHost"`
}

// ReplicaOfResponse tells whether the replication target had to change.
type ReplicaOfResponse struct {
	Changed bool `json:"changed"`
}

// errStaleEpoch rejects a command sent for an older epoch, e.g. by a former
// operator leader whose request arrives late.
var errStaleEpoch = errors.New("stale epoch")

// epochGuard runs the commands of the operator one at a time and only for the
// highest epoch it has seen.
type epochGuard struct {
	mu   sync.Mutex
	path string
}

// run runs command unless a higher epoch was accepted before. The lock covers
// the command too, so a command of an older epoch never runs after a newer one.
func (g *epochGuard) run(epoch int64, command func() error) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	accepted, err := readEpoch(g.path)
	if err != nil {
		return err
	}
	if epoch < accepted {
		return fmt.Errorf("%w: got %d, already accepted %d", errStaleEpoch, epoch, accepted)
	}
	if epoch > accepted {
		if err := writeEpoch(g.path, epoch); err != nil {
			return err
		}
	}
	return command()
}

// readEpoch returns the epoch stored at path, or 0 when there is none yet.
func readEpoch(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
}

// writeEpoch replaces the epoch at path atomically: a crash leaves either the
// old or the new file, never a torn one.
func writeEpoch(path string, epoch int64) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_, err = f.WriteString(strconv.FormatInt(epoch, 10))
	if err == nil {
		err = f.Sync() // on disk before the rename makes it visible
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// replicaOf makes Redis replicate from primaryHost, unless it already does.
// It returns true when it had to change the replication target.
func replicaOf(ctx context.Context, rdb *redis.Client, primaryHost string) (bool, error) {
	status, err := readStatus(ctx, rdb)
	if err != nil {
		return false, err
	}
	if status.Role == "slave" && status.MasterHost == primaryHost && status.MasterPort == redisPort {
		return false, nil
	}
	return true, rdb.ReplicaOf(ctx, primaryHost, strconv.Itoa(redisPort)).Err()
}
