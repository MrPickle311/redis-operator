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
	"time"

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

// EpochRequest is the body of POST /fence and POST /promote.
type EpochRequest struct {
	// Epoch is the status.primaryEpoch the operator acted on.
	Epoch int64 `json:"epoch"`
}

// CommandResponse tells whether a command changed the role or the replication
// target of Redis.
type CommandResponse struct {
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
	changed := status.Role != "slave" || status.MasterHost != primaryHost || status.MasterPort != redisPort
	if changed {
		if err := rdb.ReplicaOf(ctx, primaryHost, strconv.Itoa(redisPort)).Err(); err != nil {
			return false, err
		}
	}
	// After REPLICAOF, not before: a demoted primary is fenced, and its waiting
	// clients must get READONLY instead of writing to it. Unpausing a Redis
	// that is not paused does nothing.
	return changed, rdb.ClientUnpause(ctx).Err()
}

// fenceTimeout bounds a fence the operator never lifts, e.g. because it
// stopped in the middle of a switchover.
// ponytail: fixed hour; the operator fences again when it resumes a switchover.
const fenceTimeout = time.Hour

// fence makes writes wait until /promote or /replicaof lifts the pause. Reads,
// INFO and the replication stream keep working, so replicas can catch up.
func fence(ctx context.Context, rdb *redis.Client) error {
	// Not rdb.ClientPause: it pauses every command, and only writes must stop.
	return rdb.Do(ctx, "CLIENT", "PAUSE", fenceTimeout.Milliseconds(), "WRITE").Err()
}

// promote makes Redis a primary that takes writes and lets replicas sync: it
// lifts the fence and opens replication, closed when a primary starts. It
// returns true when Redis was a replica before.
func promote(ctx context.Context, rdb *redis.Client) (bool, error) {
	status, err := readStatus(ctx, rdb)
	if err != nil {
		return false, err
	}
	changed := status.Role != "master"
	if changed {
		if err := rdb.ReplicaOf(ctx, "NO", "ONE").Err(); err != nil {
			return false, err
		}
	}
	if err := rdb.ClientUnpause(ctx).Err(); err != nil {
		return false, err
	}
	// Replicas may sync from now on, see redisConfig.
	return changed, rdb.Do(ctx, "ACL", "SETUSER", ReplicationUser, "on").Err()
}
