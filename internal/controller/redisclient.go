package controller

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// ensureReplicaOf makes replicaHost replicate from primaryHost. It returns true
// only when it had to change the replication target, so the caller logs real
// changes instead of every reconcile.
func ensureReplicaOf(ctx context.Context, replicaHost, primaryHost string) (bool, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:        net.JoinHostPort(replicaHost, strconv.Itoa(redisPort)),
		DialTimeout: 3 * time.Second,
	})
	defer func() { _ = rdb.Close() }()

	// A replica answers ROLE with: ["slave", <primary host>, <primary port>, <state>, <offset>].
	role, err := rdb.Do(ctx, "ROLE").Slice()
	if err != nil {
		return false, err
	}
	if len(role) >= 3 && role[0] == "slave" && role[1] == primaryHost && role[2] == int64(redisPort) {
		return false, nil
	}

	return true, rdb.ReplicaOf(ctx, primaryHost, strconv.Itoa(redisPort)).Err()
}
