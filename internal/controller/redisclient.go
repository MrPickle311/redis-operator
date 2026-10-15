package controller

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/MrPickle311/redis-operator/internal/instancemanager"
)

// ensureReplicaOf makes replicaHost replicate from primaryHost. It returns true
// only when it had to change the replication target, so the caller logs real
// changes instead of every reconcile.
func (r *RedisInstanceReconciler) ensureReplicaOf(ctx context.Context, replicaHost, primaryHost, operatorPassword string) (bool, error) {
	status, err := r.InstanceManager.Status(ctx, replicaHost)
	if err != nil {
		return false, err
	}
	if status.Role == "slave" && status.MasterHost == primaryHost && status.MasterPort == redisPort {
		return false, nil
	}

	// ponytail: REPLICAOF still goes straight to Redis; step 2 moves it to the Instance Manager API.
	rdb := redis.NewClient(&redis.Options{
		Addr:        net.JoinHostPort(replicaHost, strconv.Itoa(redisPort)),
		Username:    instancemanager.OperatorUser,
		Password:    operatorPassword,
		DialTimeout: 3 * time.Second,
	})
	defer func() { _ = rdb.Close() }()

	return true, rdb.ReplicaOf(ctx, primaryHost, strconv.Itoa(redisPort)).Err()
}
