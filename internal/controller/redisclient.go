package controller

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

func setReplicaOf(ctx context.Context, replicaHost, primaryHost string) error {
	rdb := redis.NewClient(&redis.Options{
		Addr:        net.JoinHostPort(replicaHost, strconv.Itoa(redisPort)),
		DialTimeout: 3 * time.Second,
	})
	defer func(rdb *redis.Client) {
		_ = rdb.Close()
	}(rdb)

	return rdb.ReplicaOf(ctx, primaryHost, strconv.Itoa(redisPort)).Err()
}
