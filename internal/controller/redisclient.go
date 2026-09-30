package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func setReplicaOf(ctx context.Context, replicaIP, primaryIP string) error {
	rdb := redis.NewClient(&redis.Options{
		Addr:        fmt.Sprintf("%s:6379", replicaIP),
		DialTimeout: 3 * time.Second,
	})
	defer rdb.Close()

	return rdb.ReplicaOf(ctx, primaryIP, "6379").Err()
}
