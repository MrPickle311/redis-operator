package instancemanager

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

// instancePollInterval is how often the Instance Manager reads its
// RedisInstance. A read is also how it learns that the API server is reachable.
const instancePollInterval = 2 * time.Second

// selfAction is what a primary does on its own, between the commands of the
// operator, which may not reach it.
type selfAction int

const (
	keepRole selfAction = iota
	// selfDemote: the RedisInstance names another primary, e.g. after a failover
	// while this Pod was cut off from the operator.
	selfDemote
	// selfFence: the API server has been out of reach for half of failover.delay,
	// so a replica may be promoted elsewhere soon.
	selfFence
	// selfUnfence: the API server is back and this Pod is still the primary.
	selfUnfence
)

// decideSelfAction tells what a Redis in role does. instance is nil when it
// could not be read; last is the RedisInstance read last, for its spec.
// fenced tells whether the Instance Manager paused writes itself.
func decideSelfAction(instance, last *redisv1.RedisInstance, sinceRead time.Duration, role, pod string, fenced bool) selfAction {
	if role != "master" {
		return keepRole // a replica takes no writes; the operator manages it
	}
	if instance == nil {
		failover := last.Spec.Failover
		if failover.FenceOnAPILoss != nil && *failover.FenceOnAPILoss && sinceRead > failover.Delay.Duration/2 {
			return selfFence // also again while it lasts: the pause would expire
		}
		return keepRole
	}
	// While a switchover or failover runs, the operator moves the primary:
	// a target it just promoted still reads the old currentPrimary.
	if primary := instance.Status.CurrentPrimary; primary != "" && primary != pod && instance.Status.TargetPrimary == "" {
		return selfDemote
	}
	// Never during a switchover: that fence belongs to the operator.
	if fenced && instance.Status.TargetPrimary == "" {
		return selfUnfence
	}
	return keepRole
}

// selfFencing keeps a primary from taking writes when another primary may
// exist: after a failover it could not hear of, or while it cannot tell.
type selfFencing struct {
	client client.Reader
	key    types.NamespacedName
	pod    string
	rdb    *redis.Client
	guard  *epochGuard

	last     *redisv1.RedisInstance
	lastRead time.Time
	fenced   bool
}

// run reads the RedisInstance every instancePollInterval until ctx ends.
func (s *selfFencing) run(ctx context.Context) {
	ticker := time.NewTicker(instancePollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.step(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "Could not check the role of this instance:", err)
			}
		}
	}
}

func (s *selfFencing) step(ctx context.Context) error {
	instance, err := s.read(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Could not read RedisInstance:", err)
		instance = nil
	} else {
		s.last, s.lastRead = instance, time.Now()
	}
	status, err := readStatus(ctx, s.rdb)
	if err != nil {
		return err // Redis is not up: nothing to protect
	}
	if status.Role != "master" {
		s.fenced = false // the demotion lifted the pause
	}

	switch decideSelfAction(instance, s.last, time.Since(s.lastRead), status.Role, s.pod, s.fenced) {
	case selfDemote:
		primary := instance.Status.CurrentPrimary
		fmt.Fprintln(os.Stderr, "Demoting this instance: the primary is", primary)
		host := PodFQDN(primary, s.key.Name, s.key.Namespace)
		// The epoch of the status: a later command of the operator still runs.
		return s.guard.run(instance.Status.PrimaryEpoch, func() error {
			_, err := replicaOf(ctx, s.rdb, host)
			return err
		})
	case selfFence:
		if !s.fenced {
			fmt.Fprintln(os.Stderr, "Fencing this primary: the API server has been out of reach since", s.lastRead)
		}
		s.fenced = true
		return fence(ctx, s.rdb)
	case selfUnfence:
		fmt.Fprintln(os.Stderr, "Lifting the fence: the API server is back and this instance is still the primary")
		s.fenced = false
		return s.rdb.Do(ctx, "CLIENT", "UNPAUSE").Err()
	}
	return nil
}

// read gets the RedisInstance, giving up before the next poll.
func (s *selfFencing) read(ctx context.Context) (*redisv1.RedisInstance, error) {
	ctx, cancel := context.WithTimeout(ctx, instancePollInterval)
	defer cancel()
	var instance redisv1.RedisInstance
	if err := s.client.Get(ctx, s.key, &instance); err != nil {
		return nil, err
	}
	return &instance, nil
}
