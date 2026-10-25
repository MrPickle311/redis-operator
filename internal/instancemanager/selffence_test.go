package instancemanager

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

func TestDecideSelfAction(t *testing.T) {
	fenceOnAPILoss := func(on bool) *redisv1.RedisInstance {
		return &redisv1.RedisInstance{Spec: redisv1.RedisInstanceSpec{Failover: redisv1.FailoverSpec{
			Delay: metav1.Duration{Duration: 30 * time.Second}, FenceOnAPILoss: &on,
		}}}
	}
	withStatus := func(primary, target string) *redisv1.RedisInstance {
		instance := fenceOnAPILoss(true)
		instance.Status = redisv1.RedisInstanceStatus{CurrentPrimary: primary, TargetPrimary: target}
		return instance
	}
	tests := []struct {
		name      string
		instance  *redisv1.RedisInstance // nil: the read failed
		last      *redisv1.RedisInstance
		sinceRead time.Duration
		role      string
		fenced    bool
		want      selfAction
	}{
		{"the primary is this Pod", withStatus("cache-0", ""), nil, 0, "master", false, keepRole},
		{"another Pod became the primary", withStatus("cache-1", ""), nil, 0, "master", false, selfDemote},
		{"no primary recorded yet", withStatus("", ""), nil, 0, "master", false, keepRole},
		{"a replica follows the operator", withStatus("cache-1", ""), nil, 0, "slave", false, keepRole},
		{"the API server is out of reach briefly", nil, fenceOnAPILoss(true), 10 * time.Second, "master", false, keepRole},
		{"the API server is out of reach for half the delay", nil, fenceOnAPILoss(true), 16 * time.Second, "master", false, selfFence},
		{"the fence is renewed while it lasts", nil, fenceOnAPILoss(true), time.Hour, "master", true, selfFence},
		{"fencing on API loss is off", nil, fenceOnAPILoss(false), time.Hour, "master", false, keepRole},
		{"a replica cut off from the API server", nil, fenceOnAPILoss(true), time.Hour, "slave", false, keepRole},
		{"the API server is back", withStatus("cache-0", ""), nil, 0, "master", true, selfUnfence},
		// A switchover fenced this primary; lifting that fence could lose writes.
		{"the API server is back during a switchover", withStatus("cache-0", "cache-1"), nil, 0, "master", true, keepRole},
		{"the fence of a switchover is not ours", withStatus("cache-0", "cache-1"), nil, 0, "master", false, keepRole},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := decideSelfAction(tt.instance, tt.last, tt.sinceRead, tt.role, "cache-0", tt.fenced); got != tt.want {
				t.Errorf("decideSelfAction() = %d, want %d", got, tt.want)
			}
		})
	}
}
