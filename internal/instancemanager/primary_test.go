package instancemanager

import (
	"context"
	"errors"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

var instanceKey = types.NamespacedName{Namespace: "prod", Name: "cache"}

func TestWaitForInstanceReadsTheStatus(t *testing.T) {
	instance := &redisv1.RedisInstance{
		ObjectMeta: metav1.ObjectMeta{Namespace: instanceKey.Namespace, Name: instanceKey.Name},
		Status:     redisv1.RedisInstanceStatus{CurrentPrimary: "cache-1"},
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(instance).Build()

	got, err := waitForInstance(context.Background(), c, instanceKey)
	if err != nil || got.Status.CurrentPrimary != "cache-1" {
		t.Errorf("waitForInstance() = %q, %v, want the instance with primary cache-1", got.Status.CurrentPrimary, err)
	}
}

func TestWaitForInstanceWaitsUntilThePrimaryIsKnown(t *testing.T) {
	// No status yet, then no RedisInstance at all: both mean "keep waiting".
	for name, c := range map[string]*fake.ClientBuilder{
		"no currentPrimary": fake.NewClientBuilder().WithObjects(&redisv1.RedisInstance{
			ObjectMeta: metav1.ObjectMeta{Namespace: instanceKey.Namespace, Name: instanceKey.Name},
		}),
		"no RedisInstance": fake.NewClientBuilder(),
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			_, err := waitForInstance(ctx, c.WithScheme(testScheme(t)).Build(), instanceKey)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("waitForInstance() error = %v, want it to wait until the context ends", err)
			}
		})
	}
}

func TestPodFQDN(t *testing.T) {
	if got := PodFQDN("cache-1", "cache", "prod"); got != "cache-1.cache-hl.prod.svc" {
		t.Errorf("PodFQDN() = %q", got)
	}
}

func testScheme(t *testing.T) *runtime.Scheme {
	scheme := runtime.NewScheme()
	if err := redisv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}
