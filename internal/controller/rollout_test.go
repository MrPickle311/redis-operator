package controller

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

func TestSpecHash(t *testing.T) {
	build := func(mutate func(*redisv1.RedisInstance)) string {
		instance := testInstance.DeepCopy()
		instance.Spec.Image = "redis:7.2"
		mutate(instance)
		return buildRedisPod(instance, 0, "operator:v1", "data-cache-0").Annotations[annotationSpecHash]
	}
	base := build(func(*redisv1.RedisInstance) {})

	if again := build(func(*redisv1.RedisInstance) {}); again != base {
		t.Fatalf("hash of the same spec changed: %s, then %s", base, again)
	}
	for name, mutate := range map[string]func(*redisv1.RedisInstance){
		"image": func(i *redisv1.RedisInstance) { i.Spec.Image = "redis:7.4" },
		"memory limit": func(i *redisv1.RedisInstance) {
			i.Spec.Resources.Limits = map[corev1.ResourceName]resource.Quantity{"memory": resource.MustParse("1Gi")}
		},
		"policy": func(i *redisv1.RedisInstance) { i.Spec.Redis.MaxmemoryPolicy = "allkeys-lru" },
	} {
		if build(mutate) == base {
			t.Errorf("changing the %s did not change the hash", name)
		}
	}
	// The role label moves with every switchover; it must not restart the Pod.
	if got := build(func(i *redisv1.RedisInstance) { i.Status.CurrentPrimary = "cache-1" }); got != base {
		t.Error("a change of the primary changed the hash")
	}
}
