package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

func TestPendingRequest(t *testing.T) {
	const key = redisv1.AnnotationRecreate
	tests := []struct {
		name        string
		annotation  string // "" means no annotation
		handled     string
		wantPod     string
		wantPending bool
	}{
		{"no annotation", "", "", "", false},
		{"a new request", "cache-2", "", "cache-2", true},
		{"the handled value again", "cache-2", "cache-2", "", false},
		{"the same Pod with a token", "cache-2@2", "cache-2", "cache-2", true},
		{"the handled token again", "cache-2@2", "cache-2@2", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instance := &redisv1.RedisInstance{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{}}}
			if tt.annotation != "" {
				instance.Annotations[key] = tt.annotation
			}
			if tt.handled != "" {
				instance.Status.HandledRequests = map[string]string{key: tt.handled}
			}
			pod, pending := pendingRequest(instance, key)
			if pod != tt.wantPod || pending != tt.wantPending {
				t.Errorf("pendingRequest() = %q, %v; want %q, %v", pod, pending, tt.wantPod, tt.wantPending)
			}
		})
	}
}

func TestMarkHandledAndForget(t *testing.T) {
	instance := &redisv1.RedisInstance{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
		redisv1.AnnotationPromote: "cache-1",
	}}}
	markHandled(instance, redisv1.AnnotationPromote)
	if _, pending := pendingRequest(instance, redisv1.AnnotationPromote); pending {
		t.Fatal("a handled request is still pending")
	}

	delete(instance.Annotations, redisv1.AnnotationPromote)
	forgetRemovedRequests(instance)
	if len(instance.Status.HandledRequests) != 0 {
		t.Fatalf("HandledRequests = %v, want empty after the annotation is gone", instance.Status.HandledRequests)
	}
}
