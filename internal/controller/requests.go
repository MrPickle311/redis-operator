package controller

import (
	"maps"
	"strings"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

// Requests are annotations on the RedisInstance, like AnnotationPromote and
// AnnotationRecreate. The operator never removes them: it records every value
// it handled in status.handledRequests and serves only a new value. A tool
// that keeps the annotation in git, or puts it back, cannot repeat a request.

// pendingRequest returns the Pod named in the annotation key when its value
// was not handled yet. Anything after an @ only makes the value new.
func pendingRequest(instance *redisv1.RedisInstance, key string) (string, bool) {
	value, ok := instance.Annotations[key]
	if !ok || instance.Status.HandledRequests[key] == value {
		return "", false
	}
	pod, _, _ := strings.Cut(value, "@")
	return pod, true
}

// markHandled records the current value of the annotation key. The caller
// writes the status, together with the change the request made.
func markHandled(instance *redisv1.RedisInstance, key string) {
	if instance.Status.HandledRequests == nil {
		instance.Status.HandledRequests = map[string]string{}
	}
	instance.Status.HandledRequests[key] = instance.Annotations[key]
}

// forgetRemovedRequests drops the records of annotations that are gone, so
// the same value set again later is a new request.
func forgetRemovedRequests(instance *redisv1.RedisInstance) {
	maps.DeleteFunc(instance.Status.HandledRequests, func(key, _ string) bool {
		_, ok := instance.Annotations[key]
		return !ok
	})
}
