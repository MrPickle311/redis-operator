package controller

import (
	"reflect"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/instancemanager"
)

func TestConfigRequest(t *testing.T) {
	instance := &redisv1.RedisInstance{Spec: redisv1.RedisInstanceSpec{Redis: redisv1.RedisSpec{
		Persistence:     redisv1.PersistenceRDB,
		MaxmemoryPolicy: "allkeys-lru",
		MaxmemoryRatio:  80,
		Parameters:      map[string]redisv1.ParameterValue{"hz": "20"},
	}}}
	want := instancemanager.ConfigRequest{
		Persistence: "rdb", MaxmemoryPolicy: "allkeys-lru", MaxmemoryRatio: 80,
		Parameters: map[string]string{"hz": "20"},
	}
	if got := configRequest(instance); !reflect.DeepEqual(got, want) {
		t.Errorf("configRequest() = %+v, want %+v", got, want)
	}
}

func TestParametersCondition(t *testing.T) {
	recorder := events.NewFakeRecorder(10)
	r := &RedisInstanceReconciler{Recorder: recorder}
	instance := &redisv1.RedisInstance{}
	rejected := map[string]string{"hz": "ERR argument must be between 1 and 500", "databases": "ERR can't set immutable config"}

	r.setParametersCondition(instance, rejected)
	condition := meta.FindStatusCondition(instance.Status.Conditions, redisv1.ConditionParametersApplied)
	if condition.Status != metav1.ConditionFalse ||
		condition.Message != "Redis refused databases: ERR can't set immutable config; hz: ERR argument must be between 1 and 500" {
		t.Errorf("condition = %s %q, want False with every refused option, sorted", condition.Status, condition.Message)
	}
	r.setParametersCondition(instance, rejected)
	if len(recorder.Events) != 1 || !strings.Contains(<-recorder.Events, "ParametersRejected") {
		t.Error("want one ParametersRejected Event, not one per reconcile")
	}

	r.setParametersCondition(instance, nil)
	if !meta.IsStatusConditionTrue(instance.Status.Conditions, redisv1.ConditionParametersApplied) {
		t.Error("the condition stayed False once Redis took every option")
	}
}
