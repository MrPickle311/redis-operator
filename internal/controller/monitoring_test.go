package controller

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

func TestSlotBlockShards(t *testing.T) {
	three := []redisv1.ShardStatus{
		{Name: "store-0", Slots: slotRange(0, 3)},
		{Name: "store-1", Slots: slotRange(1, 3)},
		{Name: "store-2", Slots: slotRange(2, 3)},
		{Name: "store-3"}, // empty, added later
	}
	got := slotBlockShards(three, "store")
	// Block 21 (5376-5631): 85 slots of store-0, 171 of store-1.
	if len(got) != 64 || got[0] != 0 || got[21] != 1 || got[63] != 2 || slices.Contains(got, 3) {
		t.Errorf("three shards: %v", got)
	}

	moved := []redisv1.ShardStatus{
		{Name: "store-0", Slots: "0-127"},
		{Name: "store-1", Slots: "128-255,300"},
	}
	got = slotBlockShards(moved, "store")
	if got[0] != 0 || got[1] != 1 || got[2] != -1 {
		t.Errorf("a tie goes to the lower shard, a block nobody serves is -1: %v", got[:3])
	}
}

func TestStatusCollector(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := redisv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	instance := &redisv1.RedisInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "app"},
		Spec:       redisv1.RedisInstanceSpec{Instances: 3},
		Status: redisv1.RedisInstanceStatus{
			CurrentPrimary: "cache-0", TargetPrimary: "cache-2", PrimaryEpoch: 4,
			Conditions: []metav1.Condition{{Type: "Ready", Status: "False", Reason: "Switchover"}},
		},
	}
	collector := StatusCollector{Reader: fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance).Build()}
	want := `
# HELP redis_operator_instance_pod_role Role of the Pod in status: 0 replica, 1 primary, 2 becoming the primary.
# TYPE redis_operator_instance_pod_role gauge
redis_operator_instance_pod_role{namespace="app",pod="cache-0",redis_instance="cache"} 1
redis_operator_instance_pod_role{namespace="app",pod="cache-1",redis_instance="cache"} 0
redis_operator_instance_pod_role{namespace="app",pod="cache-2",redis_instance="cache"} 2
# HELP redis_operator_condition A condition of a RedisInstance or RedisCluster with its reason: 1 when True, 0 when False.
# TYPE redis_operator_condition gauge
redis_operator_condition{kind="RedisInstance",name="cache",namespace="app",reason="Switchover",type="Ready"} 0
`
	if err := testutil.CollectAndCompare(collector, strings.NewReader(want),
		"redis_operator_instance_pod_role", "redis_operator_condition"); err != nil {
		t.Error(err)
	}
}

func TestExporterSidecar(t *testing.T) {
	instance := &redisv1.RedisInstance{ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "app"}}
	if pod := buildRedisPod(instance, 0, "operator:v1", "cache-0-data"); len(pod.Spec.Containers) != 1 {
		t.Fatalf("no sidecar without spec.monitoring: %v", pod.Spec.Containers)
	}
	instance.Spec.Monitoring = redisv1.MonitoringSpec{Enabled: true, Image: "exporter:v1"}
	pod := buildRedisPod(instance, 0, "operator:v1", "cache-0-data")
	if len(pod.Spec.Containers) != 2 || pod.Spec.Containers[0].Name != appName {
		t.Fatalf("redis first, then the exporter: %v", pod.Spec.Containers)
	}
	exporter := pod.Spec.Containers[1]
	if exporter.Image != "exporter:v1" || exporter.Ports[0].ContainerPort != exporterPort ||
		!exporter.VolumeMounts[0].ReadOnly || exporter.Env[2].ValueFrom.SecretKeyRef.Name != "cache-internal-auth" {
		t.Errorf("exporter: %+v", exporter)
	}
	policy := networkPolicySpec(instance, "redis-operator-system")
	last := policy.Ingress[len(policy.Ingress)-1]
	if len(last.From) != 0 || last.Ports[0].Port.IntValue() != exporterPort {
		t.Errorf("anyone may scrape the exporter: %+v", last)
	}
}

func TestCountingRecorder(t *testing.T) {
	instance := &redisv1.RedisInstance{ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "count"}}
	recorder := CountingRecorder{EventRecorder: events.NewFakeRecorder(2)}
	recorder.Eventf(instance, nil, "Normal", "SwitchoverCompleted", "Switchover", "Moved the primary")
	recorder.Eventf(instance, nil, "Normal", "SwitchoverCompleted", "Switchover", "Moved the primary")
	if got := testutil.ToFloat64(eventsTotal.WithLabelValues("count", "RedisInstance", "cache", "Normal", "SwitchoverCompleted")); got != 2 {
		t.Errorf("counted %v Events, want 2", got)
	}
}

func TestOperationLog(t *testing.T) {
	instance := &redisv1.RedisInstance{ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "ops"}}
	instance.Status.Conditions = []metav1.Condition{
		{Type: redisv1.ConditionSwitchover, LastTransitionTime: metav1.NewTime(time.Now().Add(-time.Hour))},
	}
	var line string
	ctx := logf.IntoContext(context.Background(), funcr.New(func(prefix, args string) { line = args }, funcr.Options{}))

	startOperation(instance, "cache-1")
	operationLog(instance, "switchover", redisv1.ConditionSwitchover, "cache-0", "cache-1")(ctx)
	if !strings.Contains(line, `"msg"="Completed switchover"`) || !strings.Contains(line, `"durationMs"=0`) {
		t.Errorf("a switchover started in this process takes its time from memory: %s", line)
	}
	operationLog(instance, "switchover", redisv1.ConditionSwitchover, "cache-1", "cache-0")(ctx)
	if !strings.Contains(line, `"durationMs"=3600000`) && !strings.Contains(line, `"durationMs"=3600001`) {
		t.Errorf("one started before a restart takes it from the condition: %s", line)
	}
}
