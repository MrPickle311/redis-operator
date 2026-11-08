package controller

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/instancemanager"
)

// exporterPort is where the redis_exporter sidecar serves its metrics.
const exporterPort = 9121

// exporterContainer is the sidecar of spec.monitoring. redis_exporter reads
// Redis over the unix socket of the Pod, so it needs no TLS; the directory
// of the socket is read-only to it, as it also holds the Instance Manager.
//
// ponytail: it logs in as _operator, like the Instance Manager; add a user
// with only INFO and the like if the sidecar must hold less than the Pod.
func exporterContainer(instance *redisv1.RedisInstance, controllerMount corev1.VolumeMount,
	security *corev1.SecurityContext) corev1.Container {
	controllerMount.ReadOnly = true
	return corev1.Container{
		Name:  "exporter",
		Image: instance.Spec.Monitoring.Image,
		Ports: []corev1.ContainerPort{{ContainerPort: exporterPort, Name: "metrics"}},
		Env: []corev1.EnvVar{
			{Name: "REDIS_ADDR", Value: "unix://" + controllerDir + "/redis.sock"},
			{Name: "REDIS_USER", Value: instancemanager.OperatorUser},
			{Name: "REDIS_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: internalAuthSecretName(instance)},
				Key:                  instancemanager.OperatorPasswordKey,
			}}},
			// redis_script_values: a sample of the keys by type and memory.
			{Name: "REDIS_EXPORTER_SCRIPT", Value: controllerDir + "/" + instancemanager.KeysScript},
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("32Mi")},
			Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("64Mi")},
		},
		VolumeMounts:    []corev1.VolumeMount{controllerMount},
		SecurityContext: security,
	}
}

// slotBlockSize is how many slots one series of
// redis_operator_cluster_slot_block_shard covers: 64 blocks, a row each on a
// dashboard.
const slotBlockSize = 256

var (
	podRoleDesc = prometheus.NewDesc("redis_operator_instance_pod_role",
		"Role of the Pod in status: 0 replica, 1 primary, 2 becoming the primary.",
		[]string{"namespace", "redis_instance", "pod"}, nil)
	epochDesc = prometheus.NewDesc("redis_operator_instance_primary_epoch",
		"status.primaryEpoch: grows with every change of the primary.", []string{"namespace", "redis_instance"}, nil)
	instancesDesc = prometheus.NewDesc("redis_operator_instance_instances",
		"spec.instances.", []string{"namespace", "redis_instance"}, nil)
	readyInstancesDesc = prometheus.NewDesc("redis_operator_instance_ready_instances",
		"status.readyInstances.", []string{"namespace", "redis_instance"}, nil)
	volumeDesc = prometheus.NewDesc("redis_operator_instance_volume",
		"1 for the state of the volume of every instance in status.volumes.",
		[]string{"namespace", "redis_instance", "ordinal", "state"}, nil)
	conditionDesc = prometheus.NewDesc("redis_operator_condition",
		"A condition of a RedisInstance or RedisCluster with its reason: 1 when True, 0 when False.",
		[]string{"namespace", labelKind, "name", "type", "reason"}, nil)
	shardSlotsDesc = prometheus.NewDesc("redis_operator_cluster_shard_slots",
		"Slots the master of the shard serves.", []string{"namespace", "redis_cluster", "shard"}, nil)
	shardPrimaryDesc = prometheus.NewDesc("redis_operator_cluster_shard_primary",
		"1 for the master of every shard.", []string{"namespace", "redis_cluster", "shard", "pod"}, nil)
	slotsToMoveDesc = prometheus.NewDesc("redis_operator_cluster_slots_to_move",
		"status.slotMigration.slotsToMove, 0 when balanced.", []string{"namespace", "redis_cluster"}, nil)
	slotBlockDesc = prometheus.NewDesc("redis_operator_cluster_slot_block_shard",
		"The shard that serves most slots of a block of 256, -1 for none.",
		[]string{"namespace", "redis_cluster", "block"}, nil)
)

// Counters keep what lasted less than a scrape apart: a switchover is over
// in a second, but its Events and moved slots stay counted.
//
// ponytail: the series of a deleted object stay until the operator restarts.
var (
	eventsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "redis_operator_events_total",
		Help: "Events the operator emitted, by the object they are about and their reason.",
	}, []string{"namespace", labelKind, "name", "type", "reason"})
	slotsMovedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "redis_operator_slots_moved_total",
		Help: "Slots moved between the shards of a RedisCluster.",
	}, []string{"namespace", "redis_cluster", "from_shard", "to_shard"})
)

func init() {
	metrics.Registry.MustRegister(eventsTotal, slotsMovedTotal)
}

// CountingRecorder counts every Event in redis_operator_events_total.
type CountingRecorder struct {
	events.EventRecorder
}

func (r CountingRecorder) Eventf(regarding, related runtime.Object, eventtype, reason, action, note string, args ...any) {
	if o, ok := regarding.(client.Object); ok {
		eventsTotal.WithLabelValues(o.GetNamespace(), reflect.TypeOf(o).Elem().Name(), o.GetName(), eventtype, reason).Inc()
	}
	r.EventRecorder.Eventf(regarding, related, eventtype, reason, action, note, args...)
}

// StatusCollector turns the status of every RedisInstance and RedisCluster
// into metrics when Prometheus scrapes, read from the cache of the manager,
// so a deleted object leaves no series behind.
type StatusCollector struct {
	Reader client.Reader
}

func (c StatusCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{podRoleDesc, epochDesc, instancesDesc, readyInstancesDesc, volumeDesc,
		conditionDesc, shardSlotsDesc, shardPrimaryDesc, slotsToMoveDesc, slotBlockDesc} {
		ch <- d
	}
}

func (c StatusCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	gauge := func(desc *prometheus.Desc, value float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, labels...)
	}

	var instances redisv1.RedisInstanceList
	if err := c.Reader.List(ctx, &instances); err == nil {
		for _, i := range instances.Items {
			ns, name := i.Namespace, i.Name
			for ordinal := range i.Spec.Instances {
				pod := fmt.Sprintf("%s-%d", name, ordinal)
				role := 0.0
				switch pod {
				case i.Status.TargetPrimary:
					role = 2
				case i.Status.CurrentPrimary:
					role = 1
				}
				gauge(podRoleDesc, role, ns, name, pod)
			}
			gauge(epochDesc, float64(i.Status.PrimaryEpoch), ns, name)
			gauge(instancesDesc, float64(i.Spec.Instances), ns, name)
			gauge(readyInstancesDesc, float64(i.Status.ReadyInstances), ns, name)
			for _, v := range i.Status.Volumes {
				gauge(volumeDesc, 1, ns, name, strconv.Itoa(int(v.Ordinal)), string(v.State))
			}
			for _, cond := range i.Status.Conditions {
				gauge(conditionDesc, boolValue(cond.Status == "True"), ns, "RedisInstance", name, cond.Type, cond.Reason)
			}
		}
	}

	var clusters redisv1.RedisClusterList
	if err := c.Reader.List(ctx, &clusters); err == nil {
		for _, cl := range clusters.Items {
			ns, name := cl.Namespace, cl.Name
			for _, s := range cl.Status.Shards {
				shard := strings.TrimPrefix(s.Name, name+"-")
				gauge(shardSlotsDesc, float64(countSlots(strings.Split(s.Slots, ","))), ns, name, shard)
				if s.Primary != "" {
					gauge(shardPrimaryDesc, 1, ns, name, shard, s.Primary)
				}
			}
			toMove := 0
			if cl.Status.SlotMigration != nil {
				toMove = int(cl.Status.SlotMigration.SlotsToMove)
			}
			gauge(slotsToMoveDesc, float64(toMove), ns, name)
			for block, shard := range slotBlockShards(cl.Status.Shards, name) {
				first := block * slotBlockSize
				gauge(slotBlockDesc, float64(shard), ns, name, fmt.Sprintf("%05d-%05d", first, first+slotBlockSize-1))
			}
			for _, cond := range cl.Status.Conditions {
				gauge(conditionDesc, boolValue(cond.Status == "True"), ns, "RedisCluster", name, cond.Type, cond.Reason)
			}
		}
	}
}

// slotBlockShards returns, for every block of slotBlockSize slots, the
// number of the shard that serves most of them, or -1 when none serves any.
func slotBlockShards(shards []redisv1.ShardStatus, cluster string) []int {
	counts := make([]map[int]int, clusterSlots/slotBlockSize)
	for _, s := range shards {
		shard, err := strconv.Atoi(strings.TrimPrefix(s.Name, cluster+"-"))
		if err != nil {
			continue
		}
		for r := range strings.SplitSeq(s.Slots, ",") {
			first, last, ok := parseSlotRange(r)
			for slot := first; ok && slot <= last && slot < clusterSlots; slot++ {
				block := slot / slotBlockSize
				if counts[block] == nil {
					counts[block] = map[int]int{}
				}
				counts[block][shard]++
			}
		}
	}
	owners := make([]int, len(counts))
	for block, byShard := range counts {
		owners[block] = -1
		for shard, n := range byShard {
			if owners[block] < 0 || n > byShard[owners[block]] || n == byShard[owners[block]] && shard < owners[block] {
				owners[block] = shard
			}
		}
	}
	return owners
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// operationStarts holds when this process started an operation on an
// instance (a switchover, failover or rolling update), by
// namespace/name/key: the conditions keep whole seconds only, and a
// switchover takes a tenth of one.
var operationStarts sync.Map

func operationKey(instance *redisv1.RedisInstance, key string) string {
	return instance.Namespace + "/" + instance.Name + "/" + key
}

// started is when and why an operation started.
type started struct {
	at     time.Time
	reason string
}

// startOperation records the start of an operation and why it started; key
// tells it apart, e.g. the target of a switchover.
func startOperation(instance *redisv1.RedisInstance, key, reason string) {
	operationStarts.Store(operationKey(instance, key), started{time.Now(), reason})
}

// lastLogged holds the last operation line logged per instance, so a
// reconcile repeated after a conflict does not log it twice.
var lastLogged sync.Map

type logged struct {
	line string
	at   time.Time
}

// endOperation measures an operation that ends now with outcome (completed,
// rejected, failed, interrupted, approved, ...), so call it before its
// condition changes; the func it returns logs it as one line, "Finished
// <kind>", which the dashboards turn into a row with its time window. The
// start is the one of startOperation under key or, after a restart of the
// operator, the last change of conditionType, in whole seconds; neither: the
// operation took no time, e.g. a rejected request. A failover and a safe
// start also tell how long the primary was unavailable.
func endOperation(instance *redisv1.RedisInstance, kind, outcome, key, conditionType string, values ...any) func(context.Context) {
	end := time.Now()
	start := end
	if v, ok := operationStarts.LoadAndDelete(operationKey(instance, key)); ok && key != "" {
		start = v.(started).at
		if !slices.Contains(values, any("reason")) && v.(started).reason != "" {
			values = append(values, "reason", v.(started).reason)
		}
	} else if c := meta.FindStatusCondition(instance.Status.Conditions, conditionType); c != nil {
		start = c.LastTransitionTime.Time
	}
	values = append(values, labelKind, kind, "outcome", outcome, "epoch", instance.Status.PrimaryEpoch,
		"startMs", start.UnixMilli(), "endMs", end.UnixMilli(), "durationMs", end.Sub(start).Milliseconds())
	if c := meta.FindStatusCondition(instance.Status.Conditions, redisv1.ConditionPrimaryAvailable); (kind == "failover" || kind == "safe start") &&
		c != nil && c.Status == metav1.ConditionFalse {
		values = append(values, "downMs", end.Sub(c.LastTransitionTime.Time).Milliseconds())
	}
	return func(ctx context.Context) {
		// The same line again within seconds is the same operation, from a
		// reconcile repeated after a conflict; times differ, so leave them out.
		line := fmt.Sprint(kind, outcome, values[:len(values)-6])
		if last, ok := lastLogged.Swap(operationKey(instance, ""), logged{line, end}); ok &&
			last.(logged).line == line && end.Sub(last.(logged).at) < 10*time.Second {
			return
		}
		logf.FromContext(ctx).Info("Finished "+kind, values...)
	}
}

// volumeOps holds the operations on a volume this process started, by
// namespace/name/ordinal: a recreate asked for, or a replica moved off a
// volume that is deleted or lost. One ends when the replica is Ready again
// on another volume, after its full sync.
var volumeOps sync.Map

type volumeOp struct {
	kind, reason string
	start        time.Time
	uid          types.UID // of the volume it leaves
}

// startVolumeOperation records an operation that moves the replica of
// ordinal to a new volume; call it before the binding changes.
func startVolumeOperation(ctx context.Context, instance *redisv1.RedisInstance, ordinal int32, kind, reason string) {
	vol := volumeOf(instance, ordinal)
	op := volumeOp{kind: kind, reason: reason, start: time.Now(), uid: vol.UID}
	if _, loaded := volumeOps.LoadOrStore(operationKey(instance, strconv.Itoa(int(ordinal))), op); !loaded {
		logf.FromContext(ctx).Info("Started "+kind, "pod", instancePodName(instance, ordinal), "pvc", vol.ClaimName, "reason", reason)
	}
}

// endVolumeOperations logs, as "Finished <kind>", every operation on a volume
// whose replica is Ready again on another volume.
func endVolumeOperations(ctx context.Context, instance *redisv1.RedisInstance, ready map[int32]bool) {
	for _, vol := range instance.Status.Volumes {
		key := operationKey(instance, strconv.Itoa(int(vol.Ordinal)))
		v, ok := volumeOps.Load(key)
		if !ok || !ready[vol.Ordinal] || vol.State != redisv1.VolumeReady || vol.UID == "" || vol.UID == v.(volumeOp).uid {
			continue
		}
		volumeOps.Delete(key)
		op, end := v.(volumeOp), time.Now()
		logf.FromContext(ctx).Info("Finished "+op.kind, "to", instancePodName(instance, vol.Ordinal), "pvc", vol.ClaimName,
			"reason", op.reason, "kind", op.kind, "outcome", "completed", "epoch", instance.Status.PrimaryEpoch,
			"startMs", op.start.UnixMilli(), "endMs", end.UnixMilli(), "durationMs", end.Sub(op.start).Milliseconds())
	}
}

// lastInstances holds the spec.instances this process saw last, by
// namespace/name, to tell a change of it.
var lastInstances sync.Map

// startScaling notes a change of spec.instances as the start of a scale-up
// or scale-down. It runs first in a reconcile, before any early return, as
// the instances it compares with are the ones with a volume in the status,
// which the reconcile changes. Unseen, e.g. after a restart of the operator,
// a count that differs from those is a change too; a new RedisInstance is a
// scale-up from 0.
func startScaling(ctx context.Context, instance *redisv1.RedisInstance) {
	to := instance.Spec.Instances
	var had int32
	for _, vol := range instance.Status.Volumes {
		if vol.State != redisv1.VolumeDetached {
			had++
		}
	}
	prev, seen := lastInstances.Swap(operationKey(instance, ""), to)
	if !seen {
		prev = had
	}
	if prev.(int32) == to {
		return
	}
	if running, ok := operationStarts.LoadAndDelete(operationKey(instance, "scale-kind")); ok {
		endOperation(instance, running.(string), "interrupted", "scale", "", "reason", "spec.instances changed again")(ctx)
	}
	kind := "scale up"
	if to < prev.(int32) {
		kind = "scale down"
	}
	startOperation(instance, "scale", fmt.Sprintf("spec.instances %d -> %d", prev.(int32), to))
	operationStarts.Store(operationKey(instance, "scale-kind"), kind)
	logf.FromContext(ctx).Info("Started "+kind, "from", prev, "to", to)
}

// endScaling ends a scale-up once every instance is Ready, a scale-down once
// no instance above the count has a volume (an external one stays, Detached).
func endScaling(ctx context.Context, instance *redisv1.RedisInstance, readyCount int32) {
	kind, running := operationStarts.Load(operationKey(instance, "scale-kind"))
	if !running {
		return
	}
	to, above := instance.Spec.Instances, false
	for _, vol := range instance.Status.Volumes {
		above = above || vol.Ordinal >= to && vol.State != redisv1.VolumeDetached
	}
	if readyCount == to && !above {
		operationStarts.Delete(operationKey(instance, "scale-kind"))
		endOperation(instance, kind.(string), "completed", "scale", "", "to", fmt.Sprintf("%d instances", to))(ctx)
	}
}
