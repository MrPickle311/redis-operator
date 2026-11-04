package controller

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
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
		[]string{"namespace", "kind", "name", "type", "reason"}, nil)
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
	}, []string{"namespace", "kind", "name", "type", "reason"})
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
	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
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

// operationStarts holds when this process started to move the primary of an
// instance to a Pod, by namespace/name/target: the conditions keep whole
// seconds only, and a switchover takes a tenth of one.
var operationStarts sync.Map

func operationKey(instance *redisv1.RedisInstance, target string) string {
	return instance.Namespace + "/" + instance.Name + "/" + target
}

// startOperation records the start of a switchover or failover to target.
func startOperation(instance *redisv1.RedisInstance, target string) {
	operationStarts.Store(operationKey(instance, target), time.Now())
}

// operationLog measures a switchover or failover that ends now, so call it
// before its condition changes; the func it returns logs it as one line,
// "Completed <kind>", which the dashboards turn into a row. After a restart
// of the operator the start comes from the condition, in whole seconds. A
// failover also tells how long the primary was unavailable.
func operationLog(instance *redisv1.RedisInstance, kind, conditionType, from, to string) func(context.Context) {
	var took time.Duration
	if start, ok := operationStarts.LoadAndDelete(operationKey(instance, to)); ok {
		took = time.Since(start.(time.Time))
	} else if c := meta.FindStatusCondition(instance.Status.Conditions, conditionType); c != nil {
		took = time.Since(c.LastTransitionTime.Time)
	}
	values := []any{"kind", kind, "from", from, "to", to, "epoch", instance.Status.PrimaryEpoch, "durationMs", took.Milliseconds()}
	if c := meta.FindStatusCondition(instance.Status.Conditions, redisv1.ConditionPrimaryAvailable); kind == "failover" && c != nil {
		values = append(values, "downMs", time.Since(c.LastTransitionTime.Time).Milliseconds())
	}
	return func(ctx context.Context) {
		logf.FromContext(ctx).Info("Completed "+kind, values...)
	}
}
