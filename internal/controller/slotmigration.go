package controller

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/instancemanager"
)

// reshardBudget is how long one reconcile moves slots. Short, as the status,
// with the slots of every shard, is written once per reconcile; the next one
// follows a second later, see reshardRequeue.
//
// ponytail: other RedisClusters wait meanwhile; add MaxConcurrentReconciles
// when one operator runs many clusters.
const reshardBudget = 5 * time.Second

// reshardRequeue is when the next reconcile goes on with the slots.
const reshardRequeue = time.Second

// shardsIdle reports whether a slot may move while the first total shards
// are as they are: every instance Ready and no master moving. Ready of a
// shard would ask too much: a rolling update or a promote waits for the slots
// to move.
func (r *RedisClusterReconciler) shardsIdle(ctx context.Context, cluster *redisv1.RedisCluster, total int32) bool {
	for shard := range total {
		var child redisv1.RedisInstance
		key := types.NamespacedName{Name: shardName(cluster, shard), Namespace: cluster.Namespace}
		if err := r.Get(ctx, key, &child); err != nil ||
			child.Status.ReadyInstances != child.Spec.Instances || child.Status.TargetPrimary != "" {
			return false
		}
	}
	return true
}

// moveSlots moves slots, one at a time across the cluster, until every shard
// below spec.shards serves its share and the shards from spec.shards on none,
// for up to reshardBudget. A slot starts only while shardsIdle; the
// shards in turn start no CLUSTER FAILOVER while status.slotMigration is set,
// as a failover loses the MIGRATING flag of the source. It records the
// progress in status.slotMigration and the condition Migrating, and returns
// the nodes as they are after the moves.
func (r *RedisClusterReconciler) moveSlots(ctx context.Context, cluster *redisv1.RedisCluster,
	nodes []clusterNode, total int32) []clusterNode {
	assigned := assignedShards(cluster)
	deadline := time.Now().Add(reshardBudget)
	for {
		// The slots of a node that does not answer are not counted.
		if slices.ContainsFunc(nodes, func(n clusterNode) bool { return n.status == nil }) {
			if cluster.Status.SlotMigration != nil {
				setMigrating(cluster, metav1.ConditionTrue, "Waiting", fmt.Sprintf("%d slots to move; waiting for "+
					"every node to answer", cluster.Status.SlotMigration.SlotsToMove))
			}
			return nodes
		}
		move, ok := nextSlotMove(nodes, cluster.Spec.Shards, assigned)
		toMove := slotsToMove(nodes, cluster.Spec.Shards)
		if ok {
			toMove = max(toMove, 1) // a slot on the way may be all that is left
		}
		if toMove == 0 {
			cluster.Status.SlotMigration = nil
			setMigrating(cluster, metav1.ConditionFalse, "Balanced", "Every shard serves its share of the slots")
			return nodes
		}
		cluster.Status.SlotMigration = &redisv1.SlotMigration{SlotsToMove: int32(toMove)}

		switch {
		case !ok:
			setMigrating(cluster, metav1.ConditionTrue, "Waiting", fmt.Sprintf("%d slots to move; waiting for every slot "+
				"to have a master, and for a slot on the way to name a node of a shard", toMove))
			return nodes
		case !r.shardsIdle(ctx, cluster, total):
			setMigrating(cluster, metav1.ConditionTrue, "Waiting", fmt.Sprintf("%d slots to move; waiting for every "+
				"instance of every shard to be Ready, without a master moving", toMove))
			return nodes
		case time.Now().After(deadline):
			return nodes
		}
		setMigrating(cluster, metav1.ConditionTrue, "Moving",
			fmt.Sprintf("Moving slot %d from %s to %s; %d slots to move", move.slot, move.from.pod, move.to.pod, toMove))

		if err := r.moveSlot(ctx, cluster, move, deadline); err != nil {
			if strings.Contains(err.Error(), "BUSYKEY") {
				setMigrating(cluster, metav1.ConditionFalse, "Blocked", fmt.Sprintf("Moving slot %d from %s to %s "+
					"stopped: %s; delete the key on one of them", move.slot, move.from.pod, move.to.pod, err))
			} else {
				logf.FromContext(ctx).Error(err, "Failed to move slot", "slot", move.slot, "from", move.from.pod, "to", move.to.pod)
			}
			return nodes
		}
		nodes = r.clusterNodes(ctx, cluster, total)
	}
}

// moveSlot moves the keys of the slot and hands it over: the target imports
// it, the source sends its keys, and both take the new owner, the target
// first. Each step does nothing when done already, so a move cut short by
// the deadline, a restart or a failover goes on from where it is.
func (r *RedisClusterReconciler) moveSlot(ctx context.Context, cluster *redisv1.RedisCluster, move slotMove, deadline time.Time) error {
	im := r.InstanceManager
	from, to := move.from, move.to
	if _, err := im.Cluster(ctx, to.host, "importing",
		instancemanager.ClusterRequest{Slot: move.slot, NodeID: from.status.NodeID}); err != nil {
		return err
	}
	for {
		result, err := im.Migrate(ctx, from.host,
			instancemanager.ClusterRequest{Slot: move.slot, NodeID: to.status.NodeID, Host: to.host})
		if err != nil {
			return err
		}
		if result.Done {
			break
		}
		if time.Now().After(deadline) {
			return nil
		}
	}
	for _, node := range []clusterNode{to, from} {
		if _, err := im.Cluster(ctx, node.host, "setnode",
			instancemanager.ClusterRequest{Slot: move.slot, NodeID: to.status.NodeID}); err != nil {
			return err
		}
	}
	logf.FromContext(ctx).Info("Moved slot", "slot", move.slot, "from", from.pod, "to", to.pod)
	slotsMovedTotal.WithLabelValues(cluster.Namespace, cluster.Name, strconv.Itoa(int(from.shard)), strconv.Itoa(int(to.shard))).Inc()
	return nil
}

// removeDrainedShards deletes the RedisInstances of the shards from
// spec.shards on, with their Pods and PVCs, once they are empty; then
// planCluster makes the other nodes forget their nodes. Deleting a shard
// that still serves a slot would lose its keys, so it waits until:
//   - every slot has a master in a shard that stays: a slot without one may
//     belong to a node of a removed shard that does not answer;
//   - no node has a slot on the way: the last move may still have to hand
//     its slot over on the source.
func (r *RedisClusterReconciler) removeDrainedShards(ctx context.Context, cluster *redisv1.RedisCluster,
	nodes []clusterNode, total int32) error {
	if total == cluster.Spec.Shards {
		return nil // no shard to remove
	}
	owners := slotOwners(nodes)
	servedByRemaining := !slices.ContainsFunc(owners, func(o int) bool { return o < 0 || nodes[o].shard >= cluster.Spec.Shards })
	if !servedByRemaining || slices.ContainsFunc(nodes, hasSlotFlags) {
		return nil
	}
	for shard := cluster.Spec.Shards; shard < total; shard++ {
		child := &redisv1.RedisInstance{ObjectMeta: metav1.ObjectMeta{Name: shardName(cluster, shard), Namespace: cluster.Namespace}}
		logf.FromContext(ctx).Info("Deleting RedisInstance of removed shard", "name", child.Name)
		if err := r.Delete(ctx, child); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
}

func setMigrating(cluster *redisv1.RedisCluster, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type: redisv1.ConditionMigrating, Status: status, Reason: reason, Message: message,
		ObservedGeneration: cluster.Generation,
	})
}
