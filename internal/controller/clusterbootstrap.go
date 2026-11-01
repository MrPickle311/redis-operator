package controller

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/MrPickle311/redis-operator/internal/instancemanager"
)

// clusterSlots is the number of hash slots of a Redis Cluster.
const clusterSlots = 16384

// clusterNode is what the operator knows about one node of a RedisCluster.
type clusterNode struct {
	shard int32
	pod   string
	host  string // the Pod DNS name
	// status is nil while the node does not answer in cluster mode.
	status *instancemanager.ClusterStatus
}

// clusterAction is a command of the Instance Manager API for one node.
type clusterAction struct {
	host, command string
	req           instancemanager.ClusterRequest
}

// slotRange returns the slots of shard: an equal part of all 16384.
func slotRange(shard, shards int32) string {
	return fmt.Sprintf("%d-%d", shard*clusterSlots/shards, (shard+1)*clusterSlots/shards-1)
}

// planCluster returns the next commands that build the cluster from nodes,
// ordered by shard and ordinal. Each stage needs the previous one done, so
// only the first stage with work returns commands, and a later reconcile goes
// on: the nodes meet, then the first node of every shard takes the slots of
// the shard, then the other nodes replicate the node with the slots, then
// every node forgets the IDs no node has any more. A shard in assigned had
// its slots once: losing them all, e.g. with every volume, needs a person, as
// handing them out again would serve an empty dataset.
func planCluster(nodes []clusterNode, shards, perShard int32, assigned map[int32]bool) []clusterAction {
	if slices.ContainsFunc(nodes, func(n clusterNode) bool { return n.status == nil }) {
		return nil // every node must answer before the cluster changes
	}
	var actions []clusterAction
	// One node meets every other one; gossip tells the rest.
	if slices.ContainsFunc(nodes, func(n clusterNode) bool { return len(n.status.Nodes) < len(nodes) }) {
		for _, node := range nodes[1:] {
			actions = append(actions, clusterAction{nodes[0].host, "meet", instancemanager.ClusterRequest{Host: node.host}})
		}
		return actions
	}
	for shard := range shards {
		members := shardNodes(nodes, shard)
		if len(members) > 0 && shardMaster(members) == nil && !assigned[shard] {
			actions = append(actions, clusterAction{members[0].host, "addslots",
				instancemanager.ClusterRequest{Slots: slotRange(shard, shards)}})
		}
	}
	if len(actions) > 0 {
		return actions
	}
	for shard := range shards {
		members := shardNodes(nodes, shard)
		master := shardMaster(members)
		if master == nil {
			continue
		}
		for _, node := range members {
			if len(node.status.Slots) == 0 && node.status.MasterID != master.status.NodeID {
				actions = append(actions, clusterAction{node.host, "replicate",
					instancemanager.ClusterRequest{NodeID: master.status.NodeID}})
			}
		}
	}
	if len(actions) > 0 {
		return actions
	}
	return forgetGone(nodes, shards, perShard)
}

// forgetGone makes every node forget the IDs that no node has: a node that
// came back on an empty volume has a new ID, and the old one stays known as
// failed. Only once every node answers, so the ID of a node that is down is
// kept, and every shard has a master: an old master still holding the slots
// is what its replica takes over from.
func forgetGone(nodes []clusterNode, shards, perShard int32) []clusterAction {
	if len(nodes) != int(shards*perShard) {
		return nil
	}
	live := map[string]bool{}
	for _, node := range nodes {
		live[node.status.NodeID] = true
	}
	for shard := range shards {
		if shardMaster(shardNodes(nodes, shard)) == nil {
			return nil
		}
	}
	var actions []clusterAction
	for _, node := range nodes {
		for _, id := range node.status.Nodes {
			if !live[id] {
				actions = append(actions, clusterAction{node.host, "forget", instancemanager.ClusterRequest{NodeID: id}})
			}
		}
	}
	return actions
}

func shardNodes(nodes []clusterNode, shard int32) []clusterNode {
	var members []clusterNode
	for _, node := range nodes {
		if node.shard == shard {
			members = append(members, node)
		}
	}
	return members
}

// shardMaster returns the node of the shard that serves slots, or nil.
func shardMaster(members []clusterNode) *clusterNode {
	for i := range members {
		if members[i].status != nil && len(members[i].status.Slots) > 0 {
			return &members[i]
		}
	}
	return nil
}

// countSlots counts the slots of ranges as CLUSTER NODES lists them: "0-5460"
// or a single slot, "12000".
func countSlots(ranges []string) int {
	count := 0
	for _, r := range ranges {
		start, end, found := strings.Cut(r, "-")
		if !found {
			end = start
		}
		first, err1 := strconv.Atoi(start)
		last, err2 := strconv.Atoi(end)
		if err1 == nil && err2 == nil && last >= first {
			count += last - first + 1
		}
	}
	return count
}
