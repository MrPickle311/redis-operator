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
// the shard, then the other nodes replicate the master of the shard, then
// every node forgets the IDs no node has any more. A shard in assigned had
// its slots once: losing them all, e.g. with every volume, needs a person, as
// handing them out again would serve an empty dataset. A shard added later
// finds its slots served by the others and gets them by nextSlotMove.
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
	owners := slotOwners(nodes)
	for shard := range shards {
		members := shardNodes(nodes, shard)
		free := !slices.ContainsFunc(owners[shard*clusterSlots/shards:(shard+1)*clusterSlots/shards], func(o int) bool { return o >= 0 })
		if len(members) > 0 && shardMaster(members) == nil && !assigned[shard] && free {
			actions = append(actions, clusterAction{members[0].host, "addslots",
				instancemanager.ClusterRequest{Slots: slotRange(shard, shards)}})
		}
	}
	if len(actions) > 0 {
		return actions
	}
	for shard := range shards {
		members := shardNodes(nodes, shard)
		master := shardLeader(members, assigned[shard])
		if master == nil {
			continue
		}
		for _, node := range members {
			if node.status.NodeID != master.status.NodeID && len(node.status.Slots) == 0 && node.status.MasterID != master.status.NodeID {
				actions = append(actions, clusterAction{node.host, "replicate",
					instancemanager.ClusterRequest{NodeID: master.status.NodeID}})
			}
		}
	}
	if len(actions) > 0 {
		return actions
	}
	return forgetGone(nodes, shards, perShard, assigned)
}

// forgetGone makes every node forget the IDs that no node has: a node that
// came back on an empty volume has a new ID, and the old one stays known as
// failed. Only once every node answers, so the ID of a node that is down is
// kept, and every shard has a master: an old master still holding the slots
// is what its replica takes over from.
func forgetGone(nodes []clusterNode, shards, perShard int32, assigned map[int32]bool) []clusterAction {
	if len(nodes) != int(shards*perShard) {
		return nil
	}
	live := map[string]bool{}
	for _, node := range nodes {
		live[node.status.NodeID] = true
	}
	for shard := range shards {
		if shardLeader(shardNodes(nodes, shard), assigned[shard]) == nil {
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

// shardLeader returns the master of the shard: the node that serves slots or,
// in a shard that never had any, its first node, an empty master the others
// replicate until slots move to it.
func shardLeader(members []clusterNode, assigned bool) *clusterNode {
	if master := shardMaster(members); master != nil || assigned || len(members) == 0 {
		return master
	}
	return &members[0]
}

// slotMove moves one slot from the master from to the master to.
type slotMove struct {
	slot     int
	from, to clusterNode
}

// nextSlotMove returns the slot to move next so that every shard below shards
// serves as many slots as slotRange gives it, and the shards from shards on
// none. A slot already on the way goes first, from whichever master serves it
// now to the master of the shard it goes to: a failover loses the flags of the
// master that had them. Otherwise the first overfull shard gives its highest
// slot to the first underfull one, so no slot moves twice. Nothing moves while
// a node does not answer or a slot has no master.
func nextSlotMove(nodes []clusterNode, shards int32, assigned map[int32]bool) (slotMove, bool) {
	if slices.ContainsFunc(nodes, func(n clusterNode) bool { return n.status == nil }) {
		return slotMove{}, false
	}
	owners := slotOwners(nodes)
	if slices.Contains(owners, -1) {
		return slotMove{}, false
	}
	leader := func(shard int32) *clusterNode { return shardLeader(shardNodes(nodes, shard), assigned[shard]) }

	slot := -1
	for _, node := range nodes {
		for s := range node.status.Migrating {
			slot = minSlot(slot, s)
		}
		for s := range node.status.Importing {
			slot = minSlot(slot, s)
		}
	}
	if slot >= 0 {
		toShard := int32(-1)
		for _, node := range nodes {
			if _, ok := node.status.Importing[slot]; ok {
				toShard = node.shard
			}
		}
		for _, node := range nodes {
			if id, ok := node.status.Migrating[slot]; ok && toShard < 0 {
				for _, target := range nodes {
					if target.status.NodeID == id {
						toShard = target.shard
					}
				}
			}
		}
		from := nodes[owners[slot]]
		if from.shard == toShard {
			// The target serves it already: the source still has to let go.
			for _, node := range nodes {
				if _, ok := node.status.Migrating[slot]; ok && node.shard != toShard {
					from = node
				}
			}
		}
		// ponytail: a flag naming a node gone for good, or a target serving the
		// slot with no source left, stops all moves; a person clears the flag.
		to := leader(toShard)
		if toShard < 0 || to == nil || from.shard == toShard {
			return slotMove{}, false
		}
		return slotMove{slot, from, *to}, true
	}

	counts := shardSlotCounts(nodes, owners)
	for shard := range shards {
		to := leader(shard)
		if counts[shard] >= slotShare(shard, shards) || to == nil {
			continue
		}
		for _, donor := range nodes {
			if counts[donor.shard] <= slotShare(donor.shard, shards) {
				continue
			}
			for s := clusterSlots - 1; s >= 0; s-- {
				if nodes[owners[s]].shard == donor.shard {
					return slotMove{s, nodes[owners[s]], *to}, true
				}
			}
		}
	}
	return slotMove{}, false
}

// slotsToMove counts the slots nextSlotMove has yet to move to reach the
// shares of shards.
func slotsToMove(nodes []clusterNode, shards int32) int {
	moves := 0
	for shard, count := range shardSlotCounts(nodes, slotOwners(nodes)) {
		moves += max(0, count-slotShare(shard, shards))
	}
	return moves
}

// slotShare is how many slots the shard serves once balanced: what slotRange
// gives it, and none from shards on.
func slotShare(shard, shards int32) int {
	if shard >= shards {
		return 0
	}
	return int((shard+1)*clusterSlots/shards - shard*clusterSlots/shards)
}

// shardSlotCounts counts the slots of every shard in owners, see slotOwners.
func shardSlotCounts(nodes []clusterNode, owners []int) map[int32]int {
	counts := map[int32]int{}
	for _, o := range owners {
		if o >= 0 {
			counts[nodes[o].shard]++
		}
	}
	return counts
}

func minSlot(slot, s int) int {
	if slot < 0 || s < slot {
		return s
	}
	return slot
}

// slotOwners returns the index in nodes of the master serving each slot, as
// the masters tell it, or -1.
func slotOwners(nodes []clusterNode) []int {
	owners := slices.Repeat([]int{-1}, clusterSlots)
	for i, node := range nodes {
		if node.status == nil {
			continue
		}
		for _, r := range node.status.Slots {
			if first, last, ok := parseSlotRange(r); ok {
				for s := first; s <= last && s < clusterSlots; s++ {
					owners[s] = i
				}
			}
		}
	}
	return owners
}

// parseSlotRange parses a range as CLUSTER NODES lists it: "0-5460" or a
// single slot, "12000".
func parseSlotRange(r string) (first, last int, ok bool) {
	start, end, found := strings.Cut(r, "-")
	if !found {
		end = start
	}
	first, err1 := strconv.Atoi(start)
	last, err2 := strconv.Atoi(end)
	return first, last, err1 == nil && err2 == nil && first >= 0 && last >= first
}

// countSlots counts the slots of ranges as CLUSTER NODES lists them: "0-5460"
// or a single slot, "12000".
func countSlots(ranges []string) int {
	count := 0
	for _, r := range ranges {
		if first, last, ok := parseSlotRange(r); ok {
			count += last - first + 1
		}
	}
	return count
}
