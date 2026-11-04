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
	// One node meets every other one; gossip tells the rest. Also when a node
	// cannot reach one that runs: after every Pod restarted, each node knows
	// the others by their old addresses, and a MEET by DNS name gives the new
	// one to the ID it knows.
	live := map[string]bool{}
	for _, node := range nodes {
		live[node.status.NodeID] = true
	}
	if slices.ContainsFunc(nodes, func(n clusterNode) bool {
		return len(n.status.Nodes) < len(nodes) || slices.ContainsFunc(n.status.Unreachable, func(id string) bool { return live[id] })
	}) {
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
			if node.status.NodeID != master.status.NodeID && len(node.status.Slots) == 0 &&
				(node.status.MasterID != master.status.NodeID || node.status.StaleMaster) {
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

// nextSlotMove returns the one slot to move next, so that in the end every
// shard below shards serves its slotShare and the shards from shards on none.
// It reads only the nodes, never a state of its own, so it is safe to call
// after a restart, a failover or a move cut short. Nothing moves while a node
// does not answer, as its slots would look free, or while a slot has no
// master, as there is nothing to move it from.
func nextSlotMove(nodes []clusterNode, shards int32, assigned map[int32]bool) (slotMove, bool) {
	if slices.ContainsFunc(nodes, func(n clusterNode) bool { return n.status == nil }) {
		return slotMove{}, false
	}
	owners := slotOwners(nodes)
	if slices.Contains(owners, -1) {
		return slotMove{}, false
	}
	leader := func(shard int32) *clusterNode { return shardLeader(shardNodes(nodes, shard), assigned[shard]) }
	if slot, ok := flaggedSlot(nodes); ok {
		return finishSlotMove(nodes, owners, slot, leader)
	}
	return balanceSlotMove(nodes, owners, shards, leader)
}

// flaggedSlot returns the lowest slot that a node has a MIGRATING or
// IMPORTING flag for: a move that started and has not ended.
func flaggedSlot(nodes []clusterNode) (int, bool) {
	slot := -1
	for _, node := range nodes {
		for _, flags := range []map[int]string{node.status.Migrating, node.status.Importing} {
			for s := range flags {
				if slot < 0 || s < slot {
					slot = s
				}
			}
		}
	}
	return slot, slot >= 0
}

// finishSlotMove returns the move that ends the slot on the way. The flags
// say where it goes: the IMPORTING flag sits on the target, and the MIGRATING
// flag of the source names the target, in case a failover of the target lost
// the first. It goes from the master that serves it now, which after a
// failover of the source is the old replica without the flag; moveSlot sets
// the flags again. Once the target serves it, the source with the MIGRATING
// flag is all that is left to hand over.
//
// ponytail: a flag naming a node gone for good, or a target serving the slot
// with no source left, stops all moves; a person clears the flag.
func finishSlotMove(nodes []clusterNode, owners []int, slot int, leader func(int32) *clusterNode) (slotMove, bool) {
	toShard := int32(-1)
	if i := slices.IndexFunc(nodes, func(n clusterNode) bool { return n.status.Importing[slot] != "" }); i >= 0 {
		toShard = nodes[i].shard
	} else {
		for _, node := range nodes {
			if target := nodeByID(nodes, node.status.Migrating[slot]); target != nil {
				toShard = target.shard
				break
			}
		}
	}
	from := nodes[owners[slot]]
	if from.shard == toShard {
		i := slices.IndexFunc(nodes, func(n clusterNode) bool { return n.status.Migrating[slot] != "" && n.shard != toShard })
		if i < 0 {
			return slotMove{}, false
		}
		from = nodes[i]
	}
	to := leader(toShard)
	if toShard < 0 || to == nil {
		return slotMove{}, false
	}
	return slotMove{slot, from, *to}, true
}

// balanceSlotMove returns the move of a new slot: the highest slot of the
// first shard with more than its share goes to the first shard with less.
// A slot only ever leaves a shard with too many for one with too few, so no
// slot moves twice, and the moves are as few as slotsToMove counts.
func balanceSlotMove(nodes []clusterNode, owners []int, shards int32, leader func(int32) *clusterNode) (slotMove, bool) {
	counts := shardSlotCounts(nodes, owners)
	var to *clusterNode
	for shard := range shards {
		if counts[shard] < slotShare(shard, shards) {
			if to = leader(shard); to != nil {
				break
			}
		}
	}
	// nodes are ordered by shard, so this is the overfull shard with the lowest number.
	donor := slices.IndexFunc(nodes, func(n clusterNode) bool { return counts[n.shard] > slotShare(n.shard, shards) })
	if to == nil || donor < 0 {
		return slotMove{}, false
	}
	for s := clusterSlots - 1; s >= 0; s-- {
		if nodes[owners[s]].shard == nodes[donor].shard {
			return slotMove{s, nodes[owners[s]], *to}, true
		}
	}
	return slotMove{}, false
}

func nodeByID(nodes []clusterNode, id string) *clusterNode {
	for i := range nodes {
		if id != "" && nodes[i].status.NodeID == id {
			return &nodes[i]
		}
	}
	return nil
}

// hasSlotFlags reports whether the node has a slot on the way in or out.
func hasSlotFlags(node clusterNode) bool {
	return node.status != nil && (len(node.status.Migrating) > 0 || len(node.status.Importing) > 0)
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
