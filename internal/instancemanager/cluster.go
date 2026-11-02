package instancemanager

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// clusterBusPort is where the nodes of a Redis Cluster talk to each other.
const clusterBusPort = 16379

// clusterConfigPath is on the PVC, next to the data: a restarted node keeps
// its ID, its slots and the nodes it knows.
const clusterConfigPath = "/data/nodes.conf"

// clusterConfig returns the lines of redis.conf for a node of a Redis
// Cluster announcing hostname, its Pod DNS name.
func clusterConfig(hostname string, tls bool) string {
	// Replica migration would move a replica to another shard, out of the
	// RedisInstance that owns its Pod.
	conf := fmt.Sprintf(`cluster-enabled yes
cluster-config-file %s
cluster-port %d
cluster-announce-hostname %s
cluster-preferred-endpoint-type hostname
cluster-allow-replica-migration no
`, clusterConfigPath, clusterBusPort, hostname)
	if tls {
		conf += "tls-cluster yes\n"
	}
	return conf
}

// ClusterStatus is the part of GET /status about the Redis Cluster, present
// only in cluster mode.
type ClusterStatus struct {
	NodeID string `json:"nodeID"`
	// MasterID is the node a replica replicates from.
	MasterID string `json:"masterID,omitempty"`
	// Slots the node serves as a master, as CLUSTER NODES lists them: "0-5460".
	Slots []string `json:"slots,omitempty"`
	// State is cluster_state of CLUSTER INFO: ok or fail.
	State string `json:"state"`
	// Nodes are the IDs of the nodes this one knows, itself included. A node
	// still in the handshake of CLUSTER MEET has a temporary ID and is left out.
	Nodes []string `json:"nodes"`
	// Migrating are the slots this master moves out, to the node of the value.
	Migrating map[int]string `json:"migrating,omitempty"`
	// Importing are the slots this master takes in, from the node of the value.
	Importing map[int]string `json:"importing,omitempty"`
}

// ClusterRequest is the body of the POST /cluster/* commands; each one reads
// one field.
type ClusterRequest struct {
	// Host is the Pod DNS name of the node to meet, for /cluster/meet.
	Host string `json:"host,omitempty"`
	// Slots is the range to serve, e.g. "0-5460", for /cluster/addslots.
	Slots string `json:"slots,omitempty"`
	// NodeID is the master to replicate, for /cluster/replicate, the node to
	// forget, for /cluster/forget, the node a slot comes from, for
	// /cluster/importing, goes to, for /cluster/migrate, or belongs to, for
	// /cluster/setnode.
	NodeID string `json:"nodeID,omitempty"`
	// Slot is the slot of /cluster/importing, /cluster/migrate and /cluster/setnode.
	Slot int `json:"slot"`
}

// MigrateResponse is the answer of /cluster/migrate.
type MigrateResponse struct {
	Changed bool `json:"changed"`
	// Done tells that the node holds no key of the slot any more.
	Done bool `json:"done"`
}

// clusterNode is one line of CLUSTER NODES.
type clusterNode struct {
	id, addr, master     string
	flags                []string
	slots                []string
	migrating, importing map[int]string
}

// parseClusterNodes reads CLUSTER NODES, whose lines are
// "<id> <ip:port@cport[,hostname]> <flags> <master> <ping> <pong> <epoch> <link> <slot>...".
func parseClusterNodes(text string) []clusterNode {
	var nodes []clusterNode
	for line := range strings.Lines(text) {
		fields := strings.Fields(line)
		if len(fields) < 8 {
			continue
		}
		node := clusterNode{
			id:    fields[0],
			addr:  strings.SplitN(fields[1], "@", 2)[0],
			flags: strings.Split(fields[2], ","),
		}
		if fields[3] != "-" {
			node.master = fields[3]
		}
		for _, slot := range fields[8:] {
			// "[1234->-<id>]" moves out, "[1234-<-<id>]" comes in; only the
			// line of myself lists them.
			move, ok := strings.CutPrefix(slot, "[")
			if !ok {
				node.slots = append(node.slots, slot)
				continue
			}
			move = strings.TrimSuffix(move, "]")
			if number, id, found := strings.Cut(move, "->-"); found {
				node.migrating = addMove(node.migrating, number, id)
			} else if number, id, found := strings.Cut(move, "-<-"); found {
				node.importing = addMove(node.importing, number, id)
			}
		}
		nodes = append(nodes, node)
	}
	return nodes
}

func addMove(moves map[int]string, number, id string) map[int]string {
	slot, err := strconv.Atoi(number)
	if err != nil {
		return moves
	}
	if moves == nil {
		moves = map[int]string{}
	}
	moves[slot] = id
	return moves
}

// slotOwner returns the ID of the node that serves slot, or "".
func slotOwner(nodes []clusterNode, slot int) string {
	for _, node := range nodes {
		if slotInRanges(node.slots, slot) {
			return node.id
		}
	}
	return ""
}

// slotInRanges reports whether slot is in ranges like "0-5460" or "12000".
func slotInRanges(ranges []string, slot int) bool {
	for _, r := range ranges {
		start, end, found := strings.Cut(r, "-")
		if !found {
			end = start
		}
		first, err1 := strconv.Atoi(start)
		last, err2 := strconv.Atoi(end)
		if err1 == nil && err2 == nil && first <= slot && slot <= last {
			return true
		}
	}
	return false
}

// myself returns the node of CLUSTER NODES that answered.
func myself(nodes []clusterNode) (clusterNode, error) {
	for _, node := range nodes {
		if slices.Contains(node.flags, "myself") {
			return node, nil
		}
	}
	return clusterNode{}, fmt.Errorf("CLUSTER NODES lists no node flagged myself")
}

func readClusterStatus(ctx context.Context, rdb *redis.Client) (*ClusterStatus, error) {
	text, err := rdb.ClusterNodes(ctx).Result()
	if err != nil {
		return nil, err
	}
	nodes := parseClusterNodes(text)
	self, err := myself(nodes)
	if err != nil {
		return nil, err
	}
	info, err := rdb.ClusterInfo(ctx).Result()
	if err != nil {
		return nil, err
	}
	status := &ClusterStatus{NodeID: self.id, MasterID: self.master, Slots: self.slots,
		Migrating: self.migrating, Importing: self.importing}
	for _, node := range nodes {
		if !slices.Contains(node.flags, "handshake") {
			status.Nodes = append(status.Nodes, node.id)
		}
	}
	for line := range strings.Lines(info) {
		if state, ok := strings.CutPrefix(strings.TrimSpace(line), "cluster_state:"); ok {
			status.State = state
		}
	}
	return status, nil
}

// clusterHandlers serves the commands the operator builds and repairs a Redis
// Cluster with. Each one is idempotent and returns whether it changed
// anything; Redis refuses them outside cluster mode.
func clusterHandlers(mux *http.ServeMux, rdb *redis.Client) {
	commands := map[string]func(context.Context, *redis.Client, ClusterRequest) (bool, error){
		"meet":      clusterMeet,
		"addslots":  clusterAddSlots,
		"replicate": clusterReplicate,
		"failover":  clusterFailover,
		"forget":    clusterForget,
		"importing": clusterImporting,
		"setnode":   clusterSetNode,
	}
	// One at a time: each reads the state of the node before it acts.
	var mu sync.Mutex
	serve := func(name string, command func(context.Context, ClusterRequest) (any, error)) {
		mux.HandleFunc("POST /cluster/"+name, func(w http.ResponseWriter, r *http.Request) {
			var req ClusterRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "expected a JSON body", http.StatusBadRequest)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			response, err := command(r.Context(), req)
			if err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			writeJSON(w, response)
		})
	}
	for name, command := range commands {
		serve(name, func(ctx context.Context, req ClusterRequest) (any, error) {
			changed, err := command(ctx, rdb, req)
			return CommandResponse{Changed: changed}, err
		})
	}
	serve("migrate", func(ctx context.Context, req ClusterRequest) (any, error) {
		return clusterMigrate(ctx, rdb, req, migrateBudget)
	})
}

// clusterMeet introduces the node req.Host. CLUSTER MEET takes only an IP, so
// the name is resolved here, in the Pod.
func clusterMeet(ctx context.Context, rdb *redis.Client, req ClusterRequest) (bool, error) {
	if req.Host == "" {
		return false, fmt.Errorf("host is required")
	}
	ips, err := net.DefaultResolver.LookupHost(ctx, req.Host)
	if err != nil {
		return false, err
	}
	text, err := rdb.ClusterNodes(ctx).Result()
	if err != nil {
		return false, err
	}
	addr := net.JoinHostPort(ips[0], strconv.Itoa(redisPort))
	if slices.ContainsFunc(parseClusterNodes(text), func(n clusterNode) bool { return n.addr == addr }) {
		return false, nil
	}
	return true, rdb.ClusterMeet(ctx, ips[0], strconv.Itoa(redisPort)).Err()
}

// clusterAddSlots makes the node serve req.Slots, unless it already does. A
// node that serves other slots is refused: moving slots is not this.
func clusterAddSlots(ctx context.Context, rdb *redis.Client, req ClusterRequest) (bool, error) {
	start, end, found := strings.Cut(req.Slots, "-")
	if !found {
		return false, fmt.Errorf("slots must be a range like 0-5460, got %q", req.Slots)
	}
	status, err := readClusterStatus(ctx, rdb)
	if err != nil {
		return false, err
	}
	if slices.Equal(status.Slots, []string{req.Slots}) {
		return false, nil
	}
	if len(status.Slots) > 0 {
		return false, fmt.Errorf("the node already serves the slots %v", status.Slots)
	}
	return true, rdb.Do(ctx, "CLUSTER", "ADDSLOTSRANGE", start, end).Err()
}

// clusterReplicate makes the node a replica of the master req.NodeID.
func clusterReplicate(ctx context.Context, rdb *redis.Client, req ClusterRequest) (bool, error) {
	status, err := readClusterStatus(ctx, rdb)
	if err != nil {
		return false, err
	}
	if status.MasterID == req.NodeID {
		return false, nil
	}
	return true, rdb.ClusterReplicate(ctx, req.NodeID).Err()
}

// clusterFailover makes a replica take over from its master with CLUSTER
// FAILOVER: the master stops writes until the replica has them all, and the
// masters vote as in a failover. It returns at once; GET /status tells when
// the node became the master.
func clusterFailover(ctx context.Context, rdb *redis.Client, _ ClusterRequest) (bool, error) {
	status, err := readStatus(ctx, rdb)
	if err != nil {
		return false, err
	}
	if status.Role == "master" {
		return false, nil
	}
	return true, rdb.ClusterFailover(ctx).Err()
}

// clusterForget removes req.NodeID, e.g. the old ID of a node that lost its
// volume. Every node must forget it within a minute, or gossip brings it back.
func clusterForget(ctx context.Context, rdb *redis.Client, req ClusterRequest) (bool, error) {
	err := rdb.ClusterForget(ctx, req.NodeID).Err()
	if err != nil && strings.Contains(err.Error(), "Unknown node") {
		return false, nil
	}
	return err == nil, err
}

// migrateBudget is how long one /cluster/migrate moves keys before it
// answers, well below the timeout of the client of the operator.
const migrateBudget = 5 * time.Second

// migrateBatch is how many keys one MIGRATE moves.
const migrateBatch = 100

// clusterImporting makes this master take in req.Slot from the master
// req.NodeID: it accepts the keys MIGRATE sends and the commands of clients
// sent there by ASK. A slot it serves already needs nothing.
func clusterImporting(ctx context.Context, rdb *redis.Client, req ClusterRequest) (bool, error) {
	status, err := readClusterStatus(ctx, rdb)
	if err != nil {
		return false, err
	}
	if slotInRanges(status.Slots, req.Slot) || status.Importing[req.Slot] == req.NodeID {
		return false, nil
	}
	return true, rdb.Do(ctx, "CLUSTER", "SETSLOT", req.Slot, "IMPORTING", req.NodeID).Err()
}

// clusterMigrate moves the keys of req.Slot to the master req.NodeID at
// req.Host, which must be importing it, for up to budget. Done means the slot
// is empty here, or not served here at all; then /cluster/setnode hands it
// over. A key that exists on the target already stops it: overwriting it
// could lose the newer of the two, so a person decides.
func clusterMigrate(ctx context.Context, rdb *redis.Client, req ClusterRequest, budget time.Duration) (MigrateResponse, error) {
	status, err := readClusterStatus(ctx, rdb)
	if err != nil {
		return MigrateResponse{}, err
	}
	if !slotInRanges(status.Slots, req.Slot) {
		return MigrateResponse{Done: true}, nil
	}
	var response MigrateResponse
	if status.Migrating[req.Slot] != req.NodeID {
		if err := rdb.Do(ctx, "CLUSTER", "SETSLOT", req.Slot, "MIGRATING", req.NodeID).Err(); err != nil {
			return response, err
		}
		response.Changed = true
	}
	// The target knows the user this node runs as: one Secret for the cluster.
	var auth []any
	if opts := rdb.Options(); opts.Password != "" {
		auth = []any{"AUTH2", opts.Username, opts.Password}
	}
	migrate := func(keys []string) error {
		args := append([]any{"MIGRATE", req.Host, redisPort, "", 0, 5000}, auth...)
		args = append(args, "KEYS")
		for _, key := range keys {
			args = append(args, key)
		}
		return rdb.Do(ctx, args...).Err()
	}
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		keys, err := rdb.ClusterGetKeysInSlot(ctx, req.Slot, migrateBatch).Result()
		if err != nil {
			return response, err
		}
		if len(keys) == 0 {
			response.Done = true
			return response, nil
		}
		if err := migrate(keys); err != nil {
			if !strings.Contains(err.Error(), "BUSYKEY") {
				return response, err
			}
			// Move them one by one to name the key.
			for _, key := range keys {
				if err := migrate([]string{key}); err != nil {
					if strings.Contains(err.Error(), "BUSYKEY") {
						return response, fmt.Errorf("BUSYKEY: key %q of slot %d exists on the target already", key, req.Slot)
					}
					return response, err
				}
			}
		}
		response.Changed = true
	}
	return response, nil
}

// clusterSetNode hands req.Slot to the master req.NodeID: on the target it
// ends the import, on the source, emptied by /cluster/migrate, it ends the
// migration, and the other masters learn it sooner than by gossip.
func clusterSetNode(ctx context.Context, rdb *redis.Client, req ClusterRequest) (bool, error) {
	text, err := rdb.ClusterNodes(ctx).Result()
	if err != nil {
		return false, err
	}
	nodes := parseClusterNodes(text)
	self, err := myself(nodes)
	if err != nil {
		return false, err
	}
	_, migrating := self.migrating[req.Slot]
	_, importing := self.importing[req.Slot]
	if slotOwner(nodes, req.Slot) == req.NodeID && !migrating && !importing {
		return false, nil
	}
	return true, rdb.Do(ctx, "CLUSTER", "SETSLOT", req.Slot, "NODE", req.NodeID).Err()
}
