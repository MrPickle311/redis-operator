package instancemanager

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestParseClusterNodes(t *testing.T) {
	text := `07c3 10.0.0.1:6379@16379,cache-0-0.cache-0-hl.prod.svc myself,master - 0 0 1 connected 0-5460 [5461->-e7d1] [12001-<-e7d1]
e7d1 10.0.0.2:6379@16379,cache-1-0.cache-1-hl.prod.svc master - 0 1700000000000 2 connected 5461-10922 12000
9a2f 10.0.0.3:6379@16379,cache-0-1.cache-0-hl.prod.svc slave 07c3 0 1700000000000 1 connected
`
	nodes := parseClusterNodes(text)
	want := []clusterNode{
		{id: "07c3", addr: "10.0.0.1:6379", flags: []string{"myself", "master"}, slots: []string{"0-5460"},
			migrating: map[int]string{5461: "e7d1"}, importing: map[int]string{12001: "e7d1"}},
		{id: "e7d1", addr: "10.0.0.2:6379", flags: []string{"master"}, slots: []string{"5461-10922", "12000"}},
		{id: "9a2f", addr: "10.0.0.3:6379", flags: []string{"slave"}, master: "07c3"},
	}
	if !reflect.DeepEqual(nodes, want) {
		t.Errorf("parseClusterNodes() =\n%+v\nwant\n%+v", nodes, want)
	}
	if self, err := myself(nodes); err != nil || self.id != "07c3" {
		t.Errorf("myself() = %+v, %v", self, err)
	}
	if _, err := myself(nodes[1:]); err == nil {
		t.Error("myself() found a node in a list without one flagged myself")
	}
	for slot, want := range map[int]string{0: "07c3", 5460: "07c3", 5461: "e7d1", 12000: "e7d1", 12001: "", 16383: ""} {
		if got := slotOwner(nodes, slot); got != want {
			t.Errorf("slotOwner(%d) = %q, want %q", slot, got, want)
		}
	}
}

// TestClusterCommandsOnRedis needs two real Redis nodes in cluster mode on
// one network, port 6379 inside, with REDIS_TEST_CLUSTER_HOSTS their
// addresses on it, which nodes meet each other through:
//
//	docker network create rc
//	for n in a b; do docker run -d --rm --name rc-$n --network rc -p 0:6379 redis:7.2 --cluster-enabled yes; done
//	REDIS_TEST_CLUSTER_ADDRS=localhost:<port a>,localhost:<port b> \
//	REDIS_TEST_CLUSTER_HOSTS=<ip a>,<ip b> go test ./internal/instancemanager -run OnRedis
func TestClusterCommandsOnRedis(t *testing.T) {
	addrs, hosts := os.Getenv("REDIS_TEST_CLUSTER_ADDRS"), os.Getenv("REDIS_TEST_CLUSTER_HOSTS")
	if addrs == "" || hosts == "" {
		t.Skip("REDIS_TEST_CLUSTER_ADDRS and REDIS_TEST_CLUSTER_HOSTS not set")
	}
	ctx := context.Background()
	var nodes []*redis.Client
	for addr := range strings.SplitSeq(addrs, ",") {
		rdb := redis.NewClient(&redis.Options{Addr: addr})
		defer func() { _ = rdb.Close() }()
		nodes = append(nodes, rdb)
	}
	a, b := nodes[0], nodes[1]
	hostB := strings.Split(hosts, ",")[1]

	run := func(name string, command func(context.Context, *redis.Client, ClusterRequest) (bool, error),
		rdb *redis.Client, req ClusterRequest, want bool) {
		t.Helper()
		changed, err := command(ctx, rdb, req)
		if err != nil || changed != want {
			t.Fatalf("%s(%+v) = %v, %v, want changed %v", name, req, changed, err, want)
		}
	}
	status := func(rdb *redis.Client) *ClusterStatus {
		t.Helper()
		status, err := readStatus(ctx, rdb)
		if err != nil || status.Cluster == nil {
			t.Fatalf("readStatus() = %+v, %v, want a cluster status", status, err)
		}
		return status.Cluster
	}
	eventually := func(what string, cond func() bool) {
		t.Helper()
		for deadline := time.Now().Add(30 * time.Second); !cond(); time.Sleep(200 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting until %s", what)
			}
		}
	}

	run("clusterAddSlots", clusterAddSlots, a, ClusterRequest{Slots: "0-16383"}, true)
	run("clusterAddSlots", clusterAddSlots, a, ClusterRequest{Slots: "0-16383"}, false)
	if _, err := clusterAddSlots(ctx, a, ClusterRequest{Slots: "0-100"}); err == nil {
		t.Error("clusterAddSlots() gave other slots to a node that serves some")
	}
	if _, err := clusterAddSlots(ctx, b, ClusterRequest{Slots: "100"}); err == nil {
		t.Error("clusterAddSlots() took a slot that is not a range")
	}

	run("clusterMeet", clusterMeet, a, ClusterRequest{Host: hostB}, true)
	eventually("the nodes know each other", func() bool { return len(status(b).Nodes) == 2 })
	run("clusterMeet", clusterMeet, a, ClusterRequest{Host: hostB}, false)

	idA, idB := status(a).NodeID, status(b).NodeID
	run("clusterReplicate", clusterReplicate, b, ClusterRequest{NodeID: idA}, true)
	run("clusterReplicate", clusterReplicate, b, ClusterRequest{NodeID: idA}, false)
	eventually("b replicates a and the cluster is ok", func() bool {
		s := status(b)
		return s.MasterID == idA && s.State == "ok"
	})
	eventually("a serves every slot and the cluster is ok", func() bool {
		s := status(a)
		return reflect.DeepEqual(s.Slots, []string{"0-16383"}) && s.State == "ok"
	})

	run("clusterFailover", clusterFailover, a, ClusterRequest{}, false) // already the master
	eventually("b synced with a", func() bool {
		info, _ := b.InfoMap(ctx, "replication").Result()
		return initialSyncDone(info["Replication"])
	})
	run("clusterFailover", clusterFailover, b, ClusterRequest{}, true)
	eventually("b took over the slots", func() bool {
		s := status(b)
		return reflect.DeepEqual(s.Slots, []string{"0-16383"}) && status(a).MasterID == idB
	})

	run("clusterForget", clusterForget, b, ClusterRequest{NodeID: "0000000000000000000000000000000000000000"}, false)
}

// TestSlotMigrationOnRedis needs two fresh Redis nodes like
// TestClusterCommandsOnRedis, in REDIS_TEST_MIGRATE_ADDRS and
// REDIS_TEST_MIGRATE_HOSTS.
func TestSlotMigrationOnRedis(t *testing.T) {
	addrs, hosts := os.Getenv("REDIS_TEST_MIGRATE_ADDRS"), os.Getenv("REDIS_TEST_MIGRATE_HOSTS")
	if addrs == "" || hosts == "" {
		t.Skip("REDIS_TEST_MIGRATE_ADDRS and REDIS_TEST_MIGRATE_HOSTS not set")
	}
	ctx := context.Background()
	var nodes []*redis.Client
	for addr := range strings.SplitSeq(addrs, ",") {
		rdb := redis.NewClient(&redis.Options{Addr: addr})
		defer func() { _ = rdb.Close() }()
		nodes = append(nodes, rdb)
	}
	a, b := nodes[0], nodes[1]
	hostB := strings.Split(hosts, ",")[1]
	status := func(rdb *redis.Client) *ClusterStatus {
		t.Helper()
		status, err := readClusterStatus(ctx, rdb)
		if err != nil {
			t.Fatalf("readClusterStatus() = %v", err)
		}
		return status
	}
	must := func(name string, changed bool, err error, want bool) {
		t.Helper()
		if err != nil || changed != want {
			t.Fatalf("%s = %v, %v, want changed %v", name, changed, err, want)
		}
	}
	changed, err := clusterAddSlots(ctx, a, ClusterRequest{Slots: "0-16383"})
	must("clusterAddSlots()", changed, err, true)
	changed, err = clusterMeet(ctx, a, ClusterRequest{Host: hostB})
	must("clusterMeet()", changed, err, true)
	deadline := time.Now().Add(30 * time.Second)
	for status(b).State != "ok" || len(status(a).Nodes) != 2 {
		if time.Now().After(deadline) {
			t.Fatal("the cluster of a and b is not ok")
		}
		time.Sleep(200 * time.Millisecond)
	}
	idA, idB := status(a).NodeID, status(b).NodeID

	slot := int(a.ClusterKeySlot(ctx, "{m}").Val())
	for i := range 250 {
		a.Set(ctx, fmt.Sprintf("{m}:%d", i), i, 0)
	}
	changed, err = clusterImporting(ctx, b, ClusterRequest{Slot: slot, NodeID: idA})
	must("clusterImporting()", changed, err, true)
	changed, err = clusterImporting(ctx, b, ClusterRequest{Slot: slot, NodeID: idA})
	must("clusterImporting() again", changed, err, false)
	if status(b).Importing[slot] != idA {
		t.Fatalf("b imports %v, want slot %d from a", status(b).Importing, slot)
	}

	response, err := clusterMigrate(ctx, a, ClusterRequest{Slot: slot, Host: hostB, NodeID: idB}, time.Minute)
	if err != nil || !response.Done || !response.Changed {
		t.Fatalf("clusterMigrate() = %+v, %v, want done", response, err)
	}
	if status(a).Migrating[slot] != idB {
		t.Fatalf("a migrates %v, want slot %d to b", status(a).Migrating, slot)
	}
	for _, rdb := range []*redis.Client{b, a} {
		changed, err = clusterSetNode(ctx, rdb, ClusterRequest{Slot: slot, NodeID: idB})
		must("clusterSetNode()", changed, err, true)
		changed, err = clusterSetNode(ctx, rdb, ClusterRequest{Slot: slot, NodeID: idB})
		must("clusterSetNode() again", changed, err, false)
	}
	if got := b.Get(ctx, "{m}:249").Val(); got != "249" {
		t.Fatalf("b has {m}:249 = %q after the migration", got)
	}
	if s := status(a); len(s.Migrating) != 0 || slotInRanges(s.Slots, slot) {
		t.Fatalf("a still has slot %d: %+v", slot, s)
	}
	response, err = clusterMigrate(ctx, a, ClusterRequest{Slot: slot, Host: hostB, NodeID: idB}, time.Minute)
	if err != nil || !response.Done || response.Changed {
		t.Fatalf("clusterMigrate() of a slot moved already = %+v, %v", response, err)
	}

	// A key the target has already stops the migration, with its name.
	busy := int(a.ClusterKeySlot(ctx, "{n}").Val())
	a.Set(ctx, "{n}:1", "on a", 0)
	a.Set(ctx, "{n}:2", "on a", 0)
	changed, err = clusterImporting(ctx, b, ClusterRequest{Slot: busy, NodeID: idA})
	must("clusterImporting()", changed, err, true)
	conn := b.Conn()
	defer func() { _ = conn.Close() }()
	if err := conn.Do(ctx, "ASKING").Err(); err != nil {
		t.Fatal(err)
	}
	if err := conn.Set(ctx, "{n}:2", "on b", 0).Err(); err != nil {
		t.Fatal(err)
	}
	_, err = clusterMigrate(ctx, a, ClusterRequest{Slot: busy, Host: hostB, NodeID: idB}, time.Minute)
	if err == nil || !strings.Contains(err.Error(), `BUSYKEY: key "{n}:2"`) {
		t.Fatalf("clusterMigrate() onto a key of the target = %v, want BUSYKEY naming {n}:2", err)
	}
	if got := a.Get(ctx, "{n}:2").Val(); got != "on a" {
		t.Fatalf("a has {n}:2 = %q after BUSYKEY, want it kept", got)
	}
}
