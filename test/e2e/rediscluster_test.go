//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/MrPickle311/redis-operator/test/utils"
)

const rcName = "rc"

// rcManifest is, in namespace ns, a cluster of three shards of two nodes with TLS, a user of
// the cluster and a NetworkPolicy that lets in only Pods labeled role=client.
func rcManifest(ns string) string {
	return fmt.Sprintf(`
apiVersion: redis.operator.com/v1
kind: RedisCluster
metadata: {name: %[1]s, namespace: %[2]s}
spec:
  shards: 3
  instancesPerShard: 2
  image: %[3]s
  storage:
    volumeClaimTemplate: {size: 256Mi}
  resources:
    requests: {memory: 128Mi}
    limits: {memory: 128Mi}
  networkPolicy:
    allowFrom:
    - podSelector: {matchLabels: {role: client}}
  monitoring: {enabled: true, image: %[4]s}
---
apiVersion: redis.operator.com/v1
kind: RedisUser
metadata: {name: app, namespace: %[2]s}
spec:
  targetRef: {kind: RedisCluster, name: %[1]s}
  authentication: {generate: true}
  acl: {rules: ["~*", "+@all"]}
`, rcName, ns, redisImage, exporterImage)
}

// rcClient is a Pod with redis-cli, the CA of the cluster and the password of
// the user app; role tells whether the NetworkPolicy lets it in. It runs
// script, or waits for kubectl exec.
func rcClient(ns, name, role, script string) string {
	if script == "" {
		script = "sleep infinity"
	}
	return fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata: {name: %[1]s, namespace: %[2]s, labels: {role: %[3]s}}
spec:
  securityContext: {runAsNonRoot: true, runAsUser: 999, seccompProfile: {type: RuntimeDefault}}
  containers:
  - name: cli
    image: %[4]s
    command: [sh, -c, %[6]q]
    securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}
    env:
    - name: REDISCLI_AUTH
      valueFrom: {secretKeyRef: {name: app-redis-user, key: password}}
    volumeMounts: [{name: ca, mountPath: /ca}]
  volumes:
  - name: ca
    secret:
      secretName: %[5]s-ca
      items: [{key: ca.crt, path: ca.crt}]
`, name, ns, role, redisImage, rcName, script)
}

// rcCLI is redis-cli as the user app, over TLS, for the scripts of rcClient.
const rcCLI = "redis-cli --tls --cacert /ca/ca.crt --user app --no-auth-warning"

// Scenario: a Redis Cluster serves clients through the seed Service with the
// users of the cluster on every node, behind its NetworkPolicy; it fails over
// on its own while the operator is down, updates its Pods without
// CLUSTERDOWN, and takes back a replica that lost its volume as a new node.
// Then it grows to 4 shards and back to 3, moving slots with their keys while
// a client writes, through a restart of the operator and a failover of the
// source, and stops at a key both shards of a slot have.
//
// The specs run as two Describes of their own cluster, so the first runs in
// parallel with the rest of the suite; the second stops the operator, so it is
// Serial.
var _ = Describe("RedisCluster", Ordered, func() { rcSpecs("redis-e2e-cluster", false) })
var _ = Describe("RedisCluster changing shards", Ordered, Serial, func() { rcSpecs("redis-e2e-cluster-shards", true) })

// rcSpecs registers the specs of a cluster in namespace rcNamespace: without
// resharding the ones the operator keeps running for, with it the ones that
// stop it, fail over and move slots.
func rcSpecs(rcNamespace string, resharding bool) {
	SetDefaultEventuallyTimeout(4 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)

	seed := fmt.Sprintf("%s.%s.svc", rcName, rcNamespace)
	get := func(kind, name, jsonpath string) string {
		out, _ := kubectl("get", kind, name, "-n", rcNamespace, "-o", "jsonpath="+jsonpath)
		return out
	}
	// cli runs redis-cli in the Pod as the user app against host, and returns
	// the output with the errors of Redis.
	cli := func(pod, host string, args ...string) string {
		out, _ := utils.Run(exec.Command("kubectl", append([]string{"exec", pod, "-n", rcNamespace, "--",
			"redis-cli", "-h", host, "--tls", "--cacert", "/ca/ca.crt", "--user", "app", "--no-auth-warning"}, args...)...))
		return strings.TrimSpace(out)
	}
	nodeFQDN := func(pod string) string {
		shard := pod[:strings.LastIndex(pod, "-")]
		return fmt.Sprintf("%s.%s-hl.%s.svc", pod, shard, rcNamespace)
	}
	primaryOf := func(shard string) string { return get("redisinstance", shard, "{.status.currentPrimary}") }
	replicaOf := func(shard string) string {
		out, _ := kubectl("get", "pods", "-n", rcNamespace, "-l", "redis.operator.com/instance="+shard+",redis.operator.com/role=replica",
			"-o", "jsonpath={.items[0].metadata.name}")
		return out
	}
	clusterReady := func() string {
		return get("rediscluster", rcName, `{.status.clusterState} {.status.conditions[?(@.type=="Ready")].status}`)
	}
	condition := func(conditionType, field string) string {
		return get("rediscluster", rcName, fmt.Sprintf(`{.status.conditions[?(@.type=="%s")].%s}`, conditionType, field))
	}
	// shell runs script in the Pod client.
	shell := func(script string) string {
		out, _ := utils.Run(exec.Command("kubectl", "exec", "client", "-n", rcNamespace, "--", "sh", "-c", script))
		return strings.TrimSpace(out)
	}
	// myself returns the line of CLUSTER NODES of the node itself.
	myself := func(pod string) string {
		for _, line := range strings.Split(cli("client", nodeFQDN(pod), "cluster", "nodes"), "\n") {
			if strings.Contains(line, "myself") {
				return line
			}
		}
		return ""
	}
	// slotsOf returns the slots the node serves, by its own CLUSTER NODES.
	slotsOf := func(pod string) map[int]bool {
		slots := map[int]bool{}
		fields := strings.Fields(myself(pod))
		for _, r := range fields[min(8, len(fields)):] {
			start, end, found := strings.Cut(r, "-")
			if strings.HasPrefix(r, "[") {
				continue
			}
			if !found {
				end = start
			}
			first, _ := strconv.Atoi(start)
			last, _ := strconv.Atoi(end)
			for slot := first; slot <= last; slot++ {
				slots[slot] = true
			}
		}
		return slots
	}
	// getAll reads the keys prefix:1 to prefix:n through the seed, one value
	// per line, "" for a missing key.
	getAll := func(prefix string, n int) []string {
		out := shell(fmt.Sprintf(`seq 1 %d | sed 's/^/GET %s:/' | %s -c -h %s | grep -v '^-> '`, n, prefix, rcCLI, seed))
		return strings.Split(out, "\n")
	}
	// expectAllKeys checks that the keys data:1 to data:2000 have their values.
	expectAllKeys := func() {
		values := getAll("data", 2000)
		Expect(values).To(HaveLen(2000))
		for i, value := range values {
			Expect(value).To(Equal(strconv.Itoa(i+1)), "data:%d", i+1)
		}
	}
	// flagsPerSample checks the CLUSTER NODES lines the Pod sampler logged:
	// at most one slot with MIGRATING or IMPORTING on the node at a time.
	expectOneSlotAtATime := func(sampler string) {
		logs, err := kubectl("logs", sampler, "-n", rcNamespace)
		Expect(err).NotTo(HaveOccurred())
		flag := regexp.MustCompile(`\[\d+-[<>]-`)
		samples, flagged := 0, 0
		for _, line := range strings.Split(logs, "\n") {
			if !strings.Contains(line, "myself") {
				continue
			}
			samples++
			n := len(flag.FindAllString(line, -1))
			Expect(n).To(BeNumerically("<=", 1), "two slots on the way at once: %s", line)
			flagged += n
		}
		_, _ = fmt.Fprintf(GinkgoWriter, "%s: %d samples, %d with a slot on the way\n", sampler, samples, flagged)
		Expect(flagged).To(BeNumerically(">", 0), "the sampler never saw a slot on the way")
		_, _ = kubectl("delete", "pod", sampler, "-n", rcNamespace, "--wait=false")
	}
	// sampler logs the line of CLUSTER NODES of the node of pod as fast as it can.
	sampler := func(name, pod string) string {
		return rcClient(rcNamespace, name, "client", fmt.Sprintf(`while true; do timeout 5 %s -h %s cluster nodes | grep myself; sleep 0.1; done`,
			rcCLI, nodeFQDN(pod)))
	}
	before := map[string]map[int]bool{} // the slots of every shard before 3 → 4
	var promoted string                 // the replica of rc-2 a promote waits for

	BeforeAll(func() {
		Expect(loadImageIntoKind(redisImage)).To(Succeed())
		Expect(loadImageIntoKind(exporterImage)).To(Succeed())
		Expect(ensureRestrictedNamespace(rcNamespace)).To(Succeed())
		Expect(kubectlApply(rcManifest(rcNamespace))).To(Succeed())
		Eventually(clusterReady).Should(Equal("ok True"))
		Eventually(func() string { return get("redisuser", "app", `{.status.conditions[?(@.type=="Ready")].reason}`) }).
			Should(Equal("Applied"))
		Expect(kubectlApply(rcClient(rcNamespace, "client", "client", "") + "---" + rcClient(rcNamespace, "stranger", "other", ""))).To(Succeed())
		_, err := kubectl("wait", "pod", "client", "stranger", "-n", rcNamespace, "--for=condition=Ready", "--timeout=3m")
		Expect(err).NotTo(HaveOccurred())
		for i := range 30 {
			Expect(cli("client", seed, "-c", "set", fmt.Sprintf("key:%d", i), fmt.Sprint(i))).To(Equal("OK"))
		}
	})

	AfterAll(func() {
		if CurrentSpecReport().Failed() {
			out, _ := kubectl("get", "redisclusters,redisinstances,pods", "-n", rcNamespace, "-o", "wide", "-L", "redis.operator.com/role")
			_, _ = fmt.Fprintf(GinkgoWriter, "Resources in %s:\n%s\n", rcNamespace, out)
			out, _ = kubectl("logs", "-n", namespace, "-l", managerPodSelector, "--tail=-1")
			moves := slices.DeleteFunc(strings.Split(out, "\n"), func(line string) bool {
				return !strings.Contains(line, "cluster command") && !strings.Contains(line, "slot")
			})
			_, _ = fmt.Fprintf(GinkgoWriter, "Cluster commands of the operator:\n%s\n", strings.Join(moves, "\n"))
		}
		_, _ = kubectl("delete", "ns", rcNamespace, "--ignore-not-found", "--timeout=3m")
	})

	if !resharding {
		It("serves keys of every shard through the seed, and sends clients to the nodes by name", func() {
			Expect(cli("client", seed, "-c", "get", "key:17")).To(Equal("17"))
			Expect(cli("client", seed, "cluster", "info")).To(ContainSubstring("cluster_state:ok"))
			// Without -c, a node answers for a slot of another one with MOVED to its name.
			Eventually(func() string { return cli("client", seed, "get", "key:1") }).
				Should(MatchRegexp(`MOVED \d+ rc-\d-\d\.rc-\d-hl\.` + rcNamespace + `\.svc:6379`))
		})

		It("has the user of the cluster on every node", func() {
			for _, shard := range []string{"rc-0", "rc-1", "rc-2"} {
				for _, ordinal := range []string{"0", "1"} {
					Expect(cli("client", nodeFQDN(shard+"-"+ordinal), "ping")).To(Equal("PONG"), "%s-%s", shard, ordinal)
				}
			}
		})

		It("reports shards and slots to Prometheus: the exporter of every node, past the NetworkPolicy, and the operator", func() {
			Eventually(func(g Gomega) {
				metrics, err := kubectl("get", "--raw", "/api/v1/namespaces/"+rcNamespace+"/pods/rc-0-0:9121/proxy/metrics")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(metrics).To(ContainSubstring("redis_up 1"))
			}).Should(Succeed())
			Eventually(func(g Gomega) {
				metrics, err := operatorMetrics()
				g.Expect(err).NotTo(HaveOccurred())
				for shard, slots := range []int{5461, 5461, 5462} {
					g.Expect(metrics).To(ContainSubstring(fmt.Sprintf(
						`redis_operator_cluster_shard_slots{namespace="%s",redis_cluster="%s",shard="%d"} %d`, rcNamespace, rcName, shard, slots)))
				}
				g.Expect(metrics).To(ContainSubstring(fmt.Sprintf(
					`redis_operator_cluster_slot_block_shard{block="16128-16383",namespace="%s",redis_cluster="%s"} 2`, rcNamespace, rcName)))
			}).Should(Succeed())
		})

		It("lets in only the clients of the NetworkPolicy", func() {
			out, _ := utils.Run(exec.Command("kubectl", "exec", "stranger", "-n", rcNamespace, "--",
				"timeout", "5", "redis-cli", "-h", seed, "--tls", "--cacert", "/ca/ca.crt", "--user", "app", "--no-auth-warning", "ping"))
			Expect(out).NotTo(ContainSubstring("PONG"))
		})

		It("takes back a replica that lost its volume as a new node, and forgets the old one", func() {
			replica := replicaOf("rc-1")
			oldID := cli("client", nodeFQDN(replica), "cluster", "myid")
			Expect(oldID).To(HaveLen(40))
			_, err := kubectl("annotate", "redisinstance", "rc-1", "-n", rcNamespace, "redis.operator.com/recreate="+replica, "--overwrite")
			Expect(err).NotTo(HaveOccurred())

			var newID string
			Eventually(func() string {
				newID = cli("client", nodeFQDN(replica), "cluster", "myid")
				return newID
			}).Should(SatisfyAll(HaveLen(40), Not(Equal(oldID))))
			masterID := cli("client", nodeFQDN(primaryOf("rc-1")), "cluster", "myid")
			Eventually(func() string { return cli("client", nodeFQDN(replica), "cluster", "nodes") }).
				Should(SatisfyAll(MatchRegexp(newID+` \S+ myself,slave `+masterID), Not(ContainSubstring(oldID))))
			for _, shard := range []string{"rc-0", "rc-2"} {
				Eventually(func() string { return cli("client", nodeFQDN(primaryOf(shard)), "cluster", "nodes") }).
					ShouldNot(ContainSubstring(oldID), "the old ID stayed on %s", shard)
			}
			Eventually(clusterReady).Should(Equal("ok True"))
		})

		It("updates every Pod without CLUSTERDOWN", func() {
			By("writing through the cluster the whole time")
			// Like a client with a timeout: a node going away may leave a
			// connection hanging.
			Expect(kubectlApply(rcClient(rcNamespace, "writer", "client", fmt.Sprintf(`i=0; while true; do i=$((i+1)); `+
				`out=$(timeout 5 redis-cli -c -h %s --tls --cacert /ca/ca.crt --user app --no-auth-warning set w:$i $i 2>&1) || out="$out exit=$?"; `+
				`echo "$(date +%%T) $out"; done`, seed)))).To(Succeed())
			_, err := kubectl("wait", "pod", "writer", "-n", rcNamespace, "--for=condition=Ready", "--timeout=2m")
			Expect(err).NotTo(HaveOccurred())

			_, err = kubectl("patch", "rediscluster", rcName, "-n", rcNamespace, "--type=merge",
				"-p", `{"spec":{"resources":{"requests":{"memory":"160Mi"},"limits":{"memory":"160Mi"}}}}`)
			Expect(err).NotTo(HaveOccurred())
			Eventually(func() string {
				out, _ := kubectl("get", "pods", "-n", rcNamespace, "-l", "redis.operator.com/cluster="+rcName, "-o",
					`jsonpath={range .items[*]}{.spec.containers[0].resources.limits.memory}/{.status.containerStatuses[0].ready} {end}`)
				return out
			}, 8*time.Minute).Should(Equal(strings.TrimSpace(strings.Repeat("160Mi/true ", 6))))
			Eventually(clusterReady).Should(Equal("ok True"))

			writes, err := kubectl("logs", "writer", "-n", rcNamespace)
			Expect(err).NotTo(HaveOccurred())
			_, _ = kubectl("delete", "pod", "writer", "-n", rcNamespace, "--wait=false")
			_, _ = fmt.Fprintf(GinkgoWriter, "Writes other than OK:\n%s\n", strings.Join(slices.DeleteFunc(
				strings.Split(writes, "\n"), func(line string) bool { return strings.HasSuffix(line, " OK") }), "\n"))
			Expect(strings.Count(writes, "OK")).To(BeNumerically(">", 50), "the writer barely ran")
			Expect(writes).NotTo(ContainSubstring("CLUSTERDOWN"))
		})
		return
	}

	It("fails over on its own while the operator is down, and the operator follows", func() {
		// A cluster that has just come up records its masters within 30 s.
		Eventually(func() string { return primaryOf("rc-2") }).ShouldNot(BeEmpty())
		Eventually(func() string { return replicaOf("rc-2") }).ShouldNot(BeEmpty())
		master, replica := primaryOf("rc-2"), replicaOf("rc-2")

		By("deleting the Pod of the master while no operator recreates it")
		scaleOperator(0)
		DeferCleanup(scaleOperator, 1)
		_, err := kubectl("delete", "pod", master, "-n", rcNamespace, "--wait=true")
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() string { return cli("client", nodeFQDN(replica), "role") }, 2*time.Minute).
			Should(HavePrefix("master"), "the replica took over after cluster-node-timeout")
		Eventually(func() string { return cli("client", seed, "cluster", "info") }).Should(ContainSubstring("cluster_state:ok"))
		Expect(cli("client", seed, "-c", "get", "key:17")).To(Equal("17"))

		By("starting the operator, which records the new master and brings back the old one as a replica")
		scaleOperator(1)
		Eventually(func() string { return primaryOf("rc-2") }).Should(Equal(replica))
		Eventually(func() string { return get("pod", replica, "{.metadata.labels.redis\\.operator\\.com/role}") }).Should(Equal("primary"))
		Eventually(func() string { return get("rediscluster", rcName, `{.status.shards[?(@.name=="rc-2")].primary}`) }).Should(Equal(replica))
		Eventually(func() string { return cli("client", nodeFQDN(master), "role") }).Should(HavePrefix("slave"))
		Eventually(func() string { return get("pod", master, "{.metadata.labels.redis\\.operator\\.com/role}") }).Should(Equal("replica"))
		Eventually(clusterReady).Should(Equal("ok True"))
	})
	It("grows to 4 shards with Ready=False/Resharding and Migrating with the progress (decision 7)", func() {
		Expect(shell(fmt.Sprintf(`seq 1 2000 | awk '{print "SET data:" $1 " " $1}' | %s -c -h %s | grep -c OK`, rcCLI, seed))).
			To(Equal("2000"))
		for _, shard := range []string{"rc-0", "rc-1", "rc-2"} {
			before[shard] = slotsOf(primaryOf(shard))
		}
		Expect(kubectlApply(rcClient(rcNamespace, "reshard-writer", "client", fmt.Sprintf(`i=0; while true; do i=$((i+1)); `+
			`out=$(timeout 5 %s -c -h %s set write:$i $i 2>&1) || out="$out exit=$?"; echo "$i $out"; done`, rcCLI, seed)) +
			"---" + sampler("sampler-grow", "rc-3-0"))).To(Succeed())

		_, err := kubectl("patch", "rediscluster", rcName, "-n", rcNamespace, "--type=merge", "-p", `{"spec":{"shards":4}}`)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() string { return condition("Migrating", "reason") }, 6*time.Minute).Should(Equal("Moving"))
		Expect(condition("Ready", "reason")).To(Equal("Resharding"))
		Expect(condition("Migrating", "message")).To(MatchRegexp(`^Moving slot \d+ from rc-[0-2]-\d to rc-3-0; \d+ slots to move$`))
		Expect(strconv.Atoi(get("rediscluster", rcName, "{.status.slotMigration.slotsToMove}"))).To(BeNumerically(">", 0))
	})

	It("rejects a change of shards while slots move (decision 5)", func() {
		Expect(condition("Migrating", "status")).To(Equal("True"), "the slots moved before this spec")
		_, err := kubectl("patch", "rediscluster", rcName, "-n", rcNamespace, "--type=merge", "-p", `{"spec":{"shards":5}}`)
		Expect(err).To(MatchError(ContainSubstring("spec.shards cannot change while slots move between shards")))
		Expect(get("rediscluster", rcName, "{.spec.shards}")).To(Equal("4"))
	})

	It("holds a promote back while slots move (decision 3)", func() {
		Expect(condition("Migrating", "status")).To(Equal("True"), "the slots moved before this spec")
		master := primaryOf("rc-2")
		promoted = replicaOf("rc-2")
		_, err := kubectl("annotate", "redisinstance", "rc-2", "-n", rcNamespace, "redis.operator.com/promote="+promoted, "--overwrite")
		Expect(err).NotTo(HaveOccurred())
		Consistently(func() string { return primaryOf("rc-2") }, 20*time.Second).Should(Equal(master))
		Expect(condition("Migrating", "status")).To(Equal("True"))
	})

	It("moves no slot while an instance of a shard is not Ready (decision 3)", func() {
		Expect(condition("Migrating", "status")).To(Equal("True"), "the slots moved before this spec")
		_, err := kubectl("delete", "pod", replicaOf("rc-1"), "-n", rcNamespace, "--wait=false")
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() string { return condition("Migrating", "message") }, 2*time.Minute, time.Second).
			Should(HaveSuffix("waiting for every instance of every shard to be Ready, without a master moving"))
		Eventually(func() string { return condition("Migrating", "reason") }, 3*time.Minute).Should(Equal("Moving"))
	})

	It("goes on after a restart of the operator, from the master that took over the source", func() {
		Expect(condition("Migrating", "status")).To(Equal("True"), "the slots moved before this spec")
		scaleOperator(0)
		DeferCleanup(scaleOperator, 1)

		By("finding the slot on the way, or putting one on the way like a stopped move")
		var slot int
		var source string
		if m := regexp.MustCompile(`\[(\d+)-<-([0-9a-f]{40})\]`).FindStringSubmatch(myself("rc-3-0")); m != nil {
			slot, _ = strconv.Atoi(m[1])
			for _, line := range strings.Split(cli("client", nodeFQDN("rc-3-0"), "cluster", "nodes"), "\n") {
				if fields := strings.Fields(line); len(fields) > 1 && fields[0] == m[2] {
					_, hostname, _ := strings.Cut(fields[1], ",")
					source, _, _ = strings.Cut(hostname, ".")
				}
			}
		} else {
			// The slot the operator moves next: the highest of the first shard
			// with more than its share, so no slot moves twice.
			for _, shard := range []string{"rc-0", "rc-1", "rc-2"} {
				if slots := slotsOf(primaryOf(shard)); len(slots) > 4096 && source == "" {
					source = primaryOf(shard)
					for s := range slots {
						slot = max(slot, s)
					}
				}
			}
			Expect(cli("client", nodeFQDN("rc-3-0"), "cluster", "setslot", strconv.Itoa(slot), "importing",
				cli("client", nodeFQDN(source), "cluster", "myid"))).To(Equal("OK"))
			Expect(cli("client", nodeFQDN(source), "cluster", "setslot", strconv.Itoa(slot), "migrating",
				cli("client", nodeFQDN("rc-3-0"), "cluster", "myid"))).To(Equal("OK"))
		}
		Expect(source).To(MatchRegexp(`^rc-[0-2]-\d$`))
		_, _ = fmt.Fprintf(GinkgoWriter, "Slot %d is on the way from %s\n", slot, source)

		By("deleting the Pod of the source, so its replica takes over without the flag")
		shard := source[:strings.LastIndex(source, "-")]
		replica := replicaOf(shard)
		_, err := kubectl("delete", "pod", source, "-n", rcNamespace, "--wait=true")
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() string { return cli("client", nodeFQDN(replica), "role") }, 2*time.Minute).Should(HavePrefix("master"))
		Expect(myself(replica)).NotTo(ContainSubstring("->-"), "a failover keeps no MIGRATING flag")

		By("starting the operator, which moves the slot on from the new master")
		served := len(slotsOf("rc-3-0"))
		scaleOperator(1)
		Eventually(func() bool { return slotsOf("rc-3-0")[slot] }, 5*time.Minute).Should(BeTrue())
		Eventually(func() int { return len(slotsOf("rc-3-0")) }).Should(BeNumerically(">", served))
		Eventually(func() string { return primaryOf(shard) }).Should(Equal(replica))
	})

	It("moves only the share of the new shard, one slot at a time, and loses no key (decisions 1, 8)", func() {
		Eventually(func() string { return condition("Migrating", "reason") }, 20*time.Minute, 5*time.Second).Should(Equal("Balanced"))
		Eventually(clusterReady).Should(Equal("ok True"))
		Expect(get("rediscluster", rcName, "{.status.slotMigration}")).To(BeEmpty())

		By("checking that the old shards gave only slots, 4096 together, all to the new one")
		moved := 0
		for _, shard := range []string{"rc-0", "rc-1", "rc-2"} {
			after := slotsOf(primaryOf(shard))
			for slot := range after {
				Expect(before[shard]).To(HaveKey(slot), "%s took slot %d of another shard", shard, slot)
			}
			moved += len(before[shard]) - len(after)
		}
		Expect(moved).To(Equal(4096))
		Expect(slotsOf(primaryOf("rc-3"))).To(HaveLen(4096))

		By("checking that every sample of the new master had at most one slot on the way")
		expectOneSlotAtATime("sampler-grow")

		By("checking every key and every acknowledged write")
		expectAllKeys()
		writes, err := kubectl("logs", "reshard-writer", "-n", rcNamespace)
		Expect(err).NotTo(HaveOccurred())
		_, _ = kubectl("delete", "pod", "reshard-writer", "-n", rcNamespace, "--wait=true")
		var acknowledged []int
		for _, line := range strings.Split(strings.TrimSpace(writes), "\n") {
			if i, out, _ := strings.Cut(line, " "); out == "OK" {
				n, _ := strconv.Atoi(i)
				acknowledged = append(acknowledged, n)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Write %s\n", line)
			}
		}
		Expect(len(acknowledged)).To(BeNumerically(">", 100), "the writer barely ran")
		values := getAll("write", slices.Max(acknowledged))
		for _, i := range acknowledged {
			Expect(values[i-1]).To(Equal(strconv.Itoa(i)), "the acknowledged write:%d is gone", i)
		}

		By("checking that the promote went on once the slots moved")
		Eventually(func() string { return primaryOf("rc-2") }).Should(Equal(promoted))
	})

	It("counts every moved slot as a metric of the operator, by the shards it went between", func() {
		Eventually(func(g Gomega) {
			metrics, err := operatorMetrics()
			g.Expect(err).NotTo(HaveOccurred())
			// The operator restarted meanwhile, so the count starts from that.
			g.Expect(metrics).To(MatchRegexp(fmt.Sprintf(
				`redis_operator_slots_moved_total\{from_shard="\d",namespace="%s",redis_cluster="%s",to_shard="3"\} [1-9]`, rcNamespace, rcName)))
		}).Should(Succeed())
	})

	It("moves the keys with one Secret of the cluster, over TLS (decision 2)", func() {
		for _, shard := range []string{"rc-0", "rc-1", "rc-2", "rc-3"} {
			Expect(get("pod", primaryOf(shard), "{.spec.volumes[*].secret.secretName}")).To(ContainSubstring("rc-internal-auth"))
		}
		Expect(cli("client", nodeFQDN(primaryOf("rc-3")), "config", "get", "tls-cluster")).To(Equal("tls-cluster\nyes"))
		Expect(strconv.Atoi(cli("client", nodeFQDN(primaryOf("rc-3")), "dbsize"))).To(BeNumerically(">", 200),
			"the keys came over with MIGRATE")
	})

	It("stops at a key both shards have, and goes on once a person deletes one (decision 4)", func() {
		scaleOperator(0)
		DeferCleanup(scaleOperator, 1)
		source, target := primaryOf("rc-0"), primaryOf("rc-1")
		key := ""
		for i := 0; key == ""; i++ {
			if candidate := fmt.Sprintf("busy:%d", i); !strings.Contains(cli("client", nodeFQDN(source), "get", candidate), "MOVED") {
				key = candidate
			}
		}
		slot := cli("client", nodeFQDN(source), "cluster", "keyslot", key)
		Expect(cli("client", nodeFQDN(source), "set", key, "source")).To(Equal("OK"))
		Expect(cli("client", nodeFQDN(target), "cluster", "setslot", slot, "importing",
			cli("client", nodeFQDN(source), "cluster", "myid"))).To(Equal("OK"))
		Expect(shell(fmt.Sprintf(`printf 'ASKING\nSET %s target\n' | %s -h %s`, key, rcCLI, nodeFQDN(target)))).To(Equal("OK\nOK"))

		scaleOperator(1)
		Eventually(func() string { return condition("Migrating", "reason") }).Should(Equal("Blocked"))
		Expect(condition("Migrating", "status")).To(Equal("False"))
		Expect(condition("Migrating", "message")).To(ContainSubstring(fmt.Sprintf(`key %q of slot %s exists on the target already`, key, slot)))
		Expect(condition("Ready", "reason")).To(Equal("Resharding"))
		Expect(cli("client", nodeFQDN(source), "get", key)).To(Equal("source"), "the key on the source stays")

		By("deleting the key on the target, as a person would")
		Expect(shell(fmt.Sprintf(`printf 'ASKING\nDEL %s\n' | %s -h %s`, key, rcCLI, nodeFQDN(target)))).To(Equal("OK\n1"))
		Eventually(func() string { return condition("Migrating", "reason") }).Should(Equal("Balanced"))
		Eventually(clusterReady).Should(Equal("ok True"))
		Expect(cli("client", seed, "-c", "get", key)).To(Equal("source"))
	})

	It("shrinks to 3 shards: the slots and keys leave rc-3, then its RedisInstance and PVCs go and the others forget it (decision 6)", func() {
		ids := []string{cli("client", nodeFQDN("rc-3-0"), "cluster", "myid"), cli("client", nodeFQDN("rc-3-1"), "cluster", "myid")}
		Expect(kubectlApply(sampler("sampler-shrink", primaryOf("rc-3")))).To(Succeed())
		_, err := kubectl("wait", "pod", "sampler-shrink", "-n", rcNamespace, "--for=condition=Ready", "--timeout=2m")
		Expect(err).NotTo(HaveOccurred())

		_, err = kubectl("patch", "rediscluster", rcName, "-n", rcNamespace, "--type=merge", "-p", `{"spec":{"shards":3}}`)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() error {
			_, err := kubectl("get", "redisinstance", "rc-3", "-n", rcNamespace)
			return err
		}, 20*time.Minute, 5*time.Second).Should(MatchError(ContainSubstring("NotFound")))
		Eventually(func() string {
			out, _ := kubectl("get", "pvc", "-n", rcNamespace, "-o", "name")
			return out
		}).ShouldNot(ContainSubstring("rc-3-"))
		Eventually(clusterReady).Should(Equal("ok True"))
		Expect(condition("Migrating", "reason")).To(Equal("Balanced"))
		Expect(get("rediscluster", rcName, "{.status.shards[*].name}")).To(Equal("rc-0 rc-1 rc-2"))

		for _, shard := range []string{"rc-0", "rc-1", "rc-2"} {
			Expect(slotsOf(primaryOf(shard))).To(HaveLen([]int{5461, 5461, 5462}[shard[3]-'0']), shard)
			for _, ordinal := range []string{"0", "1"} {
				Eventually(func() string { return cli("client", nodeFQDN(shard+"-"+ordinal), "cluster", "nodes") }).
					ShouldNot(SatisfyAny(ContainSubstring(ids[0]), ContainSubstring(ids[1])), "%s-%s still knows rc-3", shard, ordinal)
			}
		}
		expectAllKeys()
		expectOneSlotAtATime("sampler-shrink")
	})

	It("comes back on its own after every Pod restarted at once, each with a new address", func() {
		_, err := kubectl("delete", "pod", "-n", rcNamespace, "-l", "redis.operator.com/cluster="+rcName, "--wait=false")
		Expect(err).NotTo(HaveOccurred())
		Eventually(clusterReady, 5*time.Minute).Should(Equal("ok True"))
		// Every replica replicates from the address its master has now.
		for _, shard := range []string{"rc-0", "rc-1", "rc-2"} {
			Eventually(func() string {
				return cli("client", nodeFQDN(replicaOf(shard)), "info", "replication")
			}, 2*time.Minute).Should(ContainSubstring("master_link_status:up"), shard)
		}
		expectAllKeys()
	})
}
