//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"os/exec"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/MrPickle311/redis-operator/test/utils"
)

const (
	rcNamespace = "redis-e2e-cluster"
	rcName      = "rc"
)

// rcManifest is a cluster of three shards of two nodes with TLS, a user of
// the cluster and a NetworkPolicy that lets in only Pods labeled role=client.
var rcManifest = fmt.Sprintf(`
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
---
apiVersion: redis.operator.com/v1
kind: RedisUser
metadata: {name: app, namespace: %[2]s}
spec:
  targetRef: {kind: RedisCluster, name: %[1]s}
  authentication: {generate: true}
  acl: {rules: ["~*", "+@all"]}
`, rcName, rcNamespace, redisImage)

// rcClient is a Pod with redis-cli, the CA of the cluster and the password of
// the user app; role tells whether the NetworkPolicy lets it in. It runs
// script, or waits for kubectl exec.
func rcClient(name, role, script string) string {
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
`, name, rcNamespace, role, redisImage, rcName, script)
}

// Scenario: a Redis Cluster serves clients through the seed Service with the
// users of the cluster on every node, behind its NetworkPolicy; it fails over
// on its own while the operator is down, updates its Pods without
// CLUSTERDOWN, and takes back a replica that lost its volume as a new node.
var _ = Describe("RedisCluster", Ordered, func() {
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

	BeforeAll(func() {
		Expect(loadImageIntoKind(redisImage)).To(Succeed())
		Expect(ensureRestrictedNamespace(rcNamespace)).To(Succeed())
		Expect(kubectlApply(rcManifest)).To(Succeed())
		Eventually(clusterReady).Should(Equal("ok True"))
		Eventually(func() string { return get("redisuser", "app", `{.status.conditions[?(@.type=="Ready")].reason}`) }).
			Should(Equal("Applied"))
		Expect(kubectlApply(rcClient("client", "client", "") + "---" + rcClient("stranger", "other", ""))).To(Succeed())
		_, err := kubectl("wait", "pod", "client", "stranger", "-n", rcNamespace, "--for=condition=Ready", "--timeout=3m")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		if CurrentSpecReport().Failed() {
			out, _ := kubectl("get", "redisclusters,redisinstances,pods", "-n", rcNamespace, "-o", "wide", "-L", "redis.operator.com/role")
			_, _ = fmt.Fprintf(GinkgoWriter, "Resources in %s:\n%s\n", rcNamespace, out)
		}
		_, _ = kubectl("delete", "ns", rcNamespace, "--ignore-not-found", "--timeout=3m")
	})

	It("serves keys of every shard through the seed, and sends clients to the nodes by name", func() {
		for i := range 30 {
			Expect(cli("client", seed, "-c", "set", fmt.Sprintf("key:%d", i), fmt.Sprint(i))).To(Equal("OK"))
		}
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
		Expect(kubectlApply(rcClient("writer", "client", fmt.Sprintf(`i=0; while true; do i=$((i+1)); `+
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

	It("fails over on its own while the operator is down, and the operator follows", func() {
		master := primaryOf("rc-2")
		replica := replicaOf("rc-2")
		Expect(master).NotTo(BeEmpty())
		Expect(replica).NotTo(BeEmpty())

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
})
