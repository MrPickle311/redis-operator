//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/MrPickle311/redis-operator/test/utils"
)

const (
	ndNamespace = "redis-e2e-nodes"
	ndName      = "nd"
)

var ndManifest = fmt.Sprintf(`
apiVersion: redis.operator.com/v1
kind: RedisInstance
metadata: {name: %s, namespace: %s}
spec:
  instances: 3
  image: %s
  storage:
    volumeClaimTemplate: {size: 256Mi}
  resources:
    requests: {memory: 128Mi}
    limits: {memory: 128Mi}
`, ndName, ndNamespace, redisImage)

var ndPassword string

// Scenario: three instances on the three workers of Kind, and the node of the
// primary goes into maintenance.
var _ = Describe("Nodes", Ordered, func() {
	SetDefaultEventuallyTimeout(3 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)

	var cordoned string

	BeforeAll(func() {
		Expect(loadImageIntoKind(redisImage)).To(Succeed())
		Expect(ensureRestrictedNamespace(ndNamespace)).To(Succeed())
		Expect(kubectlApply(ndManifest)).To(Succeed())
		Eventually(func(g Gomega) {
			out, err := kubectl("get", "secret", ndName+"-internal-auth", "-n", ndNamespace,
				"-o", "go-template={{index .data \"operator-password\" | base64decode}}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(BeEmpty())
			ndPassword = out
		}).Should(Succeed())
		for i := range 3 {
			Eventually(func() bool { return podReadyIn(ndNamespace, ndPod(i)) }).Should(BeTrue())
		}
		Eventually(func(g Gomega) { g.Expect(ndCLI(g, ndPrimary(), "SET", "nd-key", "kept")).To(Equal("OK")) }).Should(Succeed())
	})

	AfterAll(func() {
		if cordoned != "" {
			_, _ = kubectl("uncordon", cordoned)
		}
		_, _ = kubectl("delete", "ns", ndNamespace, "--ignore-not-found", "--timeout=2m")
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			out, _ := kubectl("get", "pods,pdb", "-n", ndNamespace, "-o", "wide")
			_, _ = fmt.Fprintf(GinkgoWriter, "Resources:\n%s\n", out)
			out, _ = kubectl("get", "redisinstance", ndName, "-n", ndNamespace, "-o", "jsonpath={.status}")
			_, _ = fmt.Fprintf(GinkgoWriter, "Status:\n%s\n", out)
		}
	})

	It("places every instance on a node of its own", func() {
		nodes := map[string]bool{}
		for i := range 3 {
			node := ndNodeOf(ndPod(i))
			Expect(node).NotTo(BeEmpty())
			nodes[node] = true
		}
		Expect(nodes).To(HaveLen(3), "two instances share a node")
	})

	It("refuses to evict the primary, as its PodDisruptionBudget holds it", func() {
		Eventually(func() string {
			out, _ := kubectl("get", "pdb", ndName+"-primary", "-n", ndNamespace, "-o", "jsonpath={.status.disruptionsAllowed}")
			return out
		}).Should(Equal("0"))
		cmd := exec.Command("kubectl", "create", "--raw",
			fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/eviction", ndNamespace, ndPrimary()), "-f", "-")
		cmd.Stdin = strings.NewReader(fmt.Sprintf(
			`{"apiVersion":"policy/v1","kind":"Eviction","metadata":{"name":%q,"namespace":%q}}`, ndPrimary(), ndNamespace))
		out, err := utils.Run(cmd)
		Expect(err).To(HaveOccurred(), "the primary was evicted: %s", out)
		Expect(err.Error()).To(ContainSubstring("disruption budget"))
	})

	It("switches over when the node of the primary is cordoned, so a drain goes through", func() {
		oldPrimary := ndPrimary()
		cordoned = ndNodeOf(oldPrimary)

		By("cordoning " + cordoned)
		_, err := kubectl("cordon", cordoned)
		Expect(err).NotTo(HaveOccurred())
		Eventually(ndPrimary).ShouldNot(Equal(oldPrimary), "no switchover off the cordoned node")
		newPrimary := ndPrimary()
		Expect(ndNodeOf(newPrimary)).NotTo(Equal(cordoned))
		events, err := kubectl("get", "events", "-n", ndNamespace, "--field-selector", "reason=PrimaryNodeCordoned", "-o", "name")
		Expect(err).NotTo(HaveOccurred())
		Expect(events).NotTo(BeEmpty())

		By("draining the node: the old primary is now a replica, which its budget lets go")
		_, err = kubectl("drain", cordoned, "--pod-selector=redis.operator.com/instance="+ndName,
			"--ignore-daemonsets", "--delete-emptydir-data", "--timeout=3m")
		Expect(err).NotTo(HaveOccurred())
		Expect(ndCLI(Default, newPrimary, "GET", "nd-key")).To(Equal("kept"))

		By("bringing the old primary back as a replica once the node is uncordoned")
		_, err = kubectl("uncordon", cordoned)
		Expect(err).NotTo(HaveOccurred())
		cordoned = ""
		Eventually(func() bool { return podReadyIn(ndNamespace, oldPrimary) }).Should(BeTrue())
		Eventually(func(g Gomega) {
			g.Expect(ndCLI(g, oldPrimary, "INFO", "replication")).To(ContainSubstring("role:slave"))
			g.Expect(ndCLI(g, oldPrimary, "GET", "nd-key")).To(Equal("kept"))
		}).Should(Succeed())
	})
})

func ndPod(ordinal int) string { return fmt.Sprintf("%s-%d", ndName, ordinal) }

func ndPrimary() string {
	out, _ := kubectl("get", "redisinstance", ndName, "-n", ndNamespace, "-o", "jsonpath={.status.currentPrimary}")
	return out
}

func ndNodeOf(pod string) string {
	out, _ := kubectl("get", "pod", pod, "-n", ndNamespace, "-o", "jsonpath={.spec.nodeName}")
	return out
}

func ndCLI(g Gomega, pod string, args ...string) string {
	out, err := kubectl(append([]string{"exec", pod, "-n", ndNamespace, "-c", "redis", "--",
		"redis-cli", "--user", "_operator", "--pass", ndPassword, "--no-auth-warning"}, args...)...)
	g.Expect(err).NotTo(HaveOccurred())
	return out
}

const (
	nlNamespace = "redis-e2e-node-loss"
	nlName      = "nl"
)

var nlManifest = fmt.Sprintf(`
apiVersion: redis.operator.com/v1
kind: RedisInstance
metadata: {name: %s, namespace: %s}
spec:
  instances: 2
  image: %s
  storage:
    volumeClaimTemplate: {size: 256Mi}
    recreateOnNodeLoss: true
  resources:
    requests: {memory: 128Mi}
    limits: {memory: 128Mi}
  failover:
    delay: 10s
`, nlName, nlNamespace, redisImage)

// Scenario: the node of the primary stops for good. Kind keeps every node in
// a Docker container, so stopping the container stops the node; the volumes
// of the local-path provisioner are local to their node.
var _ = Describe("Node loss", Ordered, func() {
	SetDefaultEventuallyTimeout(5 * time.Minute)
	SetDefaultEventuallyPollingInterval(3 * time.Second)

	var lost, oldPrimary, password string

	BeforeAll(func() {
		Expect(loadImageIntoKind(redisImage)).To(Succeed())
		Expect(ensureRestrictedNamespace(nlNamespace)).To(Succeed())
		Expect(kubectlApply(nlManifest)).To(Succeed())
		Eventually(func(g Gomega) {
			out, err := kubectl("get", "secret", nlName+"-internal-auth", "-n", nlNamespace,
				"-o", "go-template={{index .data \"operator-password\" | base64decode}}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(BeEmpty())
			password = out
		}).Should(Succeed())
		for i := range 2 {
			Eventually(func() bool { return podReadyIn(nlNamespace, fmt.Sprintf("%s-%d", nlName, i)) }).Should(BeTrue())
		}
	})

	AfterAll(func() {
		if lost != "" {
			By("bringing node " + lost + " back")
			_, _ = utils.Run(exec.Command("docker", "start", lost))
			_, _ = kubectl("taint", "node", lost, "node.kubernetes.io/out-of-service-")
			_, _ = kubectl("wait", "--for=condition=Ready", "node/"+lost, "--timeout=3m")
		}
		_, _ = kubectl("delete", "ns", nlNamespace, "--ignore-not-found", "--timeout=2m")
	})

	cli := func(g Gomega, pod string, args ...string) string {
		out, err := kubectl(append([]string{"exec", pod, "-n", nlNamespace, "-c", "redis", "--",
			"redis-cli", "--user", "_operator", "--pass", password, "--no-auth-warning"}, args...)...)
		g.Expect(err).NotTo(HaveOccurred())
		return out
	}
	get := func(jsonpath string) string {
		out, _ := kubectl("get", "redisinstance", nlName, "-n", nlNamespace, "-o", "jsonpath="+jsonpath)
		return out
	}
	nodeOf := func(pod string) string {
		out, _ := kubectl("get", "pod", pod, "-n", nlNamespace, "-o", "jsonpath={.spec.nodeName}")
		return out
	}

	It("fails over once the node of the primary stops", func() {
		oldPrimary = get("{.status.currentPrimary}")
		Expect(cli(Default, oldPrimary, "SET", "nl-key", "kept")).To(Equal("OK"))
		Eventually(func(g Gomega) {
			for i := range 2 {
				g.Expect(cli(g, fmt.Sprintf("%s-%d", nlName, i), "GET", "nl-key")).To(Equal("kept"))
			}
		}).Should(Succeed())

		lost = nodeOf(oldPrimary)
		By("stopping node " + lost)
		_, err := utils.Run(exec.Command("docker", "stop", lost))
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() string { return get("{.status.currentPrimary}") }).ShouldNot(Equal(oldPrimary))
		newPrimary := get("{.status.currentPrimary}")
		Expect(cli(Default, newPrimary, "GET", "nl-key")).To(Equal("kept"))
	})

	It("moves the old primary, now a replica, to a new volume on another node once the node is out of service", func() {
		claim := oldPrimary + "-data"
		oldUID := get(fmt.Sprintf("{.status.volumes[?(@.claimName==%q)].uid}", claim))

		By("marking node " + lost + " as shut down, which lets Kubernetes delete its Pods")
		_, err := kubectl("taint", "node", lost, "node.kubernetes.io/out-of-service=nodeshutdown:NoExecute")
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() bool { return podReadyIn(nlNamespace, oldPrimary) }).Should(BeTrue())
		Expect(nodeOf(oldPrimary)).NotTo(Equal(lost))
		Expect(get(fmt.Sprintf("{.status.volumes[?(@.claimName==%q)].uid}", claim))).NotTo(Equal(oldUID))
		Eventually(func(g Gomega) {
			g.Expect(cli(g, oldPrimary, "INFO", "replication")).To(ContainSubstring("role:slave"))
			g.Expect(cli(g, oldPrimary, "GET", "nl-key")).To(Equal("kept"))
		}).Should(Succeed())
		events, err := kubectl("get", "events", "-n", nlNamespace, "--field-selector", "reason=ReplicaVolumeFailing", "-o", "jsonpath={.items[*].message}")
		Expect(err).NotTo(HaveOccurred())
		Expect(events).To(ContainSubstring("is lost"))
	})
})
