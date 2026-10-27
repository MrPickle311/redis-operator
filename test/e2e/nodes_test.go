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
