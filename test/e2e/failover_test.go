//go:build e2e
// +build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	foNamespace = "redis-e2e-failover"
	foName      = "fo"
	// foDelay is spec.failover.delay: a primary without the API server
	// fences itself after half of it.
	foDelay = 8 * time.Second

	operatorDeployment = "redis-operator-controller-manager"
)

var foManifest = fmt.Sprintf(`
apiVersion: redis.operator.com/v1
kind: RedisInstance
metadata: {name: %s, namespace: %s}
spec:
  instances: 2
  image: %s
  storage:
    volumeClaimTemplate: {size: 256Mi}
  resources:
    requests: {memory: 128Mi}
    limits: {memory: 128Mi}
  failover:
    delay: %s
`, foName, foNamespace, redisImage, foDelay)

var foPassword string

// Scenario: the operator is stopped, so the Instance Managers are on their
// own, as during a network partition. The specs build on each other.
var _ = Describe("Self-fencing of the Instance Manager", Ordered, func() {
	SetDefaultEventuallyTimeout(3 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)

	var roleBinding string // the RoleBinding of the instance, to restore it

	BeforeAll(func() {
		Expect(loadImageIntoKind(redisImage)).To(Succeed())
		Expect(ensureRestrictedNamespace(foNamespace)).To(Succeed())
		Expect(kubectlApply(foManifest)).To(Succeed())
		Eventually(func(g Gomega) {
			out, err := kubectl("get", "secret", foName+"-internal-auth", "-n", foNamespace,
				"-o", "go-template={{index .data \"operator-password\" | base64decode}}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(BeEmpty())
			foPassword = out
		}).Should(Succeed())
		for i := range 2 {
			Eventually(func() bool { return podReadyIn(foNamespace, foPod(i)) }).Should(BeTrue())
		}
		Eventually(func(g Gomega) { g.Expect(foCLI(g, foPod(0), "SET", "fo-key", "before")).To(Equal("OK")) }).Should(Succeed())

		By("stopping the operator, so it cannot take part")
		scaleOperator(0)

		out, err := kubectl("get", "rolebinding", foName+"-instance", "-n", foNamespace, "-o", "json")
		Expect(err).NotTo(HaveOccurred())
		roleBinding = withoutServerFields(out)
	})

	AfterAll(func() {
		scaleOperator(1)
		_, _ = kubectl("delete", "ns", foNamespace, "--ignore-not-found", "--timeout=2m")
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			for i := range 2 {
				out, _ := kubectl("logs", foPod(i), "-n", foNamespace, "-c", "redis", "--tail=30")
				_, _ = fmt.Fprintf(GinkgoWriter, "Logs of %s:\n%s\n", foPod(i), out)
			}
		}
	})

	It("demotes a primary whose RedisInstance names another primary", func() {
		By("promoting the replica and recording it, as a failover would while the operator cannot reach the old primary")
		Expect(foCLI(Default, foPod(1), "REPLICAOF", "NO", "ONE")).To(Equal("OK"))
		_, err := kubectl("patch", "redisinstance", foName, "-n", foNamespace, "--subresource=status", "--type=merge",
			"-p", fmt.Sprintf(`{"status":{"currentPrimary":%q,"primaryEpoch":2}}`, foPod(1)))
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			info := foCLI(g, foPod(0), "INFO", "replication")
			g.Expect(info).To(ContainSubstring("role:slave"))
			g.Expect(info).To(ContainSubstring("master_host:" + foFQDN(1)))
			g.Expect(info).To(ContainSubstring("master_link_status:up"))
		}).Should(Succeed(), "the old primary kept its role")

		Expect(foCLI(Default, foPod(1), "SET", "fo-key", "after")).To(Equal("OK"))
		Eventually(func(g Gomega) { g.Expect(foCLI(g, foPod(0), "GET", "fo-key")).To(Equal("after")) }).Should(Succeed())
	})

	It("fences the primary once it cannot read its RedisInstance for half of failover.delay", func() {
		By("taking the API away from the Instance Managers")
		_, err := kubectl("delete", "rolebinding", foName+"-instance", "-n", foNamespace)
		Expect(err).NotTo(HaveOccurred())

		Eventually(foWriteBlocked).WithArguments(foPod(1)).Should(BeTrue(), "the primary kept taking writes")
		Expect(foCLI(Default, foPod(0), "GET", "fo-key")).To(Equal("after"), "the replica stopped serving reads")
	})

	It("lifts its own fence once the API server is back", func() {
		Expect(kubectlApply(roleBinding)).To(Succeed())
		Eventually(foWriteBlocked).WithArguments(foPod(1)).Should(BeFalse(), "the primary stayed fenced")
	})

	It("keeps taking writes without the API server when fenceOnAPILoss is off", func() {
		_, err := kubectl("patch", "redisinstance", foName, "-n", foNamespace, "--type=merge",
			"-p", `{"spec":{"failover":{"fenceOnAPILoss":false}}}`)
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(3 * time.Second) // one read of the Instance Manager

		_, err = kubectl("delete", "rolebinding", foName+"-instance", "-n", foNamespace)
		Expect(err).NotTo(HaveOccurred())
		Consistently(foWriteBlocked, foDelay, time.Second).WithArguments(foPod(1)).Should(BeFalse())
		Expect(kubectlApply(roleBinding)).To(Succeed())
	})
})

func foPod(ordinal int) string { return fmt.Sprintf("%s-%d", foName, ordinal) }

func foFQDN(ordinal int) string {
	return fmt.Sprintf("%s.%s-hl.%s.svc", foPod(ordinal), foName, foNamespace)
}

func foCLI(g Gomega, pod string, args ...string) string {
	out, err := kubectl(append([]string{"exec", pod, "-n", foNamespace, "-c", "redis", "--",
		"redis-cli", "--user", "_operator", "--pass", foPassword, "--no-auth-warning"}, args...)...)
	g.Expect(err).NotTo(HaveOccurred())
	return out
}

// foWriteBlocked tells whether a write on the Pod waits, as on a fenced
// primary, instead of returning within 3 seconds.
func foWriteBlocked(pod string) bool {
	_, err := kubectl("exec", pod, "-n", foNamespace, "-c", "redis", "--",
		"timeout", "3", "redis-cli", "--user", "_operator", "--pass", foPassword, "--no-auth-warning",
		"SET", "fo-probe", time.Now().String())
	return err != nil
}

func podReadyIn(namespace, name string) bool {
	out, err := kubectl("get", "pod", name, "-n", namespace,
		"-o", `jsonpath={.status.conditions[?(@.type=="Ready")].status}`)
	return err == nil && out == "True"
}

// scaleOperator sets the replicas of the controller-manager and waits for them.
func scaleOperator(replicas int) {
	_, err := kubectl("scale", "deployment", operatorDeployment, "-n", namespace, fmt.Sprintf("--replicas=%d", replicas))
	Expect(err).NotTo(HaveOccurred())
	if replicas == 0 {
		_, err = kubectl("wait", "--for=delete", "pod", "-l", managerPodSelector, "-n", namespace, "--timeout=2m")
	} else {
		_, err = kubectl("rollout", "status", "deployment", operatorDeployment, "-n", namespace, "--timeout=3m")
	}
	Expect(err).NotTo(HaveOccurred())
}

// withoutServerFields returns the object in JSON without the fields the API
// server sets, so it can be created again.
func withoutServerFields(object string) string {
	var obj map[string]any
	Expect(json.Unmarshal([]byte(object), &obj)).To(Succeed())
	metadata := obj["metadata"].(map[string]any)
	for _, field := range []string{"resourceVersion", "uid", "creationTimestamp", "managedFields"} {
		delete(metadata, field)
	}
	data, err := json.Marshal(obj)
	Expect(err).NotTo(HaveOccurred())
	return string(data)
}

const (
	afNamespace = "redis-e2e-autofailover"
	afName      = "af"
)

var afManifest = fmt.Sprintf(`
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
  failover:
    delay: 60s
`, afName, afNamespace, redisImage)

var afPassword string

// Scenario: the primary stops answering. CLIENT PAUSE ALL makes /readyz fail
// while /healthz, which checks only the Instance Manager, keeps the container.
var _ = Describe("Automatic failover", Ordered, func() {
	SetDefaultEventuallyTimeout(3 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)

	var newPrimary string

	BeforeAll(func() {
		Expect(loadImageIntoKind(redisImage)).To(Succeed())
		Expect(ensureRestrictedNamespace(afNamespace)).To(Succeed())
		Expect(kubectlApply(afManifest)).To(Succeed())
		Eventually(func(g Gomega) {
			out, err := kubectl("get", "secret", afName+"-internal-auth", "-n", afNamespace,
				"-o", "go-template={{index .data \"operator-password\" | base64decode}}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(BeEmpty())
			afPassword = out
		}).Should(Succeed())
		for i := range 3 {
			Eventually(func() bool { return podReadyIn(afNamespace, afPod(i)) }).Should(BeTrue())
		}
		Eventually(afReady).Should(Equal("True Ready"))
		Expect(afCLI(Default, afPod(0), "SET", "af-key", "before")).To(Equal("OK"))
	})

	AfterAll(func() {
		_, _ = kubectl("delete", "ns", afNamespace, "--ignore-not-found", "--timeout=2m")
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			out, _ := kubectl("get", "redisinstance", afName, "-n", afNamespace, "-o", "jsonpath={.status}")
			_, _ = fmt.Fprintf(GinkgoWriter, "Status:\n%s\n", out)
			out, _ = kubectl("logs", "-n", namespace, "-l", managerPodSelector, "--tail=50")
			_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n%s\n", out)
		}
	})

	It("keeps the primary when it comes back within failover.delay", func() {
		Expect(afCLI(Default, afPod(0), "CLIENT", "PAUSE", "25000", "ALL")).To(Equal("OK"))
		Eventually(afPrimaryAvailable).Should(Equal("False"))
		Eventually(afPrimaryAvailable).Should(Equal("True"))
		Expect(afGet("{.status.currentPrimary} {.status.primaryEpoch}")).To(Equal(afPod(0) + " 1"))
	})

	It("promotes the replica with the most writes once the primary is unavailable for failover.delay", func() {
		_, err := kubectl("patch", "redisinstance", afName, "-n", afNamespace, "--type=merge",
			"-p", `{"spec":{"failover":{"delay":"8s"}}}`)
		Expect(err).NotTo(HaveOccurred())
		Expect(afCLI(Default, afPod(0), "SET", "af-key", "replicated")).To(Equal("OK"))
		for i := 1; i < 3; i++ {
			Eventually(func(g Gomega) { g.Expect(afCLI(g, afPod(i), "GET", "af-key")).To(Equal("replicated")) }).Should(Succeed())
		}

		By("making the primary stop answering for a minute")
		Expect(afCLI(Default, afPod(0), "CLIENT", "PAUSE", "60000", "ALL")).To(Equal("OK"))
		Eventually(func() string { return afGet("{.status.currentPrimary}") }).ShouldNot(Equal(afPod(0)))
		newPrimary = afGet("{.status.currentPrimary}")

		Expect(afGet("{.status.primaryEpoch}")).To(Equal("2"))
		Expect(afGet(`{.status.conditions[?(@.type=="Failover")].reason}`)).To(Equal("Completed"))
		events, err := kubectl("get", "events", "-n", afNamespace, "--field-selector", "reason=FailoverCompleted", "-o", "name")
		Expect(err).NotTo(HaveOccurred())
		Expect(events).NotTo(BeEmpty())

		By("taking writes on the new primary, through -rw, without losing the replicated ones")
		Eventually(func(g Gomega) {
			g.Expect(afCLI(g, newPrimary, "INFO", "replication")).To(ContainSubstring("role:master"))
			g.Expect(afCLI(g, newPrimary, "-h", afName+"-rw", "SET", "af-after", "failover")).To(Equal("OK"))
		}).Should(Succeed())
		Expect(afCLI(Default, newPrimary, "GET", "af-key")).To(Equal("replicated"))
	})

	It("turns the old primary into a replica of the new one once it answers again", func() {
		Eventually(func(g Gomega) {
			info := afCLI(g, afPod(0), "INFO", "replication")
			g.Expect(info).To(ContainSubstring("role:slave"))
			g.Expect(info).To(ContainSubstring("master_host:" + afFQDN(newPrimary)))
			g.Expect(info).To(ContainSubstring("master_link_status:up"))
			g.Expect(afCLI(g, afPod(0), "GET", "af-after")).To(Equal("failover"))
		}).Should(Succeed())
		Eventually(afReady).Should(Equal("True Ready"))
	})

	It("promotes a replica instead of a primary that restarted with an empty dataset", func() {
		// Only the safe start can fail over within this delay.
		_, err := kubectl("patch", "redisinstance", afName, "-n", afNamespace, "--type=merge",
			"-p", `{"spec":{"failover":{"delay":"10m"}}}`)
		Expect(err).NotTo(HaveOccurred())
		oldPrimary := newPrimary
		Expect(afCLI(Default, oldPrimary, "SET", "af-safe", "kept")).To(Equal("OK"))
		for i := range 3 {
			Eventually(func(g Gomega) { g.Expect(afCLI(g, afPod(i), "GET", "af-safe")).To(Equal("kept")) }).Should(Succeed())
		}

		By("losing the files of the primary and killing redis-server, so it restarts empty")
		_, _ = kubectl("exec", oldPrimary, "-n", afNamespace, "-c", "redis", "--", "sh", "-c",
			`rm -rf /data/appendonlydir /data/dump.rdb; for p in /proc/[0-9]*; do `+
				`[ "${p#/proc/}" != "$$" ] && grep -qa redis-server "$p/cmdline" 2>/dev/null && kill -9 "${p#/proc/}"; done`)

		Eventually(func() string { return afGet("{.status.currentPrimary}") }).ShouldNot(Equal(oldPrimary))
		newPrimary = afGet("{.status.currentPrimary}")
		Expect(afGet(`{.status.conditions[?(@.type=="Failover")].message}`)).To(ContainSubstring("empty dataset"))
		Expect(afGet("{.status.primaryEpoch}")).To(Equal("3"))

		By("keeping the data on the replicas, and syncing the old primary from the new one")
		Expect(afCLI(Default, newPrimary, "GET", "af-safe")).To(Equal("kept"))
		Eventually(func(g Gomega) {
			info := afCLI(g, oldPrimary, "INFO", "replication")
			g.Expect(info).To(ContainSubstring("master_host:" + afFQDN(newPrimary)))
			g.Expect(afCLI(g, oldPrimary, "GET", "af-safe")).To(Equal("kept"))
		}).Should(Succeed())
		Eventually(afReady).Should(Equal("True Ready"))
	})
})

func afPod(ordinal int) string { return fmt.Sprintf("%s-%d", afName, ordinal) }

func afFQDN(pod string) string { return fmt.Sprintf("%s.%s-hl.%s.svc", pod, afName, afNamespace) }

func afCLI(g Gomega, pod string, args ...string) string {
	out, err := kubectl(append([]string{"exec", pod, "-n", afNamespace, "-c", "redis", "--",
		"redis-cli", "--user", "_operator", "--pass", afPassword, "--no-auth-warning"}, args...)...)
	g.Expect(err).NotTo(HaveOccurred())
	return out
}

func afGet(jsonpath string) string {
	out, err := kubectl("get", "redisinstance", afName, "-n", afNamespace, "-o", "jsonpath="+jsonpath)
	Expect(err).NotTo(HaveOccurred())
	return out
}

func afPrimaryAvailable() string {
	return afGet(`{.status.conditions[?(@.type=="PrimaryAvailable")].status}`)
}

func afReady() string {
	return afGet(`{.status.conditions[?(@.type=="Ready")].status} {.status.conditions[?(@.type=="Ready")].reason}`)
}
