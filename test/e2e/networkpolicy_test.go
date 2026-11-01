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
	polNamespace = "redis-e2e-pol"
	polName      = "pol"
	// polAllowed is let in by allowFrom, polOther is not.
	polAllowed = "redis-e2e-pol-app"
	polOther   = "redis-e2e-pol-other"
)

var polManifest = fmt.Sprintf(`
apiVersion: v1
kind: Namespace
metadata: {name: %[3]s, labels: {team: e2e-backend}}
---
apiVersion: v1
kind: Namespace
metadata: {name: %[4]s}
---
apiVersion: redis.operator.com/v1
kind: RedisInstance
metadata: {name: %[1]s, namespace: %[2]s}
spec:
  instances: 2
  image: %[5]s
  storage:
    volumeClaimTemplate: {size: 256Mi}
  resources:
    requests: {memory: 128Mi}
    limits: {memory: 128Mi}
  networkPolicy:
    allowFrom:
      - namespaceSelector: {matchLabels: {team: e2e-backend}}
`, polName, polNamespace, polAllowed, polOther, redisImage)

// Scenario: the NetworkPolicy of the operator lets in the clients of
// allowFrom and keeps out the others, while replication, the operator and the
// probes still work. kindnet enforces NetworkPolicies.
var _ = Describe("NetworkPolicy", Ordered, func() {
	SetDefaultEventuallyTimeout(3 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)

	get := func(jsonpath string) string {
		out, _ := kubectl("get", "redisinstance", polName, "-n", polNamespace, "-o", "jsonpath="+jsonpath)
		return out
	}
	condition := func(name string) string {
		return get(fmt.Sprintf(`{.status.conditions[?(@.type=="%[1]s")].status} {.status.conditions[?(@.type=="%[1]s")].reason}`, name))
	}
	// reach runs a shell command in the client Pod of namespace and returns
	// its output; "timeout" when the network drops the packets.
	reach := func(namespace, command string) string {
		out, err := utils.Run(exec.Command("kubectl", "exec", "client", "-n", namespace, "--",
			"bash", "-c", "timeout 5 "+command+"; echo exit=$?"))
		Expect(err).NotTo(HaveOccurred())
		if strings.Contains(out, "exit=124") {
			return "timeout"
		}
		return strings.TrimSpace(out)
	}
	ping := func(namespace string) string {
		return reach(namespace, fmt.Sprintf("redis-cli --tls --insecure -h %s-rw.%s.svc PING", polName, polNamespace))
	}
	dialIM := func(namespace string) string {
		ip := get(`{.status.currentPrimary}`)
		podIP, _ := kubectl("get", "pod", ip, "-n", polNamespace, "-o", "jsonpath={.status.podIP}")
		return reach(namespace, "bash -c 'exec 3<>/dev/tcp/"+podIP+"/8000'")
	}

	BeforeAll(func() {
		Expect(loadImageIntoKind(redisImage)).To(Succeed())
		Expect(ensureRestrictedNamespace(polNamespace)).To(Succeed())
		Expect(kubectlApply(polManifest)).To(Succeed())
		for _, namespace := range []string{polAllowed, polOther} {
			_, err := kubectl("run", "client", "-n", namespace, "--image="+redisImage, "--image-pull-policy=IfNotPresent",
				"--restart=Never", "--command", "--", "sleep", "3600")
			Expect(err).NotTo(HaveOccurred())
			_, err = kubectl("wait", "pod/client", "-n", namespace, "--for=condition=Ready", "--timeout=2m")
			Expect(err).NotTo(HaveOccurred())
		}
	})

	AfterAll(func() {
		for _, namespace := range []string{polNamespace, polAllowed, polOther} {
			_, _ = kubectl("delete", "ns", namespace, "--ignore-not-found", "--timeout=2m")
		}
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			out, _ := kubectl("get", "redisinstance,pods,networkpolicies,events", "-n", polNamespace, "-o", "wide")
			_, _ = fmt.Fprintf(GinkgoWriter, "Resources in %s:\n%s\n", polNamespace, out)
		}
	})

	It("keeps the operator, the probes and the replication working behind the policy", func() {
		Eventually(func() string { return condition("Ready") }).Should(Equal("True Ready"))
		Expect(condition("NetworkPolicyReady")).To(Equal("True Applied"))
		_, err := kubectl("get", "networkpolicy", polName, "-n", polNamespace)
		Expect(err).NotTo(HaveOccurred())

		By("switching over, which needs the operator on port 8000 and the replication on 6379")
		_, err = kubectl("annotate", "redisinstance", polName, "-n", polNamespace, "redis.operator.com/promote="+polName+"-1", "--overwrite")
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() string { return get("{.status.currentPrimary} {.status.primaryEpoch}") }).Should(Equal(polName + "-1 2"))
		Eventually(func() string { return condition("Ready") }).Should(Equal("True Ready"))
	})

	It("lets in only the clients of allowFrom", func() {
		// No user and a disabled default user: an answer at all means the
		// connection went through.
		Eventually(func() string { return ping(polAllowed) }).Should(ContainSubstring("NOAUTH"))
		Expect(ping(polOther)).To(Equal("timeout"))
	})

	It("keeps every client off the API of the Instance Manager", func() {
		Expect(dialIM(polAllowed)).To(Equal("timeout"))
		Expect(dialIM(polOther)).To(Equal("timeout"))
	})

	It("warns about a policy of the user that lets more in, which still applies", func() {
		Expect(kubectlApply(fmt.Sprintf(`
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: {name: allow-everyone, namespace: %s}
spec:
  podSelector: {matchLabels: {redis.operator.com/instance: %s}}
  ingress: [{}]
`, polNamespace, polName))).To(Succeed())

		Eventually(func() string { return condition("NetworkPolicyReady") }).Should(Equal("False ForeignPolicy"))
		Eventually(func() string {
			out, _ := kubectl("get", "events", "-n", polNamespace, "--field-selector", "reason=ForeignNetworkPolicy", "-o", "name")
			return out
		}).ShouldNot(BeEmpty())
		Eventually(func() string { return ping(polOther) }).Should(ContainSubstring("NOAUTH"), "the policy of the user does not apply")

		_, err := kubectl("delete", "networkpolicy", "allow-everyone", "-n", polNamespace)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() string { return condition("NetworkPolicyReady") }).Should(Equal("True Applied"))
		Eventually(func() string { return ping(polOther) }).Should(Equal("timeout"))
	})

	It("deletes its policy once allowFrom is gone", func() {
		_, err := kubectl("patch", "redisinstance", polName, "-n", polNamespace, "--type=json",
			"-p", `[{"op":"remove","path":"/spec/networkPolicy/allowFrom"}]`)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() error {
			_, err := kubectl("get", "networkpolicy", polName, "-n", polNamespace)
			return err
		}).Should(HaveOccurred())
		Expect(condition("NetworkPolicyReady")).To(BeEmpty(), "the condition is only for the policy of the operator")
		Eventually(func() string { return ping(polOther) }).Should(ContainSubstring("NOAUTH"))
	})
})
