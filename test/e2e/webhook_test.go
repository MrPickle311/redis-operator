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

const whNamespace = "redis-e2e-wh"

func whUser(name, target string) string {
	return fmt.Sprintf(`
apiVersion: redis.operator.com/v1
kind: RedisUser
metadata: {name: %s, namespace: %s}
spec:
  targetRef: {name: %s}
  username: app
  authentication: {generate: true}
  acl: {rules: ["~app:*", "+@read"]}
`, name, whNamespace, target)
}

// applyOutput applies a manifest and returns what kubectl printed, warnings
// of the API server included.
func applyOutput(manifest string, args ...string) (string, error) {
	cmd := exec.Command("kubectl", append([]string{"apply", "-f", "-"}, args...)...)
	cmd.Stdin = strings.NewReader(manifest)
	return utils.Run(cmd)
}

// Scenario: the API server calls the webhook of the operator, trusting the CA
// of the operator, to warn about a missing RedisInstance and to turn away a
// username that is taken, also after the operator restarts.
var _ = Describe("RedisUser webhook", Ordered, Serial, func() {
	SetDefaultEventuallyTimeout(3 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)

	caBundle := func() string {
		out, _ := kubectl("get", "validatingwebhookconfiguration", "redis-operator-validating-webhook-configuration",
			"-o", "jsonpath={.webhooks[0].clientConfig.caBundle}")
		return out
	}
	// rejected asks the webhook through a server-side dry run, which stores
	// nothing; Eventually covers a webhook that is not up yet, which the API
	// server would skip.
	rejected := func() string {
		out, _ := applyOutput(whUser("second", "cache"), "--dry-run=server")
		return out
	}

	BeforeAll(func() {
		Expect(loadImageIntoKind(redisImage)).To(Succeed())
		Expect(ensureRestrictedNamespace(whNamespace)).To(Succeed())
		Expect(kubectlApply(fmt.Sprintf(`
apiVersion: redis.operator.com/v1
kind: RedisInstance
metadata: {name: cache, namespace: %s}
spec:
  instances: 1
  image: %s
  storage:
    volumeClaimTemplate: {size: 256Mi}
  resources:
    requests: {memory: 128Mi}
    limits: {memory: 128Mi}
`, whNamespace, redisImage))).To(Succeed())

		By("waiting for the API server to reach the webhook, which it skips until then")
		Eventually(func() string {
			out, _ := applyOutput(whUser("probe", "later"), "--dry-run=server")
			return out
		}).Should(ContainSubstring("Warning: RedisInstance later does not exist yet"))
	})

	AfterAll(func() {
		_, _ = kubectl("delete", "ns", whNamespace, "--ignore-not-found", "--timeout=2m")
	})

	It("trusts the CA of the operator", func() {
		ca, err := kubectl("get", "secret", "redis-operator-ca", "-n", namespace, "-o", "jsonpath={.data.tls\\.crt}")
		Expect(err).NotTo(HaveOccurred())
		Eventually(caBundle).Should(Equal(ca))
	})

	It("only warns about a RedisInstance that does not exist yet", func() {
		out, err := applyOutput(whUser("early", "later"))
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring("Warning: RedisInstance later does not exist yet"))
		_, err = kubectl("get", "redisuser", "early", "-n", whNamespace)
		Expect(err).NotTo(HaveOccurred())
	})

	It("turns away a username another RedisUser of the instance has", func() {
		Expect(kubectlApply(whUser("first", "cache"))).To(Succeed())
		Eventually(rejected).Should(ContainSubstring(
			"spec.username: Forbidden: RedisUser first also claims the username app; neither would be applied"))
	})

	It("keeps working after the operator restarts", func() {
		before := caBundle()
		_, err := kubectl("rollout", "restart", "deployment/redis-operator-controller-manager", "-n", namespace)
		Expect(err).NotTo(HaveOccurred())
		_, err = kubectl("rollout", "status", "deployment/redis-operator-controller-manager", "-n", namespace, "--timeout=3m")
		Expect(err).NotTo(HaveOccurred())

		Expect(caBundle()).To(Equal(before))
		Eventually(rejected).Should(ContainSubstring("RedisUser first also claims the username app"))
	})
})
