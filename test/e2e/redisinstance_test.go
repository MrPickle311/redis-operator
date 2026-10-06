//go:build e2e
// +build e2e

/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
	redisNamespace = "redis-e2e"
	redisName      = "e2e-redis"
	redisImage     = "redis:7.2"
	redisInstances = 3
)

var redisInstanceManifest = fmt.Sprintf(`
apiVersion: redis.operator.com/v1
kind: RedisInstance
metadata:
  name: %s
  namespace: %s
spec:
  instances: %d
  image: %s
  storage:
    volumeClaimTemplate:
      size: 256Mi
`, redisName, redisNamespace, redisInstances, redisImage)

// Scenario: one RedisInstance with a primary (ordinal 0) and two replicas.
// The specs are Ordered and build on each other: the data written in one spec
// is expected to survive the primary restart in a later one.
var _ = Describe("RedisInstance", Ordered, func() {
	SetDefaultEventuallyTimeout(3 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)

	primary := podName(0)

	BeforeAll(func() {
		By("loading the Redis image into Kind")
		Expect(loadImageIntoKind(redisImage)).To(Succeed())

		By("creating a namespace with the restricted Pod Security level")
		Expect(ensureRestrictedNamespace(redisNamespace)).To(Succeed())

		By("creating the RedisInstance")
		Expect(kubectlApply(redisInstanceManifest)).To(Succeed())
	})

	AfterAll(func() {
		By("removing the RedisInstance namespace")
		// Waits for the deletion, so the next run on a persistent cluster
		// does not hit a namespace that is still terminating.
		_, _ = kubectl("delete", "ns", redisNamespace, "--ignore-not-found", "--timeout=2m")
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			printDebugInfo()
		}
	})

	It("starts every instance", func() {
		for i := range redisInstances {
			Eventually(podReady).WithArguments(podName(i)).Should(BeTrue(), "Pod %s is not Ready", podName(i))
		}
	})

	It("creates the headless Service", func() {
		out, err := kubectl("get", "svc", redisName+"-hl", "-n", redisNamespace,
			"-o", "jsonpath={.spec.clusterIP} {.spec.publishNotReadyAddresses}")
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(Equal("None true"), "expected clusterIP None and publishNotReadyAddresses true")
	})

	It("replicates from the primary over its DNS name", func() {
		for _, replica := range replicaPods() {
			Eventually(func(g Gomega) {
				info := redisCLI(g, replica, "INFO", "replication")
				g.Expect(info).To(ContainSubstring("master_host:" + podFQDN(0)))
				g.Expect(info).To(ContainSubstring("master_link_status:up"))
			}).Should(Succeed(), "replica %s is not replicating from %s", replica, podFQDN(0))
		}
	})

	It("propagates writes from the primary to the replicas", func() {
		Eventually(func(g Gomega) {
			g.Expect(redisCLI(g, primary, "SET", "e2e-key", "hello")).To(Equal("OK"))
		}).Should(Succeed())

		for _, replica := range replicaPods() {
			Eventually(func(g Gomega) {
				g.Expect(redisCLI(g, replica, "GET", "e2e-key")).To(Equal("hello"))
			}).Should(Succeed(), "replica %s did not receive the write", replica)
		}
	})

	It("accepts writes through -rw and rejects them through -ro", func() {
		Eventually(func(g Gomega) {
			g.Expect(redisCLI(g, primary, "-h", redisName+"-rw", "SET", "rw-key", "via-rw")).To(Equal("OK"))
		}).Should(Succeed())

		Eventually(func(g Gomega) {
			g.Expect(redisCLI(g, primary, "-h", redisName+"-ro", "GET", "rw-key")).To(Equal("via-rw"))
		}).Should(Succeed(), "a replica behind -ro did not serve the read")

		// redis-cli may exit with 0 on an error reply, so only the output is checked.
		out, _ := kubectl("exec", primary, "-n", redisNamespace, "--",
			"redis-cli", "-h", redisName+"-ro", "SET", "rw-key", "via-ro")
		Expect(out).To(ContainSubstring("READONLY"))
	})

	It("does not resync the replicas when reconciling again", func() {
		var before string
		Eventually(func(g Gomega) { before = infoField(redisCLI(g, primary, "INFO", "stats"), "sync_full") }).Should(Succeed())

		By("triggering a few reconciles by touching the RedisInstance")
		for i := range 3 {
			_, err := kubectl("annotate", "redisinstance", redisName, "-n", redisNamespace,
				fmt.Sprintf("e2e/touch=%d", i), "--overwrite")
			Expect(err).NotTo(HaveOccurred())
		}

		Consistently(func(g Gomega) string {
			return infoField(redisCLI(g, primary, "INFO", "stats"), "sync_full")
		}, 15*time.Second).Should(Equal(before), "a reconcile triggered a full resync of a replica")
	})

	It("reconnects the replicas after the primary Pod is recreated", func() {
		oldIP := podIP(primary)

		By("deleting the primary Pod")
		_, err := kubectl("delete", "pod", primary, "-n", redisNamespace)
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the operator to recreate it")
		Eventually(podReady).WithArguments(primary).Should(BeTrue())
		_, _ = fmt.Fprintf(GinkgoWriter, "Primary Pod IP changed: %s -> %s\n", oldIP, podIP(primary))

		By("checking that the replicas are connected again and still have the data")
		for _, replica := range replicaPods() {
			Eventually(func(g Gomega) {
				info := redisCLI(g, replica, "INFO", "replication")
				g.Expect(info).To(ContainSubstring("master_link_status:up"))
				g.Expect(redisCLI(g, replica, "GET", "e2e-key")).To(Equal("hello"))
			}).Should(Succeed(), "replica %s did not recover after the primary restart", replica)
		}
	})
})

// --- Naming: mirrors the operator's conventions on purpose, without importing them,
// so a bug in the operator's naming code cannot hide itself from this test.

func podName(ordinal int) string {
	return fmt.Sprintf("%s-%d", redisName, ordinal)
}

func podFQDN(ordinal int) string {
	return fmt.Sprintf("%s.%s-hl.%s.svc", podName(ordinal), redisName, redisNamespace)
}

// replicaPods returns the names of all Pods except the primary (ordinal 0).
func replicaPods() []string {
	pods := make([]string, 0, redisInstances-1)
	for i := 1; i < redisInstances; i++ {
		pods = append(pods, podName(i))
	}
	return pods
}

// --- Cluster access: everything goes through kubectl, because Pod IPs inside
// Kind are not reachable from the machine running the tests.

func podReady(name string) bool {
	out, err := kubectl("get", "pod", name, "-n", redisNamespace,
		"-o", `jsonpath={.status.conditions[?(@.type=="Ready")].status}`)
	return err == nil && out == "True"
}

func podIP(name string) string {
	out, _ := kubectl("get", "pod", name, "-n", redisNamespace, "-o", "jsonpath={.status.podIP}")
	return out
}

// redisCLI runs redis-cli inside the given Pod and returns its output.
func redisCLI(g Gomega, pod string, args ...string) string {
	out, err := kubectl(append([]string{"exec", pod, "-n", redisNamespace, "--", "redis-cli"}, args...)...)
	g.Expect(err).NotTo(HaveOccurred())
	return out
}

// infoField returns the value of a "field:value" line from Redis INFO output.
func infoField(info, field string) string {
	for line := range strings.Lines(info) {
		if value, ok := strings.CutPrefix(strings.TrimSpace(line), field+":"); ok {
			return value
		}
	}
	return ""
}

// loadImageIntoKind pulls the image only when it is missing locally;
// kind load itself skips nodes that already have it.
func loadImageIntoKind(image string) error {
	if _, err := utils.Run(exec.Command("docker", "image", "inspect", image)); err != nil {
		if _, err := utils.Run(exec.Command("docker", "pull", image)); err != nil {
			return err
		}
	}
	return utils.LoadImageToKindClusterWithName(image)
}

// printDebugInfo prints the state of the test namespace and the operator logs.
func printDebugInfo() {
	out, _ := kubectl("get", "redisinstances,pods,svc,pvc", "-n", redisNamespace, "-o", "wide")
	_, _ = fmt.Fprintf(GinkgoWriter, "Resources in %s:\n%s\n", redisNamespace, out)

	out, _ = kubectl("logs", "-n", namespace, "-l", "control-plane=controller-manager", "--tail=100")
	_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n%s\n", out)
}
