//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	rsNamespace = "redis-e2e-restore"
	rsClaim     = "restored-data"
)

// rsManifest is an instance whose primary, ordinal 0, runs on the external
// PVC rsClaim: the way a PVC restored from a VolumeSnapshot is handed over.
func rsManifest(name string) string {
	return fmt.Sprintf(`
apiVersion: redis.operator.com/v1
kind: RedisInstance
metadata: {name: %[1]s, namespace: %[2]s}
spec:
  instances: 2
  image: %[3]s
  storage:
    volumeClaimTemplate: {size: 256Mi}
    existingClaims:
      - {instanceOrdinal: 0, claimName: %[4]s}
  resources:
    requests: {memory: 128Mi}
    limits: {memory: 128Mi}
`, name, rsNamespace, redisImage, rsClaim)
}

// Scenario: the data of one instance, with an epoch above 1, starts another
// one. Kind has no CSI snapshots, but the operator sees the same thing: an
// external PVC with the data of another instance.
var _ = Describe("Restore", Ordered, func() {
	SetDefaultEventuallyTimeout(3 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)

	get := func(name, jsonpath string) string {
		out, _ := kubectl("get", "redisinstance", name, "-n", rsNamespace, "-o", "jsonpath="+jsonpath)
		return out
	}
	cli := func(g Gomega, name string, ordinal int, args ...string) string {
		password := secretValueIn(rsNamespace, name+"-internal-auth", "operator-password")
		out, err := kubectl(append([]string{"exec", fmt.Sprintf("%s-%d", name, ordinal), "-n", rsNamespace, "-c", "redis", "--",
			"redis-cli", "--tls", "--cacert", "/tls/ca.crt", "--user", "_operator", "--pass", password, "--no-auth-warning"}, args...)...)
		g.Expect(err).NotTo(HaveOccurred())
		return out
	}
	ready := func(name string) string {
		return get(name, `{.status.conditions[?(@.type=="Ready")].status}`)
	}
	switchOver := func(name string, to int, epoch string) {
		pod := fmt.Sprintf("%s-%d", name, to)
		_, err := kubectl("annotate", "redisinstance", name, "-n", rsNamespace, "redis.operator.com/promote="+pod, "--overwrite")
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() string { return get(name, "{.status.currentPrimary} {.status.primaryEpoch}") }).Should(Equal(pod + " " + epoch))
		Eventually(func() string { return ready(name) }).Should(Equal("True"))
	}

	BeforeAll(func() {
		Expect(loadImageIntoKind(redisImage)).To(Succeed())
		Expect(ensureRestrictedNamespace(rsNamespace)).To(Succeed())
	})

	AfterAll(func() {
		_, _ = kubectl("delete", "ns", rsNamespace, "--ignore-not-found", "--timeout=2m")
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			out, _ := kubectl("get", "redisinstance,pods,pvc", "-n", rsNamespace, "-o", "wide")
			_, _ = fmt.Fprintf(GinkgoWriter, "Resources in %s:\n%s\n", rsNamespace, out)
			out, _ = kubectl("get", "redisinstance", "-n", rsNamespace, "-o", "jsonpath={.items[*].status}")
			_, _ = fmt.Fprintf(GinkgoWriter, "Status:\n%s\n", out)
		}
	})

	It("leaves data and an epoch above 1 on the external PVC of the source", func() {
		Expect(kubectlApply(fmt.Sprintf(`
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: %s, namespace: %s}
spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 256Mi}}}
`, rsClaim, rsNamespace) + "---" + rsManifest("src"))).To(Succeed())
		Eventually(func() string { return ready("src") }).Should(Equal("True"))
		Eventually(func(g Gomega) {
			g.Expect(cli(g, "src", 0, "SET", "restored-key", "kept")).To(Equal("OK"))
		}).Should(Succeed())

		By("switching over away and back, so the PVC holds epoch 3")
		switchOver("src", 1, "2")
		switchOver("src", 0, "3")
		Expect(cli(Default, "src", 0, "GET", "restored-key")).To(Equal("kept"))

		_, err := kubectl("delete", "redisinstance", "src", "-n", rsNamespace, "--timeout=2m")
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() string {
			out, _ := kubectl("get", "pods", "-n", rsNamespace, "-o", "name")
			return out
		}).Should(BeEmpty())
	})

	It("starts a new instance on that PVC with its data and its own epoch", func() {
		Expect(kubectlApply(rsManifest("dst"))).To(Succeed())
		Eventually(func() string { return ready("dst") }).Should(Equal("True"))
		Expect(get("dst", "{.status.currentPrimary} {.status.primaryEpoch}")).To(Equal("dst-0 1"))

		for ordinal := range 2 {
			Eventually(func(g Gomega) {
				g.Expect(cli(g, "dst", ordinal, "GET", "restored-key")).To(Equal("kept"))
			}).Should(Succeed(), "dst-%d does not have the restored data", ordinal)
		}
		uid := get("dst", "{.metadata.uid}")
		epoch, err := kubectl("exec", "dst-0", "-n", rsNamespace, "-c", "redis", "--", "cat", "/data/.operator/epoch")
		Expect(err).NotTo(HaveOccurred())
		Expect(epoch).To(Equal("1 "+uid), "the epoch of src is still on the PVC")
	})
})
