//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// The volume scenario runs in its own namespace with its own RedisInstance:
// it deletes PVCs and Pods on purpose, and ends by losing the primary.
const (
	volNamespace = "redis-e2e-volumes"
	volName      = "vols"
	volExternal  = volName + "-ext-1"   // existing claim of instance 1
	volSpare     = volName + "-spare-3" // existing claim reserved for instance 3
)

var volManifest = fmt.Sprintf(`
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: %[3]s, namespace: %[2]s}
spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 256Mi}}}
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: %[4]s, namespace: %[2]s}
spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 256Mi}}}
---
apiVersion: redis.operator.com/v1
kind: RedisInstance
metadata: {name: %[1]s, namespace: %[2]s}
spec:
  instances: 3
  image: %[5]s
  storage:
    volumeClaimTemplate: {size: 256Mi}
    existingClaims:
      - {instanceOrdinal: 1, claimName: %[3]s}
      - {instanceOrdinal: 3, claimName: %[4]s}
  resources:
    requests: {memory: 128Mi}
    limits: {memory: 128Mi}
`, volName, volNamespace, volExternal, volSpare, redisImage)

var volPassword string

var _ = Describe("RedisInstance volumes", Ordered, func() {
	SetDefaultEventuallyTimeout(3 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)

	BeforeAll(func() {
		By("loading the Redis image into Kind")
		Expect(loadImageIntoKind(redisImage)).To(Succeed())
		Expect(ensureRestrictedNamespace(volNamespace)).To(Succeed())

		By("creating the external PVCs and the RedisInstance")
		Expect(kubectlApply(volManifest)).To(Succeed())
		Eventually(func(g Gomega) {
			out, err := kubectl("get", "secret", volName+"-internal-auth", "-n", volNamespace,
				"-o", "go-template={{index .data \"operator-password\" | base64decode}}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(BeEmpty())
			volPassword = out
		}).Should(Succeed())
	})

	AfterAll(func() {
		_, _ = kubectl("delete", "ns", volNamespace, "--ignore-not-found", "--timeout=2m")
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			out, _ := kubectl("get", "redisinstance,pods,pvc", "-n", volNamespace, "-o", "wide")
			_, _ = fmt.Fprintf(GinkgoWriter, "Resources in %s:\n%s\n", volNamespace, out)
			out, _ = kubectl("get", "redisinstance", volName, "-n", volNamespace, "-o", "jsonpath={.status}")
			_, _ = fmt.Fprintf(GinkgoWriter, "Status:\n%s\n", out)
		}
	})

	It("starts every instance on the volume the spec chose and records its UID", func() {
		for i := range 3 {
			Eventually(volPodReady).WithArguments(i).Should(BeTrue(), "Pod %s is not Ready", volPod(i))
		}
		for i, claim := range map[int]string{0: volName + "-0-data", 1: volExternal, 2: volName + "-2-data"} {
			Expect(volPodClaim(i)).To(Equal(claim))
			Expect(volBinding(i, "uid")).To(Equal(volPVCUID(claim)), "UID of the volume of instance %d", i)
		}
		Expect(volBinding(1, "external")).To(Equal("true"))
		Expect(volCondition("StorageReady")).To(Equal("True"))

		By("writing data that must survive every later spec")
		Eventually(func(g Gomega) {
			g.Expect(volCLI(g, volPrimary(), "SET", "vol-key", "kept")).To(Equal("OK"))
		}).Should(Succeed())
		for i := range 3 {
			Eventually(func(g Gomega) {
				g.Expect(volCLI(g, volPod(i), "GET", "vol-key")).To(Equal("kept"))
			}).Should(Succeed())
		}
	})

	It("rejects every change of existingClaims", func() {
		for _, patch := range []string{
			`{"spec":{"storage":{"existingClaims":[{"instanceOrdinal":1,"claimName":"other"},{"instanceOrdinal":3,"claimName":"` + volSpare + `"}]}}}`,
			`{"spec":{"storage":{"existingClaims":null}}}`,
		} {
			_, err := kubectl("patch", "redisinstance", volName, "-n", volNamespace, "--type=merge", "-p", patch)
			Expect(err).To(MatchError(ContainSubstring("existingClaims is immutable")), "patch %s", patch)
		}
	})

	It("rebuilds the same bindings after status.volumes is lost", func() {
		before := volBindings()
		_, err := kubectl("patch", "redisinstance", volName, "-n", volNamespace, "--subresource=status",
			"--type=merge", "-p", `{"status":{"volumes":null}}`)
		Expect(err).NotTo(HaveOccurred())

		Eventually(volBindings).Should(Equal(before))
	})

	It("moves a replica whose PVC was deleted to a new volume on its own, and resyncs it", func() {
		claim := volName + "-2-data"
		oldUID := volPVCUID(claim)

		By("deleting the PVC: the operator deletes the Pod that holds it, so it can go")
		_, err := kubectl("delete", "pvc", claim, "-n", volNamespace, "--wait=false")
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			uid := volPVCUIDOrEmpty(claim)
			g.Expect(uid).NotTo(BeEmpty())
			g.Expect(uid).NotTo(Equal(oldUID))
			g.Expect(volBinding(2, "uid")).To(Equal(uid))
		}).Should(Succeed(), "no new PVC was generated")
		Eventually(volPodReady).WithArguments(2).Should(BeTrue())
		Expect(volCLI(Default, volPod(2), "GET", "vol-key")).To(Equal("kept"))
		Expect(volEvents("VolumeRecreated")).NotTo(BeEmpty())
		Expect(volEvents("ReplicaVolumeFailing")).NotTo(BeEmpty())
	})

	It("keeps a replica without a Pod when its external PVC was replaced", func() {
		By("deleting the Pod and its external PVC")
		_, err := kubectl("delete", "pvc", volExternal, "-n", volNamespace, "--wait=false")
		Expect(err).NotTo(HaveOccurred())
		_, err = kubectl("delete", "pod", volPod(1), "-n", volNamespace)
		Expect(err).NotTo(HaveOccurred())
		_, err = kubectl("wait", "--for=delete", "pvc/"+volExternal, "-n", volNamespace, "--timeout=2m")
		Expect(err).NotTo(HaveOccurred())

		By("creating another PVC with the same name")
		Expect(kubectlApply(strings.SplitN(volManifest, "---", 2)[0])).To(Succeed())

		Eventually(volBinding).WithArguments(1, "state").Should(Equal("Missing"))
		Expect(volBinding(1, "message")).To(ContainSubstring("created again"))
		Expect(volCondition("StorageReady")).To(Equal("False"))
		Consistently(volPodExists, 15*time.Second).WithArguments(1).Should(BeFalse(),
			"a replica was started on a PVC that is not its volume")
	})

	It("moves that replica to a generated volume on recreate and leaves the external PVC alone", func() {
		_, err := kubectl("annotate", "redisinstance", volName, "-n", volNamespace,
			"redis.operator.com/recreate="+volPod(1), "--overwrite")
		Expect(err).NotTo(HaveOccurred())

		Eventually(volPodReady).WithArguments(1).Should(BeTrue())
		Expect(volPodClaim(1)).To(Equal(volName + "-1-data"))
		Expect(volBinding(1, "external")).To(BeEmpty())
		Expect(volCLI(Default, volPod(1), "GET", "vol-key")).To(Equal("kept"))
		Expect(volPVCUIDOrEmpty(volExternal)).NotTo(BeEmpty(), "the operator deleted an external PVC")
		Expect(volHandled("redis.operator.com/recreate")).To(Equal(volPod(1)))
		Expect(volEvents("InstanceRecreated")).NotTo(BeEmpty())
		Eventually(volCondition).WithArguments("StorageReady").Should(Equal("True"))

		By("keeping the annotation, as a GitOps tool would: the replica is not recreated again")
		Expect(volAnnotation("redis.operator.com/recreate")).To(Equal(volPod(1)))
		uid, err := kubectl("get", "pod", volPod(1), "-n", volNamespace, "-o", "jsonpath={.metadata.uid}")
		Expect(err).NotTo(HaveOccurred())
		Consistently(func() (string, error) {
			return kubectl("get", "pod", volPod(1), "-n", volNamespace, "-o", "jsonpath={.metadata.uid}")
		}, 15*time.Second).Should(Equal(uid), "a handled recreate ran again")
	})

	It("rejects recreating the primary", func() {
		primary := volPrimary()
		uid, err := kubectl("get", "pod", primary, "-n", volNamespace, "-o", "jsonpath={.metadata.uid}")
		Expect(err).NotTo(HaveOccurred())

		_, err = kubectl("annotate", "redisinstance", volName, "-n", volNamespace, "redis.operator.com/recreate="+primary, "--overwrite")
		Expect(err).NotTo(HaveOccurred())

		Eventually(volEvents).WithArguments("RecreateRejected").ShouldNot(BeEmpty())
		Eventually(volHandled).WithArguments("redis.operator.com/recreate").Should(Equal(primary))
		Consistently(func() (string, error) {
			return kubectl("get", "pod", primary, "-n", volNamespace, "-o", "jsonpath={.metadata.uid}")
		}, 10*time.Second).Should(Equal(uid), "the primary Pod was replaced")
	})

	It("reuses a reserved and a detached external PVC when scaling, and regenerates removed volumes", func() {
		By("scaling up: instance 3 gets its reserved PVC")
		volScale(4)
		Eventually(volPodReady).WithArguments(3).Should(BeTrue())
		Expect(volPodClaim(3)).To(Equal(volSpare))
		spareUID := volPVCUID(volSpare)
		Eventually(func(g Gomega) {
			g.Expect(volCLI(g, volPod(3), "GET", "vol-key")).To(Equal("kept"))
		}).Should(Succeed())

		By("scaling down: the external PVC stays, the generated one goes")
		volScale(2)
		Eventually(volPodExists).WithArguments(3).Should(BeFalse())
		Eventually(volPodExists).WithArguments(2).Should(BeFalse())
		Eventually(volBinding).WithArguments(3, "state").Should(Equal("Detached"))
		Eventually(volPVCUIDOrEmpty).WithArguments(volName + "-2-data").Should(BeEmpty())
		Expect(volPVCUID(volSpare)).To(Equal(spareUID), "the operator deleted the external PVC")
		Expect(volBinding(2, "claimName")).To(BeEmpty(), "the binding of a removed generated volume stays")
		Eventually(volCondition).WithArguments("StorageReady").Should(Equal("True"))

		By("scaling up again: same external PVC, new generated one")
		volScale(4)
		for _, i := range []int{2, 3} {
			Eventually(volPodReady).WithArguments(i).Should(BeTrue())
			Eventually(func(g Gomega) {
				g.Expect(volCLI(g, volPod(i), "GET", "vol-key")).To(Equal("kept"))
			}).Should(Succeed())
		}
		Expect(volPodClaim(3)).To(Equal(volSpare))
		Expect(volBinding(3, "uid")).To(Equal(spareUID))

		volScale(3)
		Eventually(volPodExists).WithArguments(3).Should(BeFalse())
	})

	It("switches over on its own when the PVC of the primary is deleted, and moves the old primary to a new volume", func() {
		oldPrimary := volPrimary()
		ordinal := volOrdinal(oldPrimary)
		claim := volPodClaim(ordinal)
		Expect(claim).NotTo(Equal(volExternal), "the scenario needs a generated volume under the primary")
		oldUID := volPVCUID(claim)

		By("deleting the PVC of the primary while it runs")
		_, err := kubectl("delete", "pvc", claim, "-n", volNamespace, "--wait=false")
		Expect(err).NotTo(HaveOccurred())
		Eventually(volPrimary).ShouldNot(Equal(oldPrimary), "no switchover moved the primary off its going volume")
		Expect(volEvents("PrimaryVolumeFailing")).NotTo(BeEmpty())

		By("moving the old primary, now a replica, to a new empty volume, where it resyncs")
		Eventually(func(g Gomega) {
			uid := volPVCUIDOrEmpty(claim)
			g.Expect(uid).NotTo(BeEmpty())
			g.Expect(uid).NotTo(Equal(oldUID))
		}).Should(Succeed())
		Eventually(volPodReady).WithArguments(ordinal).Should(BeTrue())
		Expect(volCLI(Default, oldPrimary, "INFO", "replication")).To(ContainSubstring("role:slave"))
		Expect(volCLI(Default, oldPrimary, "GET", "vol-key")).To(Equal("kept"))
	})

	// Last: it leaves the instance without a primary.
	It("never starts the primary on a new empty volume, and fails over instead", func() {
		primary := volPrimary()
		ordinal := volOrdinal(primary)
		claim := volPodClaim(ordinal)

		By("losing the Pod and the PVC of the primary while the operator is stopped")
		// Running, it would switch over as soon as the PVC is going.
		scaleOperator(0)
		_, err := kubectl("delete", "pvc", claim, "-n", volNamespace, "--wait=false")
		Expect(err).NotTo(HaveOccurred())
		_, err = kubectl("delete", "pod", primary, "-n", volNamespace)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() string { return volPVCUIDOrEmpty(claim) }).Should(BeEmpty())
		scaleOperator(1)

		Eventually(volBinding).WithArguments(ordinal, "state").Should(Equal("Missing"))
		Expect(volBinding(ordinal, "message")).To(ContainSubstring("primary"))
		Expect(volCondition("StorageReady")).To(Equal("False"))
		Consistently(func() bool {
			return volPodExists(ordinal) || volPVCUIDOrEmpty(claim) != ""
		}, 20*time.Second).Should(BeFalse(), "an empty primary was started")

		By("checking that the replicas kept their data")
		for i := range 3 {
			if i != ordinal {
				Expect(volCLI(Default, volPod(i), "GET", "vol-key")).To(Equal("kept"))
			}
		}

		By("failing over once the primary is gone for failover.delay")
		Eventually(volPrimary).ShouldNot(Equal(primary))
		Expect(volCLI(Default, volPrimary(), "GET", "vol-key")).To(Equal("kept"))
		if claim != volExternal {
			By("bringing the old primary back as a replica on a new generated volume")
			Eventually(volPodReady).WithArguments(ordinal).Should(BeTrue())
			Expect(volCLI(Default, primary, "GET", "vol-key")).To(Equal("kept"))
		}
	})
})

func volPod(ordinal int) string { return fmt.Sprintf("%s-%d", volName, ordinal) }

func volOrdinal(pod string) int {
	var ordinal int
	_, err := fmt.Sscanf(strings.TrimPrefix(pod, volName+"-"), "%d", &ordinal)
	Expect(err).NotTo(HaveOccurred())
	return ordinal
}

func volGet(args ...string) string {
	out, err := kubectl(append([]string{"get", "-n", volNamespace}, args...)...)
	Expect(err).NotTo(HaveOccurred())
	return out
}

func volPrimary() string {
	return volGet("redisinstance", volName, "-o", "jsonpath={.status.currentPrimary}")
}

func volCondition(condition string) string {
	return volGet("redisinstance", volName, "-o",
		fmt.Sprintf(`jsonpath={.status.conditions[?(@.type=="%s")].status}`, condition))
}

func volAnnotation(key string) string {
	return volGet("redisinstance", volName, "-o",
		"jsonpath={.metadata.annotations."+strings.ReplaceAll(key, ".", `\.`)+"}")
}

// volHandled returns the value of the request annotation key the operator last handled.
func volHandled(key string) string {
	return volGet("redisinstance", volName, "-o",
		"jsonpath={.status.handledRequests."+strings.ReplaceAll(key, ".", `\.`)+"}")
}

// volBinding returns a field of the status.volumes entry of the ordinal.
func volBinding(ordinal int, field string) string {
	return volGet("redisinstance", volName, "-o",
		fmt.Sprintf("jsonpath={.status.volumes[?(@.ordinal==%d)].%s}", ordinal, field))
}

// volBindings returns every binding as "ordinal claim uid" lines.
func volBindings() string {
	return volGet("redisinstance", volName, "-o",
		`jsonpath={range .status.volumes[*]}{.ordinal} {.claimName} {.uid}{"\n"}{end}`)
}

func volPVCUID(name string) string {
	uid := volPVCUIDOrEmpty(name)
	Expect(uid).NotTo(BeEmpty(), "PVC %s not found", name)
	return uid
}

// volPVCUIDOrEmpty returns the UID of the PVC, or "" when it does not exist
// or is being deleted.
func volPVCUIDOrEmpty(name string) string {
	out, err := kubectl("get", "pvc", name, "-n", volNamespace, "--ignore-not-found",
		"-o", "jsonpath={.metadata.deletionTimestamp}|{.metadata.uid}")
	Expect(err).NotTo(HaveOccurred())
	deleting, uid, _ := strings.Cut(out, "|")
	if deleting != "" {
		return ""
	}
	return uid
}

func volPodClaim(ordinal int) string {
	return volGet("pod", volPod(ordinal), "-o", `jsonpath={.spec.volumes[?(@.name=="data")].persistentVolumeClaim.claimName}`)
}

func volPodReady(ordinal int) bool {
	out, err := kubectl("get", "pod", volPod(ordinal), "-n", volNamespace,
		"-o", `jsonpath={.status.conditions[?(@.type=="Ready")].status}`)
	return err == nil && out == "True"
}

func volPodExists(ordinal int) bool {
	out, err := kubectl("get", "pod", volPod(ordinal), "-n", volNamespace, "--ignore-not-found", "-o", "name")
	Expect(err).NotTo(HaveOccurred())
	return out != ""
}

func volScale(instances int) {
	_, err := kubectl("patch", "redisinstance", volName, "-n", volNamespace, "--type=merge",
		"-p", fmt.Sprintf(`{"spec":{"instances":%d}}`, instances))
	Expect(err).NotTo(HaveOccurred())
}

// volEvents returns the Events of the RedisInstance with the given reason.
func volEvents(reason string) string {
	return volGet("events", "--field-selector", "reason="+reason, "-o", "name")
}

// volCLI runs redis-cli as the operator user inside the given Pod.
func volCLI(g Gomega, pod string, args ...string) string {
	out, err := kubectl(append([]string{"exec", pod, "-n", volNamespace, "-c", "redis", "--",
		"redis-cli", "--user", "_operator", "--pass", volPassword, "--no-auth-warning"}, args...)...)
	g.Expect(err).NotTo(HaveOccurred())
	return out
}
