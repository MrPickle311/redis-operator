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
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net/http"
	"os/exec"
	"slices"
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

	// imLocalPort is where the tests reach the Instance Manager API through kubectl port-forward.
	imLocalPort = 18000
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
  resources:
    requests: {memory: 128Mi}
    limits: {memory: 128Mi}
`, redisName, redisNamespace, redisInstances, redisImage)

// Passwords of the internal users, read from the Secret the operator generates.
var operatorPassword, replicationPassword string

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

		By("reading the passwords of the internal users")
		Eventually(func(g Gomega) {
			operatorPassword = secretValue(g, redisName+"-internal-auth", "operator-password")
			replicationPassword = secretValue(g, redisName+"-internal-auth", "replication-password")
		}).Should(Succeed())
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
		Eventually(readyCondition).Should(Equal("True Ready"))
		// What a user or a CI pipeline runs to wait for the instance.
		_, err := kubectl("wait", "redisinstance", redisName, "-n", redisNamespace, "--for=condition=Ready", "--timeout=1m")
		Expect(err).NotTo(HaveOccurred())
	})

	It("runs the Instance Manager as PID 1 with redis-server as its child", func() {
		out, err := kubectl("exec", primary, "-n", redisNamespace, "-c", "redis", "--", "cat", "/proc/1/cmdline")
		Expect(err).NotTo(HaveOccurred())
		// cmdline separates the arguments with NUL bytes.
		Expect(strings.Split(out, "\x00")).To(HaveExactElements("/controller/manager", "instance", "run",
			"--instance="+redisName, "--maxmemory-ratio=75", "--maxmemory-policy=noeviction", ""))

		Expect(redisCLI(Default, primary, "PING")).To(Equal("PONG"))
	})

	It("lets only the internal users in, each with its own permissions", func() {
		// redis-cli may exit with 0 on an error reply, so only the output is checked.
		out, _ := kubectl("exec", primary, "-n", redisNamespace, "-c", "redis", "--", "redis-cli", "PING")
		Expect(out).To(ContainSubstring("NOAUTH"), "the default user must be off")

		out, _ = kubectl("exec", primary, "-n", redisNamespace, "-c", "redis", "--",
			"redis-cli", "--user", "_replication", "--pass", replicationPassword, "--no-auth-warning", "GET", "e2e-key")
		Expect(out).To(ContainSubstring("NOPERM"), "the replication user must not read data")
	})

	It("derives maxmemory from the memory limit of the container", func() {
		// 75% of the 128Mi limit, read by the Instance Manager from the cgroup.
		Expect(redisCLI(Default, primary, "CONFIG", "GET", "maxmemory")).To(Equal("maxmemory\n100663296"))
		Expect(redisCLI(Default, primary, "CONFIG", "GET", "maxmemory-policy")).To(Equal("maxmemory-policy\nnoeviction"))

		Eventually(func(g Gomega) {
			out, err := kubectl("get", "redisinstance", redisName, "-n", redisNamespace,
				"-o", `jsonpath={.status.conditions[?(@.type=="MemoryLimited")].status}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}).Should(Succeed())
	})

	It("starts the replicas as replicas of the current primary", func() {
		// Read by the Instance Manager from status.currentPrimary before redis-server starts.
		conf := func(pod string) string {
			out, err := kubectl("exec", pod, "-n", redisNamespace, "-c", "redis", "--", "cat", "/controller/redis.conf")
			Expect(err).NotTo(HaveOccurred())
			return out
		}
		Expect(conf(primary)).NotTo(ContainSubstring("replicaof"))
		for i := 1; i < redisInstances; i++ {
			Expect(conf(podName(i))).To(ContainSubstring(
				fmt.Sprintf("replicaof %s.%s-hl.%s.svc 6379", primary, redisName, redisNamespace)))
		}
	})

	It("lets the Instance Managers read only their own RedisInstance", func() {
		account := fmt.Sprintf("--as=system:serviceaccount:%s:%s-instance", redisNamespace, redisName)
		for _, check := range []struct {
			verb, resource, want string
		}{
			{"get", "redisinstances.redis.operator.com/" + redisName, "yes"},
			{"get", "redisinstances.redis.operator.com/other", "no"},
			{"update", "redisinstances.redis.operator.com/" + redisName, "no"},
			{"get", "secrets", "no"},
		} {
			out, _ := kubectl("auth", "can-i", check.verb, check.resource, "-n", redisNamespace, account)
			Expect(out).To(Equal(check.want), "can-i %s %s", check.verb, check.resource)
		}
	})

	It("labels its objects so that Argo CD label tracking does not take them for its own", func() {
		out, err := kubectl("get", "pods,svc", "-n", redisNamespace, "-l", "app.kubernetes.io/instance", "-o", "name")
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(BeEmpty(), "objects with app.kubernetes.io/instance")

		out, err = kubectl("get", "pods", "-n", redisNamespace, "-l", "redis.operator.com/instance="+redisName, "-o", "name")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.Fields(out)).To(HaveLen(redisInstances))
		out, err = kubectl("get", "svc", redisName+"-rw", "-n", redisNamespace, "-o", `jsonpath={.spec.selector.redis\.operator\.com/instance}`)
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(Equal(redisName))
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

	It("sends REPLICAOF through the Instance Manager with the epoch of the primary", func() {
		Eventually(func(g Gomega) {
			out, err := kubectl("get", "redisinstance", redisName, "-n", redisNamespace, "-o", "jsonpath={.status.primaryEpoch}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("1"))
		}).Should(Succeed())

		for i := 1; i < redisInstances; i++ {
			out, err := kubectl("exec", podName(i), "-n", redisNamespace, "-c", "redis", "--", "cat", "/data/.operator/epoch")
			Expect(err).NotTo(HaveOccurred())
			Expect(out).To(Equal("1"), "replica %s did not store the epoch", podName(i))
		}
	})

	It("serves the Instance Manager API only to clients with the operator certificate", func() {
		By("forwarding a local port to the API of the primary")
		portForward := exec.Command("kubectl", "port-forward", "pod/"+primary, "-n", redisNamespace,
			fmt.Sprintf("%d:8000", imLocalPort))
		Expect(portForward.Start()).To(Succeed())
		DeferCleanup(portForward.Process.Kill)

		// The server certificate is not checked here: the test is about the
		// server rejecting the client, which has no certificate at all.
		client := &http.Client{Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
		}}
		Eventually(func(g Gomega) {
			_, err := client.Get(fmt.Sprintf("https://127.0.0.1:%d/status", imLocalPort))
			g.Expect(err).To(MatchError(ContainSubstring("certificate required")))
		}, 30*time.Second).Should(Succeed())
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

		out, _ := kubectl(redisCLIArgs(primary, "-h", redisName+"-ro", "SET", "rw-key", "via-ro")...)
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

	It("keeps a new replica out of the Services until its first sync is done", func() {
		newReplica := podName(redisInstances)
		setReplicationUser := func(state string) {
			Eventually(func(g Gomega) {
				g.Expect(redisCLI(g, primary, "ACL", "SETUSER", "_replication", state)).To(Equal("OK"))
			}).Should(Succeed())
		}
		scaleTo := func(instances int) {
			_, err := kubectl("patch", "redisinstance", redisName, "-n", redisNamespace, "--type=merge",
				"-p", fmt.Sprintf(`{"spec":{"instances":%d}}`, instances))
			Expect(err).NotTo(HaveOccurred())
		}

		By("blocking the sync: the primary no longer accepts the replication user")
		// Only new logins are refused; existing replication links stay up.
		setReplicationUser("off")
		DeferCleanup(func() {
			setReplicationUser("on")
			scaleTo(redisInstances)
			Eventually(func() bool {
				_, err := kubectl("get", "pod", newReplica, "-n", redisNamespace)
				return err != nil
			}).Should(BeTrue(), "Pod %s was not removed after scaling down", newReplica)
		})

		By("adding a replica")
		scaleTo(redisInstances + 1)
		Eventually(func(g Gomega) {
			g.Expect(redisCLI(g, newReplica, "PING")).To(Equal("PONG"))
		}).Should(Succeed(), "redis-server did not start in %s", newReplica)

		By("checking that it is running but not Ready, and that the other replicas stay Ready")
		Consistently(func(g Gomega) {
			g.Expect(infoField(redisCLI(g, newReplica, "INFO", "replication"), "master_link_status")).To(Equal("down"))
			g.Expect(podReady(newReplica)).To(BeFalse(), "a replica without the dataset of the primary is Ready")
			for _, replica := range replicaPods() {
				g.Expect(podReady(replica)).To(BeTrue(), "synced replica %s left the Services", replica)
			}
		}, 20*time.Second).Should(Succeed())

		By("unblocking the sync")
		setReplicationUser("on")
		Eventually(podReady).WithArguments(newReplica).Should(BeTrue())
		Expect(redisCLI(Default, newReplica, "GET", "e2e-key")).To(Equal("hello"),
			"the replica became Ready without the data of the primary")
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

	It("switches over to a replica on request and back, without losing writes", func() {
		switchOver := func(to, from int, epoch string) {
			By(fmt.Sprintf("annotating the RedisInstance to promote %s", podName(to)))
			_, err := kubectl("annotate", "redisinstance", redisName, "-n", redisNamespace,
				"redis.operator.com/promote="+podName(to), "--overwrite")
			Expect(err).NotTo(HaveOccurred())

			Eventually(func(g Gomega) {
				out, err := kubectl("get", "redisinstance", redisName, "-n", redisNamespace, "-o",
					`jsonpath={.status.currentPrimary} {.status.primaryEpoch} {.status.handledRequests.redis\.operator\.com/promote}`)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(Equal(podName(to)+" "+epoch+" "+podName(to)), "status and handled request after the switchover")
			}).Should(Succeed())

			By("checking that the new primary takes writes through -rw and the old one replicates from it")
			Eventually(func(g Gomega) {
				g.Expect(redisCLI(g, podName(to), "INFO", "replication")).To(ContainSubstring("role:master"))
				g.Expect(redisCLI(g, podName(to), "-h", redisName+"-rw", "SET", "switchover-key", podName(to))).To(Equal("OK"))
			}).Should(Succeed())
			// The old primary was not Ready-latched, so this also covers /replicaof
			// reaching a Pod that is Running but not Ready.
			Eventually(func(g Gomega) {
				info := redisCLI(g, podName(from), "INFO", "replication")
				g.Expect(info).To(ContainSubstring("master_host:" + podFQDN(to)))
				g.Expect(info).To(ContainSubstring("master_link_status:up"))
				g.Expect(redisCLI(g, podName(from), "GET", "switchover-key")).To(Equal(podName(to)))
			}).Should(Succeed(), "old primary %s did not become a replica of %s", podName(from), podName(to))
			Eventually(podReady).WithArguments(podName(from)).Should(BeTrue())

			for i := range redisInstances {
				Expect(redisCLI(Default, podName(i), "GET", "e2e-key")).To(Equal("hello"), "Pod %s lost data", podName(i))
			}
		}

		switchOver(1, 0, "2")
		switchOver(0, 1, "3")
	})

	It("applies spec.redis.parameters and persistence to every instance without a restart", func() {
		uids := map[string]string{}
		for i := range redisInstances {
			uid, err := kubectl("get", "pod", podName(i), "-n", redisNamespace, "-o", "jsonpath={.metadata.uid}")
			Expect(err).NotTo(HaveOccurred())
			uids[podName(i)] = uid
		}
		configOf := func(g Gomega, option string) []string {
			var values []string
			for i := range redisInstances {
				values = append(values, strings.Split(redisCLI(g, podName(i), "CONFIG", "GET", option), "\n")[1])
			}
			return values
		}
		patchRedis := func(redis string) {
			_, err := kubectl("patch", "redisinstance", redisName, "-n", redisNamespace, "--type=merge",
				"-p", `{"spec":{"redis":`+redis+`}}`)
			Expect(err).NotTo(HaveOccurred())
		}
		all := func(value string) []string { return []string{value, value, value} }

		By("turning the AOF on, as persistence defaults to aof")
		Eventually(configOf).WithArguments("appendonly").Should(Equal(all("yes")))

		By("setting parameters")
		patchRedis(`{"parameters":{"hz":"20","notify-keyspace-events":"Ex"}}`)
		Eventually(configOf).WithArguments("hz").Should(Equal(all("20")))
		Eventually(configOf).WithArguments("notify-keyspace-events").Should(Equal(all("xE")))

		By("reporting a value Redis refuses, and recovering once it is fixed")
		patchRedis(`{"parameters":{"hz":"fast"}}`)
		Eventually(readyCondition).Should(Equal("False ParametersNotApplied"))
		out, err := kubectl("get", "redisinstance", redisName, "-n", redisNamespace, "-o",
			`jsonpath={.status.conditions[?(@.type=="ParametersApplied")].message}`)
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring("hz"))
		patchRedis(`{"parameters":{"hz":"15"}}`)
		Eventually(configOf).WithArguments("hz").Should(Equal(all("15")))
		Eventually(readyCondition).Should(Equal("True Ready"))

		By("turning the AOF off: no AOF files stay, and no write is lost")
		Expect(redisCLI(Default, podName(0), "-h", redisName+"-rw", "SET", "persistence-key", "kept")).To(Equal("OK"))
		patchRedis(`{"persistence":"rdb"}`)
		Eventually(configOf).WithArguments("appendonly").Should(Equal(all("no")))
		for i := range redisInstances {
			Eventually(func() error {
				_, err := kubectl("exec", podName(i), "-n", redisNamespace, "-c", "redis", "--", "ls", "/data/appendonlydir")
				return err
			}).Should(HaveOccurred(), "%s kept its AOF files", podName(i))
			Expect(redisCLI(Default, podName(i), "GET", "persistence-key")).To(Equal("kept"))
		}
		patchRedis(`{"persistence":"aof"}`)
		Eventually(configOf).WithArguments("appendonly").Should(Equal(all("yes")))

		for name, uid := range uids {
			Expect(kubectl("get", "pod", name, "-n", redisNamespace, "-o", "jsonpath={.metadata.uid}")).To(Equal(uid),
				"%s was restarted for a change Redis applies live", name)
		}
	})

	It("rolls a new image out one Pod at a time, the primary last, without losing writes", func() {
		const newImage = "redis:7.4"
		Expect(loadImageIntoKind(newImage)).To(Succeed())
		Expect(redisCLI(Default, primary, "-h", redisName+"-rw", "SET", "rollout-key", "before")).To(Equal("OK"))

		By("changing spec.image")
		_, err := kubectl("patch", "redisinstance", redisName, "-n", redisNamespace, "--type=merge",
			"-p", fmt.Sprintf(`{"spec":{"image":%q}}`, newImage))
		Expect(err).NotTo(HaveOccurred())

		By("watching that at most one instance is down and the primary is updated last")
		var order []string
		mostDown := 0
		reasons := map[string]bool{}
		Eventually(func(g Gomega) {
			reasons[readyCondition()] = true
			down := 0
			for i := range redisInstances {
				name := podName(i)
				image, _ := kubectl("get", "pod", name, "-n", redisNamespace, "-o", "jsonpath={.spec.containers[0].image}")
				if !podReady(name) {
					down++
				} else if image == newImage && !slices.Contains(order, name) {
					order = append(order, name)
				}
			}
			mostDown = max(mostDown, down)
			g.Expect(order).To(HaveLen(redisInstances))
		}).WithPolling(time.Second).WithTimeout(6 * time.Minute).Should(Succeed())
		Expect(mostDown).To(BeNumerically("<=", 1), "more than one instance was down at once")
		Expect(order[redisInstances-1]).To(Equal(primary), "the primary was not updated last: %v", order)
		Expect(reasons).To(HaveKey("False RollingUpdate"), "the Ready condition did not show the rolling update")
		Eventually(readyCondition).Should(Equal("True Ready"))

		By("checking that the primary moved to the lowest replica, and every instance has the data")
		out, err := kubectl("get", "redisinstance", redisName, "-n", redisNamespace, "-o",
			"jsonpath={.status.currentPrimary} {.status.primaryEpoch}")
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(Equal(podName(1) + " 4"))
		Eventually(func(g Gomega) {
			for i := range redisInstances {
				g.Expect(redisCLI(g, podName(i), "GET", "rollout-key")).To(Equal("before"), "Pod %s lost data", podName(i))
				g.Expect(redisCLI(g, podName(i), "GET", "e2e-key")).To(Equal("hello"), "Pod %s lost data", podName(i))
			}
		}).Should(Succeed())
		Expect(redisCLI(Default, podName(1), "-h", redisName+"-rw", "SET", "rollout-key", "after")).To(Equal("OK"))
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

// readyCondition returns the status and reason of the Ready condition of the RedisInstance.
func readyCondition() string {
	out, _ := kubectl("get", "redisinstance", redisName, "-n", redisNamespace, "-o",
		`jsonpath={.status.conditions[?(@.type=="Ready")].status} {.status.conditions[?(@.type=="Ready")].reason}`)
	return out
}

func podIP(name string) string {
	out, _ := kubectl("get", "pod", name, "-n", redisNamespace, "-o", "jsonpath={.status.podIP}")
	return out
}

// redisCLI runs redis-cli as the operator user inside the given Pod and returns its output.
func redisCLI(g Gomega, pod string, args ...string) string {
	out, err := kubectl(redisCLIArgs(pod, args...)...)
	g.Expect(err).NotTo(HaveOccurred())
	return out
}

// redisCLIArgs returns the kubectl arguments that run redis-cli as the operator user.
func redisCLIArgs(pod string, args ...string) []string {
	return append([]string{"exec", pod, "-n", redisNamespace, "-c", "redis", "--",
		"redis-cli", "--user", "_operator", "--pass", operatorPassword, "--no-auth-warning"}, args...)
}

// secretValue returns the decoded value of a key of a Secret in the Redis namespace.
func secretValue(g Gomega, secret, key string) string {
	out, err := kubectl("get", "secret", secret, "-n", redisNamespace, "-o", "jsonpath={.data."+key+"}")
	g.Expect(err).NotTo(HaveOccurred())
	value, err := base64.StdEncoding.DecodeString(out)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(value).NotTo(BeEmpty())
	return string(value)
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
