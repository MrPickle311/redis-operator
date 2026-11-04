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
	monNamespace = "redis-e2e-mon"
	monName      = "mon"
	// exporterImage is the redis_exporter of spec.monitoring.
	exporterImage = "oliver006/redis_exporter:v1.94.0"
	// monRoleBinding lets the ServiceAccount of the operator read its own
	// metrics, for operatorMetrics.
	monRoleBinding = "redis-operator-metrics-binding-mon"
)

var monManifest = fmt.Sprintf(`
apiVersion: redis.operator.com/v1
kind: RedisInstance
metadata: {name: %[1]s, namespace: %[2]s}
spec:
  instances: 2
  image: %[3]s
  storage:
    volumeClaimTemplate: {size: 256Mi}
  resources:
    requests: {memory: 128Mi}
    limits: {memory: 128Mi}
  monitoring: {enabled: true, image: %[4]s}
`, monName, monNamespace, redisImage, exporterImage)

// Scenario: spec.monitoring adds a redis_exporter sidecar to every Pod, and
// the operator reports the status of every RedisInstance as metrics.
var _ = Describe("Monitoring", Ordered, func() {
	SetDefaultEventuallyTimeout(3 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)

	BeforeAll(func() {
		Expect(loadImageIntoKind(redisImage)).To(Succeed())
		Expect(loadImageIntoKind(exporterImage)).To(Succeed())
		Expect(ensureRestrictedNamespace(monNamespace)).To(Succeed())
		Expect(kubectlApply(monManifest)).To(Succeed())
		Eventually(func() string {
			out, _ := kubectl("get", "redisinstance", monName, "-n", monNamespace,
				"-o", `jsonpath={.status.conditions[?(@.type=="Ready")].status}`)
			return out
		}).Should(Equal("True"))
	})

	AfterAll(func() {
		_, _ = kubectl("delete", "ns", monNamespace, "--ignore-not-found", "--timeout=2m")
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			out, _ := kubectl("get", "redisinstance,pods,events", "-n", monNamespace, "-o", "wide")
			_, _ = fmt.Fprintf(GinkgoWriter, "Resources in %s:\n%s\n", monNamespace, out)
		}
	})

	It("adds a redis_exporter sidecar that reads Redis over the unix socket", func() {
		primary, _ := kubectl("get", "redisinstance", monName, "-n", monNamespace, "-o", "jsonpath={.status.currentPrimary}")
		Eventually(func(g Gomega) {
			metrics, err := kubectl("get", "--raw",
				fmt.Sprintf("/api/v1/namespaces/%s/pods/%s:9121/proxy/metrics", monNamespace, primary))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(metrics).To(ContainSubstring("redis_up 1"))
			g.Expect(metrics).To(ContainSubstring("redis_connected_slaves 1"))
		}).Should(Succeed())
	})

	It("reports the role of every Pod and the conditions as metrics of the operator", func() {
		primary, _ := kubectl("get", "redisinstance", monName, "-n", monNamespace, "-o", "jsonpath={.status.currentPrimary}")
		Eventually(func(g Gomega) {
			metrics, err := operatorMetrics()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(metrics).To(ContainSubstring(fmt.Sprintf(
				`redis_operator_instance_pod_role{namespace="%s",pod="%s",redis_instance="%s"} 1`, monNamespace, primary, monName)))
			g.Expect(metrics).To(ContainSubstring(fmt.Sprintf(
				`redis_operator_condition{kind="RedisInstance",name="%s",namespace="%s",reason="Ready",type="Ready"} 1`, monName, monNamespace)))
		}).Should(Succeed())
	})

	It("counts the Events of a switchover, which may be over between two scrapes", func() {
		primary, _ := kubectl("get", "redisinstance", monName, "-n", monNamespace, "-o", "jsonpath={.status.currentPrimary}")
		target := monName + "-1"
		if primary == target {
			target = monName + "-0"
		}
		_, err := kubectl("annotate", "redisinstance", monName, "-n", monNamespace, "redis.operator.com/promote="+target)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) {
			metrics, err := operatorMetrics()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(metrics).To(ContainSubstring(fmt.Sprintf(
				`redis_operator_events_total{kind="RedisInstance",name="%s",namespace="%s",reason="SwitchoverCompleted",type="Normal"} 1`,
				monName, monNamespace)))
		}).Should(Succeed())
	})
})

// operatorMetrics returns what the metrics endpoint of the operator serves,
// read by a curl Pod with a token of the ServiceAccount of the operator.
func operatorMetrics() (string, error) {
	// Fails with AlreadyExists after the first call, which is fine.
	_, _ = kubectl("create", "clusterrolebinding", monRoleBinding, "--clusterrole=redis-operator-metrics-reader",
		fmt.Sprintf("--serviceaccount=%s:%s", namespace, serviceAccountName))
	token, err := kubectl("create", "token", serviceAccountName, "-n", namespace)
	if err != nil {
		return "", err
	}
	const pod = "curl-monitoring"
	_, _ = kubectl("delete", "pod", pod, "-n", namespace, "--ignore-not-found")
	if _, err := kubectl("run", pod, "--restart=Never", "-n", namespace,
		"--image=curlimages/curl:latest", "--overrides", curlPodOverrides(token)); err != nil {
		return "", err
	}
	if _, err := kubectl("wait", "pod/"+pod, "-n", namespace,
		"--for=jsonpath={.status.phase}=Succeeded", "--timeout=2m"); err != nil {
		return "", err
	}
	return kubectl("logs", pod, "-n", namespace)
}
