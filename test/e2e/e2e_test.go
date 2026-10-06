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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	// namespace is where the operator is deployed.
	namespace              = "redis-operator-system"
	serviceAccountName     = "redis-operator-controller-manager"
	metricsServiceName     = "redis-operator-controller-manager-metrics-service"
	metricsRoleBindingName = "redis-operator-metrics-binding"
	metricsPodName         = "curl-metrics"
	managerPodSelector     = "control-plane=controller-manager"
)

// Scenario: the operator deployed by BeforeSuite runs and serves its metrics
// over HTTPS to a client that presents a ServiceAccount token.
var _ = Describe("Manager", Ordered, func() {
	SetDefaultEventuallyTimeout(3 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	AfterAll(func() {
		_, _ = kubectl("delete", "pod", metricsPodName, "-n", namespace, "--ignore-not-found")
		_, _ = kubectl("delete", "clusterrolebinding", metricsRoleBindingName, "--ignore-not-found")
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			printManagerDebugInfo()
		}
	})

	It("runs the controller-manager Pod", func() {
		Eventually(func(g Gomega) {
			out, err := kubectl("get", "pods", "-l", managerPodSelector, "-n", namespace,
				"-o", `jsonpath={.items[*].status.conditions[?(@.type=="Ready")].status}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"), "expected exactly one Ready controller-manager Pod")
		}).Should(Succeed())

		Eventually(func(g Gomega) {
			g.Expect(kubectl("logs", "-l", managerPodSelector, "-n", namespace, "--tail=-1")).
				To(ContainSubstring("Serving metrics server"))
		}).Should(Succeed())
	})

	It("serves metrics to an authorized ServiceAccount", func() {
		By("allowing the ServiceAccount to read metrics")
		_, err := kubectl("create", "clusterrolebinding", metricsRoleBindingName,
			"--clusterrole=redis-operator-metrics-reader",
			fmt.Sprintf("--serviceaccount=%s:%s", namespace, serviceAccountName))
		Expect(err).NotTo(HaveOccurred())

		By("requesting a ServiceAccount token")
		token, err := kubectl("create", "token", serviceAccountName, "-n", namespace)
		Expect(err).NotTo(HaveOccurred())

		// +kubebuilder:scaffold:e2e-metrics-webhooks-readiness

		By("calling the metrics endpoint from a curl Pod")
		_, err = kubectl("run", metricsPodName, "--restart=Never", "-n", namespace,
			"--image=curlimages/curl:latest", "--overrides", curlPodOverrides(token))
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			g.Expect(kubectl("get", "pod", metricsPodName, "-n", namespace, "-o", "jsonpath={.status.phase}")).
				To(Equal("Succeeded"))
		}, 5*time.Minute).Should(Succeed())

		Expect(kubectl("logs", metricsPodName, "-n", namespace)).To(ContainSubstring("< HTTP/1.1 200 OK"))
	})

	// +kubebuilder:scaffold:e2e-webhooks-checks
})

// curlPodOverrides returns a Pod spec that passes the restricted Pod Security level
// and retries the metrics request for up to a minute, while the endpoint starts.
func curlPodOverrides(token string) string {
	url := fmt.Sprintf("https://%s.%s.svc.cluster.local:8443/metrics", metricsServiceName, namespace)
	return fmt.Sprintf(`{
	"spec": {
		"serviceAccountName": %q,
		"containers": [{
			"name": "curl",
			"image": "curlimages/curl:latest",
			"command": ["/bin/sh", "-c"],
			"args": ["for i in $(seq 1 30); do curl -v -k -H 'Authorization: Bearer %s' %s && exit 0 || sleep 2; done; exit 1"],
			"securityContext": {
				"readOnlyRootFilesystem": true,
				"allowPrivilegeEscalation": false,
				"capabilities": {"drop": ["ALL"]},
				"runAsNonRoot": true,
				"runAsUser": 1000,
				"seccompProfile": {"type": "RuntimeDefault"}
			}
		}]
	}
}`, serviceAccountName, token, url)
}

// printManagerDebugInfo prints the manager logs, Pod description and namespace events.
func printManagerDebugInfo() {
	for title, args := range map[string][]string{
		"Controller logs":  {"logs", "-l", managerPodSelector, "-n", namespace, "--tail=-1"},
		"Controller Pod":   {"describe", "pod", "-l", managerPodSelector, "-n", namespace},
		"Events":           {"get", "events", "-n", namespace, "--sort-by=.lastTimestamp"},
		"curl-metrics log": {"logs", metricsPodName, "-n", namespace},
	} {
		out, _ := kubectl(args...)
		_, _ = fmt.Fprintf(GinkgoWriter, "--- %s:\n%s\n", title, out)
	}
}
