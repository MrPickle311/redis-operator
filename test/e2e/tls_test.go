//go:build e2e
// +build e2e

package e2e

import (
	"encoding/base64"
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/MrPickle311/redis-operator/test/utils"
)

const (
	btNamespace = "redis-e2e-tls"
	btName      = "bt"
)

// btManifest issues the certificate of Redis and the one of a client from a
// CA of cert-manager, and asks every client for a certificate.
var btManifest = fmt.Sprintf(`
apiVersion: cert-manager.io/v1
kind: Issuer
metadata: {name: selfsigned, namespace: %[2]s}
spec: {selfSigned: {}}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata: {name: root, namespace: %[2]s}
spec:
  isCA: true
  commonName: e2e root
  secretName: root-ca
  privateKey: {algorithm: ECDSA}
  issuerRef: {name: selfsigned}
---
apiVersion: cert-manager.io/v1
kind: Issuer
metadata: {name: root, namespace: %[2]s}
spec: {ca: {secretName: root-ca}}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata: {name: server, namespace: %[2]s}
spec:
  secretName: %[1]s-tls
  issuerRef: {name: root}
  usages: [server auth]
  dnsNames: [%[1]s-rw.%[2]s.svc, %[1]s-ro.%[2]s.svc, %[1]s-r.%[2]s.svc, "*.%[1]s-hl.%[2]s.svc"]
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata: {name: app, namespace: %[2]s}
spec:
  secretName: app-cert
  issuerRef: {name: root}
  usages: [client auth]
  commonName: app
---
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
  tls:
    serverSecretRef: {name: %[1]s-tls}
    clientAuth: required
    clientCASecretRef: {name: root-ca, key: ca.crt}
`, btName, btNamespace, redisImage)

// Scenario: the certificates come from cert-manager, and clients must show one.
var _ = Describe("TLS with certificates of the user", Ordered, func() {
	SetDefaultEventuallyTimeout(3 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)

	var password string
	pod := func(i int) string { return fmt.Sprintf("%s-%d", btName, i) }
	secret := func(g Gomega, name, key string) string {
		out, err := kubectl("get", "secret", name, "-n", btNamespace, "-o", "jsonpath={.data."+strings.ReplaceAll(key, ".", `\.`)+"}")
		g.Expect(err).NotTo(HaveOccurred())
		value, err := base64.StdEncoding.DecodeString(out)
		g.Expect(err).NotTo(HaveOccurred())
		return string(value)
	}
	tlsReady := func() string {
		out, _ := kubectl("get", "redisinstance", btName, "-n", btNamespace, "-o",
			`jsonpath={.status.conditions[?(@.type=="TLSReady")].status} {.status.conditions[?(@.type=="TLSReady")].message}`)
		return out
	}
	// cli runs redis-cli in the Pod with the client certificate of cert-manager.
	cli := func(g Gomega, p string, args ...string) string {
		out, err := kubectl(append([]string{"exec", p, "-n", btNamespace, "-c", "redis", "--",
			"redis-cli", "--tls", "--cacert", "/tls/ca.crt", "--cert", "/tmp/app.crt", "--key", "/tmp/app.key",
			"--user", "_operator", "--pass", password, "--no-auth-warning"}, args...)...)
		g.Expect(err).NotTo(HaveOccurred())
		return out
	}

	BeforeAll(func() {
		Expect(loadImageIntoKind(redisImage)).To(Succeed())
		Expect(ensureRestrictedNamespace(btNamespace)).To(Succeed())
		Eventually(func() error { return kubectlApply(btManifest) }).Should(Succeed(), "the webhook of cert-manager is not up yet")
		Eventually(func(g Gomega) {
			password = secret(g, btName+"-internal-auth", "operator-password")
			g.Expect(password).NotTo(BeEmpty())
		}).Should(Succeed())
		for i := range 2 {
			Eventually(func() bool { return podReadyIn(btNamespace, pod(i)) }).Should(BeTrue())
		}
		By("giving the Pods the client certificate of cert-manager")
		for i := range 2 {
			for _, file := range []string{"tls.crt", "tls.key"} {
				target := map[string]string{"tls.crt": "/tmp/app.crt", "tls.key": "/tmp/app.key"}[file]
				cmd := exec.Command("kubectl", "exec", "-i", pod(i), "-n", btNamespace, "-c", "redis", "--", "sh", "-c", "cat > "+target)
				cmd.Stdin = strings.NewReader(secret(Default, "app-cert", file))
				_, err := utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())
			}
		}
	})

	AfterAll(func() {
		_, _ = kubectl("delete", "ns", btNamespace, "--ignore-not-found", "--timeout=2m")
	})

	It("serves the certificate from cert-manager and replicates with client certificates required", func() {
		Expect(tlsReady()).To(HavePrefix("True"))
		mounted, err := kubectl("exec", pod(0), "-n", btNamespace, "-c", "redis", "--", "cat", "/tls/ca.crt")
		Expect(err).NotTo(HaveOccurred())
		Expect(mounted).To(Equal(strings.TrimSpace(secret(Default, btName+"-tls", "ca.crt"))))

		Expect(cli(Default, pod(0), "PING")).To(Equal("PONG"))
		out, _ := kubectl("exec", pod(0), "-n", btNamespace, "-c", "redis", "--",
			"redis-cli", "--tls", "--cacert", "/tls/ca.crt", "PING")
		Expect(out).NotTo(ContainSubstring("PONG"), "a client without a certificate got in")

		primary, err := kubectl("get", "redisinstance", btName, "-n", btNamespace, "-o", "jsonpath={.status.currentPrimary}")
		Expect(err).NotTo(HaveOccurred())
		replica := pod(0)
		if replica == primary {
			replica = pod(1)
		}
		Expect(cli(Default, primary, "SET", "bt-key", "kept")).To(Equal("OK"))
		Eventually(func(g Gomega) {
			g.Expect(cli(g, replica, "INFO", "replication")).To(ContainSubstring("master_link_status:up"))
			g.Expect(cli(g, replica, "GET", "bt-key")).To(Equal("kept"))
		}).Should(Succeed(), "the replica did not sync with the certificate the operator gave it")
	})

	It("refuses a renewed certificate that misses the Pods, and keeps serving the last good one", func() {
		served := secret(Default, btName+"-server-tls", "tls.crt")
		_, err := kubectl("patch", "certificate", "server", "-n", btNamespace, "--type=merge", "-p",
			fmt.Sprintf(`{"spec":{"dnsNames":["%[1]s-rw.%[2]s.svc","%[1]s-ro.%[2]s.svc","%[1]s-r.%[2]s.svc"]}}`, btName, btNamespace))
		Expect(err).NotTo(HaveOccurred())

		Eventually(tlsReady).Should(SatisfyAll(HavePrefix("False"), ContainSubstring(pod(0)+"."+btName+"-hl")))
		Expect(secret(Default, btName+"-server-tls", "tls.crt")).To(Equal(served), "the certificate reached the Pods")
		Expect(cli(Default, pod(0), "PING")).To(Equal("PONG"))
	})

	It("takes the certificate again once it covers every name", func() {
		_, err := kubectl("patch", "certificate", "server", "-n", btNamespace, "--type=merge", "-p",
			fmt.Sprintf(`{"spec":{"dnsNames":["%[1]s-rw.%[2]s.svc","%[1]s-ro.%[2]s.svc","%[1]s-r.%[2]s.svc","*.%[1]s-hl.%[2]s.svc"]}}`, btName, btNamespace))
		Expect(err).NotTo(HaveOccurred())
		Eventually(tlsReady).Should(HavePrefix("True"))
	})
})
