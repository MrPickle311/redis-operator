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
	aclNamespace = "redis-e2e-acl"
	aclName      = "acl"
)

// aclManifest has a user of each source: a RedisUser with a generated
// password, one with a password of the user and one in an ACL file.
var aclManifest = fmt.Sprintf(`
apiVersion: v1
kind: Secret
metadata: {name: legacy-acl, namespace: %[2]s}
stringData:
  users.acl: |
    user legacy on >legacy-pw ~legacy:* +@all
---
apiVersion: v1
kind: Secret
metadata: {name: vault-pw, namespace: %[2]s}
stringData: {password: v1}
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
  acl:
    externalSecretRef: {name: legacy-acl, key: users.acl}
---
apiVersion: redis.operator.com/v1
kind: RedisUser
metadata: {name: app, namespace: %[2]s}
spec:
  targetRef: {name: %[1]s}
  authentication: {generate: true}
  acl: {rules: ["~app:*", "+@all"]}
---
apiVersion: redis.operator.com/v1
kind: RedisUser
metadata: {name: vault, namespace: %[2]s}
spec:
  targetRef: {name: %[1]s}
  authentication:
    passwordSecretRef: {name: vault-pw, key: password}
  acl: {rules: ["~vault:*", "+@all"]}
`, aclName, aclNamespace, redisImage)

// Scenario: users of RedisUsers and of an ACL file reach every instance, a
// password rotates without a moment both are refused, and bad rules or a bad
// file leave the users that worked before.
var _ = Describe("Redis users", Ordered, func() {
	SetDefaultEventuallyTimeout(3 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)

	pods := []string{aclName + "-0", aclName + "-1"}
	get := func(kind, name, jsonpath string) string {
		out, _ := kubectl("get", kind, name, "-n", aclNamespace, "-o", "jsonpath="+jsonpath)
		return out
	}
	condition := func(kind, name, condition string) string {
		return get(kind, name, fmt.Sprintf(`{.status.conditions[?(@.type=="%[1]s")].status} {.status.conditions[?(@.type=="%[1]s")].reason}`, condition))
	}
	// as runs redis-cli in the Pod as the user, and returns the output with
	// the errors of Redis, e.g. WRONGPASS or NOPERM.
	as := func(pod, user, password string, args ...string) string {
		out, _ := utils.Run(exec.Command("kubectl", append([]string{"exec", pod, "-n", aclNamespace, "-c", "redis", "--",
			"redis-cli", "--tls", "--cacert", "/tls/ca.crt", "--user", user, "--pass", password, "--no-auth-warning"}, args...)...))
		return strings.TrimSpace(out)
	}
	// onEveryPod expects the output of a read command on each Pod, the replica
	// too; writes go to the primary, ordinal 0.
	onEveryPod := func(user, password string, matcher OmegaMatcher, args ...string) {
		for _, pod := range pods {
			Eventually(func() string { return as(pod, user, password, args...) }).Should(matcher, "on %s", pod)
		}
	}
	setSecret := func(name, data string) {
		_, err := kubectl("patch", "secret", name, "-n", aclNamespace, "--type=merge", "-p", `{"data":null,"stringData":`+data+`}`)
		Expect(err).NotTo(HaveOccurred())
	}
	var appPassword string

	BeforeAll(func() {
		Expect(loadImageIntoKind(redisImage)).To(Succeed())
		Expect(ensureRestrictedNamespace(aclNamespace)).To(Succeed())
		Expect(kubectlApply(aclManifest)).To(Succeed())
		for _, user := range []string{"app", "vault"} {
			Eventually(func() string { return condition("redisuser", user, "Ready") }).Should(Equal("True Applied"), user)
		}
		Eventually(func() string { return condition("redisinstance", aclName, "ACLReady") }).Should(Equal("True Applied"))
		appPassword = secretValueIn(aclNamespace, "app-redis-user", "password")
	})

	AfterAll(func() {
		_, _ = kubectl("delete", "ns", aclNamespace, "--ignore-not-found", "--timeout=2m")
	})

	It("lets each user in with its password and only to its keys", func() {
		Expect(as(pods[0], "app", appPassword, "SET", "app:k", "v")).To(Equal("OK"))
		onEveryPod("app", appPassword, Equal("1"), "EXISTS", "app:k")
		onEveryPod("vault", "v1", Equal("0"), "EXISTS", "vault:k")
		onEveryPod("legacy", "legacy-pw", Equal("0"), "EXISTS", "legacy:k")
		Expect(as(pods[0], "app", appPassword, "EXISTS", "legacy:k")).To(ContainSubstring("NOPERM"))
		Expect(as(pods[0], "app", "wrong", "PING")).To(ContainSubstring("WRONGPASS"))
		acl, err := kubectl("get", "secret", aclName+"-acl", "-n", aclNamespace, "-o", "jsonpath={.data}")
		Expect(err).NotTo(HaveOccurred())
		Expect(acl).NotTo(ContainSubstring("legacy-pw"), "the plaintext password of the file reached Redis")
	})

	It("accepts both passwords during a rotation and only the new one after it", func() {
		setSecret("vault-pw", `{"password":"v2","previousPassword":"v1"}`)
		onEveryPod("vault", "v2", Equal("PONG"), "PING")
		onEveryPod("vault", "v1", Equal("PONG"), "PING")

		setSecret("vault-pw", `{"password":"v2"}`)
		onEveryPod("vault", "v1", ContainSubstring("WRONGPASS"), "PING")
		onEveryPod("vault", "v2", Equal("PONG"), "PING")
	})

	It("keeps the previous rules of a user when Redis refuses the new ones", func() {
		_, err := kubectl("patch", "redisuser", "app", "-n", aclNamespace, "--type=merge",
			"-p", `{"spec":{"acl":{"rules":["~app:*","+nosuchcommand"]}}}`)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() string { return condition("redisuser", "app", "Ready") }).Should(Equal("False Rejected"))
		Eventually(func() string { return condition("redisinstance", aclName, "ACLReady") }).Should(Equal("False Rejected"))
		Eventually(func() string {
			out, _ := kubectl("get", "events", "-n", aclNamespace, "--field-selector", "reason=RulesRejected",
				"-o", "jsonpath={.items[*].involvedObject.name}")
			return out
		}).Should(ContainSubstring("app"))
		Expect(as(pods[0], "app", appPassword, "SET", "app:k", "v")).To(Equal("OK"), "the previous rules are gone")

		_, err = kubectl("patch", "redisuser", "app", "-n", aclNamespace, "--type=merge",
			"-p", `{"spec":{"acl":{"rules":["~app:*","+@read"]}}}`)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() string { return condition("redisuser", "app", "Ready") }).Should(Equal("True Applied"))
		Eventually(func() string { return as(pods[0], "app", appPassword, "SET", "app:k", "v") }).Should(ContainSubstring("NOPERM"))
		onEveryPod("app", appPassword, Equal("1"), "EXISTS", "app:k")
	})

	It("refuses a bad ACL file with the line and keeps the last good one", func() {
		setSecret("legacy-acl", `{"users.acl":"user legacy on >other-pw ~legacy:* +@all\nuser default on nopass\n"}`)
		Eventually(func() string { return condition("redisinstance", aclName, "ACLReady") }).Should(Equal("False InvalidExternalACL"))
		message := get("redisinstance", aclName, `{.status.conditions[?(@.type=="ACLReady")].message}`)
		Expect(message).To(ContainSubstring("line 2"))
		Expect(message).NotTo(ContainSubstring("other-pw"), "the message quotes a password")
		Consistently(func() string { return as(pods[0], "legacy", "legacy-pw", "PING") }, 10*time.Second).Should(Equal("PONG"))
	})

	It("removes the user of a deleted RedisUser from every instance", func() {
		_, err := kubectl("delete", "redisuser", "vault", "-n", aclNamespace, "--timeout=1m")
		Expect(err).NotTo(HaveOccurred())
		onEveryPod("vault", "v2", ContainSubstring("WRONGPASS"), "PING")
	})
})

// secretValueIn returns the decoded value of a key of a Secret.
func secretValueIn(namespace, secret, key string) string {
	var value string
	Eventually(func(g Gomega) {
		out, err := kubectl("get", "secret", secret, "-n", namespace, "-o", "go-template={{index .data \""+key+"\" | base64decode}}")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(out).NotTo(BeEmpty())
		value = out
	}).Should(Succeed())
	return value
}
