package controller

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/instancemanager"
)

func TestACLLine(t *testing.T) {
	line := aclLine("app", "secret", []redisv1.ACLRule{"~app:*", "+@read"})
	want := fmt.Sprintf("user app on #%x ~app:* +@read", sha256.Sum256([]byte("secret")))
	if line != want {
		t.Errorf("aclLine() = %q, want %q", line, want)
	}
	if strings.Contains(line, "secret ") {
		t.Error("the line holds the password, not its hash")
	}
}

var _ = Describe("RedisUser", Ordered, func() {
	const instanceName = "acl-db"

	newUser := func(name string, auth redisv1.RedisUserAuthentication) *redisv1.RedisUser {
		return &redisv1.RedisUser{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
			Spec: redisv1.RedisUserSpec{
				TargetRef:      redisv1.TargetRef{Name: instanceName},
				Authentication: auth,
				ACL:            redisv1.RedisUserACL{Rules: []redisv1.ACLRule{"~app:*", "+@read"}},
			},
		}
	}
	recorder := events.NewFakeRecorder(10)
	reconcileUser := func(name string) {
		r := &RedisUserReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: recorder}
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: objectKey(name)})
		Expect(err).NotTo(HaveOccurred())
	}
	getUser := func(name string) *redisv1.RedisUser {
		user := &redisv1.RedisUser{}
		Expect(k8sClient.Get(ctx, objectKey(name), user)).To(Succeed())
		return user
	}
	readyReason := func(name string) string {
		c := meta.FindStatusCondition(getUser(name).Status.Conditions, "Ready")
		Expect(c).NotTo(BeNil())
		return c.Reason
	}
	acl := func() string {
		secret := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, objectKey(instancemanager.ACLSecretName(instanceName)), secret)).To(Succeed())
		return string(secret.Data[instancemanager.ACLSecretKey])
	}
	// markApplied records what the Instance Managers would report: every
	// instance has the current content of <instance>-acl.
	markApplied := func() {
		instance := getInstance(instanceName)
		instance.Status.ACL = &redisv1.ACLStatus{Digest: instancemanager.ACLDigest([]byte(acl())), Applied: true}
		Expect(k8sClient.Status().Update(ctx, instance)).To(Succeed())
	}

	BeforeAll(func() {
		Expect(k8sClient.Create(ctx, newRedisInstance(instanceName, 1, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()}))).To(Succeed())
		Expect(reconcileOnce(objectKey(instanceName))).To(Succeed())
	})
	AfterAll(func() { deleteInstance(instanceName) })

	It("generates a password, and gives Redis only its hash", func() {
		Expect(k8sClient.Create(ctx, newUser("app", redisv1.RedisUserAuthentication{Generate: true}))).To(Succeed())
		reconcileUser("app")
		secret := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, objectKey("app-redis-user"), secret)).To(Succeed())
		Expect(secret.Data["username"]).To(Equal([]byte("app")))
		password := string(secret.Data["password"])
		Expect(password).NotTo(BeEmpty())
		Expect(getUser("app").Finalizers).To(ContainElement(userFinalizer))

		Expect(reconcileOnce(objectKey(instanceName))).To(Succeed())
		Expect(acl()).To(Equal(aclLine("app", password, []redisv1.ACLRule{"~app:*", "+@read"}) + "\n"))
		Expect(acl()).NotTo(ContainSubstring(password))

		reconcileUser("app")
		Expect(readyReason("app")).To(Equal("Pending"), "Ready before any instance applied the user")
		markApplied()
		reconcileUser("app")
		Expect(readyReason("app")).To(Equal("Applied"))
		Expect(getUser("app").Status.Username).To(Equal("app"))
	})

	It("waits for the Secret of the user with the password", func() {
		user := newUser("vault-user", redisv1.RedisUserAuthentication{PasswordSecretRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "from-vault"}, Key: "password"}})
		Expect(k8sClient.Create(ctx, user)).To(Succeed())
		reconcileUser("vault-user")
		Expect(readyReason("vault-user")).To(Equal("PasswordMissing"))
		Expect(reconcileOnce(objectKey(instanceName))).To(Succeed())
		Expect(acl()).NotTo(ContainSubstring("user vault-user "))

		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "from-vault", Namespace: testNamespace},
			Data:       map[string][]byte{"password": []byte("synced")},
		})).To(Succeed())
		Expect(reconcileOnce(objectKey(instanceName))).To(Succeed())
		Expect(acl()).To(ContainSubstring(aclLine("vault-user", "synced", user.Spec.ACL.Rules)))
	})

	It("applies neither of two RedisUsers that claim one username", func() {
		for _, name := range []string{"claim-a", "claim-b"} {
			user := newUser(name, redisv1.RedisUserAuthentication{Generate: true})
			user.Spec.Username = "shared"
			Expect(k8sClient.Create(ctx, user)).To(Succeed())
			reconcileUser(name)
		}
		Expect(reconcileOnce(objectKey(instanceName))).To(Succeed())
		Expect(acl()).NotTo(ContainSubstring("user shared "))
		for _, name := range []string{"claim-a", "claim-b"} {
			reconcileUser(name)
			Expect(readyReason(name)).To(Equal("Conflict"))
		}
	})

	It("reports rules Redis refused, with one Warning Event", func() {
		user := newUser("typo", redisv1.RedisUserAuthentication{Generate: true})
		user.Spec.ACL.Rules = []redisv1.ACLRule{"+@nosuchcategory"}
		Expect(k8sClient.Create(ctx, user)).To(Succeed())
		reconcileUser("typo")
		Expect(reconcileOnce(objectKey(instanceName))).To(Succeed())
		// What the Instance Managers report for it, see syncACL.
		instance := getInstance(instanceName)
		instance.Status.ACL = &redisv1.ACLStatus{
			Digest: instancemanager.ACLDigest([]byte(acl())), Applied: true,
			Rejected: map[string]string{"typo": "ERR Error in ACL SETUSER modifier '+@nosuchcategory': Unknown command or category name in ACL"},
		}
		Expect(k8sClient.Status().Update(ctx, instance)).To(Succeed())

		reconcileUser("typo")
		c := meta.FindStatusCondition(getUser("typo").Status.Conditions, "Ready")
		Expect(c.Reason).To(Equal("Rejected"))
		Expect(c.Message).To(ContainSubstring("+@nosuchcategory"))
		Expect(recorder.Events).To(Receive(SatisfyAll(ContainSubstring("Warning"), ContainSubstring("RulesRejected"))))

		reconcileUser("typo")
		Expect(recorder.Events).NotTo(Receive(), "a second Event for the same refusal")
	})

	It("reports a RedisInstance that does not exist", func() {
		user := newUser("lost", redisv1.RedisUserAuthentication{Generate: true})
		user.Spec.TargetRef.Name = "no-such-db"
		Expect(k8sClient.Create(ctx, user)).To(Succeed())
		reconcileUser("lost")
		Expect(readyReason("lost")).To(Equal("TargetNotFound"))
	})

	It("keeps a deleted RedisUser until no instance has its user any more", func() {
		Expect(k8sClient.Delete(ctx, getUser("app"))).To(Succeed())
		Expect(reconcileOnce(objectKey(instanceName))).To(Succeed())
		Expect(acl()).NotTo(ContainSubstring("user app "))

		reconcileUser("app")
		Expect(getUser("app").Finalizers).To(ContainElement(userFinalizer), "released before the instances dropped the user")
		markApplied()
		reconcileUser("app")
		err := k8sClient.Get(ctx, objectKey("app"), &redisv1.RedisUser{})
		Expect(errors.IsNotFound(err)).To(BeTrue(), "the RedisUser stayed after the instances dropped its user")
	})

	DescribeTable("validating RedisUsers with CEL rules",
		func(mutate func(*redisv1.RedisUser), wantError string) {
			user := newUser("cel-user", redisv1.RedisUserAuthentication{Generate: true})
			mutate(user)
			err := k8sClient.Create(ctx, user)
			if wantError == "" {
				Expect(err).NotTo(HaveOccurred())
				user.Finalizers = nil
				Expect(k8sClient.Delete(ctx, user)).To(Succeed())
				return
			}
			Expect(errors.IsInvalid(err)).To(BeTrue(), "expected an Invalid error, got: %v", err)
			Expect(err.Error()).To(ContainSubstring(wantError))
		},
		Entry("accepts patterns, categories and commands", func(u *redisv1.RedisUser) {
			u.Spec.ACL.Rules = []redisv1.ACLRule{"~orders:*", "%R~cache:*", "&events", "+@read", "-@dangerous", "+get"}
		}, ""),
		Entry("rejects a password in the rules", func(u *redisv1.RedisUser) {
			u.Spec.ACL.Rules = []redisv1.ACLRule{">plaintext"}
		}, "spec.acl.rules[0]"),
		Entry("rejects nopass", func(u *redisv1.RedisUser) { u.Spec.ACL.Rules = []redisv1.ACLRule{"nopass"} }, "set by the operator"),
		Entry("rejects two rules in one item", func(u *redisv1.RedisUser) {
			u.Spec.ACL.Rules = []redisv1.ACLRule{"+@read nopass"}
		}, "spec.acl.rules[0]"),
		Entry("rejects the default user", func(u *redisv1.RedisUser) { u.Spec.Username = "default" }, "defaultUser"),
		Entry("rejects a reserved internal name", func(u *redisv1.RedisUser) { u.Spec.Username = "_operator" }, "spec.username"),
		Entry("rejects a user without a password source", func(u *redisv1.RedisUser) {
			u.Spec.Authentication = redisv1.RedisUserAuthentication{}
		}, "exactly one"),
	)
})
