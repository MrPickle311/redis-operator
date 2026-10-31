package controller

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/instancemanager"
)

func hashed(password string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(password)))
}

func TestParseExternalACL(t *testing.T) {
	users, err := parseExternalACL("user reporting on >s3cret ~* +@read\n\n  user batch on #" + hashed("x") + " <old ~jobs:*\n")
	if err != nil {
		t.Fatal(err)
	}
	if want := "user reporting on #" + hashed("s3cret") + " ~* +@read"; users["reporting"] != want {
		t.Errorf("reporting = %q, want %q: a plaintext password becomes its hash", users["reporting"], want)
	}
	if want := "user batch on #" + hashed("x") + " !" + hashed("old") + " ~jobs:*"; users["batch"] != want {
		t.Errorf("batch = %q, want %q", users["batch"], want)
	}

	for _, tt := range []struct{ content, want string }{
		{"user ok on #ab\nrequirepass s3cret\n", `line 2: not a line "user <name> <rules>"`},
		{"user\n", "line 1: not a line"},
		{"user _operator on >s3cret +@all\n", "line 1: user _operator is reserved"},
		{"user default on nopass\n", "line 1: user default is reserved"},
		{"user a on >s3cret\nuser a off\n", "line 2: user a is defined twice"},
		{"user a on NOPASS ~*\n", "line 1: nopass would let user a in"},
	} {
		_, err := parseExternalACL(tt.content)
		if err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("parseExternalACL(%q) error = %v, want %q", tt.content, err, tt.want)
		}
		if err != nil && strings.Contains(err.Error(), "s3cret") {
			t.Errorf("the error %q quotes a password", err)
		}
	}
}

func TestMergeACLLeavesOutAConflict(t *testing.T) {
	content := string(mergeACL(
		map[string]string{"app": "user app on #1", "shared": "user shared on #2"},
		map[string]string{"shared": "user shared on #3", "batch": "user batch on #4"}))
	if want := "user app on #1\nuser batch on #4\n"; content != want {
		t.Errorf("mergeACL() = %q, want %q", content, want)
	}
	if mergeACL(nil, nil) != nil {
		t.Error("mergeACL() of no users is not empty")
	}
}

var _ = Describe("External ACL", Ordered, func() {
	const instanceName = "ext-acl-db"
	recorder := events.NewFakeRecorder(10)

	setFile := func(content string) {
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "vault-acl", Namespace: testNamespace}}
		err := k8sClient.Get(ctx, objectKey(secret.Name), secret)
		secret.Data = map[string][]byte{"users.acl": []byte(content)}
		if err != nil {
			Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		} else {
			Expect(k8sClient.Update(ctx, secret)).To(Succeed())
		}
	}
	acl := func() string {
		secret := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, objectKey(instancemanager.ACLSecretName(instanceName)), secret)).To(Succeed())
		return string(secret.Data[instancemanager.ACLSecretKey])
	}
	aclReady := func() *metav1.Condition {
		return meta.FindStatusCondition(getInstance(instanceName).Status.Conditions, redisv1.ConditionACLReady)
	}

	BeforeAll(func() {
		instance := newRedisInstance(instanceName, 1, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()})
		instance.Spec.ACL.ExternalSecretRef = &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "vault-acl"}, Key: "users.acl"}
		Expect(k8sClient.Create(ctx, instance)).To(Succeed())
		Expect(reconcileWith(recorder, objectKey(instanceName))).To(Succeed()) // creates the Pod and returns
	})
	AfterAll(func() { deleteInstance(instanceName) })

	It("says the Secret is missing, and gives Redis no users from it", func() {
		Expect(reconcileWith(recorder, objectKey(instanceName))).To(Succeed())
		Expect(acl()).To(BeEmpty())
		Expect(aclReady()).To(HaveField("Reason", "InvalidExternalACL"))
		Expect(aclReady().Message).To(ContainSubstring("Secret vault-acl does not exist"))
		Expect(recorder.Events).To(Receive(ContainSubstring("InvalidExternalACL")))
	})

	It("gives Redis the users of the file, with hashed passwords only", func() {
		setFile("user reporting on >s3cret ~* +@read\n")
		Expect(reconcileWith(recorder, objectKey(instanceName))).To(Succeed())
		Expect(acl()).To(Equal("user reporting on #" + hashed("s3cret") + " ~* +@read\n"))
		Expect(aclReady()).To(HaveField("Reason", "Pending"), "the file is good; envtest runs no Instance Manager to apply it")
	})

	It("refuses a whole file with one bad line, keeps the last good one, and tells where", func() {
		setFile("user reporting on >s3cret ~* +@read\nuser batch on >other ~jobs:*\nuser _replication on >x +@all\n")
		Expect(reconcileWith(recorder, objectKey(instanceName))).To(Succeed())
		Expect(acl()).To(Equal("user reporting on #"+hashed("s3cret")+" ~* +@read\n"), "batch applied from a refused file")
		Expect(aclReady()).To(HaveField("Reason", "InvalidExternalACL"))
		Expect(aclReady().Message).To(ContainSubstring("line 3: user _replication is reserved"))
		Expect(recorder.Events).To(Receive(SatisfyAll(ContainSubstring("Warning"), ContainSubstring("line 3"))))

		Expect(reconcileWith(recorder, objectKey(instanceName))).To(Succeed())
		Expect(recorder.Events).NotTo(Receive(), "a second Event for the same file")
	})

	It("applies neither a RedisUser nor a line of the file that share a username", func() {
		setFile("user reporting on >s3cret ~* +@read\nuser shared on >a +ping\n")
		user := &redisv1.RedisUser{
			ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: testNamespace},
			Spec: redisv1.RedisUserSpec{
				TargetRef:      redisv1.TargetRef{Name: instanceName},
				Authentication: redisv1.RedisUserAuthentication{Generate: true},
				ACL:            redisv1.RedisUserACL{Rules: []redisv1.ACLRule{"+ping"}},
			},
		}
		Expect(k8sClient.Create(ctx, user)).To(Succeed())
		r := &RedisUserReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: recorder}
		_, err := r.Reconcile(ctx, requestFor("shared"))
		Expect(err).NotTo(HaveOccurred())
		Expect(reconcileWith(recorder, objectKey(instanceName))).To(Succeed())
		Expect(acl()).NotTo(ContainSubstring("user shared "))
		Expect(acl()).To(ContainSubstring("user reporting "))

		_, err = r.Reconcile(ctx, requestFor("shared"))
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, objectKey("shared"), user)).To(Succeed())
		ready := meta.FindStatusCondition(user.Status.Conditions, "Ready")
		Expect(ready).To(HaveField("Reason", "Conflict"))
		Expect(ready.Message).To(ContainSubstring("externalSecretRef"))
	})
})

func requestFor(name string) reconcile.Request {
	return reconcile.Request{NamespacedName: objectKey(name)}
}
