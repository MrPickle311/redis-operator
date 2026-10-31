package controller

import (
	"context"
	"crypto/sha256"
	"fmt"
	"maps"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/instancemanager"
)

// The users of RedisUsers reach Redis through the Secret <instance>-acl: the
// operator writes the whole set of users of an instance, with hashes of the
// passwords, and every Instance Manager reads it from the API and applies it
// (see aclSync). Computing the whole set on every change, never one user,
// leaves nothing to race when users change at the same time.

// generatedPasswordKey and generatedUsernameKey are the keys of the Secret
// the operator generates for a RedisUser with authentication.generate.
const (
	generatedPasswordKey = "password"
	generatedUsernameKey = "username"
)

// redisUserName returns the name a RedisUser logs in with.
func redisUserName(user *redisv1.RedisUser) string {
	if user.Spec.Username != "" {
		return user.Spec.Username
	}
	return user.Name
}

// generatedPasswordSecretName returns the Secret with the generated password.
func generatedPasswordSecretName(user *redisv1.RedisUser) string {
	return user.Name + "-redis-user"
}

// targets reports whether the RedisUser belongs to the instance.
func targets(user *redisv1.RedisUser, instance *redisv1.RedisInstance) bool {
	return user.Namespace == instance.Namespace && user.Spec.TargetRef.Name == instance.Name &&
		(user.Spec.TargetRef.Kind == "" || user.Spec.TargetRef.Kind == "RedisInstance")
}

// usersOf returns the RedisUsers of the instance that are not being deleted.
func usersOf(ctx context.Context, c client.Reader, instance *redisv1.RedisInstance) ([]redisv1.RedisUser, error) {
	var list redisv1.RedisUserList
	if err := c.List(ctx, &list, client.InNamespace(instance.Namespace)); err != nil {
		return nil, err
	}
	var users []redisv1.RedisUser
	for _, user := range list.Items {
		if targets(&user, instance) && user.DeletionTimestamp.IsZero() {
			users = append(users, user)
		}
	}
	return users, nil
}

// previousPasswordKey is the key of the Secret of passwordSecretRef that
// keeps the old password valid during a rotation, until the user removes it.
const previousPasswordKey = "previousPassword"

// userPasswords returns the passwords Redis accepts for the user: the current
// one, then the previous one during a rotation. None when the Secret or the
// key of the current one does not exist yet.
func userPasswords(ctx context.Context, c client.Reader, user *redisv1.RedisUser) ([]string, error) {
	name, key := generatedPasswordSecretName(user), generatedPasswordKey
	if ref := user.Spec.Authentication.PasswordSecretRef; ref != nil {
		name, key = ref.Name, ref.Key
	}
	var secret corev1.Secret
	err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: user.Namespace}, &secret)
	if apierrors.IsNotFound(err) || err != nil || len(secret.Data[key]) == 0 {
		return nil, client.IgnoreNotFound(err)
	}
	passwords := []string{string(secret.Data[key])}
	if previous := string(secret.Data[previousPasswordKey]); user.Spec.Authentication.PasswordSecretRef != nil &&
		previous != "" && previous != passwords[0] {
		passwords = append(passwords, previous)
	}
	return passwords, nil
}

// aclLine returns the line of a user in the format of a Redis ACL file. Redis
// gets only the SHA-256 of each password.
func aclLine(username string, passwords []string, rules []redisv1.ACLRule) string {
	parts := []string{"user", username, "on"}
	for _, password := range passwords {
		parts = append(parts, fmt.Sprintf("#%x", sha256.Sum256([]byte(password))))
	}
	for _, rule := range rules {
		parts = append(parts, string(rule))
	}
	return strings.Join(parts, " ")
}

// redisUserLines returns the lines of the RedisUsers of the instance, by
// username. A username two RedisUsers claim is left out: guessing which one
// is right could hand one application the data of another. A user without
// its password yet is left out too.
func redisUserLines(ctx context.Context, c client.Reader, instance *redisv1.RedisInstance) (map[string]string, error) {
	users, err := usersOf(ctx, c, instance)
	if err != nil {
		return nil, err
	}
	byName := map[string][]redisv1.RedisUser{}
	for _, user := range users {
		byName[redisUserName(&user)] = append(byName[redisUserName(&user)], user)
	}
	lines := map[string]string{}
	for name, claimants := range byName {
		if len(claimants) > 1 {
			continue // a conflict, reported by each RedisUser
		}
		passwords, err := userPasswords(ctx, c, &claimants[0])
		if err != nil {
			return nil, err
		}
		if len(passwords) > 0 {
			lines[name] = aclLine(name, passwords, claimants[0].Spec.ACL.Rules)
		}
	}
	return lines, nil
}

// externalACLKey is the key of <instance>-acl with the users of the last good
// file of spec.acl.externalSecretRef; the Instance Managers ignore it.
const externalACLKey = "external.acl"

// externalACL returns the users of spec.acl.externalSecretRef as lines, by
// name, or why the file can not be used.
func externalACL(ctx context.Context, c client.Reader, instance *redisv1.RedisInstance) (map[string]string, string, error) {
	ref := instance.Spec.ACL.ExternalSecretRef
	if ref == nil {
		return nil, "", nil
	}
	var secret corev1.Secret
	err := c.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: instance.Namespace}, &secret)
	if apierrors.IsNotFound(err) {
		return nil, fmt.Sprintf("Secret %s does not exist", ref.Name), nil
	}
	if err != nil {
		return nil, "", err
	}
	content, ok := secret.Data[ref.Key]
	if !ok {
		return nil, fmt.Sprintf("Secret %s has no key %s", ref.Name, ref.Key), nil
	}
	users, err := parseExternalACL(string(content))
	if err != nil {
		return nil, fmt.Sprintf("Secret %s, key %s: %v", ref.Name, ref.Key, err), nil
	}
	return users, "", nil
}

// parseExternalACL checks a Redis ACL file and returns its users as lines,
// by name, with every password hashed. One bad line refuses the whole file:
// half of the users of a file is a state nobody wrote. Errors never quote a
// line, which may hold a password.
func parseExternalACL(content string) (map[string]string, error) {
	users := map[string]string{}
	number := 0
	for line := range strings.Lines(content) {
		number++
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if fields[0] != "user" || len(fields) < 2 {
			return nil, fmt.Errorf(`line %d: not a line "user <name> <rules>"`, number)
		}
		name, rules := fields[1], fields[2:]
		if slices.Contains(instancemanager.ReservedUsers, name) {
			return nil, fmt.Errorf("line %d: user %s is reserved", number, name)
		}
		if _, twice := users[name]; twice {
			return nil, fmt.Errorf("line %d: user %s is defined twice", number, name)
		}
		for i, rule := range rules {
			switch {
			case strings.EqualFold(rule, "nopass"):
				return nil, fmt.Errorf("line %d: nopass would let user %s in without a password", number, name)
			case strings.HasPrefix(rule, ">"):
				rules[i] = fmt.Sprintf("#%x", sha256.Sum256([]byte(rule[1:])))
			case strings.HasPrefix(rule, "<"):
				rules[i] = fmt.Sprintf("!%x", sha256.Sum256([]byte(rule[1:])))
			}
		}
		users[name] = strings.Join(append([]string{"user", name}, rules...), " ")
	}
	return users, nil
}

// mergeACL returns the content of <instance>-acl. A username both sources
// define is left out, like one two RedisUsers claim.
func mergeACL(redisUsers, external map[string]string) []byte {
	lines := maps.Clone(external)
	if lines == nil {
		lines = map[string]string{}
	}
	for name, line := range redisUsers {
		if _, conflict := lines[name]; conflict {
			delete(lines, name)
			continue
		}
		lines[name] = line
	}
	if len(lines) == 0 {
		return nil
	}
	var content strings.Builder
	for _, name := range slices.Sorted(maps.Keys(lines)) {
		content.WriteString(lines[name] + "\n")
	}
	return []byte(content.String())
}

// reconcileACLSecret writes the users of the instance to <instance>-acl and
// records its digest; whether the instances applied it comes later, see
// reconcileACLApplied.
func (r *RedisInstanceReconciler) reconcileACLSecret(ctx context.Context, instance *redisv1.RedisInstance) error {
	redisUsers, err := redisUserLines(ctx, r.Client, instance)
	if err != nil {
		return err
	}
	external, problem, err := externalACL(ctx, r.Client, instance)
	if err != nil {
		return err
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: instancemanager.ACLSecretName(instance.Name), Namespace: instance.Namespace}}
	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		lastGood := mergeACL(nil, external)
		if problem != "" {
			// Written by the operator from a good file, so it parses.
			lastGood = secret.Data[externalACLKey]
			external, _ = parseExternalACL(string(lastGood))
		}
		secret.Data = map[string][]byte{
			instancemanager.ACLSecretKey: mergeACL(redisUsers, external),
			externalACLKey:               lastGood,
		}
		return ctrl.SetControllerReference(instance, secret, r.Scheme)
	})
	if err != nil {
		return err
	}
	if result != controllerutil.OperationResultNone {
		logf.FromContext(ctx).Info("Reconciled the users of Redis", "name", secret.Name, "operation", result)
	}
	digest := instancemanager.ACLDigest(secret.Data[instancemanager.ACLSecretKey])
	if instance.Status.ACL == nil || instance.Status.ACL.Digest != digest {
		instance.Status.ACL = &redisv1.ACLStatus{Digest: digest}
	}
	instance.Status.ACL.ExternalError = problem
	// Now, not only in reconcileACLApplied: a bad file is reported even while
	// no Pod runs.
	r.setACLCondition(instance)
	return nil
}

// reconcileACLApplied records whether every running instance has applied
// <instance>-acl, and the users Redis refused. It returns true while not.
func (r *RedisInstanceReconciler) reconcileACLApplied(ctx context.Context, instance *redisv1.RedisInstance) bool {
	if instance.Status.ACL == nil {
		return false
	}
	applied, rejected := true, map[string]string{}
	for ordinal := range instance.Spec.Instances {
		var pod corev1.Pod
		if err := r.Get(ctx, types.NamespacedName{Name: instancePodName(instance, ordinal), Namespace: instance.Namespace}, &pod); err != nil ||
			pod.Status.Phase != corev1.PodRunning || !pod.DeletionTimestamp.IsZero() {
			continue
		}
		callCtx, cancel := context.WithTimeout(ctx, failoverCallTimeout)
		status, err := r.InstanceManager.Status(callCtx, podFQDN(instance, ordinal))
		cancel()
		if err != nil || status.ACLDigest != instance.Status.ACL.Digest {
			applied = false
			continue
		}
		maps.Copy(rejected, status.ACLRejected)
	}
	instance.Status.ACL.Applied = applied
	instance.Status.ACL.Rejected = nil
	if len(rejected) > 0 {
		instance.Status.ACL.Rejected = rejected
	}
	r.setACLCondition(instance)
	return !applied
}

// setACLCondition sums up status.acl in the condition ACLReady, and says once
// in an Event what people must fix.
func (r *RedisInstanceReconciler) setACLCondition(instance *redisv1.RedisInstance) {
	acl := instance.Status.ACL
	condition := metav1.Condition{Type: redisv1.ConditionACLReady, Status: metav1.ConditionFalse, ObservedGeneration: instance.Generation}
	switch {
	case acl.ExternalError != "":
		condition.Reason = "InvalidExternalACL"
		condition.Message = "The instances keep the users of the last good file. " + acl.ExternalError
	case len(acl.Rejected) > 0:
		var refusals []string
		for _, name := range slices.Sorted(maps.Keys(acl.Rejected)) {
			refusals = append(refusals, fmt.Sprintf("%s: %s", name, acl.Rejected[name]))
		}
		condition.Reason = "Rejected"
		condition.Message = "Redis refused the rules of " + strings.Join(refusals, "; ")
	case !acl.Applied:
		condition.Reason = "Pending"
		condition.Message = "Waiting for the running instances to apply the users"
	default:
		condition.Status, condition.Reason, condition.Message = metav1.ConditionTrue, "Applied", "Every running instance has the users"
	}
	previous := meta.FindStatusCondition(instance.Status.Conditions, redisv1.ConditionACLReady)
	if condition.Reason == "InvalidExternalACL" && (previous == nil || previous.Message != condition.Message) {
		r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, "InvalidExternalACL", "Configure", condition.Message)
	}
	meta.SetStatusCondition(&instance.Status.Conditions, condition)
}

// instanceOfUser maps an event of a RedisUser to its RedisInstance.
func instanceOfUser(_ context.Context, obj client.Object) []reconcile.Request {
	user, ok := obj.(*redisv1.RedisUser)
	if !ok {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: user.Namespace, Name: user.Spec.TargetRef.Name}}}
}

// usesPasswordSecret reports whether the password of the user is in the Secret.
func usesPasswordSecret(user *redisv1.RedisUser, secret string) bool {
	if ref := user.Spec.Authentication.PasswordSecretRef; ref != nil {
		return ref.Name == secret
	}
	return generatedPasswordSecretName(user) == secret
}
