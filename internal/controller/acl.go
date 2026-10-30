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

// userPassword returns the password of the user, or "" when its Secret or key
// does not exist yet.
func userPassword(ctx context.Context, c client.Reader, user *redisv1.RedisUser) (string, error) {
	name, key := generatedPasswordSecretName(user), generatedPasswordKey
	if ref := user.Spec.Authentication.PasswordSecretRef; ref != nil {
		name, key = ref.Name, ref.Key
	}
	var secret corev1.Secret
	err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: user.Namespace}, &secret)
	if apierrors.IsNotFound(err) {
		return "", nil
	}
	return string(secret.Data[key]), err
}

// aclLine returns the line of a user in the format of a Redis ACL file. Redis
// gets only the SHA-256 of the password.
func aclLine(username, password string, rules []redisv1.ACLRule) string {
	parts := []string{"user", username, "on", fmt.Sprintf("#%x", sha256.Sum256([]byte(password)))}
	for _, rule := range rules {
		parts = append(parts, string(rule))
	}
	return strings.Join(parts, " ")
}

// desiredACL returns the content of <instance>-acl. A username two RedisUsers
// claim is left out: guessing which one is right could hand one application
// the data of another. A user without its password yet is left out too.
func desiredACL(ctx context.Context, c client.Reader, instance *redisv1.RedisInstance) ([]byte, error) {
	users, err := usersOf(ctx, c, instance)
	if err != nil {
		return nil, err
	}
	byName := map[string][]redisv1.RedisUser{}
	for _, user := range users {
		byName[redisUserName(&user)] = append(byName[redisUserName(&user)], user)
	}
	var lines []string
	for _, name := range slices.Sorted(maps.Keys(byName)) {
		if len(byName[name]) > 1 {
			continue // a conflict, reported by each RedisUser
		}
		user := byName[name][0]
		password, err := userPassword(ctx, c, &user)
		if err != nil {
			return nil, err
		}
		if password != "" {
			lines = append(lines, aclLine(name, password, user.Spec.ACL.Rules))
		}
	}
	if len(lines) == 0 {
		return nil, nil
	}
	return []byte(strings.Join(lines, "\n") + "\n"), nil
}

// reconcileACLSecret writes the users of the instance to <instance>-acl and
// records its digest; whether the instances applied it comes later, see
// reconcileACLApplied.
func (r *RedisInstanceReconciler) reconcileACLSecret(ctx context.Context, instance *redisv1.RedisInstance) error {
	content, err := desiredACL(ctx, r.Client, instance)
	if err != nil {
		return err
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      instancemanager.ACLSecretName(instance.Name),
			Namespace: instance.Namespace,
		}}
	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		secret.Data = map[string][]byte{instancemanager.ACLSecretKey: content}
		return ctrl.SetControllerReference(instance, secret, r.Scheme)
	})
	if err != nil {
		return err
	}
	if result != controllerutil.OperationResultNone {
		logf.FromContext(ctx).Info("Reconciled the users of Redis", "name", secret.Name, "operation", result)
	}
	digest := instancemanager.ACLDigest(content)
	if instance.Status.ACL == nil || instance.Status.ACL.Digest != digest {
		instance.Status.ACL = &redisv1.ACLStatus{Digest: digest}
	}
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
	return !applied
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
