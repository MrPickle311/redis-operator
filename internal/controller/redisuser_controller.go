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

package controller

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/instancemanager"
)

// userFinalizer keeps a RedisUser until Redis no longer has its user, so a
// deleted RedisUser never leaves a login behind.
const userFinalizer = "redis.operator.com/user"

// userPollInterval is how often a RedisUser that waits for the instances
// checks again; the Instance Managers apply a change within seconds.
const userPollInterval = 2 * time.Second

// RedisUserReconciler reconciles a RedisUser: the Secret with a generated
// password, the finalizer, and the status. The users themselves reach Redis
// through the RedisInstance, see reconcileACLSecret.
type RedisUserReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// Recorder emits the Events of RedisUsers.
	Recorder events.EventRecorder
}

// +kubebuilder:rbac:groups=redis.operator.com,resources=redisusers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=redis.operator.com,resources=redisusers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=redis.operator.com,resources=redisusers/finalizers,verbs=update
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

func (r *RedisUserReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var user redisv1.RedisUser
	if err := r.Get(ctx, req.NamespacedName, &user); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !user.DeletionTimestamp.IsZero() {
		gone, err := r.userGone(ctx, &user)
		if err != nil || !gone {
			return ctrl.Result{RequeueAfter: userPollInterval}, err
		}
		controllerutil.RemoveFinalizer(&user, userFinalizer)
		return ctrl.Result{}, r.Update(ctx, &user)
	}
	if controllerutil.AddFinalizer(&user, userFinalizer) {
		if err := r.Update(ctx, &user); err != nil {
			return ctrl.Result{}, err
		}
	}
	if user.Spec.Authentication.Generate {
		if err := r.ensureGeneratedPassword(ctx, &user); err != nil {
			return ctrl.Result{}, err
		}
	}

	old := user.Status.DeepCopy()
	user.Status.Username = redisUserName(&user)
	condition, err := r.readyCondition(ctx, &user)
	if err != nil {
		return ctrl.Result{}, err
	}
	// Redis keeps the previous rules of a user it refused, or has no such
	// user at all, so say it once where people look, not only in the status.
	if previous := meta.FindStatusCondition(user.Status.Conditions, "Ready"); condition.Reason == "Rejected" &&
		(previous == nil || previous.Reason != "Rejected" || previous.Message != condition.Message) {
		r.Recorder.Eventf(&user, nil, corev1.EventTypeWarning, "RulesRejected", "Configure", condition.Message)
	}
	meta.SetStatusCondition(&user.Status.Conditions, condition)
	if !equalStatus(old, &user.Status) {
		if err := r.Status().Update(ctx, &user); err != nil {
			return ctrl.Result{}, err
		}
	}
	if condition.Status != metav1.ConditionTrue {
		return ctrl.Result{RequeueAfter: userPollInterval}, nil
	}
	return ctrl.Result{}, nil
}

// ensureGeneratedPassword creates the Secret <name>-redis-user once, with the
// username and a random password; the password never changes on its own.
func (r *RedisUserReconciler) ensureGeneratedPassword(ctx context.Context, user *redisv1.RedisUser) error {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: generatedPasswordSecretName(user), Namespace: user.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}
		if len(secret.Data[generatedPasswordKey]) == 0 {
			secret.Data[generatedPasswordKey] = []byte(rand.Text())
		}
		secret.Data[generatedUsernameKey] = []byte(redisUserName(user))
		return ctrl.SetControllerReference(user, secret, r.Scheme)
	})
	return err
}

// readyCondition tells whether every running instance has the user as
// specified, or why not.
func (r *RedisUserReconciler) readyCondition(ctx context.Context, user *redisv1.RedisUser) (metav1.Condition, error) {
	condition := func(status metav1.ConditionStatus, reason, message string) metav1.Condition {
		return metav1.Condition{Type: "Ready", Status: status, Reason: reason, Message: message, ObservedGeneration: user.Generation}
	}
	username := redisUserName(user)

	var instance redisv1.RedisInstance
	err := r.Get(ctx, types.NamespacedName{Name: user.Spec.TargetRef.Name, Namespace: user.Namespace}, &instance)
	if apierrors.IsNotFound(err) {
		return condition(metav1.ConditionFalse, "TargetNotFound", "RedisInstance "+user.Spec.TargetRef.Name+" does not exist"), nil
	}
	if err != nil {
		return metav1.Condition{}, err
	}
	others, err := usersOf(ctx, r.Client, &instance)
	if err != nil {
		return metav1.Condition{}, err
	}
	content, err := r.aclSecretData(ctx, &instance)
	if err != nil {
		return metav1.Condition{}, err
	}
	if external, _ := parseExternalACL(string(content[externalACLKey])); external[username] != "" {
		return condition(metav1.ConditionFalse, "Conflict",
			fmt.Sprintf("spec.acl.externalSecretRef of %s also defines the username %s; neither is applied", instance.Name, username)), nil
	}
	if claimants := otherUsersClaimingUsername(user, others); len(claimants) > 0 {
		return condition(metav1.ConditionFalse, "Conflict",
			fmt.Sprintf("RedisUser %s also claims the username %s; neither is applied", strings.Join(claimants, ", "), username)), nil
	}
	passwords, err := userPasswords(ctx, r.Client, user)
	if err != nil {
		return metav1.Condition{}, err
	}
	if len(passwords) == 0 {
		return condition(metav1.ConditionFalse, "PasswordMissing", "The Secret with the password does not exist yet, or has no such key"), nil
	}
	if reason, rejected := aclRejection(&instance, username); rejected {
		return condition(metav1.ConditionFalse, "Rejected",
			"Redis refused the rules, so it keeps the previous ones of the user, if any: "+reason), nil
	}
	applied, err := r.appliedWithLine(ctx, &instance, aclLine(username, passwords, user.Spec.ACL.Rules))
	if err != nil {
		return metav1.Condition{}, err
	}
	if !applied {
		return condition(metav1.ConditionFalse, "Pending", "Waiting for the instances of "+instance.Name+" to apply the user"), nil
	}
	return condition(metav1.ConditionTrue, "Applied", "Every running instance of "+instance.Name+" has the user"), nil
}

// otherUsersClaimingUsername returns the names of the RedisUsers, other than
// user, that log in with the same username; the operator applies none of them.
func otherUsersClaimingUsername(user *redisv1.RedisUser, others []redisv1.RedisUser) []string {
	var claimants []string
	for _, other := range others {
		if other.Name != user.Name && redisUserName(&other) == redisUserName(user) {
			claimants = append(claimants, other.Name)
		}
	}
	return claimants
}

// aclRejection returns the error Redis gave for the user, if it refused it.
func aclRejection(instance *redisv1.RedisInstance, username string) (string, bool) {
	if instance.Status.ACL == nil {
		return "", false
	}
	reason, rejected := instance.Status.ACL.Rejected[username]
	return reason, rejected
}

// appliedWithLine reports whether <instance>-acl holds the line, and every
// running instance has applied that content.
func (r *RedisUserReconciler) appliedWithLine(ctx context.Context, instance *redisv1.RedisInstance, line string) (bool, error) {
	content, err := r.aclContent(ctx, instance)
	if err != nil {
		return false, err
	}
	return bytes.Contains(content, []byte(line+"\n")) && aclApplied(instance, content), nil
}

// userGone reports whether no running instance has the user any more, or the
// instance itself is gone.
func (r *RedisUserReconciler) userGone(ctx context.Context, user *redisv1.RedisUser) (bool, error) {
	var instance redisv1.RedisInstance
	err := r.Get(ctx, types.NamespacedName{Name: user.Spec.TargetRef.Name, Namespace: user.Namespace}, &instance)
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	content, err := r.aclContent(ctx, &instance)
	if err != nil {
		return false, err
	}
	stillThere := bytes.Contains(content, []byte("user "+redisUserName(user)+" "))
	return !stillThere && aclApplied(&instance, content), nil
}

// aclContent returns the users <instance>-acl gives Redis, none when it does
// not exist.
func (r *RedisUserReconciler) aclContent(ctx context.Context, instance *redisv1.RedisInstance) ([]byte, error) {
	data, err := r.aclSecretData(ctx, instance)
	return data[instancemanager.ACLSecretKey], err
}

// aclSecretData returns the data of <instance>-acl, none when it does not exist.
func (r *RedisUserReconciler) aclSecretData(ctx context.Context, instance *redisv1.RedisInstance) (map[string][]byte, error) {
	var secret corev1.Secret
	err := r.Get(ctx, types.NamespacedName{Name: instancemanager.ACLSecretName(instance.Name), Namespace: instance.Namespace}, &secret)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	return secret.Data, err
}

// aclApplied reports whether every running instance has applied content.
func aclApplied(instance *redisv1.RedisInstance, content []byte) bool {
	return instance.Status.ACL != nil && instance.Status.ACL.Applied &&
		instance.Status.ACL.Digest == instancemanager.ACLDigest(content)
}

func equalStatus(a, b *redisv1.RedisUserStatus) bool {
	if a.Username != b.Username || len(a.Conditions) != len(b.Conditions) {
		return false
	}
	for i := range a.Conditions {
		x, y := a.Conditions[i], b.Conditions[i]
		if x.Type != y.Type || x.Status != y.Status || x.Reason != y.Reason || x.Message != y.Message || x.ObservedGeneration != y.ObservedGeneration {
			return false
		}
	}
	return true
}

// usersOfInstance maps an event of a RedisInstance, e.g. a change of what
// its instances applied, to its RedisUsers.
func (r *RedisUserReconciler) usersOfInstance(ctx context.Context, obj client.Object) []reconcile.Request {
	instance, ok := obj.(*redisv1.RedisInstance)
	if !ok {
		return nil
	}
	var list redisv1.RedisUserList
	if err := r.List(ctx, &list, client.InNamespace(instance.Namespace)); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list RedisUsers for a RedisInstance event", "instance", instance.Name)
		return nil
	}
	var requests []reconcile.Request
	for _, user := range list.Items {
		if targets(&user, instance) {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&user)})
		}
	}
	return requests
}

// SetupWithManager sets up the controller with the Manager.
func (r *RedisUserReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&redisv1.RedisUser{}).
		Owns(&corev1.Secret{}).
		Watches(&redisv1.RedisInstance{}, handler.EnqueueRequestsFromMapFunc(r.usersOfInstance)).
		Named("redisuser").
		Complete(r)
}
