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

package v1

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/controller"
)

// SetupRedisUserWebhookWithManager registers the webhook for RedisUser in the manager.
func SetupRedisUserWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &redisv1.RedisUser{}).
		WithValidator(&RedisUserValidator{client: mgr.GetClient()}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-redis-operator-com-v1-redisuser,mutating=false,failurePolicy=ignore,timeoutSeconds=5,sideEffects=None,groups=redis.operator.com,resources=redisusers,verbs=create;update,versions=v1,name=vredisuser.redis.operator.com,admissionReviewVersions=v1

// RedisUserValidator turns away a RedisUser whose username another source of
// users of its RedisInstance already has: the operator would apply neither,
// so the new one would lock out the old. A missing RedisInstance only gets a
// warning, since GitOps tools may create it later. The webhook is only a
// faster answer: the controller checks the same, as two applies may race.
type RedisUserValidator struct {
	client client.Reader
}

// ValidateCreate implements admission.Validator so a webhook will be registered for the type RedisUser.
func (v *RedisUserValidator) ValidateCreate(ctx context.Context, user *redisv1.RedisUser) (admission.Warnings, error) {
	return v.validate(ctx, user)
}

// ValidateUpdate checks only a new username or target: a conflict that came
// later, e.g. with a changed external ACL, must not block other fixes.
func (v *RedisUserValidator) ValidateUpdate(ctx context.Context, oldUser, user *redisv1.RedisUser) (admission.Warnings, error) {
	if oldUser.Spec.Username == user.Spec.Username && oldUser.Spec.TargetRef == user.Spec.TargetRef {
		return nil, nil
	}
	return v.validate(ctx, user)
}

// ValidateDelete implements admission.Validator; the webhook is not called on deletion.
func (v *RedisUserValidator) ValidateDelete(_ context.Context, _ *redisv1.RedisUser) (admission.Warnings, error) {
	return nil, nil
}

func (v *RedisUserValidator) validate(ctx context.Context, user *redisv1.RedisUser) (admission.Warnings, error) {
	var instance redisv1.RedisInstance
	err := v.client.Get(ctx, types.NamespacedName{Namespace: user.Namespace, Name: user.Spec.TargetRef.Name}, &instance)
	if apierrors.IsNotFound(err) {
		return admission.Warnings{fmt.Sprintf("RedisInstance %s does not exist yet; the user waits for it", user.Spec.TargetRef.Name)}, nil
	}
	conflict := ""
	if err == nil {
		conflict, err = controller.UsernameConflict(ctx, v.client, user, &instance)
	}
	if err != nil {
		// Only a faster answer: the controller checks again.
		return admission.Warnings{"Could not check the username for conflicts: " + err.Error()}, nil
	}
	if conflict == "" {
		return nil, nil
	}
	return nil, apierrors.NewInvalid(redisv1.GroupVersion.WithKind("RedisUser").GroupKind(), user.Name, field.ErrorList{
		field.Forbidden(field.NewPath("spec", "username"), conflict+"; neither would be applied"),
	})
}
