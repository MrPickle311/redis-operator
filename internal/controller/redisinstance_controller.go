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
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

// RedisInstanceReconciler reconciles a RedisInstance object
type RedisInstanceReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=redis.operator.com,resources=redisinstances,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=redis.operator.com,resources=redisinstances/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=redis.operator.com,resources=redisinstances/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete

func buildRedisPod(instance *redisv1.RedisInstance, ordinal int32) *corev1.Pod {
	runAsNonRoot := true
	runAsUser := int64(999)
	allowPrivEsc := false
	claimName, _ := pvcName(instance, ordinal)

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", instance.Name, ordinal),
			Namespace: instance.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":     "redis",
				"app.kubernetes.io/instance": instance.Name,
			},
		},
		Spec: corev1.PodSpec{
			SecurityContext: &corev1.PodSecurityContext{
				RunAsUser:    &runAsUser,
				FSGroup:      &runAsUser,
				RunAsNonRoot: &runAsNonRoot,
				SeccompProfile: &corev1.SeccompProfile{
					Type: corev1.SeccompProfileTypeRuntimeDefault,
				},
			},
			Containers: []corev1.Container{
				{
					Name:  "redis",
					Image: instance.Spec.Image,
					Ports: []corev1.ContainerPort{
						{ContainerPort: 6379, Name: "redis"},
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "data",
							MountPath: "/data",
						},
					},
					SecurityContext: &corev1.SecurityContext{
						AllowPrivilegeEscalation: &allowPrivEsc,
						Capabilities: &corev1.Capabilities{
							Drop: []corev1.Capability{"ALL"},
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: claimName,
						},
					},
				},
			},
		},
	}
}

func (r *RedisInstanceReconciler) cleanupExcessPods(ctx context.Context, instance *redisv1.RedisInstance) error {
	logger := logf.FromContext(ctx)

	var podList corev1.PodList

	if err := r.List(ctx, &podList,
		client.InNamespace(instance.Namespace),
		client.MatchingLabels{"app.kubernetes.io/instance": instance.Name},
	); err != nil {
		return err
	}

	for _, pod := range podList.Items {
		ordinal, err := ordinalFromPodName(pod.Name, instance.Name)
		if err != nil {
			continue
		}

		if ordinal >= instance.Spec.Instances {
			logger.Info("removing redundant Pod", "name", pod.Name)
			if err := r.Delete(ctx, &pod); err != nil && !apierrors.IsNotFound(err) {
				return err
			}

			var pvc corev1.PersistentVolumeClaim
			pvcName := fmt.Sprintf("%s-%d-data", instance.Name, ordinal)
			if err := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: instance.Namespace}, &pvc); err == nil {
				if err := r.Delete(ctx, &pvc); err != nil && !apierrors.IsNotFound(err) {
					return err
				}
			}
		}
	}

	return nil
}

func ordinalFromPodName(podName, instanceName string) (int32, error) {
	prefix := instanceName + "-"
	if !strings.HasPrefix(podName, prefix) {
		return 0, fmt.Errorf("name %q does not fit to the pattern", podName)
	}
	suffix := strings.TrimPrefix(podName, prefix)
	n, err := strconv.ParseInt(suffix, 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(n), nil
}

func buildRedisService(instance *redisv1.RedisInstance) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      instance.Name,
			Namespace: instance.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app.kubernetes.io/instance": instance.Name,
			},
			Ports: []corev1.ServicePort{
				{Port: 6379, TargetPort: intstr.FromInt32(6379), Name: "redis"},
			},
		},
	}
}

func buildRedisPVC(instance *redisv1.RedisInstance, ordinal int32) *corev1.PersistentVolumeClaim {
	volumeClaimTemplate := instance.Spec.Storage.VolumeClaimTemplate
	name, _ := pvcName(instance, ordinal)

	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: instance.Namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: volumeClaimTemplate.Size,
				},
			},
			StorageClassName: volumeClaimTemplate.StorageClassName,
		},
	}
}

func (r *RedisInstanceReconciler) ensurePVC(ctx context.Context, instance *redisv1.RedisInstance, ordinal int32) error {
	if instance.Spec.Storage.VolumeClaimTemplate == nil {
		return fmt.Errorf("instance %d has no existing claim and storage.volumeClaimTemplate is not set", ordinal)
	}

	desiredPVC := buildRedisPVC(instance, ordinal)
	if err := ctrl.SetControllerReference(instance, desiredPVC, r.Scheme); err != nil {
		return err
	}

	var existing corev1.PersistentVolumeClaim
	err := r.Get(ctx, client.ObjectKeyFromObject(desiredPVC), &existing)
	if apierrors.IsNotFound(err) {
		logf.FromContext(ctx).Info("creating PVC", "name", desiredPVC.Name)
		return ignoreAlreadyExists(r.Create(ctx, desiredPVC))
	}
	return err
}

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the RedisInstance object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.25.0/pkg/reconcile
func (r *RedisInstanceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logf.FromContext(ctx)

	var instance redisv1.RedisInstance
	if err := r.Get(ctx, req.NamespacedName, &instance); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	var readyCount int32 = 0

	for ordinal := int32(0); ordinal < instance.Spec.Instances; ordinal++ {
		if _, external := pvcName(&instance, ordinal); !external {
			if err := r.ensurePVC(ctx, &instance, ordinal); err != nil {
				return ctrl.Result{}, err
			}
		}

		desiredPod := buildRedisPod(&instance, ordinal)

		if err := ctrl.SetControllerReference(&instance, desiredPod, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}

		var existingPod corev1.Pod
		err := r.Get(ctx, types.NamespacedName{
			Name:      desiredPod.Name,
			Namespace: desiredPod.Namespace,
		}, &existingPod)

		if apierrors.IsNotFound(err) {
			logger.Info("Creating pod...", "name", desiredPod.Name)
			if err := r.Create(ctx, desiredPod); err != nil {
				return ctrl.Result{}, ignoreAlreadyExists(err)
			}
			continue
		} else if err != nil {
			return ctrl.Result{}, err
		}
		if isPodReady(&existingPod) {
			readyCount++
		}
	}

	desiredSvc := buildRedisService(&instance)
	if err := ctrl.SetControllerReference(&instance, desiredSvc, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	var existingSvc corev1.Service
	err := r.Get(ctx, types.NamespacedName{Name: desiredSvc.Name, Namespace: desiredSvc.Namespace}, &existingSvc)
	if apierrors.IsNotFound(err) {
		logger.Info("creating Service", "name", desiredSvc.Name)
		if err := r.Create(ctx, desiredSvc); err != nil {
			return ctrl.Result{}, ignoreAlreadyExists(err)
		}
	} else if err != nil {
		return ctrl.Result{}, err
	}

	if err := r.cleanupExcessPods(ctx, &instance); err != nil {
		return ctrl.Result{}, err
	}

	var primaryPod corev1.Pod
	if err := r.Get(ctx, types.NamespacedName{
		Name:      fmt.Sprintf("%s-0", instance.Name),
		Namespace: instance.Namespace,
	}, &primaryPod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if primaryPod.Status.PodIP == "" {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	for ordinal := int32(1); ordinal < instance.Spec.Instances; ordinal++ {
		replicaName := fmt.Sprintf("%s-%d", instance.Name, ordinal)
		var replicaPod corev1.Pod
		if err := r.Get(ctx, types.NamespacedName{Name: replicaName, Namespace: instance.Namespace}, &replicaPod); err != nil {
			continue
		}
		if !isPodReady(&replicaPod) || replicaPod.Status.PodIP == "" {
			continue
		}
		if err := setReplicaOf(ctx, replicaPod.Status.PodIP, primaryPod.Status.PodIP); err != nil {
			logger.Error(err, "could not set replication", "pod", replicaName)
			continue
		}
		logger.Info("replication configured", "pod", replicaName)
	}

	if instance.Status.ReadyInstances != readyCount || instance.Status.CurrentPrimary != primaryPod.Name {
		instance.Status.ReadyInstances = readyCount
		instance.Status.Phase = fmt.Sprintf("%d/%d ready", readyCount, instance.Spec.Instances)
		instance.Status.CurrentPrimary = primaryPod.Name
		if err := r.Status().Update(ctx, &instance); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// pvcName returns the PVC name used by the instance with the given ordinal.
// The second value is true when the PVC comes from spec.storage.existingClaims.
func pvcName(instance *redisv1.RedisInstance, ordinal int32) (string, bool) {
	for _, c := range instance.Spec.Storage.ExistingClaims {
		if c.InstanceOrdinal == ordinal {
			return c.ClaimName, true
		}
	}
	return fmt.Sprintf("%s-%d-data", instance.Name, ordinal), false
}

func isPodReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

func ignoreAlreadyExists(err error) error {
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

// SetupWithManager sets up the controller with the Manager.
func (r *RedisInstanceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&redisv1.RedisInstance{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Named("redisinstance").
		Complete(r)
}
