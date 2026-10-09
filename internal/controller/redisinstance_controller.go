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
	"maps"
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
	"github.com/MrPickle311/redis-operator/internal/instancemanager"
)

// RedisInstanceReconciler reconciles a RedisInstance object
type RedisInstanceReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// OperatorImage is the image of the operator itself. Its initContainer
	// copies the Instance Manager binary into every Redis Pod.
	OperatorImage string
}

// +kubebuilder:rbac:groups=redis.operator.com,resources=redisinstances,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=redis.operator.com,resources=redisinstances/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=redis.operator.com,resources=redisinstances/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete

func buildRedisPod(instance *redisv1.RedisInstance, ordinal int32, operatorImage string) *corev1.Pod {
	runAsNonRoot := true
	automountToken := false
	runAsUser := int64(999)
	allowPrivEsc := false
	claimName, _ := pvcName(instance, ordinal)

	containerSecurity := &corev1.SecurityContext{
		AllowPrivilegeEscalation: &allowPrivEsc,
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}
	controllerMount := corev1.VolumeMount{Name: controllerVolume, MountPath: controllerDir}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", instance.Name, ordinal),
			Namespace: instance.Namespace,
			Labels: map[string]string{
				labelName:     appName,
				labelInstance: instance.Name,
				labelRole:     podRole(ordinal),
			},
		},
		Spec: corev1.PodSpec{
			Hostname:                     instancePodName(instance, ordinal),
			Subdomain:                    headlessServiceName(instance),
			AutomountServiceAccountToken: &automountToken,
			SecurityContext: &corev1.PodSecurityContext{
				RunAsUser:    &runAsUser,
				FSGroup:      &runAsUser,
				RunAsNonRoot: &runAsNonRoot,
				SeccompProfile: &corev1.SeccompProfile{
					Type: corev1.SeccompProfileTypeRuntimeDefault,
				},
			},
			InitContainers: []corev1.Container{
				{
					Name:            "bootstrap",
					Image:           operatorImage,
					Command:         []string{"/manager", "bootstrap", managerPath},
					VolumeMounts:    []corev1.VolumeMount{controllerMount},
					SecurityContext: containerSecurity,
				},
			},
			Containers: []corev1.Container{
				{
					Name:  appName,
					Image: instance.Spec.Image,
					Ports: []corev1.ContainerPort{
						{ContainerPort: redisPort, Name: appName},
						{ContainerPort: instancemanager.ProbePort, Name: "probes"},
					},
					ReadinessProbe: imProbe("/readyz"),
					LivenessProbe:  imProbe("/healthz"),
					Command:        []string{managerPath, "instance", "run"},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "data",
							MountPath: "/data",
						},
						controllerMount,
					},
					SecurityContext: containerSecurity,
				},
			},
			Volumes: []corev1.Volume{
				{
					Name:         controllerVolume,
					VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
				},
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

// imProbe checks the given path on the probe port of the Instance Manager.
func imProbe(path string) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{Path: path, Port: intstr.FromInt32(instancemanager.ProbePort)},
		},
		PeriodSeconds: 5,
	}
}

func (r *RedisInstanceReconciler) cleanupExcessPods(ctx context.Context, instance *redisv1.RedisInstance) error {
	logger := logf.FromContext(ctx)

	var podList corev1.PodList

	if err := r.List(ctx, &podList,
		client.InNamespace(instance.Namespace),
		client.MatchingLabels(instanceSelector(instance)),
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

			if err := r.deleteOwnedPVC(ctx, instance, ordinal); err != nil {
				return err
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

// buildClientService returns the ClusterIP Service <name>-<suffix> for clients.
// It selects the Pods of the instance, narrowed down by the extra labels.
func buildClientService(instance *redisv1.RedisInstance, suffix string, extra map[string]string) *corev1.Service {
	selector := instanceSelector(instance)
	maps.Copy(selector, extra)

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      instance.Name + "-" + suffix,
			Namespace: instance.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: selector,
			Ports: []corev1.ServicePort{
				{Port: redisPort, TargetPort: intstr.FromInt32(redisPort), Name: appName},
			},
		},
	}
}

func buildHeadlessService(instance *redisv1.RedisInstance) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      headlessServiceName(instance),
			Namespace: instance.Namespace,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			// DNS records must exist before Pods are Ready: a replica has to
			// resolve the primary while it is still syncing.
			PublishNotReadyAddresses: true,
			Selector:                 instanceSelector(instance),
			Ports: []corev1.ServicePort{
				{
					Port:       redisPort,
					TargetPort: intstr.FromInt32(redisPort),
					Name:       appName,
				},
			},
		},
	}
}

func (r *RedisInstanceReconciler) ensureService(ctx context.Context, instance *redisv1.RedisInstance, desired *corev1.Service) error {
	if err := ctrl.SetControllerReference(instance, desired, r.Scheme); err != nil {
		return err
	}

	var existing corev1.Service
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), &existing)
	if apierrors.IsNotFound(err) {
		logf.FromContext(ctx).Info("creating Service", "name", desired.Name)
		return ignoreAlreadyExists(r.Create(ctx, desired))
	}
	return err
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

// ensureRoleLabel sets the role label on an existing Pod. The -rw and -ro
// Services select Pods by this label, so it decides where client traffic goes.
func (r *RedisInstanceReconciler) ensureRoleLabel(ctx context.Context, pod *corev1.Pod, role string) error {
	if pod.Labels[labelRole] == role {
		return nil
	}

	patch := client.MergeFrom(pod.DeepCopy())
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	pod.Labels[labelRole] = role

	logf.FromContext(ctx).Info("Updated Pod role label", "name", pod.Name, "role", role)
	return r.Patch(ctx, pod, patch)
}

// deleteOwnedPVC deletes the PVC of the given ordinal, but only when the
// RedisInstance owns it. A PVC created by the user, even one with a matching
// name, holds data the operator must never touch.
func (r *RedisInstanceReconciler) deleteOwnedPVC(ctx context.Context, instance *redisv1.RedisInstance, ordinal int32) error {
	currentPvcName, _ := pvcName(instance, ordinal)

	var pvc corev1.PersistentVolumeClaim
	if err := r.Get(ctx, types.NamespacedName{Name: currentPvcName, Namespace: instance.Namespace}, &pvc); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(&pvc, instance) {
		return nil
	}

	logf.FromContext(ctx).Info("Deleting PVC of removed instance", "name", pvc.Name)
	return client.IgnoreNotFound(r.Delete(ctx, &pvc))
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

	for _, svc := range []*corev1.Service{
		buildHeadlessService(&instance),
		buildClientService(&instance, "rw", map[string]string{labelRole: rolePrimary}),
		buildClientService(&instance, "ro", map[string]string{labelRole: roleReplica}),
		buildClientService(&instance, "r", nil),
	} {
		if err := r.ensureService(ctx, &instance, svc); err != nil {
			return ctrl.Result{}, err
		}
	}

	for ordinal := int32(0); ordinal < instance.Spec.Instances; ordinal++ {
		if _, external := pvcName(&instance, ordinal); !external {
			if err := r.ensurePVC(ctx, &instance, ordinal); err != nil {
				return ctrl.Result{}, err
			}
		}

		desiredPod := buildRedisPod(&instance, ordinal, r.OperatorImage)

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

		if err := r.ensureRoleLabel(ctx, &existingPod, podRole(ordinal)); err != nil {
			return ctrl.Result{}, err
		}

		if isPodReady(&existingPod) {
			readyCount++
		}
	}

	if err := r.cleanupExcessPods(ctx, &instance); err != nil {
		return ctrl.Result{}, err
	}

	var primaryPod corev1.Pod
	if err := r.Get(ctx, types.NamespacedName{
		Name:      instancePodName(&instance, 0),
		Namespace: instance.Namespace,
	}, &primaryPod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if primaryPod.Status.PodIP == "" {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	primaryHost := podFQDN(&instance, 0)
	replicationPending := false
	for ordinal := int32(1); ordinal < instance.Spec.Instances; ordinal++ {
		replicaName := instancePodName(&instance, ordinal)
		var replicaPod corev1.Pod
		if err := r.Get(ctx, types.NamespacedName{Name: replicaName, Namespace: instance.Namespace}, &replicaPod); err != nil {
			continue
		}
		if !isPodReady(&replicaPod) {
			continue
		}

		changed, err := ensureReplicaOf(ctx, podFQDN(&instance, ordinal), primaryHost)
		if err != nil {
			logger.Error(err, "Failed to configure replication", "pod", replicaName)
			replicationPending = true
			continue
		}
		if changed {
			logger.Info("Configured replication", "pod", replicaName, "primary", primaryHost)
		}
	}

	if instance.Status.ReadyInstances != readyCount || instance.Status.CurrentPrimary != primaryPod.Name {
		instance.Status.ReadyInstances = readyCount
		instance.Status.Phase = fmt.Sprintf("%d/%d ready", readyCount, instance.Spec.Instances)
		instance.Status.CurrentPrimary = primaryPod.Name
		if err := r.Status().Update(ctx, &instance); err != nil {
			return ctrl.Result{}, err
		}
	}

	if replicationPending {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	return ctrl.Result{}, nil
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
