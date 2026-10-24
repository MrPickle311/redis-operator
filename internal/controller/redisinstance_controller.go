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
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/imclient"
	"github.com/MrPickle311/redis-operator/internal/instancemanager"
	"github.com/MrPickle311/redis-operator/internal/pki"
	rbacv1 "k8s.io/api/rbac/v1"
)

// RedisInstanceReconciler reconciles a RedisInstance object
type RedisInstanceReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// OperatorImage is the image of the operator itself. Its initContainer
	// copies the Instance Manager binary into every Redis Pod.
	OperatorImage string
	// CA signs the certificates of the Instance Manager API; see EnsureCA.
	CA pki.KeyPair
	// InstanceManager reads the state of every instance over mTLS.
	InstanceManager *imclient.Client
	// Recorder emits the Events of RedisInstances.
	Recorder events.EventRecorder
}

// +kubebuilder:rbac:groups=redis.operator.com,resources=redisinstances,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=redis.operator.com,resources=redisinstances/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=redis.operator.com,resources=redisinstances/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch

// buildRedisPod returns the Pod of the instance with the given ordinal, with
// claimName, the PVC bound in status.volumes, as its data volume.
func buildRedisPod(instance *redisv1.RedisInstance, ordinal int32, operatorImage, claimName string) *corev1.Pod {
	runAsNonRoot := true
	automountToken := false
	runAsUser := int64(999)
	allowPrivEsc := false

	containerSecurity := &corev1.SecurityContext{
		AllowPrivilegeEscalation: &allowPrivEsc,
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}
	controllerMount := corev1.VolumeMount{Name: controllerVolume, MountPath: controllerDir}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", instance.Name, ordinal),
			Namespace: instance.Namespace,
			Labels: map[string]string{
				labelName:     appName,
				labelInstance: instance.Name,
				labelRole:     podRole(instance, ordinal),
			},
		},
		Spec: corev1.PodSpec{
			Hostname:                     instancePodName(instance, ordinal),
			Subdomain:                    headlessServiceName(instance),
			ServiceAccountName:           instanceAccountName(instance),
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
						{ContainerPort: instancemanager.StatusPort, Name: "status"},
					},
					ReadinessProbe: imProbe("/readyz"),
					LivenessProbe:  imProbe("/healthz"),
					Command:        instanceCommand(instance),
					Env: []corev1.EnvVar{
						fieldEnv("POD_NAME", "metadata.name"),
						fieldEnv("POD_NAMESPACE", "metadata.namespace"),
					},
					Resources: instance.Spec.Resources,
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "data",
							MountPath: "/data",
						},
						controllerMount,
						{
							Name:      certificatesVolume,
							MountPath: instancemanager.CertificatesDir,
							ReadOnly:  true,
						},
						{
							Name:      internalAuthVolume,
							MountPath: instancemanager.InternalAuthDir,
							ReadOnly:  true,
						},
						{
							Name:      apiAccessVolume,
							MountPath: serviceAccountDir,
							ReadOnly:  true,
						},
					},
					SecurityContext: containerSecurity,
				},
			},
			Volumes: []corev1.Volume{
				{
					Name:         controllerVolume,
					VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
				},
				{Name: apiAccessVolume, VolumeSource: apiAccessVolumeSource()},
				{
					Name: internalAuthVolume,
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{SecretName: internalAuthSecretName(instance)},
					},
				},
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: claimName,
						},
					},
				},
				{
					Name: certificatesVolume,
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{SecretName: serverSecretName(instance)},
					},
				},
			},
		},
	}

	pod.Annotations = map[string]string{annotationSpecHash: specHash(&pod.Spec)}
	return pod
}

// instanceCommand runs the Instance Manager, which starts redis-server.
func instanceCommand(instance *redisv1.RedisInstance) []string {
	command := []string{managerPath, "instance", "run",
		"--instance=" + instance.Name,
		"--maxmemory-ratio=" + strconv.Itoa(int(instance.Spec.Redis.MaxmemoryRatio)),
		"--maxmemory-policy=" + instance.Spec.Redis.MaxmemoryPolicy,
	}
	if instance.Spec.ACL.DefaultUser == redisv1.DefaultUserEnabled {
		command = append(command, "--default-user")
	}
	return command
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
			if pod.Name == instance.Status.CurrentPrimary {
				// Deleting the primary would lose the writes not yet replicated.
				r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, "ScaleDownBlocked", "ScaleDown",
					"Pod %s is the primary; switch over to a remaining instance first", pod.Name)
				continue
			}
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
	name := generatedPVCName(instance, ordinal)

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

// deleteOwnedPVC deletes the generated PVC of the given ordinal, but only
// when the RedisInstance owns it. A PVC created by the user, even one with a
// matching name, holds data the operator must never touch.
func (r *RedisInstanceReconciler) deleteOwnedPVC(ctx context.Context, instance *redisv1.RedisInstance, ordinal int32) error {
	currentPvcName := generatedPVCName(instance, ordinal)

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
	oldStatus := instance.Status.DeepCopy()
	// ponytail: the primary never changes before failover (step 4), so the epoch stays 1.
	instance.Status.PrimaryEpoch = max(instance.Status.PrimaryEpoch, 1)

	var readyCount int32 = 0
	memoryUnlimited := false
	var outdated []int32

	// Before the Pods: they cannot start until the Secrets they mount exist.
	if err := r.ensureServerSecret(ctx, &instance); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureInternalAuthSecret(ctx, &instance); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureInstanceAccount(ctx, &instance); err != nil {
		return ctrl.Result{}, err
	}

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

	forgetRemovedRequests(&instance)
	if err := r.reconcileRecreate(ctx, &instance); err != nil {
		return ctrl.Result{}, err
	}
	podExists, err := r.existingPods(ctx, &instance)
	if err != nil {
		return ctrl.Result{}, err
	}
	claims, err := r.reconcileVolumes(ctx, &instance, podExists)
	if err != nil {
		return ctrl.Result{}, err
	}
	// Right away, before any Pod mounts a volume: the binding and the UID of
	// its PVC must be recorded even when this pass returns early below.
	if !equality.Semantic.DeepEqual(oldStatus, &instance.Status) {
		if err := r.Status().Update(ctx, &instance); err != nil {
			return ctrl.Result{}, err
		}
		oldStatus = instance.Status.DeepCopy()
	}

	for ordinal := range instance.Spec.Instances {
		claimName, ok := claims[ordinal]
		if !ok && !podExists[ordinal] {
			continue // no usable volume, reported in StorageReady
		}

		desiredPod := buildRedisPod(&instance, ordinal, r.OperatorImage, claimName)

		if err := ctrl.SetControllerReference(&instance, desiredPod, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}

		var existingPod corev1.Pod
		err := r.Get(ctx, types.NamespacedName{
			Name:      desiredPod.Name,
			Namespace: desiredPod.Namespace,
		}, &existingPod)

		if apierrors.IsNotFound(err) {
			if !ok {
				continue // the Pod was deleted meanwhile, and its volume is not usable
			}
			logger.Info("Creating pod...", "name", desiredPod.Name)
			if err := r.Create(ctx, desiredPod); err != nil {
				return ctrl.Result{}, ignoreAlreadyExists(err)
			}
			memoryUnlimited = memoryUnlimited || !hasMemoryLimit(desiredPod)
			continue
		} else if err != nil {
			return ctrl.Result{}, err
		}

		if err := r.ensureRoleLabel(ctx, &existingPod, podRole(&instance, ordinal)); err != nil {
			return ctrl.Result{}, err
		}

		// A deleted Pod is going down even while it still reports Ready.
		if isPodReady(&existingPod) && existingPod.DeletionTimestamp.IsZero() {
			readyCount++
		}
		if ok && isOutdated(&existingPod, desiredPod) {
			outdated = append(outdated, ordinal)
		}
		memoryUnlimited = memoryUnlimited || !hasMemoryLimit(&existingPod)
	}

	if err := r.cleanupExcessPods(ctx, &instance); err != nil {
		return ctrl.Result{}, err
	}

	// Before the switchover: the update of the primary starts one.
	healthy := readyCount == instance.Spec.Instances && len(claims) == int(instance.Spec.Instances)
	retryRollout, err := r.reconcileRollout(ctx, &instance, outdated, healthy)
	// Written now, as a waiting switchover returns early below.
	meta.SetStatusCondition(&instance.Status.Conditions, readyCondition(&instance, readyCount, len(outdated)))
	if !equality.Semantic.DeepEqual(oldStatus, &instance.Status) {
		if err := r.Status().Update(ctx, &instance); err != nil {
			return ctrl.Result{}, err
		}
		oldStatus = instance.Status.DeepCopy()
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	// Before the primary is read: a completed switchover changes it, and the
	// replication loop below then demotes the old primary.
	if result, err := r.reconcileSwitchover(ctx, &instance); err != nil || !result.IsZero() {
		return result, err
	}
	primary := primaryOrdinal(&instance)
	var primaryPod corev1.Pod
	if err := r.Get(ctx, types.NamespacedName{
		Name:      instancePodName(&instance, primary),
		Namespace: instance.Namespace,
	}, &primaryPod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if primaryPod.Status.PodIP == "" {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	primaryHost := podFQDN(&instance, primary)
	replicationPending := false
	for ordinal := range instance.Spec.Instances {
		if ordinal == primary {
			continue
		}
		replicaName := instancePodName(&instance, ordinal)
		var replicaPod corev1.Pod
		if err := r.Get(ctx, types.NamespacedName{Name: replicaName, Namespace: instance.Namespace}, &replicaPod); err != nil {
			continue
		}
		// Running, not Ready: a replica becomes Ready only after its first
		// sync, which never happens while it replicates from a wrong primary.
		if replicaPod.Status.Phase != corev1.PodRunning || !replicaPod.DeletionTimestamp.IsZero() {
			continue
		}

		changed, err := r.InstanceManager.ReplicaOf(ctx, podFQDN(&instance, ordinal), instance.Status.PrimaryEpoch, primaryHost)
		if err != nil {
			replicationPending = true
			if isPodReady(&replicaPod) {
				logger.Error(err, "Failed to configure replication", "pod", replicaName)
			} else {
				// A starting Instance Manager answers only once redis-server runs.
				logger.V(1).Info("Instance Manager not reachable yet", "pod", replicaName, "reason", err.Error())
			}
			continue
		}
		if changed {
			logger.Info("Configured replication", "pod", replicaName, "primary", primaryHost)
		}
	}

	r.setMemoryLimitedCondition(&instance, memoryUnlimited)
	instance.Status.ReadyInstances = readyCount
	instance.Status.Phase = fmt.Sprintf("%d/%d ready", readyCount, instance.Spec.Instances)
	instance.Status.CurrentPrimary = primaryPod.Name
	if !equality.Semantic.DeepEqual(oldStatus, &instance.Status) {
		if err := r.Status().Update(ctx, &instance); err != nil {
			return ctrl.Result{}, err
		}
	}

	if replicationPending {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	return ctrl.Result{RequeueAfter: retryRollout}, nil
}

// existingPods tells which ordinals have a Pod, including one being deleted.
func (r *RedisInstanceReconciler) existingPods(ctx context.Context, instance *redisv1.RedisInstance) (map[int32]bool, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(instance.Namespace), client.MatchingLabels(instanceSelector(instance))); err != nil {
		return nil, err
	}
	exists := make(map[int32]bool, len(pods.Items))
	for _, pod := range pods.Items {
		if ordinal, err := ordinalFromPodName(pod.Name, instance.Name); err == nil {
			exists[ordinal] = true
		}
	}
	return exists, nil
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
		// External PVCs are not owned, but their deletion or replacement
		// changes the state of the volume of an instance.
		Watches(&corev1.PersistentVolumeClaim{}, handler.EnqueueRequestsFromMapFunc(r.instancesUsingPVC)).
		Owns(&corev1.Secret{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&rbacv1.Role{}).
		Owns(&rbacv1.RoleBinding{}).
		Named("redisinstance").
		Complete(r)
}
