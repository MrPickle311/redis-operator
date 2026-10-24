package controller

// Volumes of a RedisInstance.
//
// The spec chooses a volume for each instance only once, when the instance
// first needs one: the claim from spec.storage.existingClaims, or else a PVC
// generated from spec.storage.volumeClaimTemplate. That choice, with the UID
// of the PVC, is the binding kept in status.volumes. From then on the operator
// looks only at the binding, never at the spec again.
//
// Every reconcile compares each binding with its PVC and sets its state:
//
//	PVC                                        state        Pod
//	exists, same UID, not deleted              Ready        created
//	deleted, but the Pod still uses it         Terminating  keeps running
//	missing, or same name with another UID     Missing      not created
//	lost its PersistentVolume                  Lost         not created
//	external PVC of a scaled-down instance     Detached     none
//
// The only volume the operator replaces on its own is the generated PVC of a
// replica whose Pod is gone too: a replica syncs everything from the primary.
// It never replaces the volume of the primary (an empty primary would make
// every replica drop its data) or an external PVC (it belongs to the user).
// Everything else needs the recreate annotation.

import (
	"context"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

// --- Binding and observing volumes, once per reconcile ---

// reconcileVolumes updates the binding of every instance and returns the PVC
// of every instance whose volume is Ready: only those may get a Pod.
// podExists tells which instances still have a Pod.
func (r *RedisInstanceReconciler) reconcileVolumes(
	ctx context.Context, instance *redisv1.RedisInstance, podExists map[int32]bool,
) (map[int32]string, error) {
	var problems []string

	for ordinal := range instance.Spec.Instances {
		vol := volumeOf(instance, ordinal)
		previous := vol.State

		if err := r.reconcileVolume(ctx, instance, vol, podExists[ordinal]); err != nil {
			return nil, err
		}

		if vol.State != redisv1.VolumeReady {
			problems = append(problems, fmt.Sprintf("instance %d: %s", ordinal, vol.Message))
			if vol.State != previous {
				r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, "VolumeUnavailable", "Reconcile",
					"Volume of instance %d is %s: %s", ordinal, vol.State, vol.Message)
			}
		}
	}

	releaseRemovedVolumes(instance)
	setStorageReadyCondition(instance, problems)
	return readyClaims(instance), nil
}

// reconcileVolume updates one binding from its PVC.
func (r *RedisInstanceReconciler) reconcileVolume(
	ctx context.Context, instance *redisv1.RedisInstance, vol *redisv1.VolumeStatus, podExists bool,
) error {
	pvc, err := r.getPVC(ctx, instance.Namespace, vol.ClaimName)
	if err != nil {
		return err
	}

	if pvc == nil {
		return r.reconcileMissingPVC(ctx, instance, vol, podExists)
	}

	firstBinding := vol.UID == ""
	if firstBinding && !bindPVC(instance, vol, pvc) {
		return nil
	}

	observePVC(instance, vol, pvc)
	return nil
}

// reconcileMissingPVC handles a binding whose PVC does not exist. The
// operator creates a generated PVC when the instance gets its first volume,
// and replaces one only when that is safe; it never creates an external PVC.
func (r *RedisInstanceReconciler) reconcileMissingPVC(
	ctx context.Context, instance *redisv1.RedisInstance, vol *redisv1.VolumeStatus, podExists bool,
) error {
	if vol.External {
		setVolumeState(vol, redisv1.VolumeMissing, fmt.Sprintf("PVC %s not found", vol.ClaimName))
		return nil
	}

	// A known UID means the PVC existed and was deleted, with its data.
	replacing := vol.UID != ""
	if replacing {
		if reason := replaceBlocker(instance, vol.Ordinal, podExists); reason != "" {
			setVolumeState(vol, redisv1.VolumeMissing, fmt.Sprintf("PVC %s was deleted; %s", vol.ClaimName, reason))
			return nil
		}
	}

	created, err := r.createDataPVC(ctx, instance, vol.Ordinal)
	if err != nil {
		return err
	}
	vol.UID = created.UID
	setVolumeState(vol, redisv1.VolumeReady, "")

	if replacing {
		r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, "VolumeRecreated", "Reconcile",
			"Created a new empty PVC %s for replica %s; it syncs from the primary",
			vol.ClaimName, instancePodName(instance, vol.Ordinal))
	}
	return nil
}

// replaceBlocker explains why the operator must not replace the deleted
// generated PVC of the instance with a new empty one, or returns "" when it may.
func replaceBlocker(instance *redisv1.RedisInstance, ordinal int32, podExists bool) string {
	switch {
	case ordinal == primaryOrdinal(instance):
		// An empty primary would make every replica drop its data in a full resync.
		return "the instance is the primary, so it is not recreated empty; switch over to a replica first"
	case podExists:
		return "Pod " + instancePodName(instance, ordinal) + " still exists"
	case instance.Spec.Storage.VolumeClaimTemplate == nil:
		return "storage.volumeClaimTemplate is not set"
	}
	return ""
}

// bindPVC records the UID of an existing PVC that an instance gets as its
// volume for the first time. It returns false, and sets why in the state,
// when the PVC must not become the volume.
func bindPVC(instance *redisv1.RedisInstance, vol *redisv1.VolumeStatus, pvc *corev1.PersistentVolumeClaim) bool {
	if !pvc.DeletionTimestamp.IsZero() {
		// The old PVC that a recreate is replacing: wait until it is gone.
		setVolumeState(vol, redisv1.VolumeTerminating,
			fmt.Sprintf("waiting for the old PVC %s to be deleted", pvc.Name))
		return false
	}
	if !vol.External && !metav1.IsControlledBy(pvc, instance) {
		// The name of a generated PVC is predictable, so a PVC with that name
		// may hold anything: only a PVC the operator created is adopted.
		setVolumeState(vol, redisv1.VolumeMissing, fmt.Sprintf(
			"PVC %s exists but was not created by this RedisInstance; delete or rename it", pvc.Name))
		return false
	}
	vol.UID = pvc.UID
	return true
}

// observePVC sets the state of a bound volume from its PVC.
func observePVC(instance *redisv1.RedisInstance, vol *redisv1.VolumeStatus, pvc *corev1.PersistentVolumeClaim) {
	switch {
	case pvc.UID != vol.UID:
		setVolumeState(vol, redisv1.VolumeMissing, fmt.Sprintf(
			"PVC %s was deleted and created again, so it holds other data; "+
				"recreate the replica with the %s annotation", pvc.Name, redisv1.AnnotationRecreate))
	case !pvc.DeletionTimestamp.IsZero():
		setVolumeState(vol, redisv1.VolumeTerminating, fmt.Sprintf(
			"PVC %s is being deleted and stays only while Pod %s uses it",
			pvc.Name, instancePodName(instance, vol.Ordinal)))
	case pvc.Status.Phase == corev1.ClaimLost:
		setVolumeState(vol, redisv1.VolumeLost, fmt.Sprintf("PVC %s lost its PersistentVolume", pvc.Name))
	default:
		setVolumeState(vol, redisv1.VolumeReady, "")
	}
}

// releaseRemovedVolumes handles the bindings of instances removed by a
// scale-down. A generated PVC is deleted with its Pod (cleanupExcessPods), so
// its binding goes too; an external PVC stays bound, Detached, for the next
// scale-up. A primary above spec.instances keeps its Pod, so it keeps its
// volume as it is.
func releaseRemovedVolumes(instance *redisv1.RedisInstance) {
	primary := primaryOrdinal(instance)
	removed := func(volumeStatus redisv1.VolumeStatus) bool {
		return volumeStatus.Ordinal >= instance.Spec.Instances && volumeStatus.Ordinal != primary
	}

	instance.Status.Volumes = slices.DeleteFunc(instance.Status.Volumes, func(v redisv1.VolumeStatus) bool {
		return removed(v) && !v.External
	})
	for i := range instance.Status.Volumes {
		if vol := &instance.Status.Volumes[i]; removed(*vol) {
			setVolumeState(vol, redisv1.VolumeDetached, "the instance was removed by a scale-down")
		}
	}
}

// readyClaims returns the PVC of every instance below spec.instances whose
// volume is Ready.
func readyClaims(instance *redisv1.RedisInstance) map[int32]string {
	claims := map[int32]string{}
	for _, vol := range instance.Status.Volumes {
		if vol.Ordinal < instance.Spec.Instances && vol.State == redisv1.VolumeReady {
			claims[vol.Ordinal] = vol.ClaimName
		}
	}
	return claims
}

// --- The recreate annotation: the only way to change a binding ---

// reconcileRecreate moves the replica in AnnotationRecreate to a new
// generated volume. It deletes the Pod and the generated PVC (an external PVC
// is only let go, never deleted) and binds the instance to a new generated
// PVC, which reconcileVolumes creates once the old one is gone.
//
// The binding is written to the status before the annotation is removed, so
// a crash in between repeats the recreate instead of losing it.
func (r *RedisInstanceReconciler) reconcileRecreate(ctx context.Context, instance *redisv1.RedisInstance) error {
	target, requested := instance.Annotations[redisv1.AnnotationRecreate]
	if !requested {
		return nil
	}

	ordinal, reason := recreateRejection(instance, target)
	if reason != "" {
		r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, "RecreateRejected", "Recreate",
			"Cannot recreate %s: %s", target, reason)
		return r.removeAnnotation(ctx, instance, redisv1.AnnotationRecreate)
	}

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: target, Namespace: instance.Namespace}}
	if err := client.IgnoreNotFound(r.Delete(ctx, pod)); err != nil {
		return err
	}
	if err := r.deleteOwnedPVC(ctx, instance, ordinal); err != nil {
		return err
	}

	vol := volumeOf(instance, ordinal)
	*vol = redisv1.VolumeStatus{Ordinal: ordinal, ClaimName: generatedPVCName(instance, ordinal)}
	if err := r.Status().Update(ctx, instance); err != nil {
		return err
	}

	logf.FromContext(ctx).Info("Recreating replica on a new volume", "pod", target, "pvc", vol.ClaimName)
	r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, "InstanceRecreated", "Recreate",
		"Recreating %s on a new generated PVC %s", target, vol.ClaimName)
	return r.removeAnnotation(ctx, instance, redisv1.AnnotationRecreate)
}

// recreateRejection returns the ordinal of target, or explains why it cannot
// be recreated.
func recreateRejection(instance *redisv1.RedisInstance, target string) (int32, string) {
	ordinal, err := ordinalFromPodName(target, instance.Name)
	switch {
	case err != nil || ordinal >= instance.Spec.Instances:
		return 0, "it is not an instance of this RedisInstance"
	case ordinal == primaryOrdinal(instance):
		return 0, "it is the primary; switch over to a replica first"
	case instance.Status.TargetPrimary != "":
		return 0, "a switchover is in progress"
	case instance.Spec.Storage.VolumeClaimTemplate == nil:
		return 0, "storage.volumeClaimTemplate is not set, so there is no volume to generate"
	}
	return ordinal, ""
}

// --- Watching external PVCs ---

// instancesUsingPVC maps an event of a PVC to the RedisInstances that use it
// or will use it. External PVCs are not owned by the RedisInstance, so
// without it the operator would not notice one being created, deleted or
// replaced.
func (r *RedisInstanceReconciler) instancesUsingPVC(ctx context.Context, obj client.Object) []reconcile.Request {
	var instances redisv1.RedisInstanceList
	if err := r.List(ctx, &instances, client.InNamespace(obj.GetNamespace())); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list RedisInstances for a PVC event", "pvc", obj.GetName())
		return nil
	}

	var requests []reconcile.Request
	for _, instance := range instances.Items {
		chosen := slices.ContainsFunc(instance.Spec.Storage.ExistingClaims, func(c redisv1.ExistingClaim) bool {
			return c.ClaimName == obj.GetName()
		})
		bound := slices.ContainsFunc(instance.Status.Volumes, func(v redisv1.VolumeStatus) bool {
			return v.ClaimName == obj.GetName()
		})
		if chosen || bound {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&instance)})
		}
	}
	return requests
}

// --- Helpers ---

// volumeOf returns the binding of the instance with the given ordinal. An
// instance without one is bound to the volume the spec chooses, still without
// a UID: reconcileVolume records it.
func volumeOf(instance *redisv1.RedisInstance, ordinal int32) *redisv1.VolumeStatus {
	if i := slices.IndexFunc(instance.Status.Volumes, func(v redisv1.VolumeStatus) bool { return v.Ordinal == ordinal }); i >= 0 {
		return &instance.Status.Volumes[i]
	}

	claimName, external := pvcName(instance, ordinal)
	instance.Status.Volumes = append(instance.Status.Volumes,
		redisv1.VolumeStatus{Ordinal: ordinal, ClaimName: claimName, External: external})
	slices.SortFunc(instance.Status.Volumes, func(a, b redisv1.VolumeStatus) int {
		return int(a.Ordinal - b.Ordinal)
	})
	return volumeOf(instance, ordinal)
}

func setVolumeState(vol *redisv1.VolumeStatus, state redisv1.VolumeState, message string) {
	vol.State = state
	vol.Message = message
}

// setStorageReadyCondition sets StorageReady: True when every instance has a
// Ready volume, otherwise False with the problems in the message.
func setStorageReadyCondition(instance *redisv1.RedisInstance, problems []string) {
	condition := metav1.Condition{
		Type:               redisv1.ConditionStorageReady,
		Status:             metav1.ConditionTrue,
		Reason:             "VolumesReady",
		Message:            "Every instance has its volume",
		ObservedGeneration: instance.Generation,
	}
	if len(problems) > 0 {
		condition.Status = metav1.ConditionFalse
		condition.Reason = "VolumeUnavailable"
		condition.Message = strings.Join(problems, "; ")
	}
	meta.SetStatusCondition(&instance.Status.Conditions, condition)
}

// getPVC returns the PVC, or nil when it does not exist.
func (r *RedisInstanceReconciler) getPVC(ctx context.Context, namespace, name string) (*corev1.PersistentVolumeClaim, error) {
	var pvc corev1.PersistentVolumeClaim
	err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &pvc)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &pvc, nil
}

// createDataPVC creates the generated PVC of the instance with the given
// ordinal, owned by the RedisInstance.
func (r *RedisInstanceReconciler) createDataPVC(
	ctx context.Context, instance *redisv1.RedisInstance, ordinal int32,
) (*corev1.PersistentVolumeClaim, error) {
	if instance.Spec.Storage.VolumeClaimTemplate == nil {
		return nil, fmt.Errorf("instance %d has no existing claim and storage.volumeClaimTemplate is not set", ordinal)
	}
	pvc := buildRedisPVC(instance, ordinal)
	if err := ctrl.SetControllerReference(instance, pvc, r.Scheme); err != nil {
		return nil, err
	}
	if err := r.Create(ctx, pvc); err != nil {
		return nil, err
	}
	logf.FromContext(ctx).Info("Created PVC", "name", pvc.Name)
	return pvc, nil
}

// removeAnnotation marks a request as handled, so it is not served twice.
func (r *RedisInstanceReconciler) removeAnnotation(ctx context.Context, instance *redisv1.RedisInstance, key string) error {
	if _, ok := instance.Annotations[key]; !ok {
		return nil
	}
	patch := client.MergeFrom(instance.DeepCopy())
	delete(instance.Annotations, key)
	return r.Patch(ctx, instance, patch)
}
