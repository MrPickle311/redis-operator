package controller

import (
	"context"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

// volumeAction is what the operator does about a volume failing under a
// running instance.
type volumeAction int

const (
	noVolumeAction volumeAction = iota
	// switchOverPrimary: the volume of the primary is going. A switchover
	// moves the primary to a replica without losing writes; the old primary
	// is then a replica, handled by the actions below.
	switchOverPrimary
	// deleteReplicaPod: a generated volume of a replica is being deleted. It
	// goes once its Pod does, and then a new one is generated, see
	// reconcileMissingPVC; the replica syncs again.
	deleteReplicaPod
	// recreateReplica: a generated volume of a replica lost its
	// PersistentVolume. The instance moves to a new generated PVC, like recreate.
	recreateReplica
)

// volumeFailureAction picks the instance to act on, the lowest ordinal first.
// It acts on one at a time, and only while every other instance is Ready, so
// at most one instance is down for its volume. External volumes of replicas
// belong to the user: they are only reported, in StorageReady.
func volumeFailureAction(instance *redisv1.RedisInstance, ready map[int32]bool) (int32, volumeAction) {
	primary := primaryOrdinal(instance)
	for ordinal := range instance.Spec.Instances {
		i := slices.IndexFunc(instance.Status.Volumes, func(v redisv1.VolumeStatus) bool { return v.Ordinal == ordinal })
		if i < 0 {
			continue
		}
		vol := instance.Status.Volumes[i]
		failing := vol.State == redisv1.VolumeTerminating || vol.State == redisv1.VolumeLost
		if !failing || !othersReady(instance, ready, ordinal) {
			continue
		}
		switch {
		case vol.Ordinal == primary:
			if ready[primary] { // a switchover needs it; a failed one takes a failover
				return primary, switchOverPrimary
			}
		case vol.External:
		case vol.State == redisv1.VolumeTerminating:
			return vol.Ordinal, deleteReplicaPod
		default:
			return vol.Ordinal, recreateReplica
		}
	}
	return 0, noVolumeAction
}

// othersReady reports whether every instance but ordinal is Ready.
func othersReady(instance *redisv1.RedisInstance, ready map[int32]bool, ordinal int32) bool {
	for o := range instance.Spec.Instances {
		if o != ordinal && !ready[o] {
			return false
		}
	}
	return true
}

// reconcileVolumeFailures acts on a volume that fails under a running
// instance, see volumeAction. It waits while the primary moves.
func (r *RedisInstanceReconciler) reconcileVolumeFailures(ctx context.Context, instance *redisv1.RedisInstance, ready map[int32]bool) error {
	if instance.Status.TargetPrimary != "" || meta.IsStatusConditionTrue(instance.Status.Conditions, redisv1.ConditionFailover) {
		return nil
	}
	if _, requested := pendingRequest(instance, redisv1.AnnotationPromote); requested {
		return nil // the switchover asked by the user moves the primary anyway
	}

	ordinal, action := volumeFailureAction(instance, ready)
	name := instancePodName(instance, ordinal)
	claim := volumeOf(instance, ordinal).ClaimName
	logger := logf.FromContext(ctx)
	switch action {
	case switchOverPrimary:
		target, ok := r.failoverCandidate(ctx, instance)
		if !ok {
			return nil
		}
		r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, "PrimaryVolumeFailing", "Switchover",
			"Switching over to %s: PVC %s of the primary %s is %s", target, claim, name, volumeOf(instance, ordinal).State)
		return r.startSwitchover(ctx, instance, target)
	case deleteReplicaPod:
		r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, "ReplicaVolumeFailing", "DeletePod",
			"Deleting Pod %s: its PVC %s is being deleted, a new one follows", name, claim)
		logger.Info("Deleting a replica whose PVC is being deleted", "pod", name, "pvc", claim)
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: instance.Namespace}}
		return client.IgnoreNotFound(r.Delete(ctx, pod))
	case recreateReplica:
		r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, "ReplicaVolumeFailing", "Recreate",
			"Recreating %s on a new generated PVC: PVC %s lost its volume", name, claim)
		logger.Info("Recreating a replica whose PVC lost its volume", "pod", name, "pvc", claim)
		if err := r.moveToNewVolume(ctx, instance, ordinal); err != nil {
			return err
		}
		return r.Status().Update(ctx, instance)
	}
	return nil
}
