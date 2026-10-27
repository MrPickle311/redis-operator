package controller

import (
	"context"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
	// PersistentVolume, or its node (spec.storage.recreateOnNodeLoss). The
	// instance moves to a new generated PVC, like recreate.
	recreateReplica
)

// volumeFailureAction picks the instance to act on, the lowest ordinal first.
// It acts on one at a time, and only while every other instance is Ready, so
// at most one instance is down for its volume. External volumes of replicas
// belong to the user: they are only reported, in StorageReady.
//
// nodeLost tells which volumes are local to a node that is gone.
func volumeFailureAction(instance *redisv1.RedisInstance, ready, nodeLost map[int32]bool) (int32, volumeAction) {
	primary := primaryOrdinal(instance)
	for ordinal := range instance.Spec.Instances {
		i := slices.IndexFunc(instance.Status.Volumes, func(v redisv1.VolumeStatus) bool { return v.Ordinal == ordinal })
		if i < 0 {
			continue
		}
		vol := instance.Status.Volumes[i]
		failing := vol.State == redisv1.VolumeTerminating || vol.State == redisv1.VolumeLost || nodeLost[ordinal]
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

	nodeLost := map[int32]bool{}
	if instance.Spec.Storage.RecreateOnNodeLoss {
		for _, vol := range instance.Status.Volumes {
			if !vol.External && vol.Ordinal != primaryOrdinal(instance) && vol.Ordinal < instance.Spec.Instances {
				nodeLost[vol.Ordinal] = r.volumeNodeLost(ctx, instance, vol)
			}
		}
	}
	ordinal, action := volumeFailureAction(instance, ready, nodeLost)
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
		why := "PVC " + claim + " lost its volume"
		if nodeLost[ordinal] {
			why = "the node of PVC " + claim + " is lost"
		}
		r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, "ReplicaVolumeFailing", "Recreate",
			"Recreating %s on a new generated PVC: %s", name, why)
		logger.Info("Recreating a replica whose PVC lost its volume", "pod", name, "pvc", claim)
		if err := r.moveToNewVolume(ctx, instance, ordinal); err != nil {
			return err
		}
		return r.Status().Update(ctx, instance)
	}
	return nil
}

// volumeNodeLost reports whether the volume is local to a node that is gone,
// or that an administrator marked as shut down with the out-of-service taint.
// Then the volume cannot come back, and the Pod waits on it for good.
func (r *RedisInstanceReconciler) volumeNodeLost(ctx context.Context, instance *redisv1.RedisInstance, vol redisv1.VolumeStatus) bool {
	var pvc corev1.PersistentVolumeClaim
	if err := r.Get(ctx, types.NamespacedName{Name: vol.ClaimName, Namespace: instance.Namespace}, &pvc); err != nil || pvc.Spec.VolumeName == "" {
		return false
	}
	var pv corev1.PersistentVolume
	if err := r.Get(ctx, types.NamespacedName{Name: pvc.Spec.VolumeName}, &pv); err != nil {
		return false
	}
	node := localNode(&pv)
	if node == "" {
		return false // not a local volume: it follows the Pod to another node
	}
	var n corev1.Node
	if err := r.Get(ctx, types.NamespacedName{Name: node}, &n); err != nil {
		return apierrors.IsNotFound(err)
	}
	return slices.ContainsFunc(n.Spec.Taints, func(t corev1.Taint) bool { return t.Key == corev1.TaintNodeOutOfService })
}

// localNode returns the node a local PersistentVolume is bound to by its
// node affinity on the hostname, or "" for a volume any node can mount.
func localNode(pv *corev1.PersistentVolume) string {
	if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
		return ""
	}
	for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
		for _, expr := range term.MatchExpressions {
			if expr.Key == corev1.LabelHostname && expr.Operator == corev1.NodeSelectorOpIn && len(expr.Values) == 1 {
				return expr.Values[0]
			}
		}
	}
	return ""
}
