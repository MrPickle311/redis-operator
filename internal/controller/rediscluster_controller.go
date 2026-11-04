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
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/imclient"
	"github.com/MrPickle311/redis-operator/internal/instancemanager"
	"github.com/MrPickle311/redis-operator/internal/pki"
)

// RedisClusterReconciler reconciles a RedisCluster object
type RedisClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// InstanceManager builds the cluster through the Instance Managers.
	InstanceManager *imclient.Client
	// Recorder emits the Events of RedisClusters.
	Recorder events.EventRecorder
}

// +kubebuilder:rbac:groups=redis.operator.com,resources=redisclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=redis.operator.com,resources=redisclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=redis.operator.com,resources=redisclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=redis.operator.com,resources=redisinstances,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

// Reconcile keeps a RedisInstance per shard and the seed Service, and builds
// the Redis Cluster out of their Pods: meeting the nodes, handing out the
// slots and making replicas. After a change of spec.shards it moves slots
// until every shard serves its share, and deletes the shards a decrease
// removes once empty. Failovers are Redis Cluster's own.
func (r *RedisClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logf.FromContext(ctx)

	var cluster redisv1.RedisCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	oldStatus := cluster.Status.DeepCopy()

	// Before the shards: their certificates come from it.
	if cluster.Spec.TLS.TLSEnabled() {
		if err := r.ensureCA(ctx, &cluster); err != nil {
			return ctrl.Result{}, err
		}
	}
	// Before the shards too: they share it, see ensureInternalAuthSecret.
	if err := ensureInternalAuth(ctx, r.Client, r.Scheme, &cluster, clusterInternalAuthSecretName(cluster.Name)); err != nil {
		return ctrl.Result{}, err
	}
	shardsReady := true
	for shard := range cluster.Spec.Shards {
		child, err := r.ensureShard(ctx, &cluster, shard)
		if err != nil {
			return ctrl.Result{}, err
		}
		shardsReady = shardsReady && meta.IsStatusConditionTrue(child.Status.Conditions, redisv1.ConditionReady)
	}
	// The shards a decrease of spec.shards removes stay until they are empty.
	total := cluster.Spec.Shards
	for ; ; total++ {
		var child redisv1.RedisInstance
		err := r.Get(ctx, types.NamespacedName{Name: shardName(&cluster, total), Namespace: cluster.Namespace}, &child)
		if apierrors.IsNotFound(err) || (err == nil && !metav1.IsControlledBy(&child, &cluster)) {
			break
		} else if err != nil {
			return ctrl.Result{}, err
		}
	}
	if err := r.ensureSeedService(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}

	nodes := r.clusterNodes(ctx, &cluster, total)
	actions := planCluster(nodes, cluster.Spec.Shards, cluster.Spec.InstancesPerShard, assignedShards(&cluster))
	for _, action := range actions {
		changed, err := r.InstanceManager.Cluster(ctx, action.host, action.command, action.req)
		if err != nil {
			logger.Error(err, "Failed to run cluster command", "command", action.command, "node", action.host)
		} else if changed {
			logger.Info("Ran cluster command", "command", action.command, "node", action.host)
		}
	}

	// Once built: the slots move to masters with their replicas.
	if len(actions) == 0 {
		nodes = r.moveSlots(ctx, &cluster, nodes, total)
		if err := r.removeDrainedShards(ctx, &cluster, nodes, total); err != nil {
			return ctrl.Result{}, err
		}
	}

	setClusterStatus(&cluster, nodes, total, shardsReady)
	// An Event per change of Migrating marks when a resharding starts, waits and ends.
	before := meta.FindStatusCondition(oldStatus.Conditions, redisv1.ConditionMigrating)
	if now := meta.FindStatusCondition(cluster.Status.Conditions, redisv1.ConditionMigrating); now != nil &&
		(before != nil && before.Reason != now.Reason || before == nil && now.Reason != "Balanced") {
		eventType := corev1.EventTypeNormal
		if now.Reason == "Blocked" {
			eventType = corev1.EventTypeWarning
		}
		r.Recorder.Eventf(&cluster, nil, eventType, "Resharding"+now.Reason, "Reshard", "%s", now.Message)
	}
	if !equality.Semantic.DeepEqual(oldStatus, &cluster.Status) {
		if err := r.Status().Update(ctx, &cluster); err != nil {
			return ctrl.Result{}, err
		}
	}
	if cluster.Status.SlotMigration != nil {
		return ctrl.Result{RequeueAfter: reshardRequeue}, nil
	}
	if len(actions) > 0 || !meta.IsStatusConditionTrue(cluster.Status.Conditions, redisv1.ConditionReady) {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	// Redis Cluster fails over on its own, without an event of Kubernetes.
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// shardName returns the RedisInstance of the shard.
func shardName(cluster *redisv1.RedisCluster, shard int32) string {
	return fmt.Sprintf("%s-%d", cluster.Name, shard)
}

// ensureCA creates the CA every shard issues the certificate of Redis from.
func (r *RedisClusterReconciler) ensureCA(ctx context.Context, cluster *redisv1.RedisCluster) error {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: clusterCASecretName(cluster.Name), Namespace: cluster.Namespace}}
	err := r.Get(ctx, client.ObjectKeyFromObject(secret), secret)
	if !apierrors.IsNotFound(err) {
		return err
	}
	ca, err := pki.NewCA(fmt.Sprintf("%s/%s Redis CA", cluster.Namespace, cluster.Name), instanceCAValidity)
	if err != nil {
		return err
	}
	secret.Data = map[string][]byte{caCertKey: ca.CertPEM, caKeyKey: ca.KeyPEM}
	if err := ctrl.SetControllerReference(cluster, secret, r.Scheme); err != nil {
		return err
	}
	logf.FromContext(ctx).Info("Creating the CA of the TLS of Redis", "name", secret.Name)
	return ignoreAlreadyExists(r.Create(ctx, secret))
}

// ensureShard makes the RedisInstance of the shard follow the spec of the
// cluster. The fields of a RedisInstance a RedisCluster has no counterpart
// for keep their defaults.
func (r *RedisClusterReconciler) ensureShard(ctx context.Context, cluster *redisv1.RedisCluster, shard int32) (*redisv1.RedisInstance, error) {
	child := &redisv1.RedisInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:      shardName(cluster, shard),
			Namespace: cluster.Namespace,
		}}
	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, child, func() error {
		// Never adopt a RedisInstance of its own: it would turn into a node
		// of the cluster, data and all.
		if child.ResourceVersion != "" && !metav1.IsControlledBy(child, cluster) {
			return fmt.Errorf("RedisInstance %s exists and is not a shard of RedisCluster %s", child.Name, cluster.Name)
		}
		spec := &child.Spec
		spec.Instances = cluster.Spec.InstancesPerShard
		spec.Image = cluster.Spec.Image
		spec.Storage = cluster.Spec.Storage
		spec.ACL = cluster.Spec.ACL
		spec.Resources = cluster.Spec.Resources
		spec.Redis = cluster.Spec.Redis
		spec.Affinity = cluster.Spec.Affinity
		spec.TLS = cluster.Spec.TLS
		spec.NetworkPolicy = cluster.Spec.NetworkPolicy
		spec.Monitoring = cluster.Spec.Monitoring
		return ctrl.SetControllerReference(cluster, child, r.Scheme)
	})
	if result != controllerutil.OperationResultNone {
		logf.FromContext(ctx).Info("Reconciled RedisInstance of shard", "name", child.Name, "operation", result)
	}
	return child, err
}

// ensureSeedService creates the Service <name> over every node: the one
// address clients of the cluster start from.
func (r *RedisClusterReconciler) ensureSeedService(ctx context.Context, cluster *redisv1.RedisCluster) error {
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: cluster.Name, Namespace: cluster.Namespace}}
	err := r.Get(ctx, client.ObjectKeyFromObject(service), service)
	if !apierrors.IsNotFound(err) {
		return err
	}
	service.Spec = corev1.ServiceSpec{
		Type:     corev1.ServiceTypeClusterIP,
		Selector: map[string]string{labelCluster: cluster.Name},
		Ports:    []corev1.ServicePort{{Port: redisPort, TargetPort: intstr.FromInt32(redisPort), Name: appName}},
	}
	if err := ctrl.SetControllerReference(cluster, service, r.Scheme); err != nil {
		return err
	}
	logf.FromContext(ctx).Info("Creating Service", "name", service.Name)
	return ignoreAlreadyExists(r.Create(ctx, service))
}

// clusterNodes returns every node of the first total shards, ordered by shard
// and ordinal, with the cluster status of those that answer.
func (r *RedisClusterReconciler) clusterNodes(ctx context.Context, cluster *redisv1.RedisCluster, total int32) []clusterNode {
	var nodes []clusterNode
	for shard := range total {
		instance := shardName(cluster, shard)
		for ordinal := range cluster.Spec.InstancesPerShard {
			node := clusterNode{shard: shard, pod: fmt.Sprintf("%s-%d", instance, ordinal)}
			node.host = instancemanager.PodFQDN(node.pod, instance, cluster.Namespace)
			var pod corev1.Pod
			err := r.Get(ctx, types.NamespacedName{Name: node.pod, Namespace: cluster.Namespace}, &pod)
			if err == nil && pod.Status.Phase == corev1.PodRunning && pod.Status.PodIP != "" && pod.DeletionTimestamp.IsZero() {
				if status, err := r.InstanceManager.Status(ctx, node.host); err == nil {
					node.status = status.Cluster
				} else {
					logf.FromContext(ctx).V(1).Info("Instance Manager not reachable yet", "pod", node.pod, "reason", err.Error())
				}
			}
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// assignedShards returns the shards that had slots once, by status.shards.
func assignedShards(cluster *redisv1.RedisCluster) map[int32]bool {
	assigned := map[int32]bool{}
	for shard := range cluster.Spec.Shards {
		for _, s := range cluster.Status.Shards {
			if s.Name == shardName(cluster, shard) && s.Slots != "" {
				assigned[shard] = true
			}
		}
	}
	return assigned
}

// setClusterStatus records the master and the slots of every one of the first
// total shards, the state of the cluster and the conditions SlotsCovered and
// Ready. A shard without a master keeps the slots it had, see planCluster.
func setClusterStatus(cluster *redisv1.RedisCluster, nodes []clusterNode, total int32, shardsReady bool) {
	previous := map[string]string{}
	for _, s := range cluster.Status.Shards {
		previous[s.Name] = s.Slots
	}
	cluster.Status.Shards = nil
	cluster.Status.ClusterState = ""
	var served []string
	for shard := range total {
		status := redisv1.ShardStatus{Name: shardName(cluster, shard), Slots: previous[shardName(cluster, shard)]}
		if master := shardMaster(shardNodes(nodes, shard)); master != nil {
			status.Primary, status.Slots = master.pod, strings.Join(master.status.Slots, ",")
			served = append(served, master.status.Slots...)
			// A master tells the state of the cluster as it sees it.
			if cluster.Status.ClusterState == "" {
				cluster.Status.ClusterState = master.status.State
			}
		}
		cluster.Status.Shards = append(cluster.Status.Shards, status)
	}

	covered := metav1.Condition{
		Type:               redisv1.ConditionSlotsCovered,
		Status:             metav1.ConditionTrue,
		Reason:             "Covered",
		Message:            "Masters serve all 16384 slots",
		ObservedGeneration: cluster.Generation,
	}
	if uncovered := clusterSlots - countSlots(served); uncovered > 0 {
		covered.Status, covered.Reason = metav1.ConditionFalse, "Uncovered"
		covered.Message = fmt.Sprintf("%d of %d slots have no master", uncovered, clusterSlots)
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, covered)

	ready := metav1.Condition{
		Type:               redisv1.ConditionReady,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: cluster.Generation,
	}
	switch {
	case covered.Status == metav1.ConditionFalse:
		ready.Reason, ready.Message = "SlotsUncovered", "Some slots have no master, see the condition SlotsCovered"
	case cluster.Status.ClusterState != "ok":
		ready.Reason, ready.Message = "ClusterFail", "Redis Cluster reports the state "+cluster.Status.ClusterState
	case cluster.Status.SlotMigration != nil:
		ready.Reason, ready.Message = "Resharding", "Slots move between shards, see the condition Migrating"
	case !shardsReady:
		ready.Reason, ready.Message = "ShardsNotReady", "A RedisInstance of a shard is not Ready"
	default:
		ready.Status, ready.Reason, ready.Message = metav1.ConditionTrue, "Ready", "The cluster is ok and every shard is Ready"
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, ready)
}

// clusterOfPod maps a Pod of a shard to its RedisCluster.
func clusterOfPod(_ context.Context, obj client.Object) []reconcile.Request {
	name := obj.GetLabels()[labelCluster]
	if name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: name}}}
}

// SetupWithManager sets up the controller with the Manager.
func (r *RedisClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&redisv1.RedisCluster{}).
		Owns(&redisv1.RedisInstance{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.Secret{}).
		// The Pods belong to the shards; a new one may need to join.
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(clusterOfPod)).
		Named("rediscluster").
		Complete(r)
}
