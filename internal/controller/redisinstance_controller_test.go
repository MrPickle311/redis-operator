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
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

const (
	testNamespace     = "default"
	testOperatorImage = "example.com/redis-operator:test"
)

var _ = Describe("RedisInstance controller", func() {
	Context("when reconciling a RedisInstance", func() {
		const name = "reconcile-test"

		BeforeEach(func() {
			By("creating a RedisInstance with a primary and one replica")
			instance := newRedisInstance(name, 2, redisv1.StorageSpec{
				VolumeClaimTemplate: volumeClaimTemplate(),
			})
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())

			By("running a single reconcile")
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
		})

		AfterEach(func() {
			// envtest has no garbage collector: Pods and Services owned by the
			// RedisInstance stay behind and are reused by the next reconcile.
			instance := &redisv1.RedisInstance{}
			Expect(k8sClient.Get(ctx, objectKey(name), instance)).To(Succeed())
			Expect(k8sClient.Delete(ctx, instance)).To(Succeed())
		})

		It("creates a headless Service that publishes not-ready Pods", func() {
			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, objectKey(name+"-hl"), svc)).To(Succeed())

			Expect(svc.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
			Expect(svc.Spec.PublishNotReadyAddresses).To(BeTrue())
		})

		It("gives the Pod a stable DNS name through hostname and subdomain", func() {
			pod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, objectKey(name+"-0"), pod)).To(Succeed())

			Expect(pod.Spec.Hostname).To(Equal(name + "-0"))
			Expect(pod.Spec.Subdomain).To(Equal(name + "-hl"))
		})

		It("runs redis-server under the Instance Manager copied by an initContainer", func() {
			pod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, objectKey(name+"-0"), pod)).To(Succeed())

			Expect(pod.Spec.InitContainers).To(HaveLen(1))
			bootstrap := pod.Spec.InitContainers[0]
			Expect(bootstrap.Image).To(Equal(testOperatorImage))
			Expect(bootstrap.Command).To(Equal([]string{"/manager", "bootstrap", "/controller/manager"}))

			redis := pod.Spec.Containers[0]
			Expect(redis.Command).To(Equal([]string{"/controller/manager", "instance", "run",
				"--instance=" + name, "--databases=16", "--io-threads=1"}))

			By("sharing the binary through an emptyDir mounted in both containers")
			Expect(pod.Spec.Volumes).To(ContainElement(HaveField("Name", "controller")))
			for _, c := range []corev1.Container{bootstrap, redis} {
				Expect(c.VolumeMounts).To(ContainElement(HaveField("MountPath", "/controller")), "container %s", c.Name)
			}
		})

		It("probes the Instance Manager and does not automount the ServiceAccount token", func() {
			pod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, objectKey(name+"-0"), pod)).To(Succeed())
			redis := pod.Spec.Containers[0]

			Expect(redis.ReadinessProbe.HTTPGet.Path).To(Equal("/readyz"))
			Expect(redis.LivenessProbe.HTTPGet.Path).To(Equal("/healthz"))
			for _, probe := range []*corev1.Probe{redis.ReadinessProbe, redis.LivenessProbe} {
				Expect(probe.HTTPGet.Port.IntValue()).To(Equal(8001))
			}

			Expect(pod.Spec.AutomountServiceAccountToken).To(HaveValue(BeFalse()))
		})

		It("labels the first Pod as primary and the others as replicas", func() {
			for pod, role := range map[string]string{name + "-0": rolePrimary, name + "-1": roleReplica} {
				Expect(podLabels(objectKey(pod))).To(HaveKeyWithValue(labelRole, role), "Pod %s", pod)
			}
		})

		It("owns the PVCs it creates, so they are deleted with the RedisInstance", func() {
			pvc := &corev1.PersistentVolumeClaim{}
			Expect(k8sClient.Get(ctx, objectKey(name+"-0-data"), pvc)).To(Succeed())
			Expect(metav1.GetControllerOf(pvc)).NotTo(BeNil())
			Expect(metav1.GetControllerOf(pvc).Name).To(Equal(name))
		})

		DescribeTable("creates a client Service per access mode",
			func(suffix string, wantSelector map[string]string) {
				svc := &corev1.Service{}
				Expect(k8sClient.Get(ctx, objectKey(name+suffix), svc)).To(Succeed())

				Expect(svc.Spec.Type).To(Equal(corev1.ServiceTypeClusterIP))
				Expect(svc.Spec.Selector).To(Equal(wantSelector))
			},
			Entry("-rw sends reads and writes to the primary", "-rw",
				map[string]string{"redis.operator.com/instance": name, "redis.operator.com/role": "primary"}),
			Entry("-ro sends reads to replicas only", "-ro",
				map[string]string{"redis.operator.com/instance": name, "redis.operator.com/role": "replica"}),
			Entry("-r sends reads to every instance", "-r",
				map[string]string{"redis.operator.com/instance": name}),
		)
	})

	Context("with storage.existingClaims", func() {
		const name = "existing-claims-test"

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, newPVC("pvc-0"))).To(Succeed())
			instance := newRedisInstance(name, 1, redisv1.StorageSpec{ExistingClaims: existingClaims(0)})
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
		})

		AfterEach(func() {
			Expect(k8sClient.Delete(ctx, newRedisInstance(name, 1, redisv1.StorageSpec{}))).To(Succeed())
			deletePVCNow("pvc-0")
		})

		It("mounts the external PVC and does not create one of its own", func() {
			pod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, objectKey(name+"-0"), pod)).To(Succeed())
			Expect(dataClaimName(pod)).To(Equal("pvc-0"))

			Expect(pvcDeleted(name+"-0-data")).To(BeTrue(), "operator created a PVC although an external one was given")
		})
	})

	// Ordered with BeforeAll: envtest has no garbage collector, so Pods and PVCs
	// from a second run of the setup would collide with the first one.
	Context("when scaling down", Ordered, func() {
		const name = "scale-down-test"

		BeforeAll(func() {
			By("pre-creating a PVC for ordinal 2 that the operator does not own")
			Expect(k8sClient.Create(ctx, newPVC(name+"-2-data"))).To(Succeed())

			By("creating 3 instances")
			instance := newRedisInstance(name, 3, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()})
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())

			By("scaling down to 1 instance")
			Expect(k8sClient.Get(ctx, objectKey(name), instance)).To(Succeed())
			instance.Spec.Instances = 1
			Expect(k8sClient.Update(ctx, instance)).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
		})

		AfterAll(func() {
			Expect(k8sClient.Delete(ctx, newRedisInstance(name, 1, redisv1.StorageSpec{}))).To(Succeed())
		})

		It("deletes the Pods of removed instances", func() {
			for _, pod := range []string{name + "-1", name + "-2"} {
				err := k8sClient.Get(ctx, objectKey(pod), &corev1.Pod{})
				Expect(errors.IsNotFound(err)).To(BeTrue(), "Pod %s still exists", pod)
			}
		})

		It("deletes the PVC it created for a removed instance", func() {
			Expect(pvcDeleted(name + "-1-data")).To(BeTrue())
		})

		It("keeps a PVC it does not own, even when the name matches", func() {
			Expect(pvcDeleted(name + "-2-data")).To(BeFalse())
		})
	})

	// Ordered with BeforeAll for the same reason as the scale-down specs.
	Context("when status.currentPrimary points to another Pod", Ordered, func() {
		const name = "moved-primary-test"

		BeforeAll(func() {
			instance := newRedisInstance(name, 2, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()})
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())

			By("moving the primary to ordinal 1, as a switchover does")
			Expect(k8sClient.Get(ctx, objectKey(name), instance)).To(Succeed())
			instance.Status.CurrentPrimary = name + "-1"
			Expect(k8sClient.Status().Update(ctx, instance)).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())
		})

		AfterAll(func() {
			Expect(k8sClient.Delete(ctx, newRedisInstance(name, 1, redisv1.StorageSpec{}))).To(Succeed())
		})

		It("moves the role labels, and with them the -rw and -ro Services", func() {
			for pod, role := range map[string]string{name + "-0": roleReplica, name + "-1": rolePrimary} {
				Expect(podLabels(objectKey(pod))).To(HaveKeyWithValue(labelRole, role), "Pod %s", pod)
			}
		})

		It("does not delete the primary when scaling down below its ordinal", func() {
			instance := &redisv1.RedisInstance{}
			Expect(k8sClient.Get(ctx, objectKey(name), instance)).To(Succeed())
			instance.Spec.Instances = 1
			Expect(k8sClient.Update(ctx, instance)).To(Succeed())

			recorder := events.NewFakeRecorder(10)
			Expect(reconcileWith(recorder, objectKey(name))).To(Succeed())

			Expect(k8sClient.Get(ctx, objectKey(name+"-1"), &corev1.Pod{})).To(Succeed(), "the primary Pod was deleted")
			Expect(pvcDeleted(name+"-1-data")).To(BeFalse(), "the PVC of the primary was deleted")
			Expect(volume(getInstance(name), 1).State).To(Equal(redisv1.VolumeReady), "the primary lost its volume binding")
			Expect(recorder.Events).To(Receive(ContainSubstring("ScaleDownBlocked")))
		})
	})

	// Ordered with BeforeAll for the same reason as the scale-down specs.
	Context("with a promote annotation that cannot be served", Ordered, func() {
		const name = "switchover-rejected-test"

		BeforeAll(func() {
			instance := newRedisInstance(name, 2, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()})
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			Expect(reconcileOnce(objectKey(name))).To(Succeed())

			// envtest runs no kubelet, so no Pod gets an IP and the operator
			// never sets the primary itself.
			Expect(k8sClient.Get(ctx, objectKey(name), instance)).To(Succeed())
			instance.Status.CurrentPrimary = name + "-0"
			instance.Status.PrimaryEpoch = 1
			Expect(k8sClient.Status().Update(ctx, instance)).To(Succeed())
		})

		AfterAll(func() {
			Expect(k8sClient.Delete(ctx, newRedisInstance(name, 1, redisv1.StorageSpec{}))).To(Succeed())
		})

		DescribeTable("rejects the switchover once and keeps the annotation",
			func(target, wantReason string) {
				instance := &redisv1.RedisInstance{}
				Expect(k8sClient.Get(ctx, objectKey(name), instance)).To(Succeed())
				instance.Annotations = map[string]string{redisv1.AnnotationPromote: target}
				Expect(k8sClient.Update(ctx, instance)).To(Succeed())

				recorder := events.NewFakeRecorder(10)
				Expect(reconcileWith(recorder, objectKey(name))).To(Succeed())

				Expect(recorder.Events).To(Receive(SatisfyAll(
					ContainSubstring("SwitchoverRejected"), ContainSubstring(wantReason))))
				Expect(k8sClient.Get(ctx, objectKey(name), instance)).To(Succeed())
				Expect(instance.Annotations).To(HaveKeyWithValue(redisv1.AnnotationPromote, target))
				Expect(instance.Status.HandledRequests).To(HaveKeyWithValue(redisv1.AnnotationPromote, target))
				Expect(instance.Status.CurrentPrimary).To(Equal(name + "-0"))
				Expect(instance.Status.TargetPrimary).To(BeEmpty())
				Expect(instance.Status.PrimaryEpoch).To(Equal(int64(1)), "a rejected switchover must not take an epoch")

				Expect(reconcileWith(recorder, objectKey(name))).To(Succeed())
				Expect(recorder.Events).NotTo(Receive(), "a handled request was served again")
			},
			Entry("a replica that is not Ready", name+"-1", "not Ready"),
			Entry("a Pod of no instance", name+"-5", "not an instance"),
			Entry("the current primary", name+"-0", "already the primary"),
		)
	})

	It("defaults spec.failover to a 30s delay with fencing on API loss", func() {
		instance := newRedisInstance("failover-default", 1, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()})
		Expect(k8sClient.Create(ctx, instance)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, instance)).To(Succeed()) })
		Expect(instance.Spec.Failover.Delay.Duration).To(Equal(30 * time.Second))
		Expect(instance.Spec.Failover.FenceOnAPILoss).To(HaveValue(BeTrue()))

		instance.Spec.Failover.Delay.Duration = time.Second
		err := k8sClient.Update(ctx, instance)
		Expect(errors.IsInvalid(err)).To(BeTrue(), "a delay shorter than the poll of the Instance Manager was accepted: %v", err)
	})

	It("defaults spec.redis.persistence to aof", func() {
		instance := newRedisInstance("persistence-default", 1, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()})
		Expect(k8sClient.Create(ctx, instance)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, instance)).To(Succeed()) })
		Expect(instance.Spec.Redis.Persistence).To(Equal(redisv1.PersistenceAOF))
		Expect(instance.Spec.Redis.Startup).To(Equal(redisv1.RedisStartupSpec{Databases: 16, IOThreads: 1}),
			"the startup options must default to those of Redis")

		instance.Spec.Redis.Startup.IOThreads = 200
		Expect(errors.IsInvalid(k8sClient.Update(ctx, instance))).To(BeTrue(), "more I/O threads than Redis allows were accepted")
		instance.Spec.Redis.Startup.IOThreads = 1

		instance.Spec.Redis.Persistence = "disk"
		Expect(errors.IsInvalid(k8sClient.Update(ctx, instance))).To(BeTrue(), "an unknown persistence mode was accepted")
	})

	DescribeTable("validating spec.redis.parameters with CEL rules",
		func(parameters map[string]redisv1.ParameterValue, wantError string) {
			instance := newRedisInstance("cel-parameters", 1, redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate()})
			instance.Spec.Redis.Parameters = parameters
			err := k8sClient.Create(ctx, instance)

			if wantError == "" {
				Expect(err).NotTo(HaveOccurred())
				Expect(k8sClient.Delete(ctx, instance)).To(Succeed())
				return
			}
			Expect(errors.IsInvalid(err)).To(BeTrue(), "expected an Invalid error, got: %v", err)
			Expect(err.Error()).To(ContainSubstring(wantError))
		},
		Entry("accepts options the operator leaves to the user",
			map[string]redisv1.ParameterValue{"hz": "20", "notify-keyspace-events": "Ex", "appendfsync": "always", "maxmemory-samples": "10"}, ""),
		Entry("names every option the operator manages",
			map[string]redisv1.ParameterValue{"hz": "20", "port": "7000"}, "options the operator manages"),
		Entry("rejects the memory options, which have their own fields",
			map[string]redisv1.ParameterValue{"maxmemory": "1gb", "maxmemory-policy": "allkeys-lru"}, "options the operator manages"),
		Entry("rejects replication, users and files",
			map[string]redisv1.ParameterValue{"replicaof": "evil 6379", "requirepass": "x", "dir": "/tmp"}, "options the operator manages"),
		Entry("rejects persistence, TLS and cluster options",
			map[string]redisv1.ParameterValue{"appendonly": "no", "tls-port": "6380", "cluster-enabled": "yes"}, "options the operator manages"),
		Entry("rejects the commands that run code or reconfigure Redis",
			map[string]redisv1.ParameterValue{"enable-debug-command": "yes", "enable-module-command": "yes", "enable-protected-configs": "yes"}, "options the operator manages"),
		Entry("rejects a value spanning two lines",
			map[string]redisv1.ParameterValue{"hz": "20\nuser default on nopass"}, "should match"),
		Entry("sends the options that need a restart to spec.redis.startup",
			map[string]redisv1.ParameterValue{"databases": "32", "io-threads": "4"}, "spec.redis.startup"),
		Entry("rejects names that are not options",
			map[string]redisv1.ParameterValue{"Hz": "20"}, "lowercase"),
	)

	DescribeTable("validating spec.storage with CEL rules",
		func(storage redisv1.StorageSpec, accepted bool) {
			instance := newRedisInstance("cel-test", 2, storage)
			err := k8sClient.Create(ctx, instance)

			if accepted {
				Expect(err).NotTo(HaveOccurred())
				Expect(k8sClient.Delete(ctx, instance)).To(Succeed())
				return
			}
			Expect(errors.IsInvalid(err)).To(BeTrue(), "expected an Invalid error, got: %v", err)
		},
		Entry("rejects a missing volumeClaimTemplate when existingClaims do not cover every instance",
			redisv1.StorageSpec{ExistingClaims: existingClaims(0)},
			false),
		Entry("accepts an existingClaims ordinal reserved for scaling up",
			redisv1.StorageSpec{VolumeClaimTemplate: volumeClaimTemplate(), ExistingClaims: existingClaims(5)},
			true),
		Entry("rejects existingClaims that cover the count but not the ordinals of every instance",
			redisv1.StorageSpec{ExistingClaims: existingClaims(0, 5)},
			false),
		Entry("accepts existingClaims covering every instance without a volumeClaimTemplate",
			redisv1.StorageSpec{ExistingClaims: existingClaims(0, 1)},
			true),
	)
})

// dataClaimName returns the PVC mounted as the "data" volume of the Pod.
func dataClaimName(pod *corev1.Pod) string {
	for _, v := range pod.Spec.Volumes {
		if v.Name == "data" && v.PersistentVolumeClaim != nil {
			return v.PersistentVolumeClaim.ClaimName
		}
	}
	return ""
}

// pvcDeleted reports whether the PVC is gone or being deleted. envtest runs no
// controller that removes the pvc-protection finalizer, so a deleted PVC may linger.
func pvcDeleted(name string) bool {
	pvc := &corev1.PersistentVolumeClaim{}
	err := k8sClient.Get(ctx, objectKey(name), pvc)
	if errors.IsNotFound(err) {
		return true
	}
	Expect(err).NotTo(HaveOccurred())
	return pvc.DeletionTimestamp != nil
}

// newPVC returns a PVC that is not owned by any RedisInstance.
func newPVC(name string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}
}

func podLabels(key types.NamespacedName) map[string]string {
	pod := &corev1.Pod{}
	Expect(k8sClient.Get(ctx, key, pod)).To(Succeed())
	return pod.Labels
}

// reconcileOnce runs the reconciler for the given RedisInstance exactly once.
// It drops the Events: a FakeRecorder without a channel discards them.
func reconcileOnce(key types.NamespacedName) error {
	return reconcileWith(&events.FakeRecorder{}, key)
}

// reconcileWith is reconcileOnce with the given Event recorder.
func reconcileWith(recorder events.EventRecorder, key types.NamespacedName) error {
	r := &RedisInstanceReconciler{
		Client: k8sClient, Scheme: k8sClient.Scheme(), OperatorImage: testOperatorImage, CA: testCA, Recorder: recorder,
	}
	_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
	return err
}

func objectKey(name string) types.NamespacedName {
	return types.NamespacedName{Name: name, Namespace: testNamespace}
}

func newRedisInstance(name string, instances int32, storage redisv1.StorageSpec) *redisv1.RedisInstance {
	return &redisv1.RedisInstance{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: redisv1.RedisInstanceSpec{
			Instances: instances,
			Image:     "redis:7.2",
			Storage:   storage,
		},
	}
}

func volumeClaimTemplate() *redisv1.VolumeClaimTemplateSpec {
	return &redisv1.VolumeClaimTemplateSpec{Size: resource.MustParse("1Gi")}
}

// existingClaims binds each given ordinal to a PVC named pvc-<ordinal>.
func existingClaims(ordinals ...int32) []redisv1.ExistingClaim {
	claims := make([]redisv1.ExistingClaim, 0, len(ordinals))
	for _, o := range ordinals {
		claims = append(claims, redisv1.ExistingClaim{InstanceOrdinal: o, ClaimName: fmt.Sprintf("pvc-%d", o)})
	}
	return claims
}
