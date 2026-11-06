package controller

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

var update = flag.Bool("update", false, "rewrite the golden files in testdata/golden")

// TestGoldenObjects pins down every object the operator builds from a spec:
// Pods (with their spec-hash), Services, PVCs and the NetworkPolicy. A
// refactor must leave them alone, as a new spec-hash rolls every Pod of every
// instance. After an intended change:
//
//	go test ./internal/controller -run TestGoldenObjects -update
func TestGoldenObjects(t *testing.T) {
	base := func() *redisv1.RedisInstance {
		return &redisv1.RedisInstance{
			ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "app", UID: "uid-1"},
			Spec: redisv1.RedisInstanceSpec{
				Instances: 3,
				Image:     "redis:7.2",
				Storage:   redisv1.StorageSpec{VolumeClaimTemplate: &redisv1.VolumeClaimTemplateSpec{Size: resource.MustParse("1Gi")}},
				Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi")}},
				Redis: redisv1.RedisSpec{
					Persistence: redisv1.PersistenceAOF, MaxmemoryPolicy: "noeviction", MaxmemoryRatio: 75,
					Startup: redisv1.RedisStartupSpec{Databases: 16, IOThreads: 1},
				},
				Failover:    redisv1.FailoverSpec{Delay: metav1.Duration{Duration: 30e9}, FenceOnAPILoss: ptr.To(true)},
				Replication: redisv1.ReplicationSpec{MinReplicasMaxLag: 10},
				Affinity:    redisv1.AffinitySpec{PodAntiAffinityType: redisv1.PodAntiAffinityRequired},
				TLS:         redisv1.TLSSpec{Enabled: ptr.To(true), ClientAuth: redisv1.TLSClientAuthNone},
				ACL:         redisv1.ACLSpec{DefaultUser: redisv1.DefaultUserDisabled},
			},
			Status: redisv1.RedisInstanceStatus{CurrentPrimary: "cache-0"},
		}
	}
	cases := map[string]func(*redisv1.RedisInstance){
		"defaults": func(*redisv1.RedisInstance) {},
		"no-tls-preferred-default-user": func(i *redisv1.RedisInstance) {
			i.Spec.TLS.Enabled = ptr.To(false)
			i.Spec.Affinity.PodAntiAffinityType = redisv1.PodAntiAffinityPreferred
			i.Spec.ACL.DefaultUser = redisv1.DefaultUserEnabled
		},
		"client-certs-network-policy": func(i *redisv1.RedisInstance) {
			i.Spec.TLS.ClientAuth = redisv1.TLSClientAuthRequired
			i.Spec.TLS.ClientCASecretRef = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "clients-ca"}, Key: "ca.crt"}
			i.Spec.NetworkPolicy.AllowFrom = []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}}}}
		},
		"monitoring": func(i *redisv1.RedisInstance) {
			i.Spec.Monitoring = redisv1.MonitoringSpec{Enabled: true, Image: "exporter:v1"}
		},
		"shard-of-a-cluster": func(i *redisv1.RedisInstance) {
			i.Name = "store-1"
			i.OwnerReferences = []metav1.OwnerReference{{APIVersion: redisv1.GroupVersion.String(), Kind: kindRedisCluster,
				Name: "store", UID: "uid-c", Controller: ptr.To(true)}}
			i.Spec.Instances = 2
			i.Status.CurrentPrimary = ""
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			instance := base()
			mutate(instance)
			objects := map[string]any{
				"pod-0":         buildRedisPod(instance, 0, "operator:v1", "data-0"),
				"pod-1":         buildRedisPod(instance, 1, "operator:v1", "data-1"),
				"service-hl":    buildHeadlessService(instance),
				"service-rw":    buildClientService(instance, "rw", map[string]string{labelRole: rolePrimary}),
				"pvc-0":         buildRedisPVC(instance, 0),
				"networkPolicy": networkPolicySpec(instance, "redis-operator-system"),
			}
			got, err := json.MarshalIndent(objects, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join("testdata", "golden", name+".json")
			if *update {
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, append(got, '\n'), 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("%v; create it with -update", err)
			}
			if string(want) != string(got)+"\n" {
				t.Errorf("%s changed; if that is intended, run with -update and review the diff of %s", name, path)
			}
		})
	}
}
