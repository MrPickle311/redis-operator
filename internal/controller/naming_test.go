package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

func TestPodFQDN(t *testing.T) {
	instance := &redisv1.RedisInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "prod"},
	}

	tests := []struct {
		ordinal int32
		want    string
	}{
		{ordinal: 0, want: "cache-0.cache-hl.prod.svc"},
		{ordinal: 2, want: "cache-2.cache-hl.prod.svc"},
	}

	for _, tt := range tests {
		if got := podFQDN(instance, tt.ordinal); got != tt.want {
			t.Errorf("podFQDN(%d) = %q, want %q", tt.ordinal, got, tt.want)
		}
	}
}

func TestPVCName(t *testing.T) {
	instance := &redisv1.RedisInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "prod"},
		Spec: redisv1.RedisInstanceSpec{
			Storage: redisv1.StorageSpec{
				ExistingClaims: []redisv1.ExistingClaim{
					{InstanceOrdinal: 1, ClaimName: "restored-pvc"},
				},
			},
		},
	}

	tests := []struct {
		name         string
		ordinal      int32
		wantName     string
		wantExternal bool
	}{
		{name: "generated", ordinal: 0, wantName: "cache-0-data", wantExternal: false},
		{name: "external", ordinal: 1, wantName: "restored-pvc", wantExternal: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotName, gotExternal := pvcName(instance, tt.ordinal)
			if gotName != tt.wantName || gotExternal != tt.wantExternal {
				t.Errorf("pvcName(%d) = (%q, %v), want (%q, %v)",
					tt.ordinal, gotName, gotExternal, tt.wantName, tt.wantExternal)
			}
		})
	}
}
