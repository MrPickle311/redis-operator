package instancemanager

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
)

var update = flag.Bool("update", false, "rewrite the golden files in testdata/golden")

// TestGoldenRedisConfig pins down the whole redis.conf for the kinds of Pods
// there are. After an intended change:
//
//	go test ./internal/instancemanager -run TestGoldenRedisConfig -update
func TestGoldenRedisConfig(t *testing.T) {
	tls := testOptions
	tls.TLS, tls.TLSClientAuth = true, "required"
	cluster := tls
	cluster.Cluster, cluster.Instance, cluster.PodName, cluster.Namespace = true, "store-1", "store-1-0", "app"
	defaultUser := testOptions
	defaultUser.DefaultUser = true
	for name, conf := range map[string]string{
		"primary":          redisConfig(testAuth, testOptions, 100663296, "noeviction", "", true),
		"replica-tls":      redisConfig(testAuth, tls, 100663296, "allkeys-lru", "cache-0.cache-hl.app.svc", false),
		"cluster-node":     redisConfig(testAuth, cluster, 100663296, "noeviction", "", true),
		"default-user-rdb": redisConfig(testAuth, defaultUser, 0, "noeviction", "", false),
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join("testdata", "golden", name+".conf")
			if *update {
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(conf), 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("%v; create it with -update", err)
			}
			if string(want) != conf {
				t.Errorf("redis.conf of %s changed; if that is intended, run with -update and review the diff of %s", name, path)
			}
		})
	}
}
