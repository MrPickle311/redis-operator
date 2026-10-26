package instancemanager

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var testAuth = internalAuth{operatorPassword: "op-secret", replicationPassword: "repl-secret"}

var testOptions = Options{Databases: 16, IOThreads: 1}

func TestRedisConfigCreatesTheInternalUsers(t *testing.T) {
	conf := redisConfig(testAuth, testOptions, 0, "noeviction", "", false)

	for _, line := range []string{
		"user default off",
		"user _operator on >op-secret ~* &* +@all",
		"user _replication off >repl-secret +psync +replconf +ping",
		// Every instance can become a replica, so each one logs in to its primary.
		"masteruser _replication",
		"masterauth repl-secret",
	} {
		if !hasLine(conf, line) {
			t.Errorf("redis.conf has no line %q:\n%s", line, conf)
		}
	}
}

func TestRedisConfigEnablesThePasswordlessDefaultUserOnRequest(t *testing.T) {
	opts := testOptions
	opts.DefaultUser = true
	conf := redisConfig(testAuth, opts, 0, "noeviction", "", false)

	if !hasLine(conf, "user default on nopass ~* &* +@all") || hasLine(conf, "user default off") {
		t.Errorf("redis.conf does not enable the default user:\n%s", conf)
	}
}

func TestRedisConfigSetsTheMemoryLimit(t *testing.T) {
	conf := redisConfig(testAuth, testOptions, 100663296, "allkeys-lru", "", false)

	for _, line := range []string{"maxmemory 100663296", "maxmemory-policy allkeys-lru"} {
		if !hasLine(conf, line) {
			t.Errorf("redis.conf has no line %q:\n%s", line, conf)
		}
	}
}

func TestRedisConfigSetsTheStartupOptions(t *testing.T) {
	conf := redisConfig(testAuth, Options{Databases: 32, IOThreads: 4}, 0, "noeviction", "", false)
	for _, line := range []string{"databases 32", "io-threads 4"} {
		if !hasLine(conf, line) {
			t.Errorf("redis.conf has no line %q:\n%s", line, conf)
		}
	}
}

// Redis loads only the AOF when appendonly is yes, so the volume decides.
func TestRedisConfigLoadsTheAOFOnlyWhenTheVolumeHasOne(t *testing.T) {
	if conf := redisConfig(testAuth, testOptions, 0, "noeviction", "", false); !hasLine(conf, "appendonly no") {
		t.Errorf("redis.conf without an AOF on the volume:\n%s", conf)
	}
	if conf := redisConfig(testAuth, testOptions, 0, "noeviction", "", true); !hasLine(conf, "appendonly yes") {
		t.Errorf("redis.conf with an AOF on the volume:\n%s", conf)
	}
}

// A primary lets replicas sync only after the operator approved its dataset.
func TestRedisConfigOpensReplicationOnlyOnAReplica(t *testing.T) {
	if conf := redisConfig(testAuth, testOptions, 0, "noeviction", "", false); !hasLine(conf, "user _replication off >repl-secret +psync +replconf +ping") {
		t.Errorf("a primary starts with replication open:\n%s", conf)
	}
	conf := redisConfig(testAuth, testOptions, 0, "noeviction", "cache-0.cache-hl.prod.svc", false)
	if !hasLine(conf, "user _replication on >repl-secret +psync +replconf +ping") {
		t.Errorf("a replica starts with replication closed:\n%s", conf)
	}
}

func TestRedisConfigStartsAReplicaAsOne(t *testing.T) {
	if conf := redisConfig(testAuth, testOptions, 0, "noeviction", "", false); strings.Contains(conf, "replicaof") {
		t.Errorf("the primary must not start with replicaof:\n%s", conf)
	}

	conf := redisConfig(testAuth, testOptions, 0, "noeviction", "cache-0.cache-hl.prod.svc", false)
	if !hasLine(conf, "replicaof cache-0.cache-hl.prod.svc 6379") {
		t.Errorf("redis.conf of a replica has no replicaof line:\n%s", conf)
	}
}

func TestReadInternalAuth(t *testing.T) {
	tests := []struct {
		name                string
		replicationPassword string
		wantErr             bool
	}{
		{"reads both passwords", "repl-secret", false},
		{"rejects an empty password", "", true},
		// A newline would start a new redis.conf directive, e.g. "user default on nopass".
		{"rejects whitespace that would inject config", "x\nuser default on nopass", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			writeFile(t, dir, OperatorPasswordKey, "op-secret")
			writeFile(t, dir, ReplicationPasswordKey, tt.replicationPassword)

			auth, err := readInternalAuth(dir)
			if (err != nil) != tt.wantErr {
				t.Fatalf("readInternalAuth() error = %v, want error %v", err, tt.wantErr)
			}
			if !tt.wantErr && auth != testAuth {
				t.Errorf("readInternalAuth() = %+v, want %+v", auth, testAuth)
			}
		})
	}
}

func TestReadInternalAuthFailsWithoutTheSecret(t *testing.T) {
	if _, err := readInternalAuth(t.TempDir()); err == nil {
		t.Fatal("readInternalAuth() succeeded without any password file")
	}
}

func hasLine(conf, want string) bool {
	for line := range strings.Lines(conf) {
		if strings.TrimSpace(line) == want {
			return true
		}
	}
	return false
}

func writeFile(t *testing.T, dir, name, data string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
}
