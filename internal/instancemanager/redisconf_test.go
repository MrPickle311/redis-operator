package instancemanager

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var testAuth = internalAuth{operatorPassword: "op-secret", replicationPassword: "repl-secret"}

func TestRedisConfigCreatesTheInternalUsers(t *testing.T) {
	conf := redisConfig(testAuth, false)

	for _, line := range []string{
		"user default off",
		"user _operator on >op-secret ~* &* +@all",
		"user _replication on >repl-secret +psync +replconf +ping",
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
	conf := redisConfig(testAuth, true)

	if !hasLine(conf, "user default on nopass ~* &* +@all") || hasLine(conf, "user default off") {
		t.Errorf("redis.conf does not enable the default user:\n%s", conf)
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
