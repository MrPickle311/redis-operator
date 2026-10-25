package instancemanager

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode"
)

// InternalAuthDir is where the operator mounts the Secret with the passwords
// of the internal users.
const InternalAuthDir = "/internal-auth"

// The internal users of Redis and the keys of their passwords in the Secret.
const (
	OperatorUser           = "_operator"
	ReplicationUser        = "_replication"
	OperatorPasswordKey    = "operator-password"
	ReplicationPasswordKey = "replication-password"
)

// redisConfPath is in the emptyDir the Instance Manager binary was copied to:
// writable, private to the Pod and gone with it.
const redisConfPath = "/controller/redis.conf"

type internalAuth struct {
	operatorPassword    string
	replicationPassword string
}

// readInternalAuth reads the passwords of the internal users from dir.
func readInternalAuth(dir string) (internalAuth, error) {
	var auth internalAuth
	for key, password := range map[string]*string{
		OperatorPasswordKey:    &auth.operatorPassword,
		ReplicationPasswordKey: &auth.replicationPassword,
	} {
		data, err := os.ReadFile(filepath.Join(dir, key))
		if err != nil {
			return internalAuth{}, err
		}
		// The password goes into redis.conf: whitespace would end it and
		// let the rest of the line be parsed as further ACL rules.
		if len(data) == 0 || strings.ContainsFunc(string(data), unicode.IsSpace) {
			return internalAuth{}, fmt.Errorf("%s must be a non-empty password without whitespace", key)
		}
		*password = string(data)
	}
	return auth, nil
}

// redisConfig returns the redis.conf: what Redis needs before it takes the
// first command. The default user is off unless opts.DefaultUser is set, when
// it needs no password at all. A non-empty replicaOf makes Redis start as a
// replica of that host. aof tells whether an AOF is on the volume to load;
// the rest of spec.redis comes with CONFIG SET after the start.
func redisConfig(auth internalAuth, opts Options, maxmemory int64, maxmemoryPolicy, replicaOf string, aof bool) string {
	defaultACL := "user default off"
	if opts.DefaultUser {
		defaultACL = "user default on nopass ~* &* +@all"
	}
	conf := fmt.Sprintf(`%s
user %s on >%s ~* &* +@all
user %s on >%s +psync +replconf +ping
masteruser %s
masterauth %s
maxmemory %d
maxmemory-policy %s
databases %d
io-threads %d
appendonly %s
`, defaultACL,
		OperatorUser, auth.operatorPassword,
		ReplicationUser, auth.replicationPassword,
		ReplicationUser, auth.replicationPassword,
		maxmemory, maxmemoryPolicy, opts.Databases, opts.IOThreads, yesNo(aof))
	if replicaOf != "" {
		conf += fmt.Sprintf("replicaof %s %d\n", replicaOf, redisPort)
	}
	return conf
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}
