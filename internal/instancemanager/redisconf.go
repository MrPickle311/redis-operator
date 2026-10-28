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

// TLSDir is where the operator mounts the TLS Secret of Redis: tls.crt and
// tls.key, the certificate Redis serves, ca.crt, its CA, and the files below.
const TLSDir = "/tls"

// Files of the TLS Secret of Redis besides tls.crt, tls.key and ca.crt.
const (
	// TLSReplicationCert and TLSReplicationKey are the certificate a replica
	// shows the primary, which may ask for one (tls-auth-clients).
	TLSReplicationCert = "replication.crt"
	TLSReplicationKey  = "replication.key"
	// TLSTrustedCAs holds every CA Redis trusts: of the server certificate,
	// which replicas verify the primary with, and of the client certificates.
	TLSTrustedCAs = "trusted.crt"
)

// redisSocketPath is where the Instance Manager reaches redis-server: a unix
// socket in the same emptyDir, so it needs no TLS and no certificate for
// "localhost", and nothing outside the Pod can reach it.
const redisSocketPath = "/controller/redis.sock"

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
user %s %s >%s +psync +replconf +ping
masteruser %s
masterauth %s
maxmemory %d
maxmemory-policy %s
databases %d
io-threads %d
appendonly %s
unixsocket %s
unixsocketperm 700
`, defaultACL,
		OperatorUser, auth.operatorPassword,
		// A primary lets no replica sync until the operator approved its
		// dataset with /promote: an empty one would wipe the replicas.
		ReplicationUser, onOff(replicaOf != ""), auth.replicationPassword,
		ReplicationUser, auth.replicationPassword,
		maxmemory, maxmemoryPolicy, opts.Databases, opts.IOThreads, yesNo(aof), redisSocketPath)
	if opts.TLS {
		// Only TLS on 6379; the Instance Manager keeps the unix socket.
		// Replicas connect to the primary over TLS too.
		conf += fmt.Sprintf("port 0\ntls-port %d\ntls-replication yes\ntls-auth-clients %s\n",
			redisPort, tlsAuthClients(opts.TLSClientAuth))
		for _, option := range tlsFileOptions {
			conf += fmt.Sprintf("%s %s\n", option.name, option.value)
		}
	}
	if replicaOf != "" {
		conf += fmt.Sprintf("replicaof %s %d\n", replicaOf, redisPort)
	}
	return conf
}

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// tlsFileOptions are the options of redis.conf that name the files of the
// TLS Secret. Redis reads them again on CONFIG SET, see watchTLS.
var tlsFileOptions = []option{
	{"tls-cert-file", TLSDir + "/tls.crt"},
	{"tls-key-file", TLSDir + "/tls.key"},
	{"tls-client-cert-file", TLSDir + "/" + TLSReplicationCert},
	{"tls-client-key-file", TLSDir + "/" + TLSReplicationKey},
	{"tls-ca-cert-file", TLSDir + "/" + TLSTrustedCAs},
}

// tlsAuthClients maps spec.tls.clientAuth to tls-auth-clients.
func tlsAuthClients(clientAuth string) string {
	switch clientAuth {
	case "required":
		return "yes"
	case "optional":
		return "optional"
	}
	return "no"
}
