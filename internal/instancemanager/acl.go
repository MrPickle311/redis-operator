package instancemanager

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ACLSecretKey is the key of the Secret <instance>-acl that holds the users
// of RedisUsers, one line per user in the format of a Redis ACL file:
// "user <name> on #<sha256 of the password> <rules>".
const ACLSecretKey = "users.acl"

// ACLSecretName returns the Secret with the users of RedisUsers, which the
// operator writes and every Instance Manager reads.
func ACLSecretName(instance string) string {
	return instance + "-acl"
}

// ACLDigest identifies a content of the Secret, so the operator can tell
// when every Instance Manager has applied it.
func ACLDigest(content []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(content))
}

// aclResyncEvery makes every so many polls a full sync even when the Secret
// did not change, which also undoes users changed by hand with ACL SETUSER.
const aclResyncEvery = 15

// ReservedUsers are never touched by the sync: redis.conf defines them.
var ReservedUsers = []string{"default", OperatorUser, ReplicationUser}

// aclResult is what the last sync applied.
type aclResult struct {
	Digest   string
	Rejected map[string]string
}

// parseACL returns the rules of every user of an ACL file, by name. A line
// that is not "user <name> ..." is an error: the operator writes the file.
func parseACL(content string) (map[string][]string, error) {
	users := map[string][]string{}
	for line := range strings.Lines(content) {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if len(fields) < 2 || fields[0] != "user" {
			return nil, fmt.Errorf("not a user line: %q", strings.TrimSpace(line))
		}
		users[fields[1]] = fields[2:]
	}
	return users, nil
}

// syncACL gives Redis exactly the users of the file, one at a time: ACL
// SETUSER with reset replaces a user in one step, and rules Redis refuses
// fail that user alone, which keeps its previous state. Users that are not
// in the file and not reserved are deleted, as anything added by hand is.
//
// Not ACL LOAD of a file: one bad line would refuse every user, and a bad
// aclfile would keep redis-server from starting at all.
func syncACL(ctx context.Context, rdb *redis.Client, users map[string][]string) (map[string]string, error) {
	rejected := map[string]string{}
	for name, rules := range users {
		args := []any{"ACL", "SETUSER", name, "reset"}
		for _, rule := range rules {
			args = append(args, rule)
		}
		err := rdb.Do(ctx, args...).Err()
		var refused redis.Error
		switch {
		case errors.As(err, &refused):
			rejected[name] = refused.Error()
		case err != nil:
			return nil, err
		}
	}
	current, err := rdb.Do(ctx, "ACL", "USERS").StringSlice()
	if err != nil {
		return nil, err
	}
	for _, name := range current {
		if _, wanted := users[name]; wanted || slices.Contains(ReservedUsers, name) {
			continue
		}
		if err := rdb.Do(ctx, "ACL", "DELUSER", name).Err(); err != nil {
			return nil, err
		}
	}
	return rejected, nil
}

// aclSync keeps the users of Redis in line with the Secret <instance>-acl,
// which it reads from the API every instancePollInterval: a change reaches
// Redis within seconds, and a Pod starts with its users even without the
// operator.
type aclSync struct {
	c     client.Reader
	key   types.NamespacedName // of the Secret
	rdb   *redis.Client
	ready *readiness
}

func (a *aclSync) run(ctx context.Context) {
	ticker := time.NewTicker(instancePollInterval)
	defer ticker.Stop()
	polls := 0
	for {
		if err := a.step(ctx, polls%aclResyncEvery == 0); err != nil {
			fmt.Fprintln(os.Stderr, "Could not sync the users of Redis:", err)
		}
		polls++
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// step applies the Secret when it changed, or always with full.
func (a *aclSync) step(ctx context.Context, full bool) error {
	readCtx, cancel := context.WithTimeout(ctx, instancePollInterval)
	defer cancel()
	var secret corev1.Secret
	err := a.c.Get(readCtx, a.key, &secret)
	if err != nil && !apierrors.IsNotFound(err) {
		return err // keep the users Redis has
	}
	content := secret.Data[ACLSecretKey] // none yet: no users of RedisUsers
	digest := ACLDigest(content)
	if last := a.ready.acl.Load(); last != nil && last.Digest == digest && !full {
		return nil
	}
	users, err := parseACL(string(content))
	if err != nil {
		return err
	}
	rejected, err := syncACL(ctx, a.rdb, users)
	if err != nil {
		return err
	}
	a.ready.acl.Store(&aclResult{Digest: digest, Rejected: rejected})
	return nil
}
