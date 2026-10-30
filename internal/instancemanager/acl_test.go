package instancemanager

import (
	"context"
	"os"
	"reflect"
	"slices"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestParseACL(t *testing.T) {
	users, err := parseACL("user app on #ab ~app:* +@read\n\nuser other on #cd +ping\n")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{"app": {"on", "#ab", "~app:*", "+@read"}, "other": {"on", "#cd", "+ping"}}
	if !reflect.DeepEqual(users, want) {
		t.Errorf("parseACL() = %v, want %v", users, want)
	}
	if users, err := parseACL(""); err != nil || len(users) != 0 {
		t.Errorf("parseACL() of nothing = %v, %v; want no users", users, err)
	}
	if _, err := parseACL("requirepass secret\n"); err == nil {
		t.Error("parseACL() took a line that is not a user")
	}
}

func TestACLDigestTellsContentsApart(t *testing.T) {
	if ACLDigest([]byte("user a on")) == ACLDigest([]byte("user b on")) || ACLDigest(nil) != ACLDigest([]byte{}) {
		t.Error("ACLDigest() does not identify the content")
	}
}

// TestSyncACLOnRedis needs a real Redis, like TestApplyConfigOnRedis.
func TestSyncACLOnRedis(t *testing.T) {
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("REDIS_TEST_ADDR not set")
	}
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = rdb.Close() }()
	for _, user := range []string{OperatorUser, "by-hand"} {
		if err := rdb.Do(ctx, "ACL", "SETUSER", user, "on", ">x", "+ping").Err(); err != nil {
			t.Fatal(err)
		}
	}

	users, err := parseACL("user app on #" + sha256Hex("app-password") + " ~app:* +@read\n" +
		"user broken on #" + sha256Hex("x") + " +@nosuchcategory\n")
	if err != nil {
		t.Fatal(err)
	}
	rejected, err := syncACL(ctx, rdb, users)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := rejected["broken"]; !ok || len(rejected) != 1 {
		t.Errorf("rejected = %v, want only the user with a rule Redis does not know", rejected)
	}
	names, _ := rdb.Do(ctx, "ACL", "USERS").StringSlice()
	if !slices.Contains(names, "app") || slices.Contains(names, "by-hand") || !slices.Contains(names, OperatorUser) {
		t.Errorf("users = %v, want app and the internal ones, and no user added by hand", names)
	}

	app := redis.NewClient(&redis.Options{Addr: addr, Username: "app", Password: "app-password"})
	defer func() { _ = app.Close() }()
	if err := app.Get(ctx, "app:key").Err(); err != nil && err != redis.Nil {
		t.Errorf("app could not read its keys: %v", err)
	}
	if err := app.Set(ctx, "app:key", "v", 0).Err(); err == nil {
		t.Error("app could write, though its rules allow reading only")
	}
}

func sha256Hex(password string) string {
	return ACLDigest([]byte(password))
}
