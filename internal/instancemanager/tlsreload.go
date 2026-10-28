package instancemanager

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

// tlsReloadInterval is how often the Instance Manager looks for a new
// certificate. The kubelet takes up to about a minute to update a mounted
// Secret, so a renewal reaches Redis within two minutes or so.
// ponytail: polling; inotify would notice sooner, but the kubelet swaps a
// symlink (..data), which plain file watches miss.
const tlsReloadInterval = 30 * time.Second

// tlsDigest returns a digest of the content of every file of the TLS Secret
// that Redis reads. The kubelet replaces them all at once, so one digest
// tells a whole new set.
func tlsDigest() (string, error) {
	h := sha256.New()
	for _, option := range tlsFileOptions {
		data, err := os.ReadFile(option.value)
		if err != nil {
			return "", err
		}
		h.Write(data)
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// watchTLS makes Redis load the files of the TLS Secret again whenever they
// change, e.g. after a renewal, without a restart: CONFIG SET of the same
// paths makes Redis read them, and new connections get the new certificate
// while open ones go on.
func watchTLS(ctx context.Context, rdb *redis.Client) {
	last, _ := tlsDigest()
	ticker := time.NewTicker(tlsReloadInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		digest, err := tlsDigest()
		if err != nil || digest == last {
			continue
		}
		if err := reloadTLS(ctx, rdb); err != nil {
			fmt.Fprintln(os.Stderr, "Could not load the new TLS certificate:", err)
			continue // the next tick tries again
		}
		fmt.Fprintln(os.Stderr, "Loaded the new TLS certificate")
		last = digest
	}
}

// reloadTLS sets every TLS file option in one CONFIG SET, which Redis applies
// all or none: a certificate never goes without its key.
func reloadTLS(ctx context.Context, rdb *redis.Client) error {
	args := []any{"CONFIG", "SET"}
	for _, option := range tlsFileOptions {
		args = append(args, option.name, option.value)
	}
	return rdb.Do(ctx, args...).Err()
}
