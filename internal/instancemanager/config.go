package instancemanager

import (
	"context"
	"errors"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"

	"github.com/redis/go-redis/v9"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
)

// aofDir is where Redis keeps the AOF, on the volume of the instance.
const aofDir = "/data/appendonlydir"

// snapshotSchedule is when Redis takes an RDB snapshot (the default of
// Redis): after an hour with one write, 5 minutes with 100, a minute with
// 10000.
const snapshotSchedule = "3600 1 300 100 60 10000"

// The values of ConfigRequest.Persistence, as in spec.redis.persistence.
const (
	PersistenceAOF  = "aof"
	PersistenceRDB  = "rdb"
	PersistenceNone = "none"
)

// ConfigRequest is the body of POST /config: the configuration of
// spec.redis that the running Redis must have.
type ConfigRequest struct {
	Persistence     string `json:"persistence"`
	MaxmemoryPolicy string `json:"maxmemoryPolicy"`
	MaxmemoryRatio  int    `json:"maxmemoryRatio"`
	// MinReplicasToWrite and MinReplicasMaxLag are spec.replication.
	MinReplicasToWrite int               `json:"minReplicasToWrite"`
	MinReplicasMaxLag  int               `json:"minReplicasMaxLag"`
	Parameters         map[string]string `json:"parameters,omitempty"`
}

// ConfigFor returns the configuration of spec.redis of instance. The operator
// sends it to every running Pod, and the Instance Manager applies it at start.
func ConfigFor(instance *redisv1.RedisInstance) ConfigRequest {
	parameters := make(map[string]string, len(instance.Spec.Redis.Parameters))
	for name, value := range instance.Spec.Redis.Parameters {
		parameters[name] = string(value)
	}
	return ConfigRequest{
		Persistence:     string(instance.Spec.Redis.Persistence),
		MaxmemoryPolicy: instance.Spec.Redis.MaxmemoryPolicy,
		MaxmemoryRatio:  int(instance.Spec.Redis.MaxmemoryRatio),
		Parameters:      parameters,

		MinReplicasToWrite: int(instance.Spec.Replication.MinReplicasToWrite),
		MinReplicasMaxLag:  int(instance.Spec.Replication.MinReplicasMaxLag),
	}
}

// ConfigResponse tells whether POST /config changed Redis, and which options
// Redis refused.
type ConfigResponse struct {
	Changed bool `json:"changed"`
	// Rejected maps an option to the error of CONFIG SET, e.g. for a wrong
	// value or an option Redis cannot change while it runs.
	Rejected map[string]string `json:"rejected,omitempty"`
}

// option is an option of redis.conf with its value.
type option struct {
	name, value string
}

// liveOptions returns the options of req that CONFIG SET applies, in the
// order it applies them. appendonly is not one of them, see applyPersistence.
func liveOptions(req ConfigRequest, memoryLimit int64) []option {
	save := snapshotSchedule
	if req.Persistence == PersistenceNone {
		save = ""
	}
	options := []option{
		{"maxmemory", strconv.FormatInt(maxmemoryFor(memoryLimit, req.MaxmemoryRatio), 10)},
		{"maxmemory-policy", req.MaxmemoryPolicy},
		{"save", save},
		{"min-replicas-to-write", strconv.Itoa(req.MinReplicasToWrite)},
		{"min-replicas-max-lag", strconv.Itoa(req.MinReplicasMaxLag)},
	}
	for _, name := range slices.Sorted(maps.Keys(req.Parameters)) {
		options = append(options, option{name, req.Parameters[name]})
	}
	return options
}

// applyConfig makes the running Redis match req. An option Redis refuses
// goes to ConfigResponse.Rejected and the others are still applied; an error
// means Redis could not be reached.
func applyConfig(ctx context.Context, rdb *redis.Client, req ConfigRequest, memoryLimit int64, aofDir string) (ConfigResponse, error) {
	var resp ConfigResponse
	for _, o := range liveOptions(req, memoryLimit) {
		changed, err := setOption(ctx, rdb, o)
		var refused redis.Error
		if errors.As(err, &refused) {
			if resp.Rejected == nil {
				resp.Rejected = map[string]string{}
			}
			resp.Rejected[o.name] = refused.Error()
			continue
		}
		if err != nil {
			return ConfigResponse{}, err
		}
		resp.Changed = resp.Changed || changed
	}

	changed, err := applyPersistence(ctx, rdb, req.Persistence == PersistenceAOF, aofDir)
	if err != nil {
		return ConfigResponse{}, err
	}
	resp.Changed = resp.Changed || changed
	return resp, nil
}

// setOption sets o unless Redis has it already. It reports a change by the
// value Redis shows after CONFIG SET, since Redis rewrites some values, e.g.
// notify-keyspace-events "Ex" to "xE".
func setOption(ctx context.Context, rdb *redis.Client, o option) (bool, error) {
	before, err := getConfigValue(ctx, rdb, o.name)
	if err != nil || before == o.value {
		return false, err
	}
	if err := rdb.ConfigSet(ctx, o.name, o.value).Err(); err != nil {
		return false, err
	}
	after, err := getConfigValue(ctx, rdb, o.name)
	return after != before, err
}

// applyPersistence turns the AOF on or off while Redis runs. A start of Redis
// must not do it, as both ways lose data there:
//   - on: Redis writes the AOF from the dataset in memory;
//   - off: a snapshot first, so the RDB holds every write, and only then the
//     AOF files go, so no later start loads an AOF that misses writes.
func applyPersistence(ctx context.Context, rdb *redis.Client, isAof bool, aofDir string) (bool, error) {
	current, err := getConfigValue(ctx, rdb, "appendonly")
	if err != nil {
		return false, err
	}
	if isAof {
		if current == "yes" {
			return false, nil
		}
		return true, rdb.ConfigSet(ctx, "appendonly", "yes").Err()
	}

	// Also after a crash between the steps below: the AOF is off, its files are not.
	if _, err := os.Stat(aofDir); current == "no" && errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if current == "yes" {
		if err := rdb.ConfigSet(ctx, "appendonly", "no").Err(); err != nil {
			return false, err
		}
	}
	// ponytail: SAVE blocks clients while it writes the snapshot; BGSAVE and
	// polling INFO persistence if switching a large dataset off AOF matters.
	if err := rdb.Save(ctx).Err(); err != nil {
		return true, err
	}
	return true, os.RemoveAll(aofDir)
}

// aofExists reports whether an AOF is on the volume. Only then may Redis
// start with appendonly yes: on a dataset kept in RDB alone it would start
// empty, as it loads only the AOF.
func aofExists(aofDir string) bool {
	manifests, _ := filepath.Glob(filepath.Join(aofDir, "*.manifest"))
	return len(manifests) > 0
}

// getConfigValue returns the value of an option, or "" for an unknown one.
func getConfigValue(ctx context.Context, rdb *redis.Client, name string) (string, error) {
	values, err := rdb.ConfigGet(ctx, name).Result()
	return values[name], err
}
