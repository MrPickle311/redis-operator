package instancemanager

import (
	"os"
	"strconv"
	"strings"
)

// cgroupMemoryMax holds the memory limit of the container: the kubelet sets it
// from limits.memory, or from a LimitRange default, and "max" means no limit.
// ponytail: cgroup v2 only; Kubernetes 1.35 dropped cgroup v1 support.
const cgroupMemoryMax = "/sys/fs/cgroup/memory.max"

// memoryLimit returns the memory limit in bytes from the cgroup file at path,
// or 0 when the container has no limit.
func memoryLimit(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	value := strings.TrimSpace(string(data))
	if value == "max" {
		return 0, nil
	}
	return strconv.ParseInt(value, 10, 64)
}

// maxmemoryFor returns ratio percent of the memory limit. With no limit (0) it
// returns 0, which in redis.conf also means "no limit".
func maxmemoryFor(limit int64, ratio int) int64 {
	return limit * int64(ratio) / 100
}
