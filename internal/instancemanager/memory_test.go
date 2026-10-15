package instancemanager

import (
	"testing"
)

func TestMemoryLimit(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    int64
		wantErr bool
	}{
		{"reads the limit in bytes", "134217728\n", 134217728, false},
		{"returns 0 without a limit", "max\n", 0, false},
		{"rejects garbage", "lots\n", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			writeFile(t, dir, "memory.max", tt.content)

			got, err := memoryLimit(dir + "/memory.max")
			if (err != nil) != tt.wantErr {
				t.Fatalf("memoryLimit() error = %v, want error %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("memoryLimit() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestMemoryLimitFailsWithoutCgroupV2(t *testing.T) {
	if _, err := memoryLimit(t.TempDir() + "/memory.max"); err == nil {
		t.Fatal("memoryLimit() succeeded without the cgroup file")
	}
}

func TestMaxmemoryFor(t *testing.T) {
	tests := []struct {
		name  string
		limit int64
		ratio int
		want  int64
	}{
		{"takes the ratio of the limit", 128 << 20, 75, 96 << 20},
		{"keeps no limit as no limit", 0, 75, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := maxmemoryFor(tt.limit, tt.ratio); got != tt.want {
				t.Errorf("maxmemoryFor(%d, %d) = %d, want %d", tt.limit, tt.ratio, got, tt.want)
			}
		})
	}
}
