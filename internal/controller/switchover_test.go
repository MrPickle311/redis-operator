package controller

import (
	"testing"

	"github.com/MrPickle311/redis-operator/internal/instancemanager"
)

func TestCaughtUp(t *testing.T) {
	primary := instancemanager.InstanceStatus{Role: "master", ReplID: "abc", Offset: 1500}

	tests := []struct {
		name   string
		target instancemanager.InstanceStatus
		want   bool
	}{
		{"same offset", instancemanager.InstanceStatus{Role: "slave", ReplID: "abc", Offset: 1500}, true},
		{"still behind", instancemanager.InstanceStatus{Role: "slave", ReplID: "abc", Offset: 1490}, false},
		{"other replication history", instancemanager.InstanceStatus{Role: "slave", ReplID: "xyz", Offset: 1500}, false},
		// A pass before a crash promoted it; promoting again must not wait forever.
		{"already promoted", instancemanager.InstanceStatus{Role: "master", ReplID: "new", Offset: 1500}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := caughtUp(primary, tt.target); got != tt.want {
				t.Errorf("caughtUp(%+v, %+v) = %v, want %v", primary, tt.target, got, tt.want)
			}
		})
	}
}
