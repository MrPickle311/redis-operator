package controller

import (
	"strings"
	"testing"

	redisv1 "github.com/MrPickle311/redis-operator/api/v1"
	"github.com/MrPickle311/redis-operator/internal/instancemanager"
)

func TestSafeStartRejection(t *testing.T) {
	restarted := func(keys int64) instancemanager.InstanceStatus {
		// A restart gives a new replication ID and starts the offset anew.
		return instancemanager.InstanceStatus{Role: "master", ReplID: "new", Offset: 14, Keys: keys, AwaitingApproval: true}
	}
	replica := func(replID string, offset, keys int64) candidate {
		return candidate{ordinal: 1, status: instancemanager.InstanceStatus{Role: "slave", ReplID: replID, Offset: offset, Keys: keys}}
	}
	tests := []struct {
		name        string
		primary     instancemanager.InstanceStatus
		replicas    []candidate
		persistence redisv1.Persistence
		wantReject  string // "" approves
	}{
		{"no replica to compare with", restarted(0), nil, redisv1.PersistenceAOF, ""},
		{"the same history, nothing missing", restarted(0), []candidate{replica("new", 14, 5)}, redisv1.PersistenceRDB, ""},
		{"empty replicas lose nothing", restarted(0), []candidate{replica("old", 3130, 0)}, redisv1.PersistenceNone, ""},
		{"an AOF was loaded", restarted(150), []candidate{replica("old", 3130, 150)}, redisv1.PersistenceAOF, ""},
		{"an empty primary would wipe the replicas", restarted(0), []candidate{replica("old", 3130, 150)}, redisv1.PersistenceAOF, "empty dataset"},
		{"an RDB snapshot may be old", restarted(120), []candidate{replica("old", 3130, 150)}, redisv1.PersistenceRDB, "persistence rdb"},
		{"no persistence restarts empty or stale", restarted(10), []candidate{replica("old", 3130, 150)}, redisv1.PersistenceNone, "persistence none"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := safeStartRejection(tt.primary, tt.replicas, tt.persistence)
			if (got == "") != (tt.wantReject == "") || !strings.Contains(got, tt.wantReject) {
				t.Errorf("safeStartRejection() = %q, want %q", got, tt.wantReject)
			}
		})
	}
}
