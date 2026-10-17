package instancemanager

import "testing"

func TestInitialSyncDone(t *testing.T) {
	tests := []struct {
		name        string
		replication map[string]string
		want        bool
	}{
		{"synced", map[string]string{"role": "slave", "master_link_status": "up", "master_sync_in_progress": "0"}, true},
		{"primary not reachable yet", map[string]string{"role": "slave", "master_link_status": "down", "master_sync_in_progress": "0"}, false},
		{"full sync running", map[string]string{"role": "slave", "master_link_status": "down", "master_sync_in_progress": "1"}, false},
		{"fields missing", map[string]string{"role": "slave"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := initialSyncDone(tt.replication); got != tt.want {
				t.Errorf("initialSyncDone(%v) = %v, want %v", tt.replication, got, tt.want)
			}
		})
	}
}
