// Polls <storage.accounts>/mithril_state.json, mirroring the nodeState
// schema in cmd/mithril/dashboardcmd/data.go — mithril's own status/dashboard
// commands read the same file the same way.
package collect

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

type NodeState struct {
	LastSlot           uint64 `json:"last_slot"`
	LastEpoch          uint64 `json:"last_epoch"`
	LastBankhash       string `json:"last_bankhash"`
	SnapshotSlot       uint64 `json:"snapshot_slot"`
	Stage              string `json:"stage"`
	LastShutdownReason string `json:"last_shutdown_reason"`
	LastShutdownAt     string `json:"last_shutdown_at"`
	CurrentRunID       string `json:"current_run_id"`
	LastWriterVersion  string `json:"last_writer_version"`
	LastWriterCommit   string `json:"last_writer_commit"`
	Cluster            string `json:"cluster"`
}

func readNodeState(accountsPath string) (NodeState, bool) {
	f, err := os.Open(filepath.Join(accountsPath, "mithril_state.json"))
	if err != nil {
		return NodeState{}, false
	}
	defer f.Close()

	// Cap read size defensively, matching mithril's own reader.
	data := make([]byte, 1<<20)
	n, err := f.Read(data)
	if err != nil && n == 0 {
		return NodeState{}, false
	}
	var s NodeState
	if err := json.Unmarshal(data[:n], &s); err != nil {
		return NodeState{}, false
	}
	return s, true
}

// RunStatePoller polls mithril_state.json on interval and invokes onState
// whenever the file is readable, so the dashboard has slot/epoch/bankhash
// even before the WebSocket-less RPC/Prometheus layers have anything.
func RunStatePoller(ctx context.Context, accountsPath string, interval time.Duration, onState func(NodeState)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if s, ok := readNodeState(accountsPath); ok {
			onState(s)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
