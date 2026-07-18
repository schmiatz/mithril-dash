// Client for mithril's own JSON-RPC server (pkg/rpcserver, default port
// 8899). It implements a small, mithril-specific method set — no getSlot,
// no getVoteAccounts, no pubsub — so this is a plain poller, not a
// subscription like Agave's slotsUpdatesSubscribe. getBankHash is a
// mithril-only extension (not part of the Solana RPC spec).
package collect

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type rpcRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      int           `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func rpcCall(ctx context.Context, client *http.Client, url, method string, params []interface{}, out interface{}) error {
	if params == nil {
		params = []interface{}{}
	}
	body, err := json.Marshal(rpcRequest{JSONRPC: "2.0", ID: 1, Method: method, Params: params})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var rr rpcResponse
	if err := json.NewDecoder(resp.Body).Decode(&rr); err != nil {
		return err
	}
	if rr.Error != nil {
		return fmt.Errorf("rpc %s: %s (code %d)", method, rr.Error.Message, rr.Error.Code)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(rr.Result, out)
}

type EpochInfo struct {
	AbsoluteSlot     uint64 `json:"absoluteSlot"`
	BlockHeight      uint64 `json:"blockHeight"`
	Epoch            uint64 `json:"epoch"`
	SlotIndex        uint64 `json:"slotIndex"`
	SlotsInEpoch     uint64 `json:"slotsInEpoch"`
	TransactionCount uint64 `json:"transactionCount"`
}

type LatestBlockhash struct {
	Context struct {
		Slot uint64 `json:"slot"`
	} `json:"context"`
	Value *struct {
		Blockhash            string `json:"blockhash"`
		LastValidBlockHeight uint64 `json:"lastValidBlockHeight"`
	} `json:"value"`
}

// RPCSnapshot is one successful poll cycle against mithril's RPC server.
// Ok is false if the node's RPC wasn't reachable this cycle (e.g. rpc.port=0,
// or the node is between snapshot bootstrap and serving) — the dashboard
// should treat that as "unknown", not "down", since RPC is optional in
// mithril's shreds-only block delivery mode.
type RPCSnapshot struct {
	TS          time.Time
	Ok          bool
	EpochInfo   EpochInfo
	BlockHeight uint64
	Blockhash   string
	BankHash    string
}

func pollRPCOnce(ctx context.Context, client *http.Client, url string) RPCSnapshot {
	snap := RPCSnapshot{TS: time.Now()}

	var epochInfo EpochInfo
	if err := rpcCall(ctx, client, url, "getEpochInfo", nil, &epochInfo); err != nil {
		return snap
	}
	snap.EpochInfo = epochInfo
	snap.Ok = true

	var blockHeight uint64
	if err := rpcCall(ctx, client, url, "getBlockHeight", nil, &blockHeight); err == nil {
		snap.BlockHeight = blockHeight
	}

	var lbh LatestBlockhash
	if err := rpcCall(ctx, client, url, "getLatestBlockhash", nil, &lbh); err == nil && lbh.Value != nil {
		snap.Blockhash = lbh.Value.Blockhash
	}

	var bankHash string
	if epochInfo.AbsoluteSlot > 0 {
		if err := rpcCall(ctx, client, url, "getBankHash", []interface{}{float64(epochInfo.AbsoluteSlot)}, &bankHash); err == nil {
			snap.BankHash = bankHash
		}
	}

	return snap
}

// RunRPCPoller polls mithril's JSON-RPC on interval and invokes onSnapshot
// every cycle (Ok=false snapshots included, so the dashboard can show "RPC
// unreachable" rather than silently going stale).
func RunRPCPoller(ctx context.Context, url string, interval time.Duration, onSnapshot func(RPCSnapshot)) {
	client := &http.Client{Timeout: 5 * time.Second}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		onSnapshot(pollRPCOnce(ctx, client, url))
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
