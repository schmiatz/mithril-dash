// Tails <storage.logs>/latest/replay_timings.jsonl, the per-block JSON dump
// of pkg/metrics.BlockReplay that mithril's replay loop writes on every
// folded block (pkg/replay/block.go:2864-2874). That struct has no `json:`
// tags, so encoding/json marshals it under its exact Go field names — the
// mirror struct below matches those names field-for-field so decoding needs
// no tags either. Unknown/renamed fields simply zero out rather than erroring,
// so a field mithril adds or removes upstream degrades gracefully instead of
// breaking the tailer.
package collect

import (
	"context"
	"encoding/json"
	"strings"
	"time"
)

type timing struct {
	Count          uint64
	SumNanoseconds uint64
}

func (t timing) avgMs() float64 {
	if t.Count == 0 {
		return 0
	}
	return float64(t.SumNanoseconds) / float64(t.Count) / 1e6
}

func (t timing) totalMs() float64 {
	return float64(t.SumNanoseconds) / 1e6
}

// blockReplayMirror mirrors the block-level latency fields of
// pkg/metrics.BlockReplay. Tx/ix/sbpf sub-phase fields are decoded too (kept
// in Extra) but not individually named here — see StagesMs's "other" bucket.
type blockReplayMirror struct {
	Slot uint64

	PreprocessBlock     timing
	LoadBlockAccounts   timing
	TxLoop              timing
	Reward              timing
	Rent                timing
	RunIncinerator      timing
	BlockUpdateAccounts timing
	AccountsDeltaHash   timing
	BankHash            timing
	Sigverify           timing

	AccountLoader struct {
		RequestedKeys     uint64
		DurableKeys       uint64
		WorkingSetHits    uint64
		InProgressHits    uint64
		CacheHits         uint64
		IndexHits         uint64
		IndexMisses       uint64
		UniqueAppendVecs  uint64
		AppendVecAccounts uint64
		OpenFailures      uint64
		ReadFailures      uint64
		RetryAccounts     uint64
	}
}

// ReplayStage is one named block-level latency bucket for a single slot.
type ReplayStage struct {
	Name string
	Ms   float64 // total ms this stage took for this block (Count is usually 1 at block level)
}

// ReplaySample is one decoded replay_timings.jsonl line, reshaped for the
// store/UI: an ordered list of named stages plus accounts-loader hit-rate
// context (useful for explaining a slow LoadBlockAccounts).
type ReplaySample struct {
	TS      time.Time
	Slot    uint64
	Stages  []ReplayStage
	TotalMs float64

	AccountRequestedKeys uint64
	AccountCacheHits     uint64
	AccountIndexHits     uint64
	AccountIndexMisses   uint64
	AccountReadFailures  uint64
}

func decodeReplayLine(line string) (ReplaySample, bool) {
	line = strings.TrimSpace(line)
	if line == "" {
		return ReplaySample{}, false
	}
	var m blockReplayMirror
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		return ReplaySample{}, false
	}
	stages := []ReplayStage{
		{"preprocess_block", m.PreprocessBlock.totalMs()},
		{"load_block_accounts", m.LoadBlockAccounts.totalMs()},
		{"tx_loop", m.TxLoop.totalMs()},
		{"reward", m.Reward.totalMs()},
		{"rent", m.Rent.totalMs()},
		{"run_incinerator", m.RunIncinerator.totalMs()},
		{"block_update_accounts", m.BlockUpdateAccounts.totalMs()},
		{"accounts_delta_hash", m.AccountsDeltaHash.totalMs()},
		{"bank_hash", m.BankHash.totalMs()},
		{"sigverify", m.Sigverify.totalMs()},
	}
	total := 0.0
	for _, s := range stages {
		total += s.Ms
	}
	return ReplaySample{
		TS:                   time.Now(),
		Slot:                 m.Slot,
		Stages:               stages,
		TotalMs:              total,
		AccountRequestedKeys: m.AccountLoader.RequestedKeys,
		AccountCacheHits:     m.AccountLoader.CacheHits,
		AccountIndexHits:     m.AccountLoader.IndexHits,
		AccountIndexMisses:   m.AccountLoader.IndexMisses,
		AccountReadFailures:  m.AccountLoader.ReadFailures,
	}, true
}

// RunReplayTimingsTailer follows replay_timings.jsonl and invokes onSample
// for each decoded line. It reuses Tailer's rotation-aware polling, buffering
// whole JSON lines itself (a JSONL line is always terminated by '\n', same
// contract Tailer already gives per-line callbacks under).
func RunReplayTimingsTailer(ctx context.Context, baseDir string, pollInterval time.Duration, onSample func(ReplaySample)) {
	t := &Tailer{
		BaseDir:      baseDir,
		FileName:     "replay_timings.jsonl",
		PollInterval: pollInterval,
		OnLine: func(line string) {
			if sample, ok := decodeReplayLine(line); ok {
				onSample(sample)
			}
		},
	}
	t.Run(ctx)
}
