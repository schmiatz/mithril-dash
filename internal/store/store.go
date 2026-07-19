// Package store is mithril-dash's central in-memory aggregator: every
// collector (log tailer, replay_timings.jsonl tailer, Prometheus scraper,
// state-file poller) feeds events in here through one of the Apply* methods;
// the web layer only ever reads Snapshot() and the History ring buffers. One
// mutex + condition variable, mirroring the pattern of a single-process
// Python/Flask dashboard translated into idiomatic Go.
package store

import (
	"sync"
	"time"

	"github.com/stakingfacilities/mithril-dash/internal/collect"
)

const (
	maxRecentEvents  = 40
	maxReplayHistory = 3000
	maxSlotStatsHist = 3000
	maxVotingHistory = 3600 // ~1h at 1 sample/s
	maxSlotSummary   = 2000 // one point every 100 slots (~20s at 200ms/slot) — 2000 covers many hours
	maxSystemHist    = 3600 // ~1h at 1 sample/s
)

type sourceHealth struct {
	LastAt time.Time `json:"last_at"`
}

func (s sourceHealth) Status() string {
	if s.LastAt.IsZero() {
		return "waiting"
	}
	if time.Since(s.LastAt) > 30*time.Second {
		return "stale"
	}
	return "live"
}

type Overview struct {
	// Name and IdentityPubkey are static, set once at startup (-name flag;
	// validator.identity_keypair read once via collect.LoadIdentityPubkey)
	// — neither changes while mithril-dash runs.
	Name           string `json:"name"`
	IdentityPubkey string `json:"identity_pubkey"`

	Cluster            string `json:"cluster"`
	ConsensusMode      string `json:"consensus_mode"`
	RunID              string `json:"run_id"`
	WriterVersion      string `json:"writer_version"`
	WriterCommit       string `json:"writer_commit"`
	Stage              string `json:"stage"`
	LastShutdownReason string `json:"last_shutdown_reason"`
	LastShutdownAt     string `json:"last_shutdown_at"`

	CurrentSlot   uint64  `json:"current_slot"`
	CurrentEpoch  uint64  `json:"current_epoch"`
	SlotIndex     uint64  `json:"slot_index"`
	SlotsInEpoch  uint64  `json:"slots_in_epoch"`
	BankHashState string  `json:"bank_hash_state"`
	TPSLive       float64 `json:"tps_live"`

	SourceLog        string `json:"source_log"`
	SourceReplayJSON string `json:"source_replay_jsonl"`
	SourcePrometheus string `json:"source_prometheus"`
	SourceStateFile  string `json:"source_state_file"`
}

// epochAnchor derives SlotIndex/SlotsInEpoch from the cluster's known,
// fixed slots-per-epoch (config.SlotsPerEpoch — e.g. 54000). Exact from any
// cold start: no estimation, no waiting for a live epoch transition, just
// current_slot % known. mithril's own RPC hardcodes mainnet's 432,000
// regardless of cluster (pkg/rpcserver/get_epoch_info.go); since that's
// often wrong and we don't poll RPC anyway, the operator tells us the real
// value instead. If left unset (0), the epoch progress bar simply doesn't
// render (Overview.SlotsInEpoch stays 0) rather than guess.
type epochAnchor struct {
	known uint64
}

func (a *epochAnchor) apply(ov *Overview) {
	if a.known == 0 {
		return
	}
	ov.SlotsInEpoch = a.known
	ov.SlotIndex = ov.CurrentSlot % a.known
}

type PipelineState struct {
	LatestSlot        uint64                `json:"latest_slot"`
	LatestTotalMs     float64               `json:"latest_total_ms"`
	LatestStages      []collect.ReplayStage `json:"latest_stages"`
	Avg5mMs           map[string]float64    `json:"avg_5m_ms"`
	Avg30mMs          map[string]float64    `json:"avg_30m_ms"`
	Avg1hMs           map[string]float64    `json:"avg_1h_ms"`
	PromIntervalAvgMs map[string]float64    `json:"prom_interval_avg_ms"`
	PromCountTotal    map[string]uint64     `json:"prom_count_total"`

	AccountRequestedKeys uint64 `json:"account_requested_keys"`
	AccountCacheHits     uint64 `json:"account_cache_hits"`
	AccountIndexHits     uint64 `json:"account_index_hits"`
	AccountIndexMisses   uint64 `json:"account_index_misses"`
}

type BlockProdState struct {
	LatestSlotStats    *collect.SlotStatsEvent       `json:"latest_slot_stats"`
	Outcomes           map[string]map[string]float64 `json:"outcomes"` // outcome -> reason -> cumulative count
	RecentLeaderEvents []collect.LeaderSlotEvent     `json:"recent_leader_events"`
	RecentSlotStats    []collect.SlotStatsEvent      `json:"recent_slot_stats"`
	TxsPerBlockAvg     float64                       `json:"txs_per_block_avg"`
	SlotReplaysTotal   float64                       `json:"slot_replays_total"`
}

type VotingState struct {
	Latest             collect.VotingStatsEvent  `json:"latest"`
	LandedRatePct      float64                   `json:"landed_rate_pct"`
	RecentCastEvents   []collect.VoteCastEvent   `json:"recent_cast_events"`
	RecentLandedEvents []collect.VoteLandedEvent `json:"recent_landed_events"`
}

type State struct {
	GeneratedAt time.Time      `json:"generated_at"`
	Overview    Overview       `json:"overview"`
	Pipeline    PipelineState  `json:"pipeline"`
	BlockProd   BlockProdState `json:"block_production"`
	Voting      VotingState    `json:"voting"`
	Summary     Summary        `json:"summary"`
	System      SystemState    `json:"system"`
}

// SystemState mirrors the latest collect.ProcStats — OS-level metrics for
// mithril's own process (CPU%, RSS, threads, disk I/O, open FDs), polled
// directly from /proc rather than anything mithril logs itself.
type SystemState struct {
	TS    time.Time `json:"ts"`
	Found bool      `json:"found"`
	PID   int       `json:"pid"`

	RssBytes   uint64 `json:"rss_bytes"`
	NumThreads int    `json:"num_threads"`

	HasCPU bool    `json:"has_cpu"`
	CPUPct float64 `json:"cpu_pct"`
	NumCPU int     `json:"num_cpu"`

	HasIO        bool    `json:"has_io"`
	DiskReadBps  float64 `json:"disk_read_bps"`
	DiskWriteBps float64 `json:"disk_write_bps"`

	HasFD   bool `json:"has_fd"`
	OpenFDs int  `json:"open_fds"`

	HasUptime  bool    `json:"has_uptime"`
	UptimeSecs float64 `json:"uptime_secs"`

	// HasAccountsFs/AccountsFsType/AccountsIsTmpfs describe the filesystem
	// backing mithril's accounts DB — flags when it's tmpfs (RAM-backed),
	// since /proc/<pid>/io's byte counters read ~zero for tmpfs regardless
	// of load (there's no storage layer for those bytes to cross), which
	// would otherwise look indistinguishable from "disk is idle".
	HasAccountsFs   bool   `json:"has_accounts_fs"`
	AccountsFsType  string `json:"accounts_fs_type"`
	AccountsIsTmpfs bool   `json:"accounts_is_tmpfs"`
}

// SystemHistPoint is one process-stats poll, kept for the CPU%/RSS chart.
type SystemHistPoint struct {
	TS       time.Time `json:"ts"`
	CPUPct   float64   `json:"cpu_pct"`
	RssBytes uint64    `json:"rss_bytes"`
}

// Summary mirrors the latest parsed collect.SlotSummaryEvent — mithril's
// periodic ("every 100 slots") replay summary block, the richest single
// health snapshot it logs. JSON field names diverge from the Go source
// struct only where clarity needs it; otherwise this is a close 1:1 mirror.
type Summary struct {
	TS     time.Time `json:"ts"`
	Source string    `json:"source"`

	SlotsPerSec float64 `json:"slots_per_sec"`
	HasGap      bool    `json:"has_gap"`
	ReplayGap   int64   `json:"replay_gap"`
	FullGap     int64   `json:"full_gap"`
	Skipped     int     `json:"skipped"`
	WithShreds  int     `json:"with_shreds"`
	EmptyBlocks int     `json:"empty_blocks"`

	HasFinalized   bool   `json:"has_finalized"`
	FinalizedSlot  uint64 `json:"finalized_slot"`
	Switches       int    `json:"switches"`
	SwitchInRAM    int    `json:"switch_in_ram"`
	SwitchFallback int    `json:"switch_fallback"`

	HasExecChecked  bool   `json:"has_exec_checked"`
	ExecCheckedSlot uint64 `json:"exec_checked_slot"`
	Holds           int    `json:"holds"`

	HasShredStats   bool    `json:"has_shred_stats"`
	ShredReadyMed   float64 `json:"shred_ready_med"`
	ShredReadyWorst float64 `json:"shred_ready_worst"`
	ShredAsmMed     float64 `json:"shred_asm_med"`
	ShredAsmMax     float64 `json:"shred_asm_max"`

	HasRepair    bool   `json:"has_repair"`
	RepairSlots  int    `json:"repair_slots"`
	RepairShreds int    `json:"repair_shreds"`
	PeerQuality  string `json:"peer_quality"`

	TxnsMedian    float64 `json:"txns_median"`
	TxnsP90       float64 `json:"txns_p90"`
	TxnsMax       float64 `json:"txns_max"`
	CuPerTxMedian string  `json:"cu_per_tx_median"`
	CuPerTxP90    string  `json:"cu_per_tx_p90"`

	CuMedian string `json:"cu_median"`
	CuP90    string `json:"cu_p90"`
	CuMax    string `json:"cu_max"`

	ExecMedianMs float64 `json:"exec_median_ms"`
	ExecP95Ms    float64 `json:"exec_p95_ms"`
	ExecMaxMs    float64 `json:"exec_max_ms"`
	SlowBlocks   int     `json:"slow_blocks"`

	EffMedian float64 `json:"eff_median"`
	EffP95    float64 `json:"eff_p95"`
	EffMax    float64 `json:"eff_max"`

	HasRSS       bool    `json:"has_rss"`
	RssGiB       float64 `json:"rss_gib"`
	HeapGiB      float64 `json:"heap_gib"`
	HeapInuseGiB float64 `json:"heap_inuse_gib"`
	GCDelta      int     `json:"gc_delta"`
}

// SlotSummaryHistPoint is one 100-slot-summary sample, kept for the "slots
// behind" chart — sparser than the other charts (one point per ~100 slots,
// not per-slot) since that's the underlying signal's real cadence.
type SlotSummaryHistPoint struct {
	TS          time.Time `json:"ts"`
	SlotsPerSec float64   `json:"slots_per_sec"`
	HasGap      bool      `json:"has_gap"`
	ReplayGap   int64     `json:"replay_gap"`
	FullGap     int64     `json:"full_gap"`
}

// ReplayHistPoint is one replay_timings.jsonl-derived sample, kept for the
// stage-latency chart.
type ReplayHistPoint struct {
	TS      time.Time             `json:"ts"`
	Slot    uint64                `json:"slot"`
	TotalMs float64               `json:"total_ms"`
	Stages  []collect.ReplayStage `json:"stages"`
}

// SlotStatsHistPoint is one mithril.log per-slot-line sample, kept for the
// throughput/shred-timing chart.
type SlotStatsHistPoint struct {
	TS      time.Time `json:"ts"`
	Slot    uint64    `json:"slot"`
	Skipped bool      `json:"skipped"`
	Txns    int       `json:"txns"`
	ExecMs  float64   `json:"exec_ms"`
	EffMs   float64   `json:"eff_ms_per_mcu"`
	Ready   float64   `json:"ready_secs"`
	Asm     float64   `json:"asm_secs"`
	Repair  int       `json:"repair"`
}

// VotingHistPoint is one periodic voting-stats sample, kept for the
// votes-landed-over-time chart.
type VotingHistPoint struct {
	TS            time.Time `json:"ts"`
	NetworkLanded uint64    `json:"network_landed"`
	VotesCast     uint64    `json:"votes_cast"`
}

type Store struct {
	mu   sync.Mutex
	cond *sync.Cond

	overview  Overview
	pipeline  PipelineState
	blockProd BlockProdState
	voting    VotingState

	logHealth    sourceHealth
	replayHealth sourceHealth
	promHealth   sourceHealth
	stateHealth  sourceHealth

	replayHist      []ReplayHistPoint
	slotStatsHist   []SlotStatsHistPoint
	votingHist      []VotingHistPoint
	slotSummaryHist []SlotSummaryHistPoint
	systemHist      []SystemHistPoint

	summary Summary
	system  SystemState

	epoch epochAnchor

	generation uint64
}

func New(cluster, consensusMode string, slotsPerEpoch uint64, name, identityPubkey string) *Store {
	s := &Store{
		pipeline: PipelineState{
			Avg5mMs:           map[string]float64{},
			Avg30mMs:          map[string]float64{},
			Avg1hMs:           map[string]float64{},
			PromIntervalAvgMs: map[string]float64{},
			PromCountTotal:    map[string]uint64{},
		},
		blockProd: BlockProdState{Outcomes: map[string]map[string]float64{}},
		epoch:     epochAnchor{known: slotsPerEpoch},
	}
	s.overview.Name = name
	s.overview.IdentityPubkey = identityPubkey
	s.overview.Cluster = cluster
	s.overview.ConsensusMode = consensusMode
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *Store) touch() {
	s.generation++
	s.cond.Broadcast()
}

// WaitForChange blocks until the store changes or timeout elapses (whichever
// first), then returns the current generation counter. Mirrors the Python
// dashboard's `_cond.wait(timeout=30)`: callers re-snapshot unconditionally
// after it returns, whether woken by a real update or the timeout, so an SSE
// stream still gets periodic keepalives when the node is quiet.
func (s *Store) WaitForChange(since uint64, timeout time.Duration) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.generation != since {
		return s.generation
	}
	timer := time.AfterFunc(timeout, func() {
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	})
	defer timer.Stop()
	s.cond.Wait()
	return s.generation
}

// tpsBlockWindow is how many of the most recent blocks liveTPS averages
// over. A fixed block count (rather than a fixed wall-clock window) smooths
// out the jitter of slot pacing itself: a time window's block count varies
// tick to tick (bursts vs. gaps), which made the number visibly jump; a
// block count always averages over the same amount of chain activity.
const tpsBlockWindow = 30

// liveTPS sums txns over the trailing tpsBlockWindow blocks in
// slotStatsHist (skipped slots contribute 0 txns but still count as a
// block, since they still take up chain time) and divides by the actual
// elapsed wall-clock time across that span. Called under s.mu.
func (s *Store) liveTPS() float64 {
	n := len(s.slotStatsHist)
	if n < 2 {
		return 0
	}
	start := n - tpsBlockWindow
	if start < 0 {
		start = 0
	}
	window := s.slotStatsHist[start:]
	var sum int
	for _, pt := range window {
		sum += pt.Txns
	}
	span := window[len(window)-1].TS.Sub(window[0].TS).Seconds()
	if span < 0.1 {
		return 0
	}
	return float64(sum) / span
}

// Snapshot returns a JSON-serializable copy of the current state.
func (s *Store) Snapshot() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	ov := s.overview
	// Epoch progress is derived here rather than polled from mithril's RPC —
	// see epochAnchor — at zero cost to mithril's RPC server.
	s.epoch.apply(&ov)
	ov.TPSLive = round1(s.liveTPS())
	return State{
		GeneratedAt: time.Now(),
		Overview:    ov,
		Pipeline:    s.pipeline,
		BlockProd:   s.blockProd,
		Voting:      s.voting,
		Summary:     s.summary,
		System:      s.system,
	}
}

func (s *Store) Generation() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.generation
}

type HistoryKind string

const (
	HistoryReplay      HistoryKind = "replay"
	HistorySlotStats   HistoryKind = "slot_stats"
	HistoryVoting      HistoryKind = "voting"
	HistorySlotSummary HistoryKind = "slot_summary"
	HistorySystem      HistoryKind = "system"
)

// History returns up to `limit` most recent points of the given kind.
func (s *Store) History(kind HistoryKind, limit int) interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch kind {
	case HistoryReplay:
		return lastN(s.replayHist, limit)
	case HistorySlotStats:
		return lastN(s.slotStatsHist, limit)
	case HistorySystem:
		return lastN(s.systemHist, limit)
	case HistoryVoting:
		return lastN(s.votingHist, limit)
	case HistorySlotSummary:
		return lastN(s.slotSummaryHist, limit)
	default:
		return nil
	}
}

func lastN[T any](xs []T, n int) []T {
	if n <= 0 || n >= len(xs) {
		out := make([]T, len(xs))
		copy(out, xs)
		return out
	}
	out := make([]T, n)
	copy(out, xs[len(xs)-n:])
	return out
}

func appendCapped[T any](xs []T, v T, cap int) []T {
	xs = append(xs, v)
	if len(xs) > cap {
		xs = xs[len(xs)-cap:]
	}
	return xs
}

// ApplyReplaySample ingests one decoded replay_timings.jsonl line.
func (s *Store) ApplyReplaySample(sample collect.ReplaySample) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.replayHealth.LastAt = sample.TS
	s.overview.SourceReplayJSON = s.replayHealth.Status()

	s.pipeline.LatestSlot = sample.Slot
	s.pipeline.LatestTotalMs = sample.TotalMs
	s.pipeline.LatestStages = sample.Stages
	s.pipeline.AccountRequestedKeys = sample.AccountRequestedKeys
	s.pipeline.AccountCacheHits = sample.AccountCacheHits
	s.pipeline.AccountIndexHits = sample.AccountIndexHits
	s.pipeline.AccountIndexMisses = sample.AccountIndexMisses

	s.replayHist = appendCapped(s.replayHist, ReplayHistPoint{
		TS: sample.TS, Slot: sample.Slot, TotalMs: sample.TotalMs, Stages: sample.Stages,
	}, maxReplayHistory)

	recomputeStageAverages(&s.pipeline, s.replayHist)

	s.touch()
}

func recomputeStageAverages(p *PipelineState, hist []ReplayHistPoint) {
	now := time.Now()
	sums5, cnt5 := map[string]float64{}, map[string]int{}
	sums30, cnt30 := map[string]float64{}, map[string]int{}
	sums60, cnt60 := map[string]float64{}, map[string]int{}
	for _, pt := range hist {
		age := now.Sub(pt.TS)
		for _, st := range pt.Stages {
			if age <= 5*time.Minute {
				sums5[st.Name] += st.Ms
				cnt5[st.Name]++
			}
			if age <= 30*time.Minute {
				sums30[st.Name] += st.Ms
				cnt30[st.Name]++
			}
			if age <= time.Hour {
				sums60[st.Name] += st.Ms
				cnt60[st.Name]++
			}
		}
	}
	for name, sum := range sums5 {
		p.Avg5mMs[name] = round1(sum / float64(cnt5[name]))
	}
	for name, sum := range sums30 {
		p.Avg30mMs[name] = round1(sum / float64(cnt30[name]))
	}
	for name, sum := range sums60 {
		p.Avg1hMs[name] = round1(sum / float64(cnt60[name]))
	}
}

func round1(f float64) float64 {
	return float64(int(f*10+0.5)) / 10
}

// ApplyPromSnapshot ingests one Prometheus scrape.
func (s *Store) ApplyPromSnapshot(snap collect.PromSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.promHealth.LastAt = snap.TS
	s.overview.SourcePrometheus = s.promHealth.Status()

	s.overview.CurrentSlot = maxU64(s.overview.CurrentSlot, uint64(snap.Slot))
	s.overview.CurrentEpoch = maxU64(s.overview.CurrentEpoch, uint64(snap.Epoch))

	for name, ms := range snap.StageAvgMsInterval {
		s.pipeline.PromIntervalAvgMs[name] = round1(ms)
	}
	for name, cnt := range snap.StageCountTotal {
		s.pipeline.PromCountTotal[name] = cnt
	}

	s.blockProd.TxsPerBlockAvg = round1(snap.TxsPerBlockAvgTotal)
	s.blockProd.SlotReplaysTotal = snap.SlotReplaysTotal
	if len(snap.LeaderSlotOutcomes) > 0 {
		s.blockProd.Outcomes = snap.LeaderSlotOutcomes
	}

	s.touch()
}

func maxU64(a, b uint64) uint64 {
	if b > a {
		return b
	}
	return a
}

// ApplyNodeState ingests one mithril_state.json read.
func (s *Store) ApplyNodeState(ns collect.NodeState) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.stateHealth.LastAt = time.Now()
	s.overview.SourceStateFile = s.stateHealth.Status()

	s.overview.RunID = ns.CurrentRunID
	s.overview.WriterVersion = ns.LastWriterVersion
	s.overview.WriterCommit = ns.LastWriterCommit
	s.overview.Stage = ns.Stage
	s.overview.LastShutdownReason = ns.LastShutdownReason
	s.overview.LastShutdownAt = ns.LastShutdownAt
	s.overview.BankHashState = ns.LastBankhash
	if ns.Cluster != "" {
		s.overview.Cluster = ns.Cluster
	}
	s.overview.CurrentSlot = maxU64(s.overview.CurrentSlot, ns.LastSlot)
	s.overview.CurrentEpoch = maxU64(s.overview.CurrentEpoch, ns.LastEpoch)

	s.touch()
}

// ApplySlotSummary ingests one completed 100-slot summary block (see
// collect.SlotSummaryAccumulator). Fed separately from ApplyLogEvent since
// it's assembled by the accumulator across several consecutive log lines,
// not parsed from a single one.
func (s *Store) ApplySlotSummary(e collect.SlotSummaryEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logHealth.LastAt = time.Now()
	s.overview.SourceLog = s.logHealth.Status()

	s.summary = Summary{
		TS: e.TS, Source: e.Source,
		SlotsPerSec: e.SlotsPerSec, HasGap: e.HasGap, ReplayGap: e.ReplayGap, FullGap: e.FullGap,
		Skipped: e.Skipped, WithShreds: e.WithShreds, EmptyBlocks: e.EmptyBlocks,
		HasFinalized: e.HasFinalized, FinalizedSlot: e.FinalizedSlot,
		Switches: e.Switches, SwitchInRAM: e.SwitchInRAM, SwitchFallback: e.SwitchFallback,
		HasExecChecked: e.HasExecChecked, ExecCheckedSlot: e.ExecCheckedSlot, Holds: e.Holds,
		HasShredStats: e.HasShredStats, ShredReadyMed: e.ShredReadyMed, ShredReadyWorst: e.ShredReadyWorst,
		ShredAsmMed: e.ShredAsmMed, ShredAsmMax: e.ShredAsmMax,
		HasRepair: e.HasRepair, RepairSlots: e.RepairSlots, RepairShreds: e.RepairShreds, PeerQuality: e.PeerQuality,
		TxnsMedian: e.TxnsMedian, TxnsP90: e.TxnsP90, TxnsMax: e.TxnsMax,
		CuPerTxMedian: e.CuPerTxMedian, CuPerTxP90: e.CuPerTxP90,
		CuMedian: e.CuMedian, CuP90: e.CuP90, CuMax: e.CuMax,
		ExecMedianMs: e.ExecMedianMs, ExecP95Ms: e.ExecP95Ms, ExecMaxMs: e.ExecMaxMs, SlowBlocks: e.SlowBlocks,
		EffMedian: e.EffMedian, EffP95: e.EffP95, EffMax: e.EffMax,
		HasRSS: e.HasRSS, RssGiB: e.RssGiB, HeapGiB: e.HeapGiB, HeapInuseGiB: e.HeapInuseGiB, GCDelta: e.GCDelta,
	}

	s.slotSummaryHist = appendCapped(s.slotSummaryHist, SlotSummaryHistPoint{
		TS: e.TS, SlotsPerSec: e.SlotsPerSec, HasGap: e.HasGap, ReplayGap: e.ReplayGap, FullGap: e.FullGap,
	}, maxSlotSummary)

	if e.HasFinalized {
		s.overview.CurrentSlot = maxU64(s.overview.CurrentSlot, e.FinalizedSlot)
	}

	s.touch()
}

// ApplyProcStats ingests one /proc poll of mithril's own OS process (see
// collect.RunProcStatsPoller). Found=false samples are still applied — the
// dashboard shows "process not found" rather than silently going stale.
func (s *Store) ApplyProcStats(ps collect.ProcStats) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.system = SystemState{
		TS: ps.TS, Found: ps.Found, PID: ps.PID,
		RssBytes: ps.RssBytes, NumThreads: ps.NumThreads,
		HasCPU: ps.HasCPU, CPUPct: ps.CPUPct, NumCPU: ps.NumCPU,
		HasIO: ps.HasIO, DiskReadBps: ps.DiskReadBps, DiskWriteBps: ps.DiskWriteBps,
		HasFD: ps.HasFD, OpenFDs: ps.OpenFDs,
		HasUptime: ps.HasUptime, UptimeSecs: ps.UptimeSecs,
		HasAccountsFs: ps.HasAccountsFs, AccountsFsType: ps.AccountsFsType, AccountsIsTmpfs: ps.AccountsIsTmpfs,
	}

	if ps.Found {
		s.systemHist = appendCapped(s.systemHist, SystemHistPoint{
			TS: ps.TS, CPUPct: ps.CPUPct, RssBytes: ps.RssBytes,
		}, maxSystemHist)
	}

	s.touch()
}

// ApplyLogEvent ingests one parsed mithril.log line (see
// collect.ParseMithrilLogLine); unrecognized types are ignored.
func (s *Store) ApplyLogEvent(ev interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logHealth.LastAt = time.Now()
	s.overview.SourceLog = s.logHealth.Status()

	switch e := ev.(type) {
	case collect.VoteCastEvent:
		s.voting.RecentCastEvents = appendCapped(s.voting.RecentCastEvents, e, maxRecentEvents)
	case collect.VoteLandedEvent:
		s.voting.RecentLandedEvents = appendCapped(s.voting.RecentLandedEvents, e, maxRecentEvents)
	case collect.VotingStatsEvent:
		s.voting.Latest = e
		if e.VotesCastThisRun > 0 {
			s.voting.LandedRatePct = round1(float64(e.NetworkLanded) / float64(e.VotesCastThisRun) * 100)
		}
		s.votingHist = appendCapped(s.votingHist, VotingHistPoint{
			TS: e.TS, NetworkLanded: e.NetworkLanded, VotesCast: e.VotesCastThisRun,
		}, maxVotingHistory)
	case collect.LeaderSlotEvent:
		s.blockProd.RecentLeaderEvents = appendCapped(s.blockProd.RecentLeaderEvents, e, maxRecentEvents)
	case collect.SlotStatsEvent:
		s.blockProd.LatestSlotStats = &e
		s.blockProd.RecentSlotStats = appendCapped(s.blockProd.RecentSlotStats, e, maxRecentEvents)
		s.slotStatsHist = appendCapped(s.slotStatsHist, SlotStatsHistPoint{
			TS: e.TS, Slot: e.Slot, Skipped: e.Skipped, Txns: e.Txns, ExecMs: e.ExecMs,
			EffMs: e.EffMsPerMcu, Ready: e.ReadySecs, Asm: e.AsmSecs, Repair: e.Repaired,
		}, maxSlotStatsHist)
		s.overview.CurrentSlot = maxU64(s.overview.CurrentSlot, e.Slot)
	}

	s.touch()
}
