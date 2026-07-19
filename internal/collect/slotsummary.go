// Parses mithril's periodic ("every 100 slots") multi-line replay summary
// block (pkg/replay/block.go:2732-2814), e.g.:
//
//	=== 100 Slot Summary ===
//	  source: turbine
//	  progress: 4.0 slots/sec | behind shred tip: replay 161, full 7 | skipped 11 (2 with shreds) | empty blocks 68
//	  consensus: finalized slot 4493576 | switches 0
//	  safety: exec checked slot -- | holds 0
//	  shreds: ready median -13.4s, worst +4.0s (neg = assembled before replay needed it) | asm median 2.8s, max 16.5s
//	  repair: 91 slots, 54155 shreds | peers 99/103 (timely 97%, late 0%, lat p50 107ms, score p50 0.96 p90 1.00)
//	  txns: median 0 | p90 7778 | max 29532 | cu/tx median 150 | p90 150
//	  cu: median 0.0M | p90 1.2M | max 4.4M
//	  execution: median 49ms | p95 277ms | max 804ms | >200ms 15
//	  efficiency: median 228.6ms/Mcu | p95 513.4ms/Mcu | max 682.0ms/Mcu
//	  resources: rss 2.0GiB | heap 1.3GiB | heap inuse 1.4GiB | gc 82
//
// This is the only place mithril reports how far replay is behind the
// turbine shred tip it has actually received ("behind shred tip"),
// independent of — and far more frequent than — the
// replay_frontier/live_slot figures on leader-slot log lines (which only
// fire on our own leader windows, not every slot).
//
// Each clause is its own mlog.Log.InfofPrecise call (a separate line), so
// this can't be parsed by the single-line regex dispatch in mithrillog.go;
// SlotSummaryAccumulator.Feed keeps a small amount of state across
// consecutive lines and emits one complete SlotSummaryEvent when it sees
// the terminal "resources:" line, which is always last.
package collect

import (
	"regexp"
	"strings"
	"time"
)

type SlotSummaryEvent struct {
	TS time.Time

	Source string

	SlotsPerSec float64
	HasGap      bool // false in RPC-only / no-turbine-receiver operation
	ReplayGap   int64
	FullGap     int64
	Skipped     int
	WithShreds  int
	EmptyBlocks int

	HasFinalized   bool
	FinalizedSlot  uint64
	Switches       int
	SwitchInRAM    int
	SwitchFallback int

	HasExecChecked  bool
	ExecCheckedSlot uint64
	Holds           int

	HasShredStats   bool
	ShredReadyMed   float64
	ShredReadyWorst float64
	ShredAsmMed     float64
	ShredAsmMax     float64

	HasRepair    bool
	RepairSlots  int
	RepairShreds int
	PeerQuality  string // raw "peers N/M (...)" clause, empty if untracked

	TxnsMedian    float64
	TxnsP90       float64
	TxnsMax       float64
	CuPerTxMedian string
	CuPerTxP90    string

	CuMedian string
	CuP90    string
	CuMax    string

	ExecMedianMs float64
	ExecP95Ms    float64
	ExecMaxMs    float64
	SlowBlocks   int

	EffMedian float64
	EffP95    float64
	EffMax    float64

	HasRSS       bool
	RssGiB       float64
	HeapGiB      float64
	HeapInuseGiB float64
	GCDelta      int
}

var (
	summaryMarkerRe   = regexp.MustCompile(`^===\s*100 Slot Summary\s*===$`)
	summarySourceRe   = regexp.MustCompile(`^source:\s*(\S+)$`)
	summaryProgressRe = regexp.MustCompile(
		`^progress:\s*([\d.]+) slots/sec(?:\s*\|\s*behind shred tip: replay (\d+), full (\d+))?\s*\|\s*skipped (\d+)(?:\s*\((\d+) with shreds\))?\s*\|\s*empty blocks (\d+)$`)
	summaryConsensusRe = regexp.MustCompile(
		`^consensus:\s*finalized slot (--|\d+)\s*\|\s*switches (\d+)(?:\s*\(in-RAM (\d+), fallback (\d+)\))?$`)
	summarySafetyRe = regexp.MustCompile(`^safety:\s*exec checked slot (--|\d+)\s*\|\s*holds (\d+)$`)
	summaryShredsRe = regexp.MustCompile(
		`^shreds:\s*ready median ([+-][\d.]+)s, worst ([+-][\d.]+)s \(neg = assembled before replay needed it\)\s*\|\s*asm median ([\d.]+)s, max ([\d.]+)s$`)
	summaryRepairRe = regexp.MustCompile(`^repair:\s*(\d+) slots, (\d+) shreds(?:\s*\|\s*(.*))?$`)
	summaryTxnsRe   = regexp.MustCompile(
		`^txns:\s*median ([\d.]+)\s*\|\s*p90 ([\d.]+)\s*\|\s*max ([\d.]+)\s*\|\s*cu/tx median (\S+)\s*\|\s*p90 (\S+)$`)
	summaryCuRe   = regexp.MustCompile(`^cu:\s*median (\S+)\s*\|\s*p90 (\S+)\s*\|\s*max (\S+)$`)
	summaryExecRe = regexp.MustCompile(
		`^execution:\s*median ([\d.]+)ms\s*\|\s*p95 ([\d.]+)ms\s*\|\s*max ([\d.]+)ms\s*\|\s*>200ms (\d+)$`)
	summaryEffRe = regexp.MustCompile(
		`^efficiency:\s*median ([\d.]+)ms/Mcu\s*\|\s*p95 ([\d.]+)ms/Mcu\s*\|\s*max ([\d.]+)ms/Mcu$`)
	summaryResourcesRe = regexp.MustCompile(
		`^resources:(?:\s*rss ([\d.]+)GiB\s*\|)?\s*heap ([\d.]+)GiB\s*\|\s*heap inuse ([\d.]+)GiB\s*\|\s*gc (\d+)$`)
)

// SlotSummaryAccumulator assembles one SlotSummaryEvent across the
// consecutive lines of a single "100 Slot Summary" block. Not safe for
// concurrent use — feed it from a single tailer goroutine.
type SlotSummaryAccumulator struct {
	cur     SlotSummaryEvent
	started bool
}

// Feed processes one raw mithril.log line (prefix included; stripped
// internally). Returns (event, true) once the terminal "resources:" line
// completes a block; otherwise (zero, false).
func (a *SlotSummaryAccumulator) Feed(raw string) (SlotSummaryEvent, bool) {
	line := strings.TrimSpace(stripPrefix(strings.TrimRight(raw, "\r\n")))

	if summaryMarkerRe.MatchString(line) {
		a.cur = SlotSummaryEvent{}
		a.started = true
		return SlotSummaryEvent{}, false
	}
	if !a.started {
		return SlotSummaryEvent{}, false
	}

	if m := summarySourceRe.FindStringSubmatch(line); m != nil {
		a.cur.Source = m[1]
		return SlotSummaryEvent{}, false
	}
	if m := summaryProgressRe.FindStringSubmatch(line); m != nil {
		a.cur.SlotsPerSec = parseF64(m[1])
		if m[2] != "" {
			a.cur.HasGap = true
			a.cur.ReplayGap = int64(parseU64(m[2]))
			a.cur.FullGap = int64(parseU64(m[3]))
		}
		a.cur.Skipped = parseInt(m[4])
		a.cur.WithShreds = parseInt(m[5])
		a.cur.EmptyBlocks = parseInt(m[6])
		return SlotSummaryEvent{}, false
	}
	if m := summaryConsensusRe.FindStringSubmatch(line); m != nil {
		if m[1] != "--" {
			a.cur.HasFinalized = true
			a.cur.FinalizedSlot = parseU64(m[1])
		}
		a.cur.Switches = parseInt(m[2])
		a.cur.SwitchInRAM = parseInt(m[3])
		a.cur.SwitchFallback = parseInt(m[4])
		return SlotSummaryEvent{}, false
	}
	if m := summarySafetyRe.FindStringSubmatch(line); m != nil {
		if m[1] != "--" {
			a.cur.HasExecChecked = true
			a.cur.ExecCheckedSlot = parseU64(m[1])
		}
		a.cur.Holds = parseInt(m[2])
		return SlotSummaryEvent{}, false
	}
	if m := summaryShredsRe.FindStringSubmatch(line); m != nil {
		a.cur.HasShredStats = true
		a.cur.ShredReadyMed = parseF64(m[1])
		a.cur.ShredReadyWorst = parseF64(m[2])
		a.cur.ShredAsmMed = parseF64(m[3])
		a.cur.ShredAsmMax = parseF64(m[4])
		return SlotSummaryEvent{}, false
	}
	if m := summaryRepairRe.FindStringSubmatch(line); m != nil {
		a.cur.HasRepair = true
		a.cur.RepairSlots = parseInt(m[1])
		a.cur.RepairShreds = parseInt(m[2])
		a.cur.PeerQuality = m[3]
		return SlotSummaryEvent{}, false
	}
	if m := summaryTxnsRe.FindStringSubmatch(line); m != nil {
		a.cur.TxnsMedian = parseF64(m[1])
		a.cur.TxnsP90 = parseF64(m[2])
		a.cur.TxnsMax = parseF64(m[3])
		a.cur.CuPerTxMedian = m[4]
		a.cur.CuPerTxP90 = m[5]
		return SlotSummaryEvent{}, false
	}
	if m := summaryCuRe.FindStringSubmatch(line); m != nil {
		a.cur.CuMedian = m[1]
		a.cur.CuP90 = m[2]
		a.cur.CuMax = m[3]
		return SlotSummaryEvent{}, false
	}
	if m := summaryExecRe.FindStringSubmatch(line); m != nil {
		a.cur.ExecMedianMs = parseF64(m[1])
		a.cur.ExecP95Ms = parseF64(m[2])
		a.cur.ExecMaxMs = parseF64(m[3])
		a.cur.SlowBlocks = parseInt(m[4])
		return SlotSummaryEvent{}, false
	}
	if m := summaryEffRe.FindStringSubmatch(line); m != nil {
		a.cur.EffMedian = parseF64(m[1])
		a.cur.EffP95 = parseF64(m[2])
		a.cur.EffMax = parseF64(m[3])
		return SlotSummaryEvent{}, false
	}
	if m := summaryResourcesRe.FindStringSubmatch(line); m != nil {
		if m[1] != "" {
			a.cur.HasRSS = true
			a.cur.RssGiB = parseF64(m[1])
		}
		a.cur.HeapGiB = parseF64(m[2])
		a.cur.HeapInuseGiB = parseF64(m[3])
		a.cur.GCDelta = parseInt(m[4])
		a.cur.TS = time.Now()
		event := a.cur
		a.started = false
		return event, true
	}
	return SlotSummaryEvent{}, false
}
