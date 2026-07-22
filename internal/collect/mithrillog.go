// Parsers for the plain-text lines mithril's own logger (pkg/mlog) writes to
// <storage.logs>/latest/mithril.log. These formats are owned by mithril, not
// us — each regex is anchored to the exact fmt.Sprintf format string at its
// source call site (cited in comments) so a format change there is a
// one-line fix here, not a silent parse failure.
//
// mlog prefixes every line with a relative-elapsed-time tag like "(+12m34s) "
// or "(+  30m45.123s) " (time since the mithril process started, NOT a wall
// clock). We strip it and stamp events with our own ingestion time instead,
// since we're tailing live.
package collect

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

var linePrefixRe = regexp.MustCompile(`^\(\+\s*[^)]*\)\s*`)

func stripPrefix(line string) string {
	return linePrefixRe.ReplaceAllString(line, "")
}

type VoteCastEvent struct {
	TS       time.Time
	VoteType string
	Slot     uint64
	Rank     int
	Block    string
}

type VoteLandedEvent struct {
	TS            time.Time
	Source        string
	Proof         string
	VoteType      string
	Slot          uint64
	Rank          int
	Certificate   string
	Block         string
	NetworkLanded uint64
}

type VotingStatsEvent struct {
	TS                time.Time
	VotesCastThisRun  uint64
	NetworkLanded     uint64
	LastLandedSlot    uint64
	BroadcastQueued   uint64
	BroadcastDropped  uint64
	PeerSends         uint64
	PeerSendErrors    uint64
	ActiveConnections uint64
}

// LeaderSlotEvent is one "broadcast"/"missed local leader" outcome, parsed
// from pkg/blockprod/leader.go's recordLeaderSlotOutcomeLocked. This line's
// field set has grown over time on mithril's actively-developed alpenglow
// branch (terminal=/cause=/finalization_ms=/local_handoff_ms= were all added
// after reason=/replay_frontier=/live_slot=), so — unlike this package's
// other line formats — it's parsed as a generic bag of key=value tokens
// rather than a fixed-position regex: a field mithril adds or reorders just
// leaves the corresponding struct field zero, instead of silently breaking
// the whole match the way a positional regex would.
type LeaderSlotEvent struct {
	TS             time.Time
	Slot           uint64
	Broadcast      bool // true=broadcast local leader, false=missed
	Reason         string
	ReplayFrontier uint64
	LiveSlot       uint64
	Detail         string // free-text failure detail (missed outcomes only)

	// Populated on broadcast (successful) outcomes only — mithril has
	// nothing to report here for a missed slot, since production never
	// completed.
	Block            string
	ParentSlot       uint64
	Txns             int
	WindowElapsedMs  float64
	FinalizationMs   float64
	DeadlineMarginMs float64
	LocalHandoffMs   float64
	Terminal         string
	Cause            string
}

type SlotStatsEvent struct {
	TS             time.Time
	Slot           uint64
	Leader         string // short form, e.g. "7abc...Q9x"
	Skipped        bool
	Txns           int
	CULabel        string // e.g. "31.0M"
	ExecMs         float64
	EffMsPerMcu    float64
	HasEff         bool
	HasShreds      bool
	ReadySecs      float64
	AsmSecs        float64
	Repaired       int
	PartialShreds  int
	RepairedShreds int
}

// voteCastRe matches pkg/consensus/voter.go:522
//
//	"ALPENGLOW voting: cast %s vote slot=%d rank=%d block=%s"
var voteCastRe = regexp.MustCompile(`ALPENGLOW voting: cast (\S+) vote slot=(\d+) rank=(\d+) block=(\S+)`)

// voteLandedRe matches pkg/consensus/voter.go:802
//
//	"ALPENGLOW voting: vote landed source=votor-quic proof=verified-aggregate vote=%s slot=%d rank=%d certificate=%s block=%s network_landed=%d"
var voteLandedRe = regexp.MustCompile(`ALPENGLOW voting: vote landed source=(\S+) proof=(\S+) vote=(\S+) slot=(\d+) rank=(\d+) certificate=(\S+) block=(\S+) network_landed=(\d+)`)

// votingStatsRe matches pkg/consensus/voter.go:872
var votingStatsRe = regexp.MustCompile(`alpenglow voting stats: votes_cast_this_run=(\d+) network_landed=(\d+) last_landed_slot=(\d+) broadcast_queued=(\d+) broadcast_dropped=(\d+) peer_sends=(\d+) peer_send_errors=(\d+) active_connections=(\d+)`)

// leaderBroadcastPrefix/leaderMissedPrefix match pkg/blockprod/leader.go's
// recordLeaderSlotOutcomeLocked (Infof/Warnf calls a few lines apart in the
// same function). Everything after the prefix is "key=value key=value ..."
// tokens — parsed generically by parseLeaderKV below rather than a
// fixed-position regex, since this line's field set (terminal=/cause=/
// finalization_ms=/local_handoff_ms= were all added after the original
// reason=/replay_frontier=/live_slot=) keeps growing on this actively
// developed branch, sometimes ahead of whatever commit this comment cites.
const (
	leaderBroadcastPrefix = "ALPENGLOW block production: broadcast local leader "
	leaderMissedPrefix    = "ALPENGLOW block production: missed local leader "
)

var kvTokenRe = regexp.MustCompile(`(\w+)=(\S+)`)

// parseLeaderKV extracts every "key=value" token from a leader-slot outcome
// line. detail= is handled specially: mithril always writes it last with a
// free-text value that can itself contain spaces (e.g. a wrapped error
// message), unlike every other field here which is a single token — so it's
// split out first and everything before it is token-scanned.
func parseLeaderKV(rest string) map[string]string {
	kv := map[string]string{}
	if idx := strings.Index(rest, "detail="); idx >= 0 {
		kv["detail"] = strings.TrimSpace(rest[idx+len("detail="):])
		rest = rest[:idx]
	}
	for _, m := range kvTokenRe.FindAllStringSubmatch(rest, -1) {
		kv[m[1]] = m[2]
	}
	return kv
}

func newLeaderSlotEvent(now time.Time, broadcast bool, kv map[string]string) LeaderSlotEvent {
	return LeaderSlotEvent{
		TS: now, Slot: parseU64(kv["slot"]), Broadcast: broadcast,
		Reason: kv["reason"], ReplayFrontier: parseU64(kv["replay_frontier"]), LiveSlot: parseU64(kv["live_slot"]),
		Detail: kv["detail"],

		Block: kv["block"], ParentSlot: parseU64(kv["parent_slot"]), Txns: parseInt(kv["txns"]),
		WindowElapsedMs: parseF64(kv["window_elapsed_ms"]), FinalizationMs: parseF64(kv["finalization_ms"]),
		DeadlineMarginMs: parseF64(kv["deadline_margin_ms"]), LocalHandoffMs: parseF64(kv["local_handoff_ms"]),
		Terminal: kv["terminal"], Cause: kv["cause"],
	}
}

// slotStatsRe matches pkg/replay/summary_stats.go buildSlotStatsLine.
var slotStatsRe = regexp.MustCompile(`^slot (\d+)\s*\|\s*leader (\S+)\s*\|\s*txns\s+(\d+)\s*\|\s*cu\s+(\S+)\s*\|\s*exec\s+([\d.]+)ms\s*\|\s*eff\s+(?:(--)|([\d.]+)ms/Mcu)(?:\s*\|\s*shreds\(ready\s*([+-]?[\d.]+)s,\s*asm\s*([\d.]+)s,\s*repair (\d+)\))?$`)

// slotSkippedRe matches pkg/replay/summary_stats.go buildSkippedStatsLine.
var slotSkippedRe = regexp.MustCompile(`^slot (\d+)\s*\|\s*leader (\S+)\s*\|\s*txns\s+\S+\s*\|\s*cu\s+\S+\s*\|\s*exec\s+\S+\s*\|\s*eff\s+\S+\s*\|\s*skipped(?:\s*\|\s*shreds seen (\d+) \(repaired (\d+)\) — block never completed)?$`)

func parseU64(s string) uint64 {
	v, _ := strconv.ParseUint(s, 10, 64)
	return v
}

func parseInt(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}

func parseF64(s string) float64 {
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

// ParseMithrilLogLine attempts each known pattern against one line of
// mithril.log and returns the first typed event that matches, or nil.
func ParseMithrilLogLine(raw string) interface{} {
	line := stripPrefix(strings.TrimRight(raw, "\r\n"))
	now := time.Now()

	if m := voteCastRe.FindStringSubmatch(line); m != nil {
		return VoteCastEvent{
			TS: now, VoteType: m[1], Slot: parseU64(m[2]),
			Rank: parseInt(m[3]), Block: m[4],
		}
	}
	if m := voteLandedRe.FindStringSubmatch(line); m != nil {
		return VoteLandedEvent{
			TS: now, Source: m[1], Proof: m[2], VoteType: m[3], Slot: parseU64(m[4]),
			Rank: parseInt(m[5]), Certificate: m[6], Block: m[7], NetworkLanded: parseU64(m[8]),
		}
	}
	if m := votingStatsRe.FindStringSubmatch(line); m != nil {
		return VotingStatsEvent{
			TS: now, VotesCastThisRun: parseU64(m[1]), NetworkLanded: parseU64(m[2]),
			LastLandedSlot: parseU64(m[3]), BroadcastQueued: parseU64(m[4]), BroadcastDropped: parseU64(m[5]),
			PeerSends: parseU64(m[6]), PeerSendErrors: parseU64(m[7]), ActiveConnections: parseU64(m[8]),
		}
	}
	if rest, ok := strings.CutPrefix(line, leaderBroadcastPrefix); ok {
		return newLeaderSlotEvent(now, true, parseLeaderKV(rest))
	}
	if rest, ok := strings.CutPrefix(line, leaderMissedPrefix); ok {
		return newLeaderSlotEvent(now, false, parseLeaderKV(rest))
	}
	if m := slotSkippedRe.FindStringSubmatch(line); m != nil {
		ev := SlotStatsEvent{TS: now, Slot: parseU64(m[1]), Leader: m[2], Skipped: true}
		if m[3] != "" {
			ev.PartialShreds = parseInt(m[3])
			ev.RepairedShreds = parseInt(m[4])
			ev.HasShreds = true
		}
		return ev
	}
	if m := slotStatsRe.FindStringSubmatch(line); m != nil {
		ev := SlotStatsEvent{
			TS: now, Slot: parseU64(m[1]), Leader: m[2], Txns: parseInt(m[3]),
			CULabel: m[4], ExecMs: parseF64(m[5]),
		}
		if m[7] != "" {
			ev.HasEff = true
			ev.EffMsPerMcu = parseF64(m[7])
		}
		if m[8] != "" {
			ev.HasShreds = true
			ev.ReadySecs = parseF64(m[8])
			ev.AsmSecs = parseF64(m[9])
			ev.Repaired = parseInt(m[10])
		}
		return ev
	}
	return nil
}
