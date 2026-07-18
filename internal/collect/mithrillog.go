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

type LeaderSlotEvent struct {
	TS             time.Time
	Slot           uint64
	Broadcast      bool // true=broadcast local leader, false=missed
	Reason         string
	ReplayFrontier uint64
	LiveSlot       uint64
	Detail         string
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

// leaderBroadcastRe matches pkg/blockprod/leader.go:587
//
//	"ALPENGLOW block production: broadcast local leader slot=%d reason=%s replay_frontier=%d live_slot=%d %s"
var leaderBroadcastRe = regexp.MustCompile(`ALPENGLOW block production: broadcast local leader slot=(\d+) reason=(\S+) replay_frontier=(\d+) live_slot=(\d+)\s*(.*)`)

// leaderMissedRe matches the sibling Warnf line in the same function
//
//	"ALPENGLOW block production: missed local leader slot=%d reason=%s replay_frontier=%d live_slot=%d detail=%s"
var leaderMissedRe = regexp.MustCompile(`ALPENGLOW block production: missed local leader slot=(\d+) reason=(\S+) replay_frontier=(\d+) live_slot=(\d+) detail=(.*)`)

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
	if m := leaderBroadcastRe.FindStringSubmatch(line); m != nil {
		return LeaderSlotEvent{
			TS: now, Slot: parseU64(m[1]), Broadcast: true, Reason: m[2],
			ReplayFrontier: parseU64(m[3]), LiveSlot: parseU64(m[4]), Detail: strings.TrimSpace(m[5]),
		}
	}
	if m := leaderMissedRe.FindStringSubmatch(line); m != nil {
		return LeaderSlotEvent{
			TS: now, Slot: parseU64(m[1]), Broadcast: false, Reason: m[2],
			ReplayFrontier: parseU64(m[3]), LiveSlot: parseU64(m[4]), Detail: strings.TrimSpace(m[5]),
		}
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
