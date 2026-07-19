// Polls Linux /proc for mithril's own process — CPU%, RSS, threads, open
// FDs, disk I/O — independent of anything mithril logs itself. mithril's
// periodic summary block (slotsummary.go) only reports RSS/heap/GC once
// every ~100 slots; /proc gives all of this at whatever interval we want,
// including CPU and disk I/O which mithril doesn't report at all. Linux-only
// by nature (no /proc on macOS/BSD/Windows) — the poller just reports
// nothing if it can't find /proc or the process, so it degrades to "no
// data" rather than erroring on unsupported platforms.
package collect

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// clockTicksPerSec is sysconf(_SC_CLK_TCK), which Go can't read without
// cgo — but it is 100 on essentially every Linux system in practice
// (the kernel constant HZ used to vary this default; every mainstream
// distro's glibc has returned 100 for over a decade).
const clockTicksPerSec = 100

// FindPID scans /proc for a process whose cmdline contains every string in
// matchAll (e.g. []string{"mithril", " run"} to find the long-running
// validator specifically, not a one-off `mithril status`/`mithril
// dashboard` invocation). Returns 0, false if none is found or /proc isn't
// available (non-Linux).
func FindPID(matchAll []string) (int, bool) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, false
	}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		cmdline, err := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		if err != nil {
			continue
		}
		// cmdline is NUL-separated argv; join with spaces for substring matching.
		joined := strings.ReplaceAll(string(cmdline), "\x00", " ")
		matched := true
		for _, m := range matchAll {
			if !strings.Contains(joined, m) {
				matched = false
				break
			}
		}
		if matched {
			return pid, true
		}
	}
	return 0, false
}

// procStatFields is the subset of /proc/<pid>/stat (see proc(5)) we need.
// comm (field 2) is parenthesized and may itself contain spaces or
// parens, so parsing anchors on the LAST ')' rather than naively
// space-splitting the whole line.
type procStatFields struct {
	UtimeTicks uint64
	StimeTicks uint64
	NumThreads int
	StartTicks uint64
}

func parseProcStat(data []byte) (procStatFields, bool) {
	s := string(data)
	lastParen := strings.LastIndex(s, ")")
	if lastParen == -1 || lastParen+2 > len(s) {
		return procStatFields{}, false
	}
	rest := strings.Fields(s[lastParen+1:])
	// rest[0] is field 3 (state); utime=field14 -> rest[11], stime=field15
	// -> rest[12], num_threads=field20 -> rest[17], starttime=field22 ->
	// rest[19].
	if len(rest) < 20 {
		return procStatFields{}, false
	}
	var f procStatFields
	f.UtimeTicks, _ = strconv.ParseUint(rest[11], 10, 64)
	f.StimeTicks, _ = strconv.ParseUint(rest[12], 10, 64)
	f.NumThreads, _ = strconv.Atoi(rest[17])
	f.StartTicks, _ = strconv.ParseUint(rest[19], 10, 64)
	return f, true
}

// parseProcStatus extracts VmRSS (bytes) from /proc/<pid>/status.
func parseProcStatus(data []byte) (rssBytes uint64, ok bool) {
	sc := bufio.NewScanner(strings.NewReader(string(data)))
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0, false
		}
		kb, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, false
		}
		return kb * 1024, true
	}
	return 0, false
}

// parseProcIO extracts cumulative read_bytes/write_bytes from
// /proc/<pid>/io. Requires matching-user or root permission; a caller
// without access should treat a false return as "unavailable", not an error.
func parseProcIO(data []byte) (readBytes, writeBytes uint64, ok bool) {
	sc := bufio.NewScanner(strings.NewReader(string(data)))
	var gotRead, gotWrite bool
	for sc.Scan() {
		line := sc.Text()
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "read_bytes:":
			readBytes, _ = strconv.ParseUint(fields[1], 10, 64)
			gotRead = true
		case "write_bytes:":
			writeBytes, _ = strconv.ParseUint(fields[1], 10, 64)
			gotWrite = true
		}
	}
	return readBytes, writeBytes, gotRead && gotWrite
}

// parseUptimeSeconds extracts the first field of /proc/uptime (seconds
// since boot).
func parseUptimeSeconds(data []byte) (float64, bool) {
	fields := strings.Fields(string(data))
	if len(fields) < 1 {
		return 0, false
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// countOpenFDs counts entries in /proc/<pid>/fd — the number of file
// descriptors the process currently holds open. Requires matching-user or
// root permission.
func countOpenFDs(pid int) (int, bool) {
	entries, err := os.ReadDir(filepath.Join("/proc", strconv.Itoa(pid), "fd"))
	if err != nil {
		return 0, false
	}
	return len(entries), true
}

// mountEntry is one parsed line of /proc/mounts.
type mountEntry struct {
	MountPoint string
	FsType     string
}

// parseMounts parses /proc/mounts (device, mountpoint, fstype, options, …).
func parseMounts(data []byte) []mountEntry {
	var entries []mountEntry
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		entries = append(entries, mountEntry{MountPoint: fields[1], FsType: fields[2]})
	}
	return entries
}

// pickFilesystemType finds which mount entry actually backs targetDev,
// preferring the longest mount point on ties (nested mounts share a path
// prefix, so the innermost/most-specific one is the real answer). This
// mirrors mithril's own pkg/progress/progress.go:getMountPoint — matching
// by device ID rather than naive path-prefix string comparison, so a bind
// mount or a mount nested under another mount can't be misattributed.
// statDev is injected so the matching logic is unit-testable without real
// syscalls; FilesystemType below supplies the real one.
func pickFilesystemType(entries []mountEntry, targetDev uint64, statDev func(path string) (uint64, bool)) (string, bool) {
	var best string
	bestLen := -1
	for _, e := range entries {
		dev, ok := statDev(e.MountPoint)
		if !ok || dev != targetDev {
			continue
		}
		if len(e.MountPoint) > bestLen {
			best = e.FsType
			bestLen = len(e.MountPoint)
		}
	}
	return best, bestLen >= 0
}

// FilesystemType reports the filesystem type backing path (e.g. "ext4",
// "xfs", "tmpfs") by resolving its device ID via stat(2) and matching it
// against /proc/mounts. Used to flag when mithril's accounts DB
// (storage.accounts) sits on tmpfs: /proc/<pid>/io's byte counters only
// count bytes that actually crossed the storage layer, so tmpfs — being
// RAM, with no storage layer at all — reads as ~zero disk I/O no matter how
// much the accounts DB is hammered. Without this, that would look
// indistinguishable from "the disk isn't doing anything."
func FilesystemType(path string) (string, bool) {
	var pathStat syscall.Stat_t
	if err := syscall.Stat(path, &pathStat); err != nil {
		return "", false
	}
	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return "", false
	}
	return pickFilesystemType(parseMounts(data), uint64(pathStat.Dev), func(p string) (uint64, bool) {
		var st syscall.Stat_t
		if err := syscall.Stat(p, &st); err != nil {
			return 0, false
		}
		return uint64(st.Dev), true
	})
}

// IsTmpfsLike reports whether fsType is a RAM-backed filesystem (tmpfs or
// the older ramfs) — no physical disk I/O will ever show up for either.
func IsTmpfsLike(fsType string) bool {
	return fsType == "tmpfs" || fsType == "ramfs"
}

// ProcStats is one poll's worth of OS-level process metrics for mithril.
type ProcStats struct {
	TS    time.Time
	Found bool
	PID   int

	RssBytes   uint64
	NumThreads int

	// CPUPct is 0 on the first sample after (re)acquiring the PID — it's a
	// rate computed from the delta against the previous sample, matching
	// `top`'s convention: 100% == one full core saturated, so a
	// multi-threaded process can exceed 100%.
	HasCPU bool
	CPUPct float64
	NumCPU int

	HasIO        bool
	DiskReadBps  float64
	DiskWriteBps float64

	HasFD   bool
	OpenFDs int

	HasUptime  bool
	UptimeSecs float64

	// HasAccountsFs/AccountsFsType/AccountsIsTmpfs describe the filesystem
	// backing accountsPath (mithril's storage.accounts), so a dashboard can
	// flag "this is tmpfs — disk I/O will read ~zero regardless of load"
	// instead of silently showing a misleading near-zero figure.
	HasAccountsFs   bool
	AccountsFsType  string
	AccountsIsTmpfs bool
}

type procStatsPoller struct {
	matchAll     []string
	numCPU       int
	accountsPath string

	pid            int
	pidFound       bool
	prevUtime      uint64
	prevStime      uint64
	prevReadBytes  uint64
	prevWriteBytes uint64
	prevIOOK       bool
	prevSampleAt   time.Time
	haveSample     bool
}

func (p *procStatsPoller) poll() ProcStats {
	now := time.Now()
	if !p.pidFound {
		if pid, ok := FindPID(p.matchAll); ok {
			p.pid = pid
			p.pidFound = true
			p.haveSample = false // fresh PID: no valid delta yet
		} else {
			return ProcStats{TS: now, Found: false}
		}
	}

	pidDir := filepath.Join("/proc", strconv.Itoa(p.pid))
	statData, err := os.ReadFile(filepath.Join(pidDir, "stat"))
	if err != nil {
		// Process likely exited (e.g. mithril restarted) — forget the PID
		// and re-discover it on the next poll.
		p.pidFound = false
		return ProcStats{TS: now, Found: false}
	}
	stat, ok := parseProcStat(statData)
	if !ok {
		return ProcStats{TS: now, Found: false}
	}

	out := ProcStats{TS: now, Found: true, PID: p.pid, NumThreads: stat.NumThreads, NumCPU: p.numCPU}

	if statusData, err := os.ReadFile(filepath.Join(pidDir, "status")); err == nil {
		if rss, ok := parseProcStatus(statusData); ok {
			out.RssBytes = rss
		}
	}

	if p.haveSample {
		wallDelta := now.Sub(p.prevSampleAt).Seconds()
		if wallDelta > 0.1 {
			cpuTicks := float64((stat.UtimeTicks - p.prevUtime) + (stat.StimeTicks - p.prevStime))
			out.HasCPU = true
			out.CPUPct = (cpuTicks / clockTicksPerSec) / wallDelta * 100
		}
	}

	if ioData, err := os.ReadFile(filepath.Join(pidDir, "io")); err == nil {
		if rb, wb, ok := parseProcIO(ioData); ok {
			if p.haveSample && p.prevIOOK {
				wallDelta := now.Sub(p.prevSampleAt).Seconds()
				if wallDelta > 0.1 {
					out.HasIO = true
					out.DiskReadBps = float64(rb-p.prevReadBytes) / wallDelta
					out.DiskWriteBps = float64(wb-p.prevWriteBytes) / wallDelta
				}
			}
			p.prevReadBytes, p.prevWriteBytes, p.prevIOOK = rb, wb, true
		} else {
			p.prevIOOK = false
		}
	} else {
		p.prevIOOK = false
	}

	if fds, ok := countOpenFDs(p.pid); ok {
		out.HasFD = true
		out.OpenFDs = fds
	}

	if uptimeData, err := os.ReadFile("/proc/uptime"); err == nil {
		if sysUptime, ok := parseUptimeSeconds(uptimeData); ok {
			out.HasUptime = true
			out.UptimeSecs = sysUptime - float64(stat.StartTicks)/clockTicksPerSec
		}
	}

	if p.accountsPath != "" {
		if fsType, ok := FilesystemType(p.accountsPath); ok {
			out.HasAccountsFs = true
			out.AccountsFsType = fsType
			out.AccountsIsTmpfs = IsTmpfsLike(fsType)
		}
	}

	p.prevUtime, p.prevStime, p.prevSampleAt, p.haveSample = stat.UtimeTicks, stat.StimeTicks, now, true
	return out
}

// RunProcStatsPoller polls mithril's own OS process on interval and invokes
// onSample every cycle (Found=false samples included, so the dashboard can
// show "process not found" rather than silently going stale). matchAll
// narrows process discovery — pass []string{"mithril", " run"} for the
// long-running validator specifically. accountsPath (mithril's
// storage.accounts) is checked each poll for whether it's tmpfs-backed —
// pass "" to skip that check.
func RunProcStatsPoller(ctx context.Context, matchAll []string, numCPU int, accountsPath string, interval time.Duration, onSample func(ProcStats)) {
	p := &procStatsPoller{matchAll: matchAll, numCPU: numCPU, accountsPath: accountsPath}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		onSample(p.poll())
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
