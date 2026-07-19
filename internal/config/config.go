// Package config holds mithril-dash's runtime configuration: where to find
// the mithril node's logs, state file, and Prometheus endpoint — all
// read-only, external observation points. Nothing here changes mithril.
package config

import (
	"flag"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

type Config struct {
	// LogDir is mithril's `storage.logs` base directory. mithril-dash follows
	// <LogDir>/latest (mlog's symlink to the current run dir) to tail
	// mithril.log and replay_timings.jsonl.
	LogDir string

	// AccountsPath is mithril's `storage.accounts` directory, which holds
	// mithril_state.json.
	AccountsPath string

	// PrometheusURL is mithril's built-in Prometheus exporter (hardcoded to
	// :9090 in mithril itself as of this writing).
	PrometheusURL string

	// HTTPAddr is the address mithril-dash's own dashboard listens on.
	HTTPAddr string

	// ConsensusMode and Cluster are informational, read from mithril's config
	// (`-mithril-config`) when given — mithril exposes neither over the
	// state file or Prometheus, so without a config pointer these just show
	// "unknown" in the dashboard header.
	ConsensusMode string
	Cluster       string

	// SlotsPerEpoch, when known, is the cluster's exact epoch length — set
	// it and the epoch progress bar is exact from any cold start
	// (slot_index = current_slot % SlotsPerEpoch), no waiting for a live
	// epoch transition. Leave at 0 (unset) to fall back to auto-estimating
	// it from observed (slot, epoch) pairs — see internal/store's
	// epochAnchor — for clusters whose epoch length isn't known upfront.
	SlotsPerEpoch uint64

	// MithrilProcessMatch selects mithril's OS process among everything in
	// /proc, by requiring every string here to appear in its cmdline. The
	// default targets the long-running validator specifically ("mithril
	// run"), not a one-off `mithril status`/`mithril dashboard` invocation.
	// Linux-only (see internal/collect/procstats.go) — harmless to leave on
	// elsewhere, it just never finds a match.
	MithrilProcessMatch []string

	// Name is shown in the dashboard header in place of "Mithril" (e.g. "SF
	// Community Validator"). Purely cosmetic — has no effect on what's
	// monitored.
	Name string

	// IdentityKeypairPath is mithril's `validator.identity_keypair` file —
	// read once at startup to derive and display the validator's public
	// key (see internal/collect/identity.go). Never touches the private
	// key material beyond that one read.
	IdentityKeypairPath string

	ScrapeInterval    time.Duration
	StatePollInterval time.Duration
	ProcStatsInterval time.Duration
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// mithrilTOML is the tiny slice of config.example.toml's shape we care about.
type mithrilTOML struct {
	Storage struct {
		Logs     string `toml:"logs"`
		Accounts string `toml:"accounts"`
	} `toml:"storage"`
	Consensus struct {
		Mode string `toml:"mode"`
	} `toml:"consensus"`
	Network struct {
		Cluster string `toml:"cluster"`
	} `toml:"network"`
	Validator struct {
		IdentityKeypair string `toml:"identity_keypair"`
	} `toml:"validator"`
}

// peekMithrilConfigFlag scans os.Args by hand for -mithril-config/
// --mithril-config before flag.Parse runs, so its values can seed the
// defaults of the flags declared below (flag.Parse itself only supports a
// single pass, and we want config-file values to lose to explicit flags/env,
// not the other way around).
func peekMithrilConfigFlag(args []string) string {
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "-mithril-config" || a == "--mithril-config":
			if i+1 < len(args) {
				return args[i+1]
			}
		case len(a) > len("-mithril-config=") && a[:len("-mithril-config=")] == "-mithril-config=":
			return a[len("-mithril-config="):]
		case len(a) > len("--mithril-config=") && a[:len("--mithril-config=")] == "--mithril-config=":
			return a[len("--mithril-config="):]
		}
	}
	return ""
}

// Load parses flags (falling back to MITHRIL_DASH_* env vars, then mithril's
// own config.toml if -mithril-config points at one, then hardcoded defaults
// matching config.example.toml) into a Config.
func Load() Config {
	var mc mithrilTOML
	if path := peekMithrilConfigFlag(os.Args[1:]); path != "" {
		// Best-effort: a missing/unparseable file just means no defaults get
		// seeded from it, not a fatal error — flags/env still work standalone.
		_, _ = toml.DecodeFile(path, &mc)
	}

	logDefault := mc.Storage.Logs
	if logDefault == "" {
		logDefault = "/mnt/mithril-logs"
	}
	accountsDefault := mc.Storage.Accounts
	if accountsDefault == "" {
		accountsDefault = "/mnt/mithril-accounts"
	}

	var c Config
	flag.String("mithril-config", "", "path to mithril's own config.toml; seeds --log-dir/--accounts-path/--identity-keypair/cluster/consensus-mode defaults")
	flag.StringVar(&c.Name, "name", "", `name shown in the dashboard header instead of "Mithril" (e.g. "SF Community Validator")`)
	flag.StringVar(&c.IdentityKeypairPath, "identity-keypair", envOr("MITHRIL_DASH_IDENTITY_KEYPAIR", mc.Validator.IdentityKeypair),
		"path to mithril's validator.identity_keypair file, to display the validator's public key (never reads/exposes the private key material)")
	flag.StringVar(&c.LogDir, "log-dir", envOr("MITHRIL_DASH_LOG_DIR", logDefault),
		"mithril storage.logs directory (contains the `latest` run symlink)")
	flag.StringVar(&c.AccountsPath, "accounts-path", envOr("MITHRIL_DASH_ACCOUNTS_PATH", accountsDefault),
		"mithril storage.accounts directory (contains mithril_state.json)")
	flag.StringVar(&c.PrometheusURL, "prometheus-url", envOr("MITHRIL_DASH_PROMETHEUS_URL", "http://127.0.0.1:9090/metrics"),
		"mithril's Prometheus /metrics endpoint")
	flag.StringVar(&c.HTTPAddr, "http-addr", envOr("MITHRIL_DASH_HTTP_ADDR", ":8090"),
		"address for mithril-dash's own dashboard web server")
	flag.DurationVar(&c.ScrapeInterval, "scrape-interval", 3*time.Second, "Prometheus scrape interval")
	flag.DurationVar(&c.StatePollInterval, "state-poll-interval", 2*time.Second, "mithril_state.json poll interval")
	slotsPerEpoch := flag.Uint64("slots-per-epoch", 0,
		"exact slots-per-epoch for this cluster, if known (e.g. 54000) — makes the epoch progress bar exact immediately; 0 = auto-estimate")
	processMatch := flag.String("mithril-process-match", "mithril, run",
		"comma-separated substrings that must ALL appear in a process's cmdline to identify it as mithril (Linux /proc only); default targets `mithril run` specifically")
	flag.DurationVar(&c.ProcStatsInterval, "proc-stats-interval", 2*time.Second, "mithril OS-process (/proc) poll interval")
	flag.Parse()

	c.ConsensusMode = mc.Consensus.Mode
	c.Cluster = mc.Network.Cluster
	if *slotsPerEpoch == 0 {
		if v := os.Getenv("MITHRIL_DASH_SLOTS_PER_EPOCH"); v != "" {
			if n, err := strconv.ParseUint(v, 10, 64); err == nil {
				*slotsPerEpoch = n
			}
		}
	}
	c.SlotsPerEpoch = *slotsPerEpoch

	for _, part := range strings.Split(*processMatch, ",") {
		if part = strings.TrimSpace(part); part != "" {
			c.MithrilProcessMatch = append(c.MithrilProcessMatch, part)
		}
	}

	return c
}
