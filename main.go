// mithril-dash is a standalone, read-only monitoring dashboard for a
// Overclock-Validator/mithril node. It runs alongside mithril on the same
// machine and observes it purely from the outside: tailing mithril.log and
// replay_timings.jsonl, scraping mithril's Prometheus exporter, and polling
// mithril_state.json. It never touches mithril's process, config, storage,
// or RPC server — RPC was deliberately left out, since everything it could
// offer was either redundant with these sources or not worth the extra load
// its getBankHash call would put on the accounts DB mithril's replay hot
// path also uses.
package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/stakingfacilities/mithril-dash/internal/collect"
	"github.com/stakingfacilities/mithril-dash/internal/config"
	"github.com/stakingfacilities/mithril-dash/internal/store"
	"github.com/stakingfacilities/mithril-dash/internal/web"
)

func main() {
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st := store.New(cfg.Cluster, cfg.ConsensusMode, cfg.SlotsPerEpoch)

	// 100ms keeps pace with mithril's ~200ms Alpenglow slot cadence: since we
	// stamp each line with our own ingestion time (mithril's log timestamps
	// are relative-to-process-start, not wall clock — see mithrillog.go),
	// polling slower than the slot cadence would let several real slots land
	// in one poll batch and get near-identical timestamps, which would
	// starve the 1s-window live-TPS calc in store.go of real spacing.
	const logPollInterval = 100 * time.Millisecond

	var slotSummary collect.SlotSummaryAccumulator
	logTailer := &collect.Tailer{
		BaseDir:      cfg.LogDir,
		FileName:     "mithril.log",
		PollInterval: logPollInterval,
		OnLine: func(line string) {
			if ev := collect.ParseMithrilLogLine(line); ev != nil {
				st.ApplyLogEvent(ev)
			}
			// Every raw line also feeds the "100 Slot Summary" accumulator
			// (a separate, stateful multi-line parse) since it's the only
			// place mithril reports how far replay is behind the turbine
			// shred tip it has received.
			if summary, ok := slotSummary.Feed(line); ok {
				st.ApplySlotSummary(summary)
			}
		},
	}
	go logTailer.Run(ctx)

	go collect.RunReplayTimingsTailer(ctx, cfg.LogDir, logPollInterval, st.ApplyReplaySample)
	go collect.RunPromScraper(ctx, cfg.PrometheusURL, cfg.ScrapeInterval, st.ApplyPromSnapshot)
	go collect.RunStatePoller(ctx, cfg.AccountsPath, cfg.StatePollInterval, st.ApplyNodeState)
	go collect.RunProcStatsPoller(ctx, cfg.MithrilProcessMatch, runtime.NumCPU(), cfg.AccountsPath, cfg.ProcStatsInterval, st.ApplyProcStats)

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: web.NewServer(st).Handler()}
	go func() {
		log.Printf("mithril-dash listening on %s (log-dir=%s accounts-path=%s prometheus=%s)",
			cfg.HTTPAddr, cfg.LogDir, cfg.AccountsPath, cfg.PrometheusURL)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down…")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}
