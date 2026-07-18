// mithril-dash is a standalone, read-only monitoring dashboard for a
// Overclock-Validator/mithril node. It runs alongside mithril on the same
// machine and observes it purely from the outside: tailing mithril.log and
// replay_timings.jsonl, scraping mithril's Prometheus exporter, and polling
// mithril_state.json and mithril's JSON-RPC. It never touches mithril's
// process, config, or storage.
package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
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

	st := store.New(cfg.Cluster, cfg.ConsensusMode)

	logTailer := &collect.Tailer{
		BaseDir:      cfg.LogDir,
		FileName:     "mithril.log",
		PollInterval: 500 * time.Millisecond,
		OnLine: func(line string) {
			if ev := collect.ParseMithrilLogLine(line); ev != nil {
				st.ApplyLogEvent(ev)
			}
		},
	}
	go logTailer.Run(ctx)

	go collect.RunReplayTimingsTailer(ctx, cfg.LogDir, 500*time.Millisecond, st.ApplyReplaySample)
	go collect.RunPromScraper(ctx, cfg.PrometheusURL, cfg.ScrapeInterval, st.ApplyPromSnapshot)
	go collect.RunStatePoller(ctx, cfg.AccountsPath, cfg.StatePollInterval, st.ApplyNodeState)
	go collect.RunRPCPoller(ctx, cfg.RPCURL, cfg.RPCPollInterval, st.ApplyRPCSnapshot)

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: web.NewServer(st).Handler()}
	go func() {
		log.Printf("mithril-dash listening on %s (log-dir=%s accounts-path=%s prometheus=%s rpc=%s)",
			cfg.HTTPAddr, cfg.LogDir, cfg.AccountsPath, cfg.PrometheusURL, cfg.RPCURL)
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
