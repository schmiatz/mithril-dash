// Scrapes mithril's built-in Prometheus exporter (pkg/statsd, hardcoded to
// :9090/metrics as of this writing — see StartMetricsServer). Histograms and
// counters there are cumulative for the process lifetime, so we snapshot on
// every scrape and diff against the previous snapshot to get "how much did
// this change in the last interval" — the closest thing to a live rate
// without needing a real Prometheus server + PromQL in front of this.
package collect

import (
	"context"
	"net/http"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// stageHistograms are the block-level replay latency metrics mithril
// exports as Prometheus histograms (pkg/statsd/statsd.go MetricToType); this
// is the same stage set surfaced per-slot by the replay_timings.jsonl
// tailer, kept here too because Prometheus survives a mithril-dash restart
// and gives a longer/cumulative view the in-memory ring buffer can't.
var stageHistogramNames = []string{
	"preprocess_block", "load_block_accounts", "tx_loop", "reward", "rent",
	"run_incinerator", "block_update_accounts", "accounts_delta_hash", "bank_hash",
}

type PromSnapshot struct {
	TS time.Time

	// StageAvgMsInterval is Δsum/Δcount since the previous scrape for each
	// stage histogram — average latency of that stage over the interval.
	StageAvgMsInterval map[string]float64
	// StageCountTotal is the cumulative event count for each stage histogram.
	StageCountTotal map[string]uint64

	Epoch float64
	Slot  float64

	SlotReplaysTotal    float64
	TxsPerBlockAvgTotal float64 // cumulative sum/count average since process start

	// LeaderSlotOutcomes[outcome][reason] = cumulative count.
	LeaderSlotOutcomes map[string]map[string]float64
}

type promScraper struct {
	url  string
	prev map[string]*dto.MetricFamily
}

func newPromScraper(url string) *promScraper {
	return &promScraper{url: url}
}

func histSumCount(mf *dto.MetricFamily) (sum float64, count uint64) {
	if mf == nil {
		return 0, 0
	}
	for _, m := range mf.GetMetric() {
		h := m.GetHistogram()
		if h == nil {
			continue
		}
		sum += h.GetSampleSum()
		count += h.GetSampleCount()
	}
	return sum, count
}

func gaugeValue(mf *dto.MetricFamily) float64 {
	if mf == nil {
		return 0
	}
	for _, m := range mf.GetMetric() {
		if g := m.GetGauge(); g != nil {
			return g.GetValue()
		}
	}
	return 0
}

func labelValue(m *dto.Metric, name string) string {
	for _, lp := range m.GetLabel() {
		if lp.GetName() == name {
			return lp.GetValue()
		}
	}
	return ""
}

func (p *promScraper) scrape(ctx context.Context, client *http.Client) (PromSnapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.url, nil)
	if err != nil {
		return PromSnapshot{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return PromSnapshot{}, err
	}
	defer resp.Body.Close()

	// mithril's exporter (promauto, default settings) emits classic ASCII
	// metric names, so the legacy scheme is the correct match; the
	// zero-value TextParser{} has an unset scheme and panics on first parse.
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return PromSnapshot{}, err
	}

	snap := PromSnapshot{
		TS:                 time.Now(),
		StageAvgMsInterval: map[string]float64{},
		StageCountTotal:    map[string]uint64{},
		LeaderSlotOutcomes: map[string]map[string]float64{},
	}

	for _, name := range stageHistogramNames {
		mf := families[name]
		sum, count := histSumCount(mf)
		snap.StageCountTotal[name] = count
		if prevMF, ok := p.prev[name]; ok {
			prevSum, prevCount := histSumCount(prevMF)
			dCount := count - prevCount
			dSum := sum - prevSum
			if dCount > 0 && dSum >= 0 {
				snap.StageAvgMsInterval[name] = dSum / float64(dCount)
			}
		}
	}

	snap.Epoch = gaugeValue(families["epoch"])
	snap.Slot = gaugeValue(families["slot"])

	if mf := families["slot_replays"]; mf != nil {
		for _, m := range mf.GetMetric() {
			if c := m.GetCounter(); c != nil {
				snap.SlotReplaysTotal += c.GetValue()
			}
		}
	}
	if mf := families["txs_per_block"]; mf != nil {
		sum, count := histSumCount(mf)
		if count > 0 {
			snap.TxsPerBlockAvgTotal = sum / float64(count)
		}
	}

	if mf := families["block_production_leader_slots_total"]; mf != nil {
		for _, m := range mf.GetMetric() {
			c := m.GetCounter()
			if c == nil {
				continue
			}
			outcome := labelValue(m, "outcome")
			reason := labelValue(m, "reason")
			if snap.LeaderSlotOutcomes[outcome] == nil {
				snap.LeaderSlotOutcomes[outcome] = map[string]float64{}
			}
			snap.LeaderSlotOutcomes[outcome][reason] += c.GetValue()
		}
	}

	p.prev = families
	return snap, nil
}

// RunPromScraper polls the Prometheus endpoint on interval and invokes
// onSnapshot with each successfully parsed scrape. Scrape errors (endpoint
// not up yet, network hiccup) are silently skipped — the next tick retries.
func RunPromScraper(ctx context.Context, url string, interval time.Duration, onSnapshot func(PromSnapshot)) {
	client := &http.Client{Timeout: 5 * time.Second}
	scraper := newPromScraper(url)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if snap, err := scraper.scrape(ctx, client); err == nil {
			onSnapshot(snap)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
