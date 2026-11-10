package app

import (
	"context"
	"time"

	binancesdk "github.com/marxms/go-whale/internal/adapters/outbound/binance"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/health"
	"github.com/marxms/go-whale/internal/market"
	"github.com/marxms/go-whale/internal/recovery"
)

// fleetLivenessInterval is how often /health's fleet-liveness verdict is refreshed. It
// is deliberately independent of the 30-minute gap-recovery pass: a health signal that
// old cannot page anyone.
const fleetLivenessInterval = time.Minute

// candleLatestReader is the CONSUMER read path. TieredStore implements it and reconciles
// the hot and cold tiers, so a frozen engine buffer cannot mask a cold tier that kept
// advancing through REST recovery.
type candleLatestReader interface {
	GetCandlesDesc(ctx context.Context, exchange, marketType, symbol, timeframe string, limit int) ([]candle.Candle, error)
}

// persistedLatestTimestamp reads the watermark from the consumer read path. It is the one
// freshness source for both /health and the gap-recovery backstop: "fresh" means what the
// graph can read, not what the engine's buffer remembers.
type persistedLatestTimestamp struct {
	store candleLatestReader
}

func (p persistedLatestTimestamp) GetLatestTimestamp(
	ctx context.Context, exchangeID, marketType, symbol, tf string,
) (int64, bool) {
	if p.store == nil {
		return 0, false
	}
	readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	candles, err := p.store.GetCandlesDesc(readCtx, exchangeID, marketType, market.NormalizeSymbol(symbol), tf, 1)
	if err != nil || len(candles) == 0 {
		return 0, false
	}
	return candles[0].Timestamp, true
}

// consumerLatestTimestamp binds the persisted adapter to the store the HTTP consumers
// read. A nil store is unconfigured, never fresh by default.
func (a *App) consumerLatestTimestamp() recovery.LatestTimestampProvider {
	if a.store == nil {
		return persistedLatestTimestamp{}
	}
	return persistedLatestTimestamp{store: a.store}
}

// livenessHysteresis publishes a scope as failing only after two consecutive failing
// samples. A single sample over the threshold must not flip /health to 503: one Redis
// timeout is not a frozen fleet.
type livenessHysteresis struct {
	counts map[string]int
}

func (h *livenessHysteresis) observe(failing []string) []string {
	if h.counts == nil {
		h.counts = make(map[string]int, len(failing))
	}
	current := make(map[string]struct{}, len(failing))
	for _, scope := range failing {
		current[scope] = struct{}{}
		h.counts[scope]++
	}
	for scope := range h.counts {
		if _, ok := current[scope]; !ok {
			delete(h.counts, scope)
		}
	}
	published := make([]string, 0, len(failing))
	for _, scope := range failing {
		if h.counts[scope] >= 2 {
			published = append(published, scope)
		}
	}
	return published
}

// runFleetLiveness refreshes the failing-scope list and the per-scope freshness detail
// every interval until the stream context is canceled. The first sample runs immediately:
// waiting a full interval left a freshly restarted process reporting nothing about the
// watermarks it already had.
func (a *App) runFleetLiveness(interval time.Duration) {
	if a.manager == nil || a.metricsCollector == nil {
		return
	}
	a.sampleAndPublishFleetLiveness()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-a.streamCtx.Done():
			return
		case <-ticker.C:
			a.sampleAndPublishFleetLiveness()
		}
	}
}

// sampleAndPublishFleetLiveness samples the persisted watermarks and publishes both the
// hysteresis-filtered failing scopes and the freshness detail. The feed-producer report
// comes from the same sample so the two components never disagree.
func (a *App) sampleAndPublishFleetLiveness() {
	subs := a.manager.ListSubscriptions()
	now := time.Now()
	failing, freshness := a.sampleFleetLivenessFromConfig(
		a.streamCtx, a.consumerLatestTimestamp(), subs, now,
	)
	a.metricsCollector.SetFleetLiveness(a.livenessHysteresis.observe(failing), freshness)
	a.metricsCollector.SetFeedProducers(a.feedProducerReports(now, freshness))
}

// sampleFleetLivenessFromConfig computes PER-SYMBOL fleet freshness for every enabled scope
// from the persisted LatestTimestampProvider (never the in-memory buffer, per the
// consumer-path contract), sets the StreamSymbolsTotal/Stale gauges, and returns the
// canonical scopes whose stale fraction exceeds health.LivenessStaleFraction together
// with the per-scope detail that verdict is made of.
//
// The fraction rule lives in internal/health; the staleness predicate lives in
// internal/recovery. This method only converts configuration into typed scopes and
// binds the two.
func (a *App) sampleFleetLivenessFromConfig(ctx context.Context, provider recovery.LatestTimestampProvider, subs []string, now time.Time) ([]string, []health.CandleFreshness) {
	scopes := make([]health.LivenessScope, 0, len(a.cfg.Exchanges))
	for _, exCfg := range a.cfg.Exchanges {
		if !exCfg.Enabled {
			continue
		}
		marketType, err := market.FromConfig(exCfg.MarketType)
		if err != nil {
			continue
		}
		scopes = append(scopes, health.LivenessScope{ExchangeID: exCfg.ID, MarketType: marketType})
	}

	fastestTF, fastestMs, haveTF := recovery.FastestInterval(a.timeframes())
	staleLagMs := int64(recovery.StalenessIntervals) * fastestMs
	nowMs := now.UnixMilli()
	isStale := func(latestMs int64, haveData bool) bool {
		if !haveTF {
			return true
		}
		return recovery.IsStaleWatermark(latestMs, haveData, nowMs, staleLagMs)
	}

	return health.SampleFleetLiveness(
		ctx, scopes, provider, a.livenessSubscriptions(subs), fastestTF, isStale, a.streamMetrics,
	)
}

// feedProducerReports names the producer of the newest readable candle for the mounted
// kline feed's scope. The feed's last-frame clock and the persisted watermark together
// distinguish ws_kline from rest_total without a second monitor: while the feed is the
// only writer, a watermark at or before its last frame (within one bucket of tolerance)
// came from the feed; a watermark strictly newer than that came from REST.
func (a *App) feedProducerReports(now time.Time, freshness []health.CandleFreshness) []health.FeedProducerReport {
	a.klineMu.Lock()
	feed := a.klineFeed
	scope := a.klineScope
	a.klineMu.Unlock()
	if feed == nil || scope == "" {
		return nil
	}

	canonical := health.CanonicalScope(scope)
	silent, havePartitions := feed.SecondsSinceLastFrame()
	// SilentSeconds reports the WORST partition for the alarm, but the producer is
	// classified against the NEWEST one: a silent partition must not relabel a candle a
	// live partition wrote as rest_total.
	newestSilent := silent
	if provider, ok := feed.(interface {
		PartitionStats() []binancesdk.PartitionStat
	}); ok {
		partitions := provider.PartitionStats()
		if len(partitions) == 0 {
			havePartitions = false
		} else {
			newest := partitions[0].LastFrameAge
			for _, p := range partitions[1:] {
				if p.LastFrameAge < newest {
					newest = p.LastFrameAge
				}
			}
			newestSilent = newest.Seconds()
		}
	}
	report := health.FeedProducerReport{Scope: canonical, SilentSeconds: silent, HaveFeed: true}

	// A feed that never spoke is a configuration failure, not a REST hand-off: the tape is
	// still the writer, so no candle in this scope carries either producer label.
	if fault, ok := feed.(interface{ ConfigFault(time.Time) bool }); ok && fault.ConfigFault(now) {
		report.Producer = health.FeedProducerNone
		return []health.FeedProducerReport{report}
	}

	var freshest health.CandleFreshness
	found := false
	for _, f := range freshness {
		if health.CanonicalScope(f.Scope) == canonical {
			freshest, found = f, true
			break
		}
	}

	switch {
	case !found || !freshest.HaveData:
		report.Producer = health.FeedProducerNone
	case !havePartitions:
		// A feed with no live partition is dead, not quiet. Whatever is readable was
		// written by REST or by the tape, never by this feed.
		report.Producer = health.FeedProducerRESTTotal
	default:
		_, fastestMs, haveTF := recovery.FastestInterval(a.timeframes())
		toleranceMs := int64(0)
		if haveTF {
			toleranceMs = fastestMs
		}
		lastFrameMs := now.Add(-time.Duration(newestSilent * float64(time.Second))).UnixMilli()
		if freshest.FreshestAtMs > lastFrameMs+toleranceMs {
			// The readable candle is newer than the feed's last frame can explain: REST
			// wrote it while the feed was silent.
			report.Producer = health.FeedProducerRESTTotal
		} else {
			report.Producer = health.FeedProducerWSKline
		}
	}
	return []health.FeedProducerReport{report}
}

// recordNonCoinSymbols stores the venue markets whose underlying is not a coin. TradFi
// follows the venue's own trading hours, so a closed equity market must not make the whole
// crypto fleet read unhealthy. The connection health and gap recovery still cover them.
func (a *App) recordNonCoinSymbols(exchangeID, marketType string, markets map[string]market.MarketInfo) {
	for _, info := range markets {
		if info.UnderlyingType == nil || *info.UnderlyingType == "COIN" {
			continue
		}
		if info.Symbol == nil {
			continue
		}
		symbol := market.NormalizeSymbol(*info.Symbol)
		if symbol == "" {
			continue
		}
		a.nonCoinSymbols.Store(exchangeID+":"+marketType+":"+symbol, struct{}{})
	}
}

// livenessSubscriptions drops the TradFi symbols from the liveness denominator.
func (a *App) livenessSubscriptions(subs []string) []string {
	out := make([]string, 0, len(subs))
	for _, key := range subs {
		if _, nonCoin := a.nonCoinSymbols.Load(key); nonCoin {
			continue
		}
		out = append(out, key)
	}
	return out
}
