package app

import (
	"context"
	"net/http"
	"testing"
	"time"

	binancesdk "github.com/marxms/go-whale/internal/adapters/outbound/binance"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/candle/engine"
	"github.com/marxms/go-whale/internal/health"
	"github.com/marxms/go-whale/internal/market"
	"github.com/marxms/go-whale/internal/platform/config"
)

type fixedTimestampProvider map[string]int64

func (p fixedTimestampProvider) GetLatestTimestamp(_ context.Context, exchange, marketType, symbol, tf string) (int64, bool) {
	value, ok := p[exchange+":"+marketType+":"+symbol+":"+tf]
	return value, ok
}

func livenessStrptr(value string) *string { return &value }

func livenessApp() *App {
	return &App{cfg: config.Config{
		Exchanges:    []config.ExchangeConfig{{ID: "binance", Enabled: true, MarketType: "future"}},
		CandleEngine: config.CandleEngineConfig{Timeframes: []string{"5m"}},
	}}
}

// A closed TradFi market must not make the crypto fleet read unhealthy: the liveness
// denominator excludes venue markets whose underlying is not a coin.
func TestSampleFleetLivenessExcludesNonCoinMarkets(t *testing.T) {
	now := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	app := livenessApp()
	app.recordNonCoinSymbols("binance", "future", map[string]market.MarketInfo{
		"AAPL/USDT:USDT": {
			Symbol:         livenessStrptr("AAPL/USDT:USDT"),
			UnderlyingType: livenessStrptr("EQUITY"),
		},
		"BTC/USDT:USDT": {
			Symbol:         livenessStrptr("BTC/USDT:USDT"),
			UnderlyingType: livenessStrptr("COIN"),
		},
		"NO-UNDERLYING/USDT": {
			Symbol: livenessStrptr("NO-UNDERLYING/USDT"),
		},
	})

	provider := fixedTimestampProvider{
		"binance:future:BTC/USDT:5m":  now.Add(-time.Minute).UnixMilli(),
		"binance:future:AAPL/USDT:5m": now.Add(-time.Hour).UnixMilli(),
	}
	subs := []string{"binance:future:BTC/USDT", "binance:future:AAPL/USDT"}

	if failing, _ := app.sampleFleetLivenessFromConfig(context.Background(), provider, subs, now); len(failing) != 0 {
		t.Fatalf("failing scopes = %v, want none: the stale TradFi symbol must leave the denominator", failing)
	}
}

func TestSampleFleetLivenessCountsCoinMarkets(t *testing.T) {
	now := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	app := livenessApp()

	provider := fixedTimestampProvider{
		"binance:future:BTC/USDT:5m": now.Add(-time.Hour).UnixMilli(),
	}
	if failing, _ := app.sampleFleetLivenessFromConfig(
		context.Background(), provider, []string{"binance:future:BTC/USDT"}, now,
	); len(failing) != 1 || failing[0] != "binance:future" {
		t.Fatalf("failing scopes = %v, want [binance:future]: a stale coin is still a stale fleet", failing)
	}
}

// TestLivenessUsesReadPath proves the freshness provider reads the consumer path, not the
// engine's buffer-first GetLatestTimestamp: a frozen hot tier with a cold tier advanced by
// REST recovery must yield the cold watermark.
func TestLivenessUsesReadPath(t *testing.T) {
	const ex, mt, sym, tf = "binance", "future", "BTC/USDT", "5m"
	now := time.Now()
	staleHot := candle.BucketTimestamp(now.Add(-30*time.Minute).UnixMilli(), tf)
	freshCold := candle.BucketTimestamp(now.Add(-5*time.Minute).UnixMilli(), tf)

	eng := engine.NewCandleEngine(map[string]int{"default": 64})
	eng.UpdateCandle(ex, mt, sym, tf, candle.Candle{
		Timestamp: staleHot, Open: 100, High: 101, Low: 99, Close: 100, Volume: 1,
	})
	cold := newFakeColdStore()
	key, err := candle.SeriesKeyOf(ex, mt, sym, tf)
	if err != nil {
		t.Fatalf("series key: %v", err)
	}
	cold.rows[key] = map[int64]candle.Candle{freshCold: {Timestamp: freshCold}}
	ts := engine.NewTieredStore(eng, nil, config.CandleEngineConfig{})
	ts.SetColdStore(cold)

	provider := persistedLatestTimestamp{store: ts}
	got, ok := provider.GetLatestTimestamp(context.Background(), ex, mt, sym, tf)
	if !ok || got != freshCold {
		t.Fatalf("persisted watermark = (%d, %v), want cold %d: the read path reconciles hot and cold", got, ok, freshCold)
	}

	// The buffer-first reader the provider replaces would answer with the frozen hot
	// bucket. Asserting it here keeps the distinction this test exists for.
	if engineLatest, ok := eng.GetLatestTimestamp(context.Background(), ex, mt, sym, tf); !ok || engineLatest != staleHot {
		t.Fatalf("engine buffer watermark = (%d, %v), want stale hot %d", engineLatest, ok, staleHot)
	}
}

func livenessReportComponent(report health.Report, name string) (health.ComponentState, bool) {
	for _, c := range report.Components {
		if c.Name == name {
			return c.State, true
		}
	}
	return "", false
}

// TestFeedProducerReportsRestOnlyWhenWatermarkIsNewerThanLastFrame proves the app-side
// classification the health component renders: a watermark newer than the feed's last
// frame can only come from REST; one at or before it is the feed's candle.
func TestFeedProducerReportsRestOnlyWhenWatermarkIsNewerThanLastFrame(t *testing.T) {
	now := time.Date(2026, 11, 7, 12, 4, 0, 0, time.UTC)
	app := &App{
		klineFeed:  &controlledKlineFeed{have: true, silent: 600},
		klineScope: "binance:future",
		cfg:        config.Config{CandleEngine: config.CandleEngineConfig{Timeframes: []string{"5m"}}},
	}
	freshness := []health.CandleFreshness{{
		Scope: "binance:future", Symbols: 1, Stale: 0,
		FreshestAtMs: candle.BucketTimestamp(now.Add(-time.Minute).UnixMilli(), "5m"), HaveData: true,
	}}

	reports := app.feedProducerReports(now, freshness)
	if len(reports) != 1 || reports[0].Producer != health.FeedProducerRESTTotal {
		t.Fatalf("producer = %+v, want rest_total: the candle is newer than the silent feed's last frame", reports)
	}

	// The feed spoke in the current bucket: the newest candle is the feed's.
	app.klineFeed = &controlledKlineFeed{have: true, silent: 60}
	reports = app.feedProducerReports(now, freshness)
	if len(reports) != 1 || reports[0].Producer != health.FeedProducerWSKline {
		t.Fatalf("producer = %+v, want ws_kline: the feed's last frame explains the candle", reports)
	}

	// No readable candle at all: neither producer.
	freshness[0].HaveData = false
	reports = app.feedProducerReports(now, freshness)
	if len(reports) != 1 || reports[0].Producer != health.FeedProducerNone {
		t.Fatalf("producer = %+v, want none with no readable candle", reports)
	}
}

// TestConnectedButNoCandlesIsUnhealthy proves the failure mode the probe missed: every
// connection reports connected+healthy, the engine buffer is empty and the consumer read
// path has no candle, so after the startup grace /health is 503 naming the scope.
func TestConnectedButNoCandlesIsUnhealthy(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	app := livenessApp()

	eng := engine.NewCandleEngine(map[string]int{"default": 64})
	if _, ok := eng.GetLatestTimestamp(ctx, "binance", "future", "BTC/USDT", "5m"); ok {
		t.Fatal("fixture must start with an empty engine buffer")
	}
	ts := engine.NewTieredStore(eng, nil, config.CandleEngineConfig{})
	provider := persistedLatestTimestamp{store: ts}
	if _, ok := provider.GetLatestTimestamp(ctx, "binance", "future", "BTC/USDT", "5m"); ok {
		t.Fatal("fixture must start with an empty consumer read path")
	}

	failing, freshness := app.sampleFleetLivenessFromConfig(
		ctx, provider, []string{"binance:future:BTC/USDT"}, now,
	)
	if len(failing) != 1 || failing[0] != "binance:future" {
		t.Fatalf("failing scopes = %v, want [binance:future]", failing)
	}
	if len(freshness) != 1 || freshness[0].HaveData || freshness[0].Stale != 1 {
		t.Fatalf("freshness = %+v, want one stale scope with no readable data", freshness)
	}

	// The verdict waits for the second sample; after it, the aggregation must be 503.
	h := &livenessHysteresis{}
	h.observe(failing)
	published := h.observe(failing)
	if len(published) != 1 || published[0] != "binance:future" {
		t.Fatalf("published failing scopes = %v, want [binance:future]", published)
	}

	report := health.NewAggregator(health.Probes{
		StartedAt: func() time.Time { return now.Add(-2 * health.StartupGrace) },
		Connections: func() []health.Connection {
			return []health.Connection{{Exchange: "binance", MarketType: "future", Connected: true, Healthy: true, Symbols: 1}}
		},
		LivenessFailing: func() ([]string, bool) { return published, true },
		CandleFreshness: func() ([]health.CandleFreshness, bool) { return freshness, true },
	}).Evaluate()

	if report.Status != "unhealthy" || report.HTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("status = %q/%d, want unhealthy/503", report.Status, report.HTTPStatus)
	}
	if len(report.StaleScopes) != 1 || report.StaleScopes[0] != "binance:future" {
		t.Fatalf("stale scopes = %v, want the scope named", report.StaleScopes)
	}
	if state, ok := livenessReportComponent(report, "fleet_liveness"); !ok || state != health.ComponentUnhealthy {
		t.Fatalf("fleet_liveness = %q (found=%v), want unhealthy", state, ok)
	}
	if len(report.CandleFreshness) != 1 || report.CandleFreshness[0].Symbols != 1 || report.CandleFreshness[0].Stale != 1 {
		t.Fatalf("candle_freshness = %+v, want one monitored and one stale symbol", report.CandleFreshness)
	}

	// The scope is OWNED by the kline feed and the feed never spoke: the producer is
	// none, not REST, because the tape is still the writer behind a failed hand-off.
	app.klineFeed = &configFaultFeed{controlledKlineFeed: controlledKlineFeed{have: true, silent: 600}}
	app.klineScope = "binance:future"
	producers := app.feedProducerReports(now, freshness)
	if len(producers) != 1 || producers[0].Producer != health.FeedProducerNone {
		t.Fatalf("producer = %+v, want none for an owned scope with zero frames", producers)
	}
}

// TestHealthRequiresTwoFailedSamples proves the hysteresis: the freshness field is
// published on the first sample, but the 503 verdict waits for the second consecutive
// failing sample. One Redis timeout must not page anyone.
func TestHealthRequiresTwoFailedSamples(t *testing.T) {
	published := []string(nil)
	freshness := []health.CandleFreshness(nil)
	aggregator := health.NewAggregator(health.Probes{
		StartedAt: func() time.Time { return time.Now().Add(-2 * health.StartupGrace) },
		Connections: func() []health.Connection {
			return []health.Connection{{Exchange: "binance", MarketType: "future", Connected: true, Healthy: true, Symbols: 1}}
		},
		LivenessFailing: func() ([]string, bool) { return published, true },
		CandleFreshness: func() ([]health.CandleFreshness, bool) { return freshness, true },
	})

	h := &livenessHysteresis{}
	published = h.observe([]string{"binance:future"})
	freshness = []health.CandleFreshness{{
		Scope: "binance:future", Symbols: 1, Stale: 1, HaveData: false,
	}}
	if len(published) != 0 {
		t.Fatalf("one failing sample published %v; the verdict requires two", published)
	}
	report := aggregator.Evaluate()
	if report.HTTPStatus != http.StatusOK {
		t.Fatalf("HTTPStatus after one failing sample = %d, want 200", report.HTTPStatus)
	}
	if state, ok := livenessReportComponent(report, "fleet_liveness"); !ok || state != health.ComponentOK {
		t.Fatalf("fleet_liveness after one failing sample = %q (found=%v), want ok", state, ok)
	}
	if len(report.CandleFreshness) != 1 || report.CandleFreshness[0].Stale != 1 {
		t.Fatalf("candle_freshness after one sample = %+v, want the stale count published immediately", report.CandleFreshness)
	}

	// A failing sample followed by a healthy one resets the streak.
	published = h.observe(nil)
	if len(published) != 0 {
		t.Fatalf("a recovered sample still published %v", published)
	}
	published = h.observe([]string{"binance:future"})
	if len(published) != 0 {
		t.Fatalf("the streak did not reset after recovery: %v", published)
	}

	published = h.observe([]string{"binance:future"})
	if len(published) != 1 || published[0] != "binance:future" {
		t.Fatalf("two consecutive failing samples published %v, want [binance:future]", published)
	}
	report = aggregator.Evaluate()
	if report.HTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("HTTPStatus after two consecutive failing samples = %d, want 503", report.HTTPStatus)
	}
	if len(report.StaleScopes) != 1 || report.StaleScopes[0] != "binance:future" {
		t.Fatalf("stale scopes = %v, want [binance:future]", report.StaleScopes)
	}
}

type partitionKlineFeed struct {
	controlledKlineFeed
	stats []binancesdk.PartitionStat
}

func (f *partitionKlineFeed) PartitionStats() []binancesdk.PartitionStat { return f.stats }

func TestFeedProducerUsesNewestPartition(t *testing.T) {
	now := time.Date(2026, 11, 7, 12, 4, 0, 0, time.UTC)
	feed := &partitionKlineFeed{
		controlledKlineFeed: controlledKlineFeed{have: true, silent: 600},
		stats: []binancesdk.PartitionStat{
			{ID: 0, LastFrameAge: 600 * time.Second},
			{ID: 1, LastFrameAge: 10 * time.Second},
		},
	}
	app := &App{
		klineFeed:  feed,
		klineScope: "binance:future",
		cfg:        config.Config{CandleEngine: config.CandleEngineConfig{Timeframes: []string{"5m"}}},
	}
	freshness := []health.CandleFreshness{{
		Scope: "binance:future", Symbols: 2, FreshestAtMs: now.Add(-10 * time.Second).UnixMilli(), HaveData: true,
	}}

	reports := app.feedProducerReports(now, freshness)
	if len(reports) != 1 || reports[0].Producer != health.FeedProducerWSKline {
		t.Fatalf("producer = %+v, want ws_kline: the live partition wrote the candle", reports)
	}
	if reports[0].SilentSeconds != 600 {
		t.Fatalf("silence = %v, want the worst partition (600s) for the alarm", reports[0].SilentSeconds)
	}

	feed.stats = []binancesdk.PartitionStat{{ID: 0, LastFrameAge: 600 * time.Second}}
	reports = app.feedProducerReports(now, freshness)
	if len(reports) != 1 || reports[0].Producer != health.FeedProducerRESTTotal {
		t.Fatalf("producer = %+v, want rest_total when every partition is older than the candle", reports)
	}
}
