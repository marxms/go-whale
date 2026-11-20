package app

import (
	"net/http"
	"time"

	httpapi "github.com/marxms/go-whale/internal/adapters/inbound/httpapi"
	prommetrics "github.com/marxms/go-whale/internal/adapters/outbound/prometheus"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/health"
	"github.com/marxms/go-whale/internal/market"
	"github.com/marxms/go-whale/internal/platform/config"
)

// buildHTTP constructs the metrics collector and the /health probes.
func (a *App) buildHTTP(cfg config.Config, tfReg *candle.TimeframeRegistry, metricsProvider *prommetrics.Provider) {
	a.metricsCollector = httpapi.NewMetricsCollector(a.manager, a.log)
	a.metricsCollector.SetGaugeSink(metricsProvider.HealthSink())
	a.metricsCollector.SetTimeframeRegistry(tfReg)
	// One row per feed partition: the row is what distinguishes a silent partition from a dead
	// fleet without reading the process's sockets.
	a.metricsCollector.SetPartitions(a.partitionHealth)
	a.metricsCollector.SetFeedReconnectExhausted(a.klineReconnectExhaustedProbe)

	// The per-TIMEFRAME data age. Without it a derived timeframe can freeze under a live 5m while
	// /health keeps saying healthy.
	if a.candleEngine != nil {
		engine := a.candleEngine
		exchange, marketType := a.defaultExchange, a.defaultMarketType
		timeframes := cfg.CandleEngine.Timeframes
		a.metricsCollector.SetTimeframeFreshness(func() ([]health.TimeframeFreshness, bool) {
			if len(timeframes) == 0 {
				return nil, false
			}
			nowMs := time.Now().UnixMilli()
			rows := engine.TimeframeFreshness(exchange, marketType, timeframes, nowMs)
			out := make([]health.TimeframeFreshness, 0, len(rows))
			for _, row := range rows {
				out = append(out, health.TimeframeFreshness{
					Scope:        row.Scope,
					Timeframe:    row.Timeframe,
					NewestAgeMs:  row.NewestAgeMs,
					HaveData:     row.HaveData,
					StaleAfterMs: timeframeFreezeThreshold(row.Timeframe),
				})
			}
			return out, true
		})
	}

	// Give /health its one INTEGRITY assertion: the age of the last successful cold-tier
	// commit. When QuestDB is disabled or failed to init, qdbStore is nil and the probe
	// omits the field rather than asserting a durability it was never configured to have.
	if a.qdbStore != nil {
		coldStore := a.qdbStore
		a.metricsCollector.SetColdWriteFreshness(func() (time.Duration, bool) {
			return time.Since(coldStore.Stats().LastFlushTime), true
		})
	}

	// Declare the exchange:market_type scopes /health must see live after the startup
	// grace, so a totally absent exchange is reported 503 instead of a false "healthy".
	var expectedScopes []string
	for _, exCfg := range cfg.Exchanges {
		if !exCfg.Enabled {
			continue
		}
		marketType, err := market.FromConfig(exCfg.MarketType)
		if err != nil {
			a.logger().Warn("skipping expected scope", "exchange", exCfg.ID, "error", err)
			continue
		}
		expectedScopes = append(expectedScopes, exCfg.ID+":"+marketType.String())
	}
	a.metricsCollector.SetExpectedScopes(expectedScopes)

	// Declare how many symbols each scope is CONFIGURED to stream, so /health and the
	// metrics can compare it against what is effectively subscribed. Without this the
	// probe cannot tell a fully-subscribed fleet from one that stopped at 40%.
	configuredCoverage := make(map[string]int, len(cfg.Exchanges))
	for _, exCfg := range cfg.Exchanges {
		if !exCfg.Enabled {
			continue
		}
		marketType, err := market.FromConfig(exCfg.MarketType)
		if err != nil {
			a.logger().Warn("skipping configured coverage", "exchange", exCfg.ID, "error", err)
			continue
		}
		scope := exCfg.ID + ":" + marketType.String()
		configuredCoverage[scope] += market.DeclaredSymbolCount(exCfg.Discovery.Enabled, exCfg.Discovery.MaxAssets, len(exCfg.Symbols))
	}
	a.metricsCollector.SetConfiguredCoverage(configuredCoverage)
}

// timeframeFreezeThreshold is how old a timeframe's newest bucket may get before /health calls it
// frozen: four intervals, floored at ten minutes so a single missed 5m boundary is not an alarm.
func timeframeFreezeThreshold(tf string) int64 {
	ms, ok := candle.ParseTimeframeMs(tf)
	if !ok || ms <= 0 {
		return 0
	}
	if threshold := 4 * ms; threshold > 10*60*1000 {
		return threshold
	}
	return 10 * 60 * 1000
}

// newHTTPRouter assembles the production router.
//
// The three ports enter by PARAMETER because the `App` field is Redis's concrete type, and the
// assembly must be verifiable without a Redis. The production caller passes `a.derivStore` for
// all three: the concrete type implements the three capabilities, and each route receives only
// its own.
func (a *App) newHTTPRouter(
	derivReader derivatives.Reader,
	derivBaselines derivatives.BaselineReader,
	derivPrices derivatives.PriceReader,
) http.Handler {
	return httpapi.NewRouterWithMetricsAndLogger(
		httpapi.ReadPorts{
			Candles:      a.store,
			BatchCandles: a.store,
			Symbols:      a.store,
			AllSymbols:   a.store,
			LastTrades:   a.store,
			Metrics:      a.metricsHandler,
		},
		a.defaultExchange, a.defaultMarketType, a.metricsCollector,
		derivReader, derivBaselines, derivPrices,
		a.tfReg, a.wsServer, a.marginBrackets,
		a.derivWindowReader(), a.log, a.durableTimeframes()...,
	)
}

// newHTTPServer assembles the production server.
//
// The deadline matters: the durable read has its OWN budget, smaller than this one, so it
// degrades instead of blowing up in the middle of writing the body.
func (a *App) newHTTPServer(router http.Handler) *http.Server {
	return &http.Server{
		Addr:              ":" + a.cfg.Port,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      httpapi.ServerWriteTimeout,
		IdleTimeout:       60 * time.Second,
	}
}
