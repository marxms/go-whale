package engine

import (
	"context"
	"log/slog"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// Cold-tier DATA-completeness gauges. Low cardinality on purpose: labeled by
// exchange/market_type/timeframe only (NOT per-symbol — that would be unbounded). They
// aggregate across symbols so an alert fires on the scope; the operator drills into the
// offending symbol via the ad-hoc GROUP BY query. Without this signal, a multi-hour hole in the
// cold tier goes unreported.
// coldGapScanner is the cold-store capability the monitor needs (one aggregated query
// per scope). The durable adapter implements it.
type coldGapScanner interface {
	ScanScopeCompleteness(ctx context.Context, exchange, marketType, timeframe string) (candle.ScopeCompleteness, error)
}

// activeSymbolLister lists currently-subscribed symbols. *redisstore.RedisStore implements it.
type activeSymbolLister interface {
	GetAllActiveSymbols(ctx context.Context) ([]redisstore.SymbolInfo, error)
}

// ColdCompletenessMonitor periodically checks the durable tier for missing buckets per
// (exchange, market_type, symbol, timeframe) and exposes the aggregate as Prometheus
// gauges — closing the monitoring blind spot that let a 20h hole go unreported.
type ColdCompletenessMonitor struct {
	cold     coldGapScanner
	lister   activeSymbolLister
	tfReg    *candle.TimeframeRegistry
	interval time.Duration
	metrics  Metrics
	log      *slog.Logger
}

// SetMetrics injects the observability port. The composition root calls it before Start.
func (m *ColdCompletenessMonitor) SetMetrics(metrics Metrics) { m.metrics = metrics }

// NewColdCompletenessMonitor builds the monitor. interval <= 0 defaults to 5m.
func NewColdCompletenessMonitor(cold coldGapScanner, lister activeSymbolLister, tfReg *candle.TimeframeRegistry, interval time.Duration, logs ...*slog.Logger) *ColdCompletenessMonitor {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	return &ColdCompletenessMonitor{cold: cold, lister: lister, tfReg: tfReg, interval: interval, log: logger.OrDiscard(logs...)}
}

// Start runs the periodic completeness scan in a goroutine until ctx is canceled.
func (m *ColdCompletenessMonitor) Start(ctx context.Context) {
	go func() {
		// Immediate first scan so the gauges reflect reality without waiting a full
		// interval — no cold-start blind spot after a restart.
		if err := m.scanOnce(ctx); err != nil {
			logger.OrDiscard(m.log).Warn("cold completeness: initial scan failed", "error", err)
		}
		ticker := time.NewTicker(m.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := m.scanOnce(ctx); err != nil {
					logger.OrDiscard(m.log).Warn("cold completeness: scan failed", "error", err)
				}
			}
		}
	}()
}

// scanOnce runs one full completeness pass (one aggregated query per scope×timeframe)
// and updates the gauges. A per-scope error is logged and skipped so one bad scope never
// aborts the whole scan.
func (m *ColdCompletenessMonitor) scanOnce(ctx context.Context) error {
	symbols, err := m.lister.GetAllActiveSymbols(ctx)
	if err != nil {
		return err
	}

	// Distinct (exchange, market_type) scopes AND their symbol counts. The count is the
	// denominator the absence check needs; it comes from the list already in hand, so
	// detecting "the cold tier answered with nothing" costs no additional query.
	scopeSymbols := make(map[[2]string]int)
	for _, s := range symbols {
		scopeSymbols[[2]string{s.Exchange, s.MarketType}]++
	}

	for scope, expected := range scopeSymbols {
		for _, tf := range m.tfReg.All() {
			res, err := m.cold.ScanScopeCompleteness(ctx, scope[0], scope[1], tf)
			if err != nil {
				logger.OrDiscard(m.log).Warn("cold completeness: scope scan failed",
					"exchange", scope[0], "market_type", scope[1], "timeframe", tf, "error", err)
				continue
			}
			m.metrics.setColdMissingBuckets(scope[0], scope[1], tf, float64(res.MissingBuckets))
			m.metrics.setColdSymbolsWithGaps(scope[0], scope[1], tf, float64(res.SymbolsWithGaps))

			// Clamped at zero rather than allowed to go negative. The scan can legitimately
			// return MORE symbols than are currently subscribed — a symbol deranked minutes
			// ago still has rows inside the 48h window — and a negative "absent" would be
			// nonsense on a dashboard. Over-reporting present is not the failure being
			// detected here; reporting absent as present is.
			absent := expected - res.SymbolsPresent
			if absent < 0 {
				absent = 0
			}
			m.metrics.setColdSymbolsExpected(scope[0], scope[1], tf, float64(expected))
			m.metrics.setColdSymbolsAbsent(scope[0], scope[1], tf, float64(absent))

			// The catastrophic case gets a log line, not just a series. A scope where the cold
			// tier answered for NOT ONE subscribed symbol must be legible to someone reading logs
			// before they ever open a dashboard.
			if expected > 0 && res.SymbolsPresent == 0 {
				logger.OrDiscard(m.log).Error("cold completeness: cold tier holds no rows for any subscribed symbol; the write path is down",
					"exchange", scope[0],
					"market_type", scope[1],
					"timeframe", tf,
					"expected_symbols", expected,
					"lookback", candle.CompletenessLookback)
			}
		}
	}
	return nil
}
