package recovery

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/market"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// PeriodicBackstopInterval is how often the always-on, staleness-gated backstop
// evaluates active symbols and enqueues gap recovery for ONLY those whose most
// recent persisted candle on the fastest timeframe is stale.
//
// It is only a backstop: the per-stream watchdog, reconnect-triggered recovery, and
// seed-on-subscribe cover the common cases. Because the staleness gate filters the
// per-tick fan-out down to just the lagging symbols, a healthy ~500-symbol scope
// collapses from ~4000 REST calls/cycle to ~0 in steady state.
const PeriodicBackstopInterval = 30 * time.Minute

// StalenessIntervals is the number of fastest-timeframe intervals a symbol's most
// recent PERSISTED candle may lag before the periodic backstop treats it as stale.
// It must exceed one interval: a healthy symbol naturally lags ~1–2 intervals (the
// current bucket has not closed yet, plus persistence lag). Three intervals is a
// conservative default.
const StalenessIntervals = 3

// Scope is one enabled exchange scope the backstop evaluates.
type Scope struct {
	ExchangeID string
	MarketType market.MarketType
}

// DisconnectStat is the minimal connection observation the proof metric needs.
// It is declared here so recovery does not depend on the ingestion DTO.
type DisconnectStat struct {
	ExchangeID     string
	MarketType     string
	LastDisconnect time.Time
}

// Backstop reconciles staleness against the persisted store.
type Backstop struct {
	Orchestrator Orchestrator
	Provider     LatestTimestampProvider
	Scopes       []Scope
	Timeframes   []string
	Metrics      Metrics
	log          *slog.Logger
}

// NewBackstop builds the periodic reconciliation policy.
func NewBackstop(orch Orchestrator, provider LatestTimestampProvider, scopes []Scope, timeframes []string, metrics Metrics, logs ...*slog.Logger) *Backstop {
	return &Backstop{
		Orchestrator: orch,
		Provider:     provider,
		Scopes:       scopes,
		Timeframes:   timeframes,
		Metrics:      metrics,
		log:          logger.OrDiscard(logs...),
	}
}

// ReconcileOnce performs a single staleness-gated reconciliation tick. For each
// scope it filters the currently-active symbols through the persisted-state gate
// and enqueues a [now-window, now] gap-recovery batch for ONLY the stale subset; a
// scope with no stale symbols is skipped entirely. It returns the number of scopes
// for which symbols were enqueued.
//
// scopeHadDisconnect reports whether a disconnect preceded this tick for the given
// (exchangeID, internalMarketType) scope; when it returns false for a scope that
// enqueued stale symbols, the proof metric is incremented for that scope.
func (b *Backstop) ReconcileOnce(
	ctx context.Context,
	subs []string,
	now time.Time,
	window time.Duration,
	scopeHadDisconnect func(exchangeID, internalMarketType string) bool,
) int {
	if b.Orchestrator == nil || len(subs) == 0 {
		return 0
	}

	fastestTF, fastestIntervalMs, haveTF := FastestInterval(b.Timeframes)
	staleLagMs := int64(StalenessIntervals) * fastestIntervalMs
	nowMs := now.UnixMilli()

	scopesReconciled := 0
	for _, scope := range b.Scopes {
		internalMarketType := scope.MarketType.String()

		prefix := scope.ExchangeID + ":" + internalMarketType + ":"
		stale := make([]string, 0, len(subs))
		for _, key := range subs {
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			sym := strings.TrimPrefix(key, prefix)
			if IsStale(ctx, b.Provider, scope.ExchangeID, internalMarketType, sym, fastestTF, haveTF, nowMs, staleLagMs) {
				stale = append(stale, sym)
			}
		}
		if len(stale) == 0 {
			continue
		}

		// EnqueueBatch keys on the raw config market type; the worker normalises it
		// via market.FromConfig when re-deriving the gap. Scope.MarketType is already
		// the canonical internal type, which is what the worker keys on.
		b.Orchestrator.EnqueueBatch(scope.ExchangeID, internalMarketType, stale, now.Add(-window), now)
		scopesReconciled++
		logger.OrDiscard(b.log).Info("periodic gap reconciliation: enqueued stale symbols",
			"symbols", len(stale),
			"exchange", scope.ExchangeID,
			"market_type", internalMarketType,
			"window", window)

		if scopeHadDisconnect == nil || !scopeHadDisconnect(scope.ExchangeID, internalMarketType) {
			b.Metrics.IncPeriodicBackstopEnqueue(scope.ExchangeID, internalMarketType)
		}
	}
	return scopesReconciled
}

// IsStale reports whether a symbol's most recent PERSISTED candle on the fastest
// timeframe lags by at least the staleness threshold, fail-toward-recovery:
//   - nil provider OR no usable timeframe -> stale (preserve blind-backstop / safe).
//   - unknown timestamp (ok=false) OR non-positive timestamp -> stale (recover).
//   - otherwise stale iff now - latest >= stalenessIntervals * fastestIntervalMs.
//
// It reads only persisted state via the provider (never the in-memory aggregation
// buffer): the target failure mode is pre-persistence loss, which a buffer read
// would mask.
func IsStale(
	ctx context.Context,
	provider LatestTimestampProvider,
	exchangeID, internalMarketType, symbol, fastestTF string,
	haveTF bool,
	nowMs, staleLagMs int64,
) bool {
	if provider == nil || !haveTF {
		return true
	}
	internalSymbol := market.NormalizeSymbol(symbol)
	latestTs, ok := provider.GetLatestTimestamp(ctx, exchangeID, internalMarketType, internalSymbol, fastestTF)
	return IsStaleWatermark(latestTs, ok, nowMs, staleLagMs)
}

// IsStaleWatermark applies the same staleness rule to a watermark the caller already
// read. Health samples the provider once per symbol and derives both the failing verdict
// and the published freshness detail from that single read, so "fresh" means one thing.
func IsStaleWatermark(latestMs int64, haveData bool, nowMs, staleLagMs int64) bool {
	if !haveData || latestMs <= 0 {
		return true
	}
	return nowMs-latestMs >= staleLagMs
}

// FastestInterval returns the smallest configured timeframe interval in
// milliseconds. The fastest timeframe is the earliest staleness signal: it closes
// first, so a coarser timeframe cannot be stale without it also being stale. It
// returns (0, false) when no timeframe parses to a positive interval.
func FastestInterval(timeframes []string) (string, int64, bool) {
	var fastestTF string
	var fastestMs int64
	for _, tf := range timeframes {
		secs := candle.ParseTimeframeToSeconds(tf)
		if secs <= 0 {
			continue
		}
		ms := secs * 1000
		if fastestMs == 0 || ms < fastestMs {
			fastestMs = ms
			fastestTF = tf
		}
	}
	if fastestMs == 0 {
		return "", 0, false
	}
	return fastestTF, fastestMs, true
}

// ScopeDisconnectPredicate builds the proof-metric disconnect predicate from the
// clients' connection stats. It reports true for an (exchangeID,
// internalMarketType) scope when any client matching that scope recorded a
// disconnect at or after since. Client stats carry the RAW market type, so it is
// canonicalised via market.FromConfig to match the internal market type the caller
// queries with. A nil/empty slice yields a predicate that is always false.
func ScopeDisconnectPredicate(stats []DisconnectStat, since time.Time) func(exchangeID, internalMarketType string) bool {
	return func(exchangeID, internalMarketType string) bool {
		for _, s := range stats {
			if s.ExchangeID != exchangeID {
				continue
			}
			canonicalMarketType, err := market.FromConfig(s.MarketType)
			if err != nil {
				continue
			}
			if canonicalMarketType.String() != internalMarketType {
				continue
			}
			if !s.LastDisconnect.IsZero() && !s.LastDisconnect.Before(since) {
				return true
			}
		}
		return false
	}
}
