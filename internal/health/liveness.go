package health

import (
	"context"
	"strings"

	"github.com/marxms/go-whale/internal/market"
)

// LivenessStaleFraction is the per-scope fraction of MONITORED symbols allowed to be
// stale before the scope is reported as failing per-symbol liveness. A mass kill once
// froze ~97% of the fleet while the aggregate scope still read healthy (last_message_age
// ~0, held up by ~17 survivors); a 20% ceiling turns a material partial outage into a
// loud, actionable /health 503 instead of a silent gap. The caller excludes venue TradFi
// markets from the denominator: they follow their own trading hours and a closed equity
// market is not a broken crypto feed.
const LivenessStaleFraction = 0.2

// LivenessScope is one canonical exchange scope.
type LivenessScope struct {
	ExchangeID string
	MarketType market.MarketType
}

// LivenessProvider is the persisted-state freshness source. It is deliberately the
// same shape as the gap-recovery worker's provider (never the in-memory buffer).
type LivenessProvider interface {
	GetLatestTimestamp(ctx context.Context, exchangeID, marketType, symbol, tf string) (int64, bool)
}

// LivenessMetrics is the two per-symbol fleet freshness gauges.
type LivenessMetrics interface {
	SetSymbolsTotal(exchange, marketType string, n float64)
	SetSymbolsStale(exchange, marketType string, n float64)
}

// CandleFreshness is one scope's per-symbol freshness detail, derived from the same
// persisted watermark as the failing verdict. Stale counts SYMBOLS, never the age of the
// newest one: a single leading symbol must not mask the rest of a frozen fleet. HaveData
// is false when no symbol in the scope has a readable candle at all.
type CandleFreshness struct {
	Scope        string
	Symbols      int
	Stale        int
	FreshestAtMs int64
	HaveData     bool
}

// SampleFleetLiveness computes PER-SYMBOL fleet freshness for every scope from the
// persisted provider, sets the StreamSymbolsTotal/Stale gauges, and returns the
// canonical "exchange:internalMarketType" scopes whose stale fraction exceeds
// LivenessStaleFraction, plus the per-scope detail that verdict is made of. It COUNTS
// instead of enqueuing — the same source of truth the gap backstop uses — so the
// aggregate-green-but-fleet-frozen failure mode is observed.
//
// The provider is read ONCE per symbol: the same watermark feeds both isStale and the
// published detail, so /health and the backstop cannot disagree about "fresh". isStale
// receives that watermark so the staleness predicate lives in its owning package
// (recovery) while the fleet-fraction rule lives here.
func SampleFleetLiveness(
	ctx context.Context,
	scopes []LivenessScope,
	provider LivenessProvider,
	subs []string,
	fastestTF string,
	isStale func(latestMs int64, haveData bool) bool,
	metrics LivenessMetrics,
) (failing []string, freshness []CandleFreshness) {
	freshness = make([]CandleFreshness, 0, len(scopes))
	for _, scope := range scopes {
		internalMarketType := scope.MarketType.String()
		prefix := scope.ExchangeID + ":" + internalMarketType + ":"

		total, stale := 0, 0
		var freshest int64
		haveData := false
		for _, key := range subs {
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			total++
			sym := strings.TrimPrefix(key, prefix)

			var latestMs int64
			var have bool
			if provider != nil && fastestTF != "" {
				latestMs, have = provider.GetLatestTimestamp(
					ctx, scope.ExchangeID, internalMarketType, market.NormalizeSymbol(sym), fastestTF,
				)
			}
			if have && (!haveData || latestMs > freshest) {
				freshest, haveData = latestMs, true
			}
			if isStale(latestMs, have) {
				stale++
			}
		}

		if metrics != nil {
			metrics.SetSymbolsTotal(scope.ExchangeID, internalMarketType, float64(total))
			metrics.SetSymbolsStale(scope.ExchangeID, internalMarketType, float64(stale))
		}

		canonical := scope.ExchangeID + ":" + internalMarketType
		if total > 0 && float64(stale)/float64(total) > LivenessStaleFraction {
			failing = append(failing, canonical)
		}
		freshness = append(freshness, CandleFreshness{
			Scope:        canonical,
			Symbols:      total,
			Stale:        stale,
			FreshestAtMs: freshest,
			HaveData:     haveData,
		})
	}
	return failing, freshness
}
