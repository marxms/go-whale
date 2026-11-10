package engine

import (
	"context"
	"fmt"
	"sort"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
)

// ============================================================================
// SOLE-COPY GUARD — prevents the `allkeys-lru` flip before the migration
// ============================================================================
//
// Under `allkeys-lru`, the LEAST TOUCHED keys leave first. A hot candle cell with no durable
// rows is exactly the least touched in the keyspace — a weekly chart is opened rarely — so the
// policy destroys its only copy. This guard measures at startup whether a sole-copy cell still
// exists and cross-references that with the CURRENT policy read from the server. A sole copy under
// an evicting policy is FATAL. See docs/decisions/0012-sole-copy-guard-before-lru.md.
//
// The question is answered with a boundary comparison, not a candle count: the ZSET's lowest
// score against the table's `MIN(timestamp)`. Counting candles would turn a startup guard into a
// scan.
//
// A symbol with no durable row does not appear in the `GROUP BY`; a map returning zero for it
// would make it look covered since 1970. It is counted separately and named, because its entire
// series is a sole copy.
//
// "Could not measure" alarms and does not fail: a fatal gate on the ABSENCE of the measurement
// would turn momentary cold-tier unavailability into a service that cannot start.

// soleCopyGuardHot is the hot half: which keys exist and which is the oldest candle of each.
type soleCopyGuardHot interface {
	OldestCandleTimestamps(ctx context.Context, timeframe string) (map[redisstore.CandleKeyRef]int64, error)
}

// soleCopyGuardCold is the durable half: the per-symbol boundary, in a scope, in a timeframe.
type soleCopyGuardCold interface {
	OldestTimestampsByScope(ctx context.Context, exchange, marketType, timeframe string) (map[string]time.Time, error)
}

// SoleCopyCellRisk is the verdict for ONE cell (timeframe).
type SoleCopyCellRisk struct {
	Timeframe string
	// Keys is how many keys the hot tier has for this cell.
	Keys int
	// KeysWithSoleCopy is in how many of them the oldest Redis candle predates the durable
	// coverage — or there is no coverage at all.
	KeysWithSoleCopy int
	// KeysWithoutColdRows is the subset with NO durable row: the entire series is a sole
	// copy, and it is the case an optimistic map would make look covered.
	KeysWithoutColdRows int
	// Sample names up to five symbols, so the alarm points somewhere.
	Sample []string
}

// SoleCopyRisk is the verdict of the whole guard.
type SoleCopyRisk struct {
	// Evaluated is false when either of the two boundaries could not be read. In that case the
	// verdict is NOT "it is clean".
	Evaluated bool
	// Reason says why it was not evaluated, when it was not.
	Reason string
	Cells  []SoleCopyCellRisk
}

// KeysWithSoleCopy sums the keys at risk across all cells.
func (r SoleCopyRisk) KeysWithSoleCopy() int {
	var total int
	for _, c := range r.Cells {
		total += c.KeysWithSoleCopy
	}
	return total
}

// Detail is the line the gate prints.
func (r SoleCopyRisk) Detail() string {
	if !r.Evaluated {
		return r.Reason
	}
	partes := make([]string, 0, len(r.Cells))
	for _, c := range r.Cells {
		partes = append(partes, fmt.Sprintf("%s: %d of %d key(s) with a sole copy (%d with no durable row at all) %v",
			c.Timeframe, c.KeysWithSoleCopy, c.Keys, c.KeysWithoutColdRows, c.Sample))
	}
	return joinCommaSpace(partes)
}

func joinCommaSpace(parts []string) string {
	var out string
	for i, p := range parts {
		if i > 0 {
			out += "; "
		}
		out += p
	}
	return out
}

// SoleCopyGuard measures whether a sole-copy cell still exists.
type SoleCopyGuard struct {
	hot  soleCopyGuardHot
	cold soleCopyGuardCold
}

// NewSoleCopyGuard assembles the guard. A nil `cold` is a legitimate state — a deployment without a durable tier —
// and the guard reports it as NOT EVALUATED, never as clean: without a durable tier EVERY hot candle is
// a sole copy, which is the worst possible outcome under `allkeys-lru`.
func NewSoleCopyGuard(hot soleCopyGuardHot, cold soleCopyGuardCold) *SoleCopyGuard {
	return &SoleCopyGuard{hot: hot, cold: cold}
}

// Probe measures the declared cells.
func (g *SoleCopyGuard) Probe(ctx context.Context, timeframes []string) SoleCopyRisk {
	if g == nil || g.hot == nil {
		return SoleCopyRisk{Reason: "hot tier unavailable: the hot boundary could not be read"}
	}
	if g.cold == nil {
		return SoleCopyRisk{Reason: "NO durable tier configured: every hot candle is a sole copy"}
	}
	if len(timeframes) == 0 {
		return SoleCopyRisk{Reason: "no cell declared for the guard"}
	}

	risk := SoleCopyRisk{Evaluated: true}

	for _, tf := range timeframes {
		quente, err := g.hot.OldestCandleTimestamps(ctx, tf)
		if err != nil {
			return SoleCopyRisk{Reason: fmt.Sprintf("hot boundary of %s unreadable: %v", tf, err)}
		}

		cell := SoleCopyCellRisk{Timeframe: tf, Keys: len(quente)}

		// The durable boundary is read ONCE PER SCOPE, and the scopes come from the hot keys
		// themselves: asking about a scope that does not exist in Redis answers nothing useful.
		duravel := make(map[string]map[string]time.Time)
		for ref := range quente {
			escopo := ref.Exchange + ":" + ref.MarketType
			if _, lido := duravel[escopo]; lido {
				continue
			}
			m, err := g.cold.OldestTimestampsByScope(ctx, ref.Exchange, ref.MarketType, tf)
			if err != nil {
				return SoleCopyRisk{Reason: fmt.Sprintf("durable boundary of %s %s unreadable: %v", escopo, tf, err)}
			}
			duravel[escopo] = m
		}

		var emRisco []string
		for ref, hotOldest := range quente {
			coldOldest, temLinha := duravel[ref.Exchange+":"+ref.MarketType][ref.Symbol]
			switch {
			case !temLinha:
				// With NO durable row the entire series is a sole copy.
				cell.KeysWithoutColdRows++
				cell.KeysWithSoleCopy++
				emRisco = append(emRisco, ref.Symbol)
			case hotOldest < coldOldest.UnixMilli():
				cell.KeysWithSoleCopy++
				emRisco = append(emRisco, ref.Symbol)
			}
		}

		// Deterministic order: an alarm that changes shape between identical startups is not
		// usable as evidence.
		sort.Strings(emRisco)
		if len(emRisco) > 5 {
			emRisco = append(emRisco[:5:5], fmt.Sprintf("…+%d", len(emRisco)-5))
		}
		cell.Sample = emRisco

		risk.Cells = append(risk.Cells, cell)
	}

	return risk
}
