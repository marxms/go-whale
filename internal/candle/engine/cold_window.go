package engine

import (
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

const (
	// coldReadMaxLookback caps how far back any cold read may reach. It matches the
	// retention horizon: data older than this does not exist, so asking for it only costs
	// partitions to open.
	coldReadMaxLookback = 400 * 24 * time.Hour

	// coldReadSlack multiplies the theoretical window so gaps, late buckets and
	// retention-boundary effects do not make a read return short. A read that comes back
	// with fewer candles than asked is a correctness problem; a slightly wider window is
	// only a performance one.
	coldReadSlack = 2
)

// coldReadFrom computes the lower bound for a cold read that wants `limit` buckets of
// `timeframe` ending at `to`.
//
// Passing time.Time{} — the zero time — gives the engine no interval to prune on: it scans every
// partition while filtering unindexed SYMBOL columns, and the work is discarded when the client's
// deadline expires.
//
// The bound is deliberately generous (slack, then capped at retention) because returning
// short is a correctness bug while scanning a little extra is not. What matters is that it is
// never UNBOUNDED: an unparseable timeframe falls back to the retention cap rather than to a
// full-table scan.
func coldReadFrom(to time.Time, timeframe string, limit int) time.Time {
	if to.IsZero() {
		to = time.Now()
	}
	if limit <= 0 {
		limit = 1
	}

	tf, ok := parseTimeframeDuration(timeframe)
	if !ok {
		return to.Add(-coldReadMaxLookback)
	}

	span := time.Duration(limit) * tf * coldReadSlack
	if span > coldReadMaxLookback || span <= 0 {
		span = coldReadMaxLookback
	}
	return to.Add(-span)
}

// parseTimeframeDuration converts a timeframe label into a duration, reusing the canonical
// parser so this never drifts from bucket alignment.
func parseTimeframeDuration(timeframe string) (time.Duration, bool) {
	ms, ok := candle.ParseTimeframeMs(timeframe)
	if !ok || ms <= 0 {
		return 0, false
	}
	return time.Duration(ms) * time.Millisecond, true
}
