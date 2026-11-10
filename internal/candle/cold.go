package candle

import (
	"context"
	"time"
)

// ColdStore is the durable candle tier as the core consumes it.
//
// The port is owned here, not by the adapter: the tier's consumers are the
// tiered reader, the completeness monitor, the sole-copy guard and the
// migration. The concrete implementation lives in adapters/outbound/questdb.
type ColdStore interface {
	// WriteBatches submits one DAY of closed candles across all series together.
	// Success acknowledges ingestion. Implementations with an asynchronous WAL
	// additionally require their apply barrier before another DAY. A failure
	// before transmission may be retried; ColdWriteUncertainError fences the
	// stream instead of replaying an old snapshot.
	WriteBatches(ctx context.Context, groups map[SeriesKey][]Candle) error

	// QueryRange reads candles within a time range, ordered by timestamp
	// descending. Returns at most `limit` candles.
	QueryRange(ctx context.Context, exchange, marketType, symbol, timeframe string, from, to time.Time, limit int) ([]Candle, error)

	// GetOldestTimestamp returns the earliest candle timestamp for a specific
	// series. Returns zero time if no data exists.
	GetOldestTimestamp(ctx context.Context, exchange, marketType, symbol, timeframe string) (time.Time, error)

	// Close flushes pending writes and releases connections.
	Close() error

	// Stats returns operational metrics for health monitoring.
	Stats() ColdStoreStats
}

// ColdStoreStats holds operational metrics for a ColdStore implementation.
type ColdStoreStats struct {
	Connected     bool
	RowsWritten   int64
	LastFlushTime time.Time
	PendingWrites int
	Errors        int64
	Reconnects    int64
}

// ColdStoreApplyBarrier separates ingestion ACK from persisted WAL application.
// Adapters without asynchronous WAL need only WriteBatches.
type ColdStoreApplyBarrier interface {
	CaptureApplyTarget(context.Context) (int64, error)
	WaitApplied(context.Context, int64) error
}

// ColdWriteUncertainError means the request was sent but its outcome is unknown.
// It must not enter the ordinary retry queue: a later revision may already exist.
type ColdWriteUncertainError struct{ Err error }

func (e *ColdWriteUncertainError) Error() string {
	return "cold write outcome uncertain: " + e.Err.Error()
}
func (e *ColdWriteUncertainError) Unwrap() error { return e.Err }

// ColdAppender routes CLOSED candles to the durable cold tier through the
// writer's single serialized path. The durable sender is not goroutine-safe,
// so seed/backfill MUST NOT write the store directly.
type ColdAppender interface {
	EnqueueClosedBlocking(ctx context.Context, exchange, marketType, symbol, timeframe string, c Candle) error
}

// CompletenessLookback is the FLOOR of the cold-completeness scan window. It keeps the
// scan on a handful of partitions for the fine timeframes and makes the metric converge:
// a symbol that stops streaming leaves the window instead of reporting a hole forever.
const CompletenessLookback = 48 * time.Hour

// CompletenessLookbackFor scales the window to the timeframe being scanned.
//
// # A FIXED WINDOW MAKES THE METRIC LIE ON COARSE TIMEFRAMES
//
// 48h was chosen for "every configured timeframe up to 1d". A 1w bucket is 168h, so NO 1w
// bucket boundary falls inside a 48h window and the current bucket's own timestamp sits
// before it. Every symbol therefore has zero 1w rows in range, and the scan reports them
// all as absent.
//
// Three buckets is the smallest window that can distinguish "no bucket was due" from "the
// tier stopped writing": one to be in progress, one just closed, one to compare against.
func CompletenessLookbackFor(timeframe string) time.Duration {
	tfMs, ok := ParseTimeframeMs(timeframe)
	if !ok || tfMs <= 0 {
		return CompletenessLookback
	}
	if scaled := 3 * time.Duration(tfMs) * time.Millisecond; scaled > CompletenessLookback {
		return scaled
	}
	return CompletenessLookback
}

// ExpectedBuckets returns how many closed buckets SHOULD exist between minMs and maxMs
// (inclusive) for a timeframe of tfMs. Pure, so it is unit-testable without a database.
// Guards invalid input (returns 0) so a degenerate scope never reports phantom gaps.
func ExpectedBuckets(minMs, maxMs, tfMs int64) int64 {
	if tfMs <= 0 || maxMs < minMs {
		return 0
	}
	return (maxMs-minMs)/tfMs + 1
}

// ScopeCompleteness is one (exchange, market_type, timeframe) scope's cold-tier
// completeness measurement.
//
// SymbolsPresent is the field that makes the other two readable. Missing
// buckets are counted INSIDE each symbol's own [MIN, MAX] window — a
// measurement a symbol with no rows at all cannot participate in: it never
// appears in the GROUP BY, contributes nothing, and leaves MissingBuckets at
// 0. Reported alone, that is indistinguishable from perfect coverage.
type ScopeCompleteness struct {
	// MissingBuckets totals expected-minus-present buckets over the symbols that
	// HAVE rows.
	MissingBuckets int64
	// SymbolsWithGaps is how many of those symbols are missing at least one bucket.
	SymbolsWithGaps int
	// SymbolsPresent is how many DISTINCT symbols returned any row for this scope
	// inside the lookback window. Zero means the scope wrote nothing at all.
	SymbolsPresent int
}
