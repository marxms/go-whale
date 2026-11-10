package engine

import (
	"sync"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// ContinuityTracker detects a missing open-time bucket in the closed-candle stream
// and asks for a targeted backfill of only the missing range for that single
// symbol/timeframe.
//
// Landmine #1 (buffer ordering): at OnCandleClose time the just-closed candle is
// already the latest entry in the engine's in-memory buffer, so
// CandleEngine.GetLatestTimestamp would return the closed candle's OWN open-time
// (buffer hit) and never see a gap. We therefore compare against the last open-time
// the ingest layer actually OBSERVED close for this key — maintained here. A missing
// bucket in the close stream is exactly a candle that was finalized-but-skipped
// (pre-persistence loss), independent of buffer/persist ordering.
//
// The timeframe interval is derived from the candle's timeframe via
// candle.ParseTimeframeToSeconds, never hardcoded.
//
// Idempotency / no double-fire: the first close for a key (lastClosed == 0) and any
// contiguous close are no-ops. On a real gap the missing range is handed to the
// repair sink — no new queue, job type, or detector.

// BucketRepairer is the repair capability the tracker needs. *recovery.Manager
// satisfies it; the interface is declared here so this package does not import the
// recovery orchestration.
type BucketRepairer interface {
	EnqueueTargetedBucket(exchangeID, marketType, symbol, timeframe string, timestamp int64)
	EnqueueBatch(exchangeID, marketType string, symbols []string, since, until time.Time)
}

// RepairMetrics is the observability port for the two repair triggers.
type RepairMetrics interface {
	IncBootBucketSettle(exchange, marketType, timeframe string)
	IncGappedBucketRepair(exchange, marketType, timeframe string)
}

// ContinuityTracker holds the per-key last-closed open-time baseline.
type ContinuityTracker struct {
	mu         sync.Mutex
	lastClosed map[CompositeKey]int64
	bootAt     time.Time
	metrics    RepairMetrics
}

// NewContinuityTracker builds a tracker. bootAt is when this process started; it
// identifies the ONE bucket per stream key that cannot be assembled correctly —
// the bucket that was already open when we began watching.
func NewContinuityTracker(bootAt time.Time, metrics RepairMetrics) *ContinuityTracker {
	return &ContinuityTracker{
		lastClosed: make(map[CompositeKey]int64),
		bootAt:     bootAt,
		metrics:    metrics,
	}
}

// Observe records one closed candle and emits the repairs its state implies.
func (t *ContinuityTracker) Observe(orch BucketRepairer, key CompositeKey, closed candle.Candle) {
	tfSeconds := candle.ParseTimeframeToSeconds(key.Timeframe)
	if tfSeconds <= 0 {
		return
	}
	intervalMs := tfSeconds * 1000

	t.mu.Lock()
	lastClosed := t.lastClosed[key]
	// Record the current close as the new baseline before releasing the lock.
	t.lastClosed[key] = closed.Timestamp
	t.mu.Unlock()

	// THE BUCKET THAT WAS OPEN WHEN THIS PROCESS STARTED — named directly, not inferred.
	//
	// It is the one bucket the system cannot assemble correctly: built from a REST
	// snapshot covering [bucketStart, T_fetch] combined with a live stream that began
	// delivering at some other instant, reconciled by MAX. A proxy that can drift onto
	// a neighboring bucket is not a trigger, it is a coincidence.
	if !t.bootAt.IsZero() && closed.Timestamp == candle.BucketTimestamp(t.bootAt.UnixMilli(), key.Timeframe) {
		if orch != nil {
			t.incBootBucketSettle(key)
			// EnqueueTargetedBucket, not EnqueueBatch: a batch re-derives its window from
			// the ring head, which is at or past the present and can never reach a bucket
			// that has already closed.
			orch.EnqueueTargetedBucket(key.Exchange, key.MarketType, key.Symbol, key.Timeframe, closed.Timestamp)
		}
		// Deliberately NOT returning: a boot bucket can also be the far side of a gap.
	}

	// A BUCKET WHOSE LIVE TAPE ARRIVED INCOMPLETE — the same remedy, for a hole INSIDE
	// a bucket rather than a whole bucket missing. HasGap is raised by the ingest path
	// when the exchange's consecutive trade ids come up short.
	if closed.HasGap && orch != nil {
		t.incGappedBucketRepair(key)
		orch.EnqueueTargetedBucket(key.Exchange, key.MarketType, key.Symbol, key.Timeframe, closed.Timestamp)
	}

	// First close for the key — no baseline, so no gap can be inferred yet.
	if lastClosed <= 0 {
		return
	}
	// Contiguous (or out-of-order/duplicate) → nothing to recover.
	if closed.Timestamp <= lastClosed+intervalMs {
		return
	}
	if orch == nil {
		return
	}

	since := time.UnixMilli(lastClosed + intervalMs)
	until := time.UnixMilli(closed.Timestamp)
	orch.EnqueueBatch(key.Exchange, key.MarketType, []string{key.Symbol}, since, until)
}

// Prune drops open-time continuity baselines whose symbol is no longer live,
// bounding the otherwise-unbounded map as the top-N reranks. keep holds
// "exchange:marketType:symbol" composites. Deleting a baseline is safe: the next
// close for that key is treated as a first-close no-op and repopulates.
func (t *ContinuityTracker) Prune(keep map[string]struct{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for k := range t.lastClosed {
		if _, live := keep[k.Exchange+":"+k.MarketType+":"+k.Symbol]; !live {
			delete(t.lastClosed, k)
		}
	}
}

// LatestClosed returns the recorded baseline for a key. It exists for tests and
// for diagnostics that must not reach into the map.
func (t *ContinuityTracker) LatestClosed(key CompositeKey) (int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ts, ok := t.lastClosed[key]
	return ts, ok
}

// Len reports how many baselines the tracker holds.
func (t *ContinuityTracker) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.lastClosed)
}

func (t *ContinuityTracker) incBootBucketSettle(key CompositeKey) {
	if t.metrics != nil {
		t.metrics.IncBootBucketSettle(key.Exchange, key.MarketType, key.Timeframe)
	}
}

func (t *ContinuityTracker) incGappedBucketRepair(key CompositeKey) {
	if t.metrics != nil {
		t.metrics.IncGappedBucketRepair(key.Exchange, key.MarketType, key.Timeframe)
	}
}
