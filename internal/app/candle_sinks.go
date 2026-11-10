package app

import (
	"github.com/marxms/go-whale/internal/adapters/inbound/realtime"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/candle/engine"
)

// continuityTracker returns the process-lifetime continuity tracker, creating it
// lazily when a test builds an App literal. Production initializes it in NewApp.
func (a *App) continuityTracker() *engine.ContinuityTracker {
	a.continuityOnce.Do(func() {
		if a.continuity == nil {
			a.continuity = engine.NewContinuityTracker(a.bootAt, a.metrics)
		}
	})
	return a.continuity
}

// onCandleClose fans a freshly closed bucket out to every sink.
//
// # WHY THIS IS A METHOD AND NOT A CLOSURE INSIDE THE QUESTDB BRANCH
//
// Registering it inside `if qdbErr == nil` lets a QuestDB that failed to start take FIVE
// unrelated mechanisms down with it, and the log says only that cold storage was disabled:
//
//	qdbPipeline.Enqueue          cold durability          — genuinely gone, correctly reported
//	tieredStore.WriteClosedToHot the hot-tier close write — the -8,85% deficit returns
//	checkOpenTimeContinuity      missing-bucket detection — gaps stop being detected at all
//	realtimeProducer.PublishClose the outbound event bus  — consumers stop receiving closes
//	hub.IngestClose              the chart WS edge        — every connected chart goes silent
//
// Only the first has anything to do with QuestDB. The other four depend on Redis and on
// in-process state, and there is no reason for them to share a fate with the cold tier. That
// coupling is not hypothetical: three separate production incidents on this branch had
// QuestDB failing or unreachable at boot.
//
// Every field is read at CALL time rather than captured, which is what lets the registration
// happen before the composition root has finished assembling — the same reason
// checkOpenTimeContinuity already reached for a.gapRecovery this way.
func (a *App) onCandleClose(key engine.CompositeKey, closed candle.Candle) {
	// Cold durability. Nil exactly when QuestDB is disabled or failed to start, which is the
	// one degradation this function is allowed to have.
	if a.qdbPipeline != nil {
		a.qdbPipeline.Enqueue(key.Exchange, key.MarketType, key.Symbol, key.Timeframe, closed)
	}

	// Write the CLOSED bucket to the hot tier as well. The periodic snapshot only ever
	// writes the NEWEST candle, so when a bucket rolled between two sweeps its final trades
	// never reached Redis and the hot copy froze at the last snapshot. Measured across 15
	// buckets on 5 symbols: 9 under-reported, 6 exact, ZERO over-reported, with the deficit
	// scaling with trade rate (BTC/USDT reached -8,85%).
	if a.tieredStore != nil {
		a.tieredStore.WriteClosedToHot(key, closed)
	}

	// Ingest-time open-time continuity → targeted backfill. Detects a missing bucket in the
	// close stream and enqueues only the missing range for this single symbol/TF.
	// A TYPED NIL IS NOT A NIL INTERFACE. a.gapRecovery is a *recovery.Manager, and
	// assigning a nil one straight into the repair parameter produces an interface that
	// is NON-nil (it carries the type) wrapping a nil pointer — so the callee's `orch == nil`
	// guard passes and the first method call dereferences nil.
	var repair engine.BucketRepairer
	if a.gapRecovery != nil {
		repair = a.gapRecovery
	}
	a.continuityTracker().Observe(repair, key, closed)

	scope := realtime.Scope{
		Exchange:   key.Exchange,
		MarketType: key.MarketType,
		Symbol:     key.Symbol,
		Timeframe:  key.Timeframe,
	}
	// Async: returns immediately, no network and no lock, so a slow or stalled Redis cannot
	// stall the goroutine that processes trades. A full queue drops the newest event and
	// counts it; the subscriber detects the sequence gap and reconciles.
	if a.realtimeProducer != nil {
		a.realtimeProducer.PublishClose(scope, closed)
	}
	if a.hub != nil {
		a.hub.IngestClose(scope, closed)
	}
}

// onCandleRevised routes a correction to an already-closed bucket down the same path as its
// close.
//
// Without this the engine repaired itself in RAM and told nobody. A trade arriving after its
// bucket rolled is merged by mergeHistorical — 98 occurrences in one production window — but
// the periodic snapshot only writes the NEWEST bucket per key, so every durable copy stayed
// at the pre-correction value. Measured on BTC/USDT 5m bucket 01:05: engine 279.98300,
// QuestDB 279.93100.
//
// This is not a reconciliation job, a sweep or a new goroutine: the trigger is the merge that
// already happens, the corrected value is already in memory, and no request is made to the
// exchange. The sinks are idempotent by construction — the cold table carries DEDUP UPSERT
// KEYS and the hot write is a snapshot-total, which replaces rather than accumulates.
//
// checkOpenTimeContinuity is DELIBERATELY not called here: it treats each close as the newest
// baseline, and feeding it an older bucket would drag that baseline backwards and make the
// next genuine close look like a gap.
func (a *App) onCandleRevised(key engine.CompositeKey, revised candle.Candle) {
	if a.qdbPipeline != nil {
		a.qdbPipeline.Enqueue(key.Exchange, key.MarketType, key.Symbol, key.Timeframe, revised)
	}
	a.onSeedCandleRevised(key, revised)
}

// Seed admission uses its own cancellable cold path; this fanout must not
// enqueue a second copy that could overtake the retained repair.
func (a *App) onSeedCandleRevised(key engine.CompositeKey, revised candle.Candle) {
	if a.tieredStore != nil {
		a.tieredStore.WriteClosedToHot(key, revised)
	}

	scope := realtime.Scope{
		Exchange:   key.Exchange,
		MarketType: key.MarketType,
		Symbol:     key.Symbol,
		Timeframe:  key.Timeframe,
	}
	// candle.revised, not candle.close: the wire already distinguishes them. A close carries
	// a sequence the consumer uses for gap detection, so re-emitting one for an old bucket
	// would read as an out-of-order frame and could trigger a false resync.
	if a.realtimeProducer != nil {
		a.realtimeProducer.PublishRevised(scope, revised)
	}
	if a.hub != nil {
		a.hub.IngestRevised(scope, revised)
	}
}
