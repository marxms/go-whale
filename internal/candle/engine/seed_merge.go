package engine

import (
	"github.com/marxms/go-whale/internal/candle"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// candleSeedMergeTotal counts buckets folded into a live buffer by a seed or repair batch.
//
// It is SEPARATE from candle_late_update_total on purpose. That counter exists to expose
// late TRADES being silently discarded; a boot folds thousands of buckets through the same
// value rule without a single late trade among them. Sharing one counter would make the
// late-update signal fire on every boot, and a signal that always fires is a signal nobody
// reads — which is the exact blindness the late-update counter was added to remove.
//
// The outcomes, and what each one means when it moves:
//
//	inserted        the live stream never delivered that bucket and the batch supplied it. A
//	                burst at boot is a restart healing itself; a steady rate is a stream that
//	                keeps holing, which is a different problem.
//	merged          the bucket was present on both sides and unioned. Routine.
//	stored_exceeded the union kept a STORED total that was strictly greater than the
//	                exchange's. This is the ratchet made visible: the union takes the MAX, so
//	                a hot value that is too high can never be corrected downward by a repair,
//	                and the hot tier wins over cold on read. A non-zero rate here means the
//	                hot tier is diverging UPWARD from the exchange and the divergence is
//	                self-sealing — it will not heal on its own.
const (
	seedOutcomeMerged   = "merged"
	seedOutcomeInserted = "inserted"
	seedOutcomeExceeded = "stored_exceeded"
	seedOutcomeHealed   = "healed"
	seedOutcomeLowered  = "lowered"
)

// reportSeedFold publishes what a fold observed. It MUST be called after the caller has released
// ce.mu: it writes Prometheus counters and can emit a log line, and a stderr write under the
// global lock blocks every reader of every buffer behind it.
//
// Zero counts are not recorded so the series stays readable: an idle merge is not an event.
func (ce *CandleEngine) reportSeedFold(key CompositeKey, stats candle.SeedFoldStats) {
	record := func(outcome string, n int) {
		if n > 0 {
			ce.metrics.addSeedMerge(key.Exchange, key.MarketType, key.Timeframe, outcome, float64(n))
		}
	}
	record(seedOutcomeMerged, stats.Merged)
	record(seedOutcomeInserted, stats.Inserted)
	record(seedOutcomeExceeded, stats.Exceeded)
	record(seedOutcomeHealed, stats.Healed)
	record(seedOutcomeLowered, stats.Lowered)

	// An insert means the buffer did not have that bucket. The defect this replaces was
	// invisible precisely because it said nothing, so the fold is not allowed to be silent
	// either — but it must also not cry wolf, because two very different events land here:
	//
	//	inserted >> merged   a buffer that was nearly EMPTY got filled. Routine at boot,
	//	                     when the warm tier had little or nothing to offer.
	//	inserted alongside a  the buffer that was populated was HOLED, and the live stream is
	//	substantial merged    dropping buckets it should have delivered. That is a defect.
	//
	// Reporting both counts is what lets an operator tell them apart at a glance; reporting
	// only the inserts made a routine boot look like 4,000 simultaneous failures.
	if stats.Inserted > 0 {
		logger.OrDiscard(ce.log).Warn("seed batch folded",
			"exchange", key.Exchange,
			"market_type", key.MarketType,
			"symbol", key.Symbol,
			"timeframe", key.Timeframe,
			"inserted", stats.Inserted,
			"merged", stats.Merged,
			"buffer_len", stats.BufferLen)
	}

	// A batch that arrives out of order is a PRODUCER defect. The fold repairs it so the
	// buffer stays sorted, but repairing it quietly would hide the bug in whoever built the
	// batch — and the same slice is written to cold and Redis, where nothing re-sorts it.
	if stats.Unsorted {
		logger.OrDiscard(ce.log).Warn("seed batch was not oldest-first; sorted a copy to keep the buffer ordered",
			"exchange", key.Exchange,
			"market_type", key.MarketType,
			"symbol", key.Symbol,
			"timeframe", key.Timeframe)
	}
}
