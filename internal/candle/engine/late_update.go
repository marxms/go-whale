package engine

// candleLateUpdateTotal counts updates that arrive for a bucket which has already rolled
// over, labeled by the merge branch that handled them.
//
// A `lateTradeCount` field carried end to end — declared, validated, merged and served over
// HTTP — with ZERO producers is always zero, and the `omitempty` tag hides it. The defect it was
// meant to expose (silent discard of late updates) would be not merely unmeasured but
// UNMEASURABLE.
//
// It is a metric rather than a candle field on purpose: the value is operational, not part
// of the candle contract. Carrying it per candle bloated the payload and the cold table for
// a number nothing consumed.
// Late-update reasons. Every path through mergeHistorical MUST record one — a late update
// that is neither merged nor counted is indistinguishable from one that never arrived,
// which is exactly the failure this metric exists to make impossible.
//
// inserted and evicted are DISJOINT and must stay that way: inserted asserts the bucket is
// in the ring (so the close-stream continuity check can carry it to durability), evicted
// asserts it is not (so nothing downstream will). Counting the second as the first is the
// original defect — see the trim in mergeHistorical.
const (
	lateReasonAdditive = "additive"
	lateReasonMaxTotal = "max_total"
	lateReasonInserted = "inserted"
	lateReasonEvicted  = "evicted"
)

// lateReasons is the CLOSED SET of branches a late update can take. Every path through
// mergeHistorical records exactly one, so this list is also the contract that says the branches
// are exhaustive.
func lateReasons() []string {
	return []string{
		lateReasonAdditive, lateReasonMaxTotal, lateReasonInserted, lateReasonEvicted,
	}
}

// PreregisterLateUpdateReasons materializes candle_late_update_total AT ZERO for a scope, before
// any late update can occur.
//
// # ABSENCE AND ZERO ARE DIFFERENT ANSWERS, AND ONLY ZERO IS EVIDENCE
//
// A CounterVec creates a series on its FIRST WithLabelValues call. This counter is incremented at
// two sites inside mergeHistorical, so an absent series would make two very different readings
// identical: "no update ever arrived for a bucket that had already rolled" — implausible, because
// processCandleTrades runs OUTSIDE the handler mutex, so two batches for one symbol can invert —
// and "the routing never reaches this branch", which would mean late trades are being absorbed
// somewhere that does not account for them. Those explanations demand opposite fixes, and without
// a pre-registered zero the metric could not tell them apart.
//
// Pre-registering turns the silence into a measurement: a series pinned at zero says "I looked,
// and nothing was late". This is the same correction already applied to trades_discarded_total,
// for the same reason, after the same failure to distinguish no-data from no-event.
func (ce *CandleEngine) PreregisterLateUpdateReasons(exchange, marketType string, timeframes []string) {
	for _, tf := range timeframes {
		for _, reason := range lateReasons() {
			// WithLabelValues CREATES the series at zero without incrementing it, so this is
			// safe to call repeatedly — subscribe runs per symbol and must never disturb a
			// counter that has already moved.
			ce.metrics.preregisterLateUpdate(exchange, marketType, tf, reason)
		}
	}
}
