package engine

import (
	"testing"
)

// TestLateUpdateCountersExposeZero makes the silence of candle_late_update_total mean something.
//
// # THE READING THAT DECIDED NOTHING
//
// The counter is incremented at two sites inside mergeHistorical. After nine hours with 500
// symbols, production served NO candle_late_update_total series at all — because a CounterVec
// materializes a series only on its first WithLabelValues call.
//
// That absence is consistent with two opposite worlds:
//
//   - No update ever arrived for an already-rolled bucket. Implausible: processCandleTrades runs
//     OUTSIDE the handler mutex, so two batches for the same symbol can reach the engine in
//     inverted order, and the older one lands on a bucket that has already rolled.
//   - The routing never reaches that branch, meaning late trades are absorbed somewhere that does
//     not account for them — which is the shape of the loss under investigation.
//
// The two demand opposite fixes. A metric that cannot separate them is not evidence, and this is
// the third time in this activity that an unmaterialised counter has stalled a diagnosis:
// trades_discarded_total came back NOT-VERIFIED for want of the number, and cold_missing_buckets
// read an empty result as perfect through a real outage.
func TestLateUpdateCountersExposeZero(t *testing.T) {
	// A scope no other test touches. The counter is package-global and shared across the whole
	// test binary, so asserting "starts at zero" on a scope another test may have incremented
	// makes this test order-dependent — it would pass alone and fail in the suite, which is the
	// worst failure mode a gate can have.
	const exchange, market = "preregister-probe", "future"
	timeframes := []string{"5m", "15m", "1h"}

	rec := newEngineTestMetrics()
	ce := NewCandleEngine(nil)
	ce.SetMetrics(rec.port())
	ce.PreregisterLateUpdateReasons(exchange, market, timeframes)

	for _, tf := range timeframes {
		for _, reason := range lateReasons() {
			if !rec.latePrereg[key(exchange, market, tf, reason)] {
				t.Errorf("%s/%s was not pre-registered — absence and zero are different answers", tf, reason)
			}
			if v := rec.late[key(exchange, market, tf, reason)]; v != 0 {
				t.Errorf("%s/%s starts at %v, want 0 — pre-registration must CREATE the series, "+
					"never seed it with a count", tf, reason, v)
			}
		}
	}
}

// TestPreregisterLateUpdate_IsIdempotent keeps the call safe on the path that makes it: subscribe
// runs per symbol, so this is invoked repeatedly for one scope and must never disturb a counter
// that has already recorded a real late update.
func TestPreregisterLateUpdate_IsIdempotent(t *testing.T) {
	const exchange, market, tf = "bingx", "spot", "30m"

	rec := newEngineTestMetrics()
	ce := NewCandleEngine(nil)
	ce.SetMetrics(rec.port())
	ce.PreregisterLateUpdateReasons(exchange, market, []string{tf})
	rec.port().CandleLateUpdateTotal(exchange, market, tf, lateReasonAdditive)
	before := rec.late[key(exchange, market, tf, lateReasonAdditive)]

	ce.PreregisterLateUpdateReasons(exchange, market, []string{tf})
	after := rec.late[key(exchange, market, tf, lateReasonAdditive)]

	if before != after {
		t.Errorf("pre-registration reset a live counter: %v → %v. It must only CREATE series, "+
			"never zero one that has already recorded a late update", before, after)
	}
}
