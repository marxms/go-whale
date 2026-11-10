package ingestion

import "testing"

// TestDiscardCountersExposeZero closes gate G5, and it closes it on the axis that made the
// gate unverifiable rather than by asserting the producer exists.
//
// A Prometheus CounterVec creates a series only on its FIRST WithLabelValues call. After
// 4h55m and 500 symbols in production, /metrics carried no tradesDiscarded series at all.
// That reads as "no data" and is indistinguishable from "the metric is not wired".
// Pre-registering every (exchange, market_type, reason) combination at subscribe time makes
// the zero observable, which is what turns G5 from an argument into a measurement.
func TestDiscardCountersExposeZero(t *testing.T) {
	const exchange, market = "orderfree-exchange", "orderfree-market"

	m := newTestMetrics()
	h := &StreamTradeHandler{metrics: m.port()}
	h.PreregisterDiscardReasons(exchange, market)

	for _, reason := range discardReasons() {
		key := labels(exchange, market, reason)
		if !m.preregDiscard[key] {
			t.Errorf("reason %q was not pre-registered — a counter that only appears once it "+
				"fires cannot answer 'was anything discarded?'", reason)
		}
		if v := m.discarded[key]; v != 0 {
			t.Errorf("reason %q starts at %v, want 0 — pre-registration must CREATE the series, "+
				"never seed it with a count", reason, v)
		}
	}
	if !m.preregMissing[labels(exchange, market)] {
		t.Error("tradesMissing series was not pre-registered at zero")
	}
}

// TestPreregisterIsIdempotent keeps the call safe on the path that will make it: subscribe
// runs per symbol, so this is invoked repeatedly for the same scope and must not disturb a
// counter that has already moved.
func TestPreregisterIsIdempotent(t *testing.T) {
	const exchange, market = "bingx", "spot"

	m := newTestMetrics()
	h := &StreamTradeHandler{metrics: m.port()}
	h.PreregisterDiscardReasons(exchange, market)
	h.metrics.incDiscard(exchange, market, discardReasons()[0])
	before := m.discarded[labels(exchange, market, discardReasons()[0])]

	h.PreregisterDiscardReasons(exchange, market)
	after := m.discarded[labels(exchange, market, discardReasons()[0])]

	if before != after {
		t.Errorf("pre-registration reset a live counter: %v → %v. It must only CREATE series, "+
			"never zero one that has already recorded a discard", before, after)
	}
}
