package ingestion

import (
	"testing"
	"time"
)

// TestTape_MissingTradeIsCounted pins the SIGNAL. It only compiles once trades_missing_total
// exists, which is itself the point: before it, the process had no way to express "a trade the
// exchange issued never reached us" and reported unanimous zeros through the whole event.
func TestTape_MissingTradeIsCounted(t *testing.T) {
	store := &captureStore{}
	h, m := handlerWithMetrics(store, "binance", "future", []string{"1m"})
	base := time.UnixMilli(1_785_728_700_000)

	before := m.missing[labels("binance", "future")]
	ids := withoutID(contiguousIDs(500000, 500060), 500030)
	if err := h.HandleTradesWithContext("binance", "future", "SOL/USDT", tapeAt(base, ids...)); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if got := m.missing[labels("binance", "future")] - before; got != 1 {
		t.Errorf("trades_missing_total moved %.0f, want 1", got)
	}
}

// TestTape_ReorderingIsNotCounted is the counter-side of the frontier guard: the metric must
// stay flat on a COMPLETE tape delivered out of order. A detector without the frontier fired
// 103 times on the real BNB tape from the incident with adjacent pairs swapped.
func TestTape_ReorderingIsNotCounted(t *testing.T) {
	store := &captureStore{}
	h, m := handlerWithMetrics(store, "binance", "future", []string{"1m"})
	base := time.UnixMilli(1_785_728_700_000)

	before := m.missing[labels("binance", "future")]
	for _, batch := range [][]int64{
		contiguousIDs(910201, 910250),
		contiguousIDs(910151, 910200),
		contiguousIDs(910101, 910150),
	} {
		if err := h.HandleTradesWithContext("binance", "future", "DOT/USDT", tapeAt(base, batch...)); err != nil {
			t.Fatalf("handle: %v", err)
		}
	}
	if got := m.missing[labels("binance", "future")] - before; got != 0 {
		t.Errorf("trades_missing_total moved %.0f on a COMPLETE tape delivered out of order — "+
			"the reordering frontier is not holding", got)
	}
}

// TestTape_NonContiguousVenueIsNeverCounted is the counter-side of the venue gate.
func TestTape_NonContiguousVenueIsNeverCounted(t *testing.T) {
	store := &captureStore{}
	h, m := handlerWithMetrics(store, "bingx", "future", []string{"1m"})
	base := time.UnixMilli(1_785_728_700_000)

	before := m.missing[labels("bingx", "future")]
	if err := h.HandleTradesWithContext("bingx", "future", "ETH/USDT",
		tapeAt(base, 100, 250, 900, 1500, 4000, 9000)); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if got := m.missing[labels("bingx", "future")] - before; got != 0 {
		t.Errorf("trades_missing_total moved %.0f on a venue whose ids were never dense", got)
	}
}
