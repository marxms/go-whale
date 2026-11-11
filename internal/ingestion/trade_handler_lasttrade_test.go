package ingestion

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/ingestion/streaming"
)

// THE GUARD THAT WAS HOLDING THE INDEX WAS NOT THE GUARD ANYONE WAS LOOKING AT.
//
// `if len(updates) == 0 { return nil }` sat above the last-trade write and read as an efficiency
// check. It was also the only thing keeping `candleTrades[len(candleTrades)-1]` from indexing -1:
// no trades meant no buckets meant no updates meant early return.
//
// Making the batch conditional so SetLastTrade could still run under venue ownership removed that
// return, and with it the accident that was holding the index. An independent verifier's probe
// found it by calling the path with an empty slice; it panicked.
func TestProcessCandleTrades_EmptySliceDoesNotPanic(t *testing.T) {
	handler := NewStreamTradeHandler(
		&persistenceFailingStore{}, "binance", "future",
		candle.NewTimeframeRegistry([]string{"1m"}),
	)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic on empty candleTrades: %v", r)
		}
	}()
	if err := handler.processCandleTrades(context.Background(), "binance", "future", "BTC/USDT", nil, tradeGap{}); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

// SetLastTrade MUST STILL RUN WHEN THE VENUE OWNS EVERY TIMEFRAME.
//
// With all timeframes venue-owned the bucket loop skips every one, `updates` is always empty, and
// an early return there makes the last-trade write unreachable — freezing /api/trades and the
// lastTrade field of every timeseries response at the instant ownership is declared.
func TestProcessCandleTrades_VenueOwnedAll_LastTradeStillWritten(t *testing.T) {
	tfs := []string{"5m", "15m", "1h"}
	store := &persistenceFailingStore{}
	handler := NewStreamTradeHandler(store, "binance", "future", candle.NewTimeframeRegistry(tfs))
	for _, tf := range tfs {
		handler.MarkVenueOwnedCandles("binance", "future", tf)
	}

	err := handler.processCandleTrades(context.Background(), "binance", "future", "BTC/USDT",
		[]candle.Trade{{ID: "1", Timestamp: time.Now().UnixMilli(), Price: 100, Amount: 1, Side: "buy"}}, tradeGap{})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if store.batchCalls != 0 {
		t.Fatalf("batch attempted with nothing to write: %d", store.batchCalls)
	}
	if store.lastCalls != 1 {
		t.Fatalf("SetLastTrade calls = %d, want 1", store.lastCalls)
	}
}

// BACK-PRESSURE MUST SURVIVE PARTIAL OWNERSHIP.
//
// One timeframe owned by the venue, one still on the tape: the batch is non-empty, so a critical
// persistence failure must still return before the last-trade write is attempted. Reordering the
// two writes would have quietly converted a hard stop into a warning.
func TestProcessCandleTrades_PartialOwnership_BackPressureIntact(t *testing.T) {
	tfs := []string{"5m", "15m"}
	store := &persistenceFailingStore{batchErr: context.DeadlineExceeded}
	handler := NewStreamTradeHandler(store, "binance", "future", candle.NewTimeframeRegistry(tfs))
	handler.MarkVenueOwnedCandles("binance", "future", "5m")

	err := handler.processCandleTrades(context.Background(), "binance", "future", "BTC/USDT",
		[]candle.Trade{{ID: "1", Timestamp: time.Now().UnixMilli(), Price: 100, Amount: 1, Side: "buy"}}, tradeGap{})
	var criticalErr *streaming.CriticalPersistenceError
	if !errors.As(err, &criticalErr) {
		t.Fatalf("back-pressure lost on partial ownership: err=%v", err)
	}
	if store.lastCalls != 0 {
		t.Fatalf("SetLastTrade ran after a critical batch failure: %d", store.lastCalls)
	}
}
