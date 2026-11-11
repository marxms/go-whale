package ingestion

import (
	"context"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// TestProcessCandleTrades_UpdatesAreOldestFirst pins an ordering the live path relied on
// without ever establishing it.
//
// # THE DEFECT
//
// The batch loop iterated the bucket map directly:
//
//	for timestamp, bucketCandle := range ohlcvc { ... }
//
// Go randomizes map iteration order, so a batch that straddles a bucket boundary — which is
// every batch that crosses one, at every boundary, for every symbol and timeframe — could
// emit the NEWER bucket first. The engine then:
//
//  1. opens bucket T+1, which CLOSES T and publishes it at whatever it held right then,
//     missing the trades still queued behind it in the same batch;
//  2. receives T, now a historical update, and repairs it through mergeHistorical;
//  3. publishes a revision to undo the close it should never have made.
//
// The value converges, but only because the late-correction path exists. Every consumer
// downstream sees a close followed by a correction, and candle_late_update_total counts
// trades as "late" that were never late — they were in the same batch, in the wrong order.
//
// The seed path already enforces this contract and says so out loud: mergeBatchIntoBuffer
// checks slices.IsSortedFunc and WARNS that "the producer violated the contract". The live
// path is the busiest producer of all and was never held to it.
//
// # WHY SORTING THE KEYS IS THE WHOLE FIX
//
// A map has no order to preserve, so nothing is being reordered — an order is being
// established where there was none. Buckets within one BuildOHLCVC result are disjoint and
// already comparable by timestamp, which is exactly the order the engine's three-branch
// merge expects.
func TestProcessCandleTrades_UpdatesAreOldestFirst(t *testing.T) {
	tfReg := candle.NewTimeframeRegistry([]string{"1m"})
	store := &capturingStore{}
	handler := NewStreamTradeHandler(store, "binance", "future", tfReg)

	// Trades spanning FIVE consecutive 1m buckets. With five keys in the map, the odds of
	// a random iteration landing on sorted order by chance are 1/120 per run, so a
	// regression is caught reliably rather than flakily.
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	const minute = int64(60_000)
	var trades []candle.Trade
	for i := range 5 {
		trades = append(trades, candle.Trade{
			ID:        string(rune('a' + i)),
			Timestamp: base + int64(i)*minute + 1000,
			Price:     100 + float64(i),
			Amount:    1,
			Side:      "buy",
		})
	}

	if err := handler.processCandleTrades(context.Background(), "binance", "future", "BTC/USDT", trades, tradeGap{}); err != nil {
		t.Fatalf("processCandleTrades: %v", err)
	}
	if len(store.updates) != 5 {
		t.Fatalf("expected 5 bucket updates, got %d", len(store.updates))
	}

	for i := 1; i < len(store.updates); i++ {
		prev, cur := store.updates[i-1].Candle.Timestamp, store.updates[i].Candle.Timestamp
		if cur <= prev {
			t.Fatalf("update %d has timestamp %d after %d — the batch reached the store out of "+
				"order. The engine will close the newer bucket first, publish it missing the "+
				"trades still queued behind it, then repair the older one through the "+
				"late-correction path and emit a revision to undo its own close", i, cur, prev)
		}
	}
}

// TestProcessCandleTrades_OrderHoldsAcrossTimeframes keeps the guarantee per (symbol,
// timeframe), which is the grain the engine's ring buffers actually use. Interleaving
// timeframes is harmless — they address different buffers — but each one's own sequence
// must be monotonic.
func TestProcessCandleTrades_OrderHoldsAcrossTimeframes(t *testing.T) {
	tfReg := candle.NewTimeframeRegistry([]string{"1m", "5m"})
	store := &capturingStore{}
	handler := NewStreamTradeHandler(store, "binance", "future", tfReg)

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	const minute = int64(60_000)
	var trades []candle.Trade
	for i := range 12 {
		trades = append(trades, candle.Trade{
			ID:        string(rune('a' + i)),
			Timestamp: base + int64(i)*minute + 1000,
			Price:     100, Amount: 1, Side: "buy",
		})
	}

	if err := handler.processCandleTrades(context.Background(), "binance", "future", "BTC/USDT", trades, tradeGap{}); err != nil {
		t.Fatalf("processCandleTrades: %v", err)
	}

	lastPerTF := map[string]int64{}
	for _, u := range store.updates {
		if prev, ok := lastPerTF[u.Timeframe]; ok && u.Candle.Timestamp <= prev {
			t.Fatalf("timeframe %s went backwards: %d after %d", u.Timeframe, u.Candle.Timestamp, prev)
		}
		lastPerTF[u.Timeframe] = u.Candle.Timestamp
	}
	if len(lastPerTF) != 2 {
		t.Fatalf("expected both timeframes to produce updates, got %v", lastPerTF)
	}
}
