package ingestion

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/trade"
)

// captureStore keeps every candle that reached persistence so a test can assert on the FIELDS
// of the bar, not merely on its volume.
type captureStore struct {
	mu      sync.Mutex
	updates []redisstore.CandleUpdate
}

func (s *captureStore) AddStreamingCandlesBatch(_ context.Context, updates []redisstore.CandleUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updates = append(s.updates, updates...)
	return nil
}

func (s *captureStore) SetLastTrade(_ context.Context, _, _, _ string, _ candle.Trade) error {
	return nil
}

func (s *captureStore) all() []redisstore.CandleUpdate {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]redisstore.CandleUpdate(nil), s.updates...)
}

// tapeAt builds one aggTrade per id, all inside the same minute, so the whole batch lands in
// a single bucket and the assertions are about that bucket alone.
func tapeAt(base time.Time, ids ...int64) []trade.Trade {
	out := make([]trade.Trade, 0, len(ids))
	for i, id := range ids {
		out = append(out, trade.Trade{
			ID:        strconv.FormatInt(id, 10),
			Symbol:    "XRP/USDT",
			Price:     1,
			Amount:    1,
			Side:      "sell",
			Timestamp: base.Add(time.Duration(i) * time.Millisecond).UnixMilli(),
		})
	}
	return out
}

func contiguousIDs(from, to int64) []int64 {
	out := make([]int64, 0, to-from+1)
	for id := from; id <= to; id++ {
		out = append(out, id)
	}
	return out
}

func withoutID(ids []int64, drop int64) []int64 {
	out := make([]int64, 0, len(ids))
	for _, id := range ids {
		if id != drop {
			out = append(out, id)
		}
	}
	return out
}

// TestTape_UnderCountedBucketMarksItselfForRepair is the failing-case anchor for an
// under-counted bucket.
//
// # WHAT IT REPRODUCES
//
// XRP/USDT 5m: our bucket closed at 5.614.460,40 against the exchange's
// own aggTrade tape at 5.644.246,00 — 29.785,60 short, 100% of it in takerSell, takerBuy exact
// to 1e-16, sideUnknown zero, buy+sell == volume. Internally consistent, and wrong. The same
// deficit came out of the 5m, 15m and 30m rings, three independent sums, so the trades never
// reached the aggregator; and every counter in the process read zero, because all of them
// measure what we did with the trades that ARRIVED.
//
// One aggTrade of the tape is missing here, from a contiguous id sequence — the exact shape
// identified on BNBUSDT, where a=856396243 was absent while 856396242 and 856396244 both
// arrived. The bar cannot be made whole from memory; the only correct behavior left is for it
// to SAY it is short.
func TestTape_UnderCountedBucketMarksItselfForRepair(t *testing.T) {
	store := &captureStore{}
	h := NewStreamTradeHandler(store, "binance", "future", candle.NewTimeframeRegistry([]string{"1m"}))
	base := time.UnixMilli(1_785_728_700_000) // 03:45:00 UTC, the incident bucket

	ids := withoutID(contiguousIDs(767575152, 767575200), 767575180)
	if err := h.HandleTradesWithContext("binance", "future", "XRP/USDT", tapeAt(base, ids...)); err != nil {
		t.Fatalf("handle: %v", err)
	}

	updates := store.all()
	if len(updates) == 0 {
		t.Fatal("no candle reached persistence")
	}
	marked := false
	for _, u := range updates {
		if u.Candle.HasGap {
			marked = true
		}
	}
	if !marked {
		t.Errorf("the bucket closed claiming to be complete after an aggregate trade the exchange " +
			"issued never arrived. This is the whole defect: a bar short by a measurable amount, " +
			"internally consistent, persisted, and indistinguishable from a correct one")
	}
}

// TestTape_ReorderingIsNotReportedAsLoss is the guard that killed the first design.
//
// The SDK dispatches every websocket message on its own goroutine, so ids arrive out of order
// as a matter of course. A detector without a reordering frontier fired 103 times on the real
// BNB tape from the incident with adjacent pairs swapped — a tape that was COMPLETE. A signal
// that cries loss on ordinary delivery is worse than no signal: it trains the operator to
// ignore it, which is how the silent version of this defect survives a second time.
func TestTape_ReorderingIsNotReportedAsLoss(t *testing.T) {
	store := &captureStore{}
	h := NewStreamTradeHandler(store, "binance", "future", candle.NewTimeframeRegistry([]string{"1m"}))
	base := time.UnixMilli(1_785_728_700_000)

	// Highest ids first, then the earlier ones — the exact shape of goroutine-dispatched
	// delivery, and the shape the existing out-of-order test already drives.
	for _, batch := range [][]int64{
		contiguousIDs(900201, 900250),
		contiguousIDs(900151, 900200),
		contiguousIDs(900101, 900150),
	} {
		if err := h.HandleTradesWithContext("binance", "future", "ADA/USDT", tapeAt(base, batch...)); err != nil {
			t.Fatalf("handle: %v", err)
		}
	}

	for _, u := range store.all() {
		if u.Candle.HasGap {
			t.Fatal("a complete bucket marked itself for repair — every reorder would become a " +
				"REST job on the shared budget")
		}
	}
}

// TestTape_NonContiguousVenueIsNeverJudged pins the gate.
//
// Reading a hole in the ids as a lost message is only valid where the venue numbers trades
// CONSECUTIVELY. That is Binance's aggTrade `a`, verified against the exchange on the incident
// tape. This handler also serves bingx spot and future (config.yaml), whose ids
// carry no such guarantee — and there the damage would be double: a permanent false alarm AND
// a targeted REST repair for every bucket, on a shared per-IP budget. That is not a nuisance,
// it is a self-inflicted outage.
func TestTape_NonContiguousVenueIsNeverJudged(t *testing.T) {
	store := &captureStore{}
	h := NewStreamTradeHandler(store, "bingx", "future", candle.NewTimeframeRegistry([]string{"1m"}))
	base := time.UnixMilli(1_785_728_700_000)

	// Sparse by nature, not by loss.
	if err := h.HandleTradesWithContext("bingx", "future", "BTC/USDT",
		tapeAt(base, 100, 250, 900, 1500, 4000, 9000)); err != nil {
		t.Fatalf("handle: %v", err)
	}

	for _, u := range store.all() {
		if u.Candle.HasGap {
			t.Fatal("a bingx bucket asked for a REST repair because its ids are not consecutive")
		}
	}
}
