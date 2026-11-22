package ingestion

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/trade"
)

type repairCaptureStore struct {
	mu      sync.Mutex
	updates []candle.CandleUpdate
}

func (s *repairCaptureStore) AddStreamingCandlesBatch(_ context.Context, updates []candle.CandleUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updates = append(s.updates, updates...)
	return nil
}

func (s *repairCaptureStore) SetLastTrade(_ context.Context, _, _, _ string, _ candle.Trade) error {
	return nil
}

func (s *repairCaptureStore) lastBucket() (candle.Candle, candle.CandleUpdate, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for index := len(s.updates) - 1; index >= 0; index-- {
		if s.updates[index].Timeframe == "1m" {
			return s.updates[index].Candle, s.updates[index], true
		}
	}
	return candle.Candle{}, candle.CandleUpdate{}, false
}

type countingGapQueue struct{ calls int }

func (q *countingGapQueue) EnqueueBatch(_, _ string, _ []string, _, _ time.Time) { q.calls++ }

func seqTradesAt(base int64, from, to int64) []trade.Trade {
	out := make([]trade.Trade, 0, to-from+1)
	for id := from; id <= to; id++ {
		out = append(out, trade.Trade{
			ID:        strconv.FormatInt(id, 10),
			Symbol:    "BTC/USDT",
			Price:     100,
			Amount:    1,
			Side:      trade.Buy,
			Timestamp: base + id*10,
		})
	}
	return out
}

// A confirmed provider gap and a local drop are different classes, and the
// bucket carrying the gap stays PENDING until an independent, identical settled
// REST read validates the repair. Enqueueing a repair never marks it repaired.
func TestMissingStaysSeparateAndRepairPendingUntilValidated(t *testing.T) {
	store := &repairCaptureStore{}
	handler, metrics := handlerWithMetrics(store, "binance", "future", []string{"1m"})
	base := time.Now().Add(-2 * time.Minute).Truncate(time.Minute).UnixMilli()

	// id 10 never arrives; 1..9 and 11..20 do, all inside one 1m bucket.
	for _, batch := range [][]trade.Trade{seqTradesAt(base, 1, 9), seqTradesAt(base, 11, 20)} {
		if err := handler.HandleTradesWithContext(context.Background(), "binance", "future", "BTC/USDT", batch); err != nil {
			t.Fatalf("handle: %v", err)
		}
	}

	if got := metrics.missing[labels("binance", "future")]; got != 1 {
		t.Fatalf("missing=%v, want 1 confirmed provider gap", got)
	}
	// A provider gap is not a local drop: no BUFFER_* / duplicate / stale reason moved.
	for reason, count := range metrics.discarded {
		if count != 0 {
			t.Fatalf("provider gap leaked into local discard %q (%v)", reason, count)
		}
	}

	bar, update, ok := store.lastBucket()
	if !ok {
		t.Fatal("no 1m candle was written")
	}
	if !bar.HasGap {
		t.Fatal("a confirmed gap must mark the affected bucket HasGap (pending repair)")
	}

	// Enqueue is a request, not a result: the bucket stays pending.
	queue := &countingGapQueue{}
	queue.EnqueueBatch("binance", "future", []string{"BTC/USDT"}, time.Now().Add(-time.Minute), time.Now())
	if queue.calls != 1 {
		t.Fatalf("enqueue calls=%d, want 1", queue.calls)
	}
	barAfter, _, ok := store.lastBucket()
	if !ok || !barAfter.HasGap {
		t.Fatal("enqueue marked the bucket repaired; only a validated repair may clear the mark")
	}
	if got := metrics.missing[labels("binance", "future")]; got != 1 {
		t.Fatalf("enqueue invented missing trades: %v", got)
	}

	// A failed/partial repair does not clear the mark; only an independent,
	// identical, settled REST total does.
	tfMs := int64(60_000)
	now := time.Now().UnixMilli()
	bucket := update.Candle.Timestamp
	stored := candle.Candle{
		Timestamp: bucket, Open: 1, High: 2, Low: 1, Close: 2,
		Volume: 10, TakerBuyVolume: 6, TakerSellVolume: 4,
		Source: string(candle.CandleMergeSourceRESTTotal), FetchedAt: now - 2000, HasGap: true,
	}
	overlap := stored
	overlap.HasGap, overlap.RequestedAt, overlap.FetchedAt = false, stored.FetchedAt, now-1000
	if healed := candle.UnionSeedInto(stored, overlap, tfMs, now); !healed.HasGap {
		t.Fatal("a non-independent repair cleared the pending mark")
	}
	valid := stored
	valid.HasGap, valid.RequestedAt, valid.FetchedAt = false, now-1500, now-1000
	if healed := candle.UnionSeedInto(stored, valid, tfMs, now); healed.HasGap {
		t.Fatal("an independent identical settled total did not clear the pending mark")
	}
}
