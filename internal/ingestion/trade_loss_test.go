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

// volumeStore sums the volume of every candle that reached persistence — the same quantity
// the parity harness compares against Binance.
type volumeStore struct {
	mu     sync.Mutex
	volume float64
}

func (s *volumeStore) AddStreamingCandlesBatch(_ context.Context, updates []redisstore.CandleUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, u := range updates {
		s.volume += u.Candle.Volume
	}
	return nil
}

func (s *volumeStore) SetLastTrade(_ context.Context, _, _, _ string, _ candle.Trade) error {
	return nil
}

func (s *volumeStore) total() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.volume
}

// TestScheduleClients_NeverEnqueuesAnInFlightClient pins the second half of the
// trade-loss fix.
//
// Re-enqueueing EVERY client on EVERY tick without checking whether a worker is already
// draining it, with several workers consuming that queue, drains the same symbol twice at
// once. Order-tolerant deduplication (seenWindow) means this does not cost volume, but it
// still costs a redundant GetTrades and doubles the work on the hottest path in the process
// for nothing.
func TestScheduleClients_NeverEnqueuesAnInFlightClient(t *testing.T) {
	m := NewHybridManager(nil, 4, nil)
	state := &HybridClientState{Symbol: "BTC/USDT", Exchange: "binance", Key: "binance:future:BTC/USDT"}
	m.clients[state.Key] = state

	// First tick: the client is idle, so it must be scheduled exactly once.
	m.scheduleClients()
	if got := len(m.processQueue); got != 1 {
		t.Fatalf("first tick enqueued %d, want 1", got)
	}

	// Ticks two and three land while the client is still in flight (a worker has not
	// finished draining it). Neither may enqueue it again — a second worker picking it up
	// is precisely what breaks the watermark's ordering precondition.
	m.scheduleClients()
	m.scheduleClients()
	if got := len(m.processQueue); got != 1 {
		t.Fatalf("in-flight client was enqueued %d times — a second worker can now drain the same symbol concurrently, which silently drops trades", got)
	}

	// Once the worker releases the claim, scheduling resumes normally.
	<-m.processQueue
	state.inFlight.Store(false)
	m.scheduleClients()
	if got := len(m.processQueue); got != 1 {
		t.Fatalf("after release, enqueued %d, want 1 — the claim leaked and this symbol would never be drained again", got)
	}
}

// TestScheduleClients_ReleasesClaimWhenQueueIsFull pins the failure path.
//
// If the claim were taken and the enqueue then failed, the client would stay marked
// in-flight forever and stop being drained — trading a small, sparse loss for a total one
// on that symbol. This is the more dangerous half of the fix.
func TestScheduleClients_ReleasesClaimWhenQueueIsFull(t *testing.T) {
	m := NewHybridManager(nil, 1, nil)
	m.processQueue = make(chan *HybridClientState, 1)

	blocker := &HybridClientState{Key: "binance:future:AAA"}
	victim := &HybridClientState{Key: "binance:future:BBB"}
	m.processQueue <- blocker // queue is now full

	m.clients[victim.Key] = victim
	m.scheduleClients()

	if victim.inFlight.Load() {
		t.Fatal("claim leaked after a failed enqueue — this symbol would never be scheduled again")
	}
}

// TestTradeHandler_OutOfOrderBatchesLoseNothing is the guarantee the monotonic watermark
// could not give.
//
// The Binance SDK dispatches every websocket message on its own goroutine
// (common/websocket.go executeCallbacks), so a symbol's trades arrive UNORDERED. Under the
// old watermark a batch holding lower ids that landed after a batch holding higher ids was
// discarded wholesale as "already seen" — permanent volume loss, measured in production at
// gap=1 on binance:future:REZ/USDT.
//
// Delivering the batches in reverse order is the exact shape of that failure.
func TestTradeHandler_OutOfOrderBatchesLoseNothing(t *testing.T) {
	store := &volumeStore{}
	h := NewStreamTradeHandler(store, "binance", "future", candle.NewTimeframeRegistry([]string{"1m"}))

	// REVERSED on purpose: highest ids first, so the "already seen" check is exercised
	// against ids that were never seen.
	for _, batch := range [][]trade.Trade{
		makeSeqTrades(201, 250),
		makeSeqTrades(151, 200),
		makeSeqTrades(101, 150),
	} {
		if err := h.HandleTradesWithContext("binance", "future", "BTC/USDT", batch); err != nil {
			t.Fatalf("handle: %v", err)
		}
	}

	const want = 150 // 150 trades of amount 1
	if got := store.total(); got != want {
		t.Fatalf("aggregated volume %.0f, want %.0f — %.0f units were lost to out-of-order delivery", got, float64(want), want-got)
	}
}

// TestTradeHandler_ReplayIsStillRejected pins what the watermark exists for.
//
// A fix that "solved" the loss by weakening deduplication would inflate volume instead —
// the same corruption in the other direction, and harder to spot because it looks like
// healthy activity.
func TestTradeHandler_ReplayIsStillRejected(t *testing.T) {
	store := &volumeStore{}
	h := NewStreamTradeHandler(store, "binance", "future", candle.NewTimeframeRegistry([]string{"1m"}))

	batch := makeSeqTrades(1, 50)
	for i := 0; i < 2; i++ {
		if err := h.HandleTradesWithContext("binance", "future", "BTC/USDT", batch); err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
	}

	if got := store.total(); got != 50 {
		t.Fatalf("aggregated volume %.0f, want 50 — a replayed batch inflated the bucket", got)
	}
}

// makeSeqTrades builds trades with consecutive numeric ids, amount 1 each, all inside the
// same recent minute so they land in one bucket.
func makeSeqTrades(from, to int64) []trade.Trade {
	base := time.Now().Add(-30 * time.Second)
	out := make([]trade.Trade, 0, to-from+1)
	for id := from; id <= to; id++ {
		out = append(out, trade.Trade{
			ID:        strconv.FormatInt(id, 10),
			Symbol:    "BTC/USDT",
			Price:     100,
			Amount:    1,
			Side:      "buy",
			Timestamp: base.Add(time.Duration(id) * time.Millisecond).UnixMilli(),
		})
	}
	return out
}
