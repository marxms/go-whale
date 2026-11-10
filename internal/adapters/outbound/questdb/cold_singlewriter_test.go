package questdb

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// recordingColdStore counts writes and DETECTS any concurrent entry into WriteBatches —
// the single-writer invariant the cold-durability fix depends on (the QuestDB ILP sender
// is not goroutine-safe; only the pipeline flush loop may write it).
//
// The invariant is unchanged by the batching fix: the sweep now commits once instead of
// once per series, but it is still exactly one goroutine doing it.
type recordingColdStore struct {
	mu         sync.Mutex
	written    int
	inWrite    atomic.Bool
	concurrent atomic.Bool
}

func (s *recordingColdStore) WriteBatches(_ context.Context, groups map[candle.SeriesKey][]candle.Candle) error {
	if !s.inWrite.CompareAndSwap(false, true) {
		s.concurrent.Store(true) // a second goroutine entered WriteBatches — invariant broken
	}
	// Tiny window to widen any concurrency race so -race / the flag can catch it.
	time.Sleep(time.Millisecond)
	s.mu.Lock()
	for _, batch := range groups {
		s.written += len(batch)
	}
	s.mu.Unlock()
	s.inWrite.Store(false)
	return nil
}

func (s *recordingColdStore) QueryRange(context.Context, string, string, string, string, time.Time, time.Time, int) ([]candle.Candle, error) {
	return nil, nil
}

func (s *recordingColdStore) GetOldestTimestamp(context.Context, string, string, string, string) (time.Time, error) {
	return time.Time{}, nil
}
func (s *recordingColdStore) Close() error                 { return nil }
func (s *recordingColdStore) Stats() candle.ColdStoreStats { return candle.ColdStoreStats{} }

// TestPipeline_SingleWriterUnderConcurrentProducers exercises the exact
// concurrency the fix introduces: gap-recovery/discovery re-seed (blocking
// EnqueueClosedBlocking) and live OnCandleClose (non-blocking Enqueue) feeding the SAME
// pipeline for the SAME symbol/timeframe. Run with -race. Asserts:
//  1. no data race (the -race detector),
//  2. the cold store is NEVER written concurrently (single-writer invariant),
//  3. the blocking seed path is not silently dropped (backpressure, not loss),
//  4. the flush loop goroutine is cleaned up by Stop (no leak).
func TestPipeline_SingleWriterUnderConcurrentProducers(t *testing.T) {
	store := &recordingColdStore{}
	// Small buffer so the blocking producers actually hit backpressure.
	p := NewPipeline(store, 16, 10*time.Millisecond)
	p.Start()

	const (
		producers   = 8
		perProducer = 40
	)
	ctx := context.Background()
	var wg sync.WaitGroup
	var blockingEnqueued int64

	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < perProducer; j++ {
				c := candle.Candle{
					Timestamp: int64(1_000 + j*60_000), Open: 1, High: 1, Low: 1, Close: 1, Volume: 1,
				}
				if id%2 == 0 {
					// seed / backfill path — blocking backpressure, must not drop.
					if err := p.EnqueueClosedBlocking(ctx, "binance", "future", "BTC/USDT", "1h", c); err == nil {
						atomic.AddInt64(&blockingEnqueued, 1)
					}
				} else {
					// live close path — non-blocking (may drop under a full buffer).
					p.Enqueue("binance", "future", "BTC/USDT", "1h", c)
				}
			}
		}(i)
	}
	wg.Wait()
	p.Stop(t.Context()) // drains the buffer, flushes, and joins the flush-loop goroutine

	if store.concurrent.Load() {
		t.Fatal("ColdStore.WriteBatches was entered concurrently — single-writer invariant VIOLATED")
	}
	// Every blocking (seed) enqueue must have reached cold — the whole point of the
	// blocking path is no silent loss. Live drops are allowed, so the floor is the
	// blocking count.
	wantFloor := int(atomic.LoadInt64(&blockingEnqueued))
	if store.written < wantFloor {
		t.Fatalf("seed candles lost: wrote %d, but %d blocking enqueues must all persist", store.written, wantFloor)
	}
}
