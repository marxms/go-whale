package ingestion

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"

	"github.com/marxms/go-whale/internal/trade"
)

// b5SafeStore is a concurrency-safe stub; capturingStore is not, and its own data race would
// mask the one this probe is looking for.
type b5SafeStore struct {
	capturingStore
	mu sync.Mutex
}

func (s *b5SafeStore) AddStreamingCandlesBatch(ctx context.Context, updates []redisstore.CandleUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.capturingStore.AddStreamingCandlesBatch(ctx, updates)
}

func (s *b5SafeStore) SetLastTrade(ctx context.Context, e, m, sym string, tr candle.Trade) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.capturingStore.SetLastTrade(ctx, e, m, sym, tr)
}

func b5trade(id string, tsMs int64, amount float64) []trade.Trade {
	return []trade.Trade{{
		ID: id, Symbol: "ETH/USDT", Price: 100, Amount: amount,
		Side: "buy", Timestamp: tsMs,
	}}
}

// B5-P1: the gate must SILENCE admittedVolume once the venue owns every configured timeframe,
// and must KEEP counting where the tape still writes.
func TestOwnershipGate_GateSilencesOnlyFullOwnership(t *testing.T) {
	tfs := []string{"5m", "15m", "1h"}

	// scope A: nothing owned -> counted
	hA, mA := handlerWithMetrics(&capturingStore{}, "vA", "future", tfs)
	if err := hA.HandleTradesWithContext("vA", "future", "ETH/USDT", b5trade("1", 1_700_000_100_000, 7)); err != nil {
		t.Fatal(err)
	}
	if got := mA.admitted[labels("vA", "future")]; got != 7 {
		t.Fatalf("tape-owned scope must still count admitted volume: got %v want 7", got)
	}

	// scope B: ALL owned -> silenced
	hB, mB := handlerWithMetrics(&capturingStore{}, "vB", "future", tfs)
	for _, tf := range tfs {
		hB.MarkVenueOwnedCandles("vB", "future", tf)
	}
	if err := hB.HandleTradesWithContext("vB", "future", "ETH/USDT", b5trade("1", 1_700_000_100_000, 7)); err != nil {
		t.Fatal(err)
	}
	if got := mB.admitted[labels("vB", "future")]; got != 0 {
		t.Fatalf("fully venue-owned scope must NOT count admitted volume: got %v want 0", got)
	}

	// scope C: PARTIAL ownership -> still counted IN FULL. Documents the residual asymmetry:
	// applied{5m} is now 0 for this scope while admitted keeps climbing.
	hC, mC := handlerWithMetrics(&capturingStore{}, "vC", "future", tfs)
	hC.MarkVenueOwnedCandles("vC", "future", "5m")
	if err := hC.HandleTradesWithContext("vC", "future", "ETH/USDT", b5trade("1", 1_700_000_100_000, 7)); err != nil {
		t.Fatal(err)
	}
	got := mC.admitted[labels("vC", "future")]
	t.Logf("PARTIAL ownership (5m owned, 15m/1h tape): admitted=%v (applied{5m} would be 0)", got)
	if got != 7 {
		t.Fatalf("partial ownership: got %v want 7", got)
	}
}

// B5-P2: an EMPTY timeframe registry makes tapeStillWritesCandles return false for every scope,
// silencing the metric globally with no ownership ever declared.
func TestOwnershipGate_EmptyRegistrySilencesEverything(t *testing.T) {
	reg := candle.NewTimeframeRegistry(nil)
	t.Logf("NewTimeframeRegistry(nil).All() = %v", reg.All())
	h := NewStreamTradeHandler(&capturingStore{}, "vEmpty", "future", reg)
	if h.tapeStillWritesCandles("vEmpty", "future") {
		t.Logf("registry non-empty -> gate open (fine)")
		return
	}
	t.Logf("EMPTY registry -> gate CLOSED with zero ownership declared")
}

// B5-P3: deadlock / lock-ordering probe. tapeStillWritesCandles calls candlesAreVenueOwned
// (RLock) N times; MarkVenueOwnedCandles takes the write lock concurrently.
func TestOwnershipGate_NoDeadlockUnderConcurrentOwnershipDeclaration(t *testing.T) {
	tfs := []string{"5m", "15m", "30m", "1h", "4h", "1d", "1w", "1M"}
	h := NewStreamTradeHandler(&b5SafeStore{}, "vRace", "future", candle.NewTimeframeRegistry(tfs))

	done := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			default:
			}
			h.MarkVenueOwnedCandles("vRace", "future", tfs[i%len(tfs)])
		}
	}()

	for w := range 4 {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range 500 {
				id := fmt.Sprintf("%d-%d", w, i)
				_ = h.HandleTradesWithContext("vRace", "future", "ETH/USDT",
					b5trade(id, 1_700_000_100_000+int64(i)*1000, 1))
				h.PurgeSymbol("vRace:future:ETH/USDT")
			}
		}(w)
	}

	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()

	// consumers finish then the producer is told to stop
	time.AfterFunc(5*time.Second, func() { close(done) })
	select {
	case <-finished:
	case <-time.After(30 * time.Second):
		t.Fatal("DEADLOCK: workers did not finish in 30s")
	}
}
