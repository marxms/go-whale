package ingestion_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/ingestion"
	"github.com/marxms/go-whale/internal/ingestion/sourcetest"
	"github.com/marxms/go-whale/internal/ingestion/streaming"
	"github.com/marxms/go-whale/internal/market"
	"github.com/marxms/go-whale/internal/trade"
)

// pathStore records what actually reached persistence through the HybridManager path.
type pathStore struct {
	mu         sync.Mutex
	updates    int
	lastTrades int
}

func (s *pathStore) AddStreamingCandlesBatch(_ context.Context, updates []candle.CandleUpdate) error {
	s.mu.Lock()
	s.updates += len(updates)
	s.mu.Unlock()
	return nil
}

func (s *pathStore) SetLastTrade(_ context.Context, _, _, _ string, _ candle.Trade) error {
	s.mu.Lock()
	s.lastTrades++
	s.mu.Unlock()
	return nil
}

func (s *pathStore) counts() (updates, lastTrades int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.updates, s.lastTrades
}

// TestAttachedSourceThatProducedNothingIsNotDelivering is the AC-02 delivery-path proof
// at the consumer boundary: a source whose Subscribe returned nil is ATTACHED, and the
// boot signal must still say "not delivering" until a trade actually reaches the
// aggregator. It then flips only after the source produces data.
//
// The wiring is the real one (HybridManager -> StreamTradeHandler -> store). The only
// double is the source, registered through the same factory catalog production uses,
// so a new adapter needs no core change to be exercised this way.
func TestAttachedSourceThatProducedNothingIsNotDelivering(t *testing.T) {
	t.Parallel()

	fake := sourcetest.NewFakeSource(sourcetest.WithCapacity(8))
	store := &pathStore{}
	handler := ingestion.NewStreamTradeHandler(store, "binance", "future",
		candle.NewTimeframeRegistry([]string{"5m"}))

	manager := ingestion.NewHybridManager(handler, 2, map[string]streaming.ClientFactory{
		"binance": func(streaming.ClientConfig) (streaming.StreamingClient, error) {
			return fake, nil
		},
	})
	manager.Start(t.Context())
	t.Cleanup(manager.Stop)
	t.Cleanup(func() { _ = fake.Close() })

	if err := manager.Subscribe(context.Background(), "BTC/USDT", "binance", market.Future); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Attached and silent. Give the scheduler at least one full adaptive-polling turn so
	// the assertion is about an actively polled stream, not a not-yet-started one.
	time.Sleep(350 * time.Millisecond)
	if handler.Delivering("binance", "future", "BTC/USDT") {
		t.Fatal("Subscribe returned success but no trade was ever admitted: an ATTACHED stream is not a DELIVERING one")
	}
	if updates, lastTrades := store.counts(); updates != 0 || lastTrades != 0 {
		t.Fatalf("silent stream reached persistence (updates=%d lastTrades=%d): the boot gate would seed against data that does not exist", updates, lastTrades)
	}

	fake.Deliver("BTC/USDT", trade.Trade{
		ID:        "1",
		Symbol:    "BTC/USDT",
		Price:     100,
		Amount:    1,
		Side:      trade.Buy,
		Timestamp: 1_700_000_000_000,
	})

	deadline := time.Now().Add(3 * time.Second)
	for !handler.Delivering("binance", "future", "BTC/USDT") {
		if time.Now().After(deadline) {
			t.Fatal("a trade was buffered by the source but never admitted by the ingestion path")
		}
		time.Sleep(5 * time.Millisecond)
	}
	for {
		updates, lastTrades := store.counts()
		if updates > 0 && lastTrades > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("delivery signal flipped but the batch/last-trade writes never reached the store (updates=%d lastTrades=%d)", updates, lastTrades)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
