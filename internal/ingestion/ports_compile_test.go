package ingestion

import (
	"context"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

// minimalTradeStore implements ONLY the batch write and last-trade write the handler
// declares. The handler must not need any other candle capability to aggregate a tape.
type minimalTradeStore struct{}

func (minimalTradeStore) AddStreamingCandlesBatch(context.Context, []candle.CandleUpdate) error {
	return nil
}

func (minimalTradeStore) SetLastTrade(context.Context, string, string, string, candle.Trade) error {
	return nil
}

var _ CombinedStore = minimalTradeStore{}

func TestMinimalStoreSatisfiesTheTradeHandler(t *testing.T) {
	handler := NewStreamTradeHandler(minimalTradeStore{}, "binance", "future",
		candle.NewTimeframeRegistry([]string{"5m"}))
	if handler == nil {
		t.Fatal("the trade handler rejected the minimal store")
	}
}
