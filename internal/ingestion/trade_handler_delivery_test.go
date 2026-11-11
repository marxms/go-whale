package ingestion

import (
	"context"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/trade"
)

func deliveryTestHandler() *StreamTradeHandler {
	return NewStreamTradeHandler(&capturingStore{}, "binance", "future",
		candle.NewTimeframeRegistry([]string{"5m"}))
}

func feedOneTrade(t *testing.T, h *StreamTradeHandler, exchange, marketType, symbol string) {
	t.Helper()
	err := h.HandleTradesWithContext(context.Background(), exchange, marketType, symbol, []trade.Trade{{
		ID:        "1",
		Symbol:    symbol,
		Price:     100,
		Amount:    1,
		Side:      "buy",
		Timestamp: 1_700_000_000_000,
	}})
	if err != nil {
		t.Fatalf("HandleTradesWithContext: %v", err)
	}
}

// TestDeliveringReflectsAdmittedTrades ties the boot gate's signal to the thing it has to
// mean: not "the venue accepted our subscribe frame" but "trades for this symbol are reaching
// the aggregator". That distinction IS the defect — the combined-stream attach reported
// 500/500 symbols at 13:41:14 while the transport's own verification found 335 delivering ten
// seconds later, and everything the boot did in between was done against a silent stream.
func TestDeliveringReflectsAdmittedTrades(t *testing.T) {
	t.Parallel()

	h := deliveryTestHandler()

	if h.Delivering("binance", "future", "ETH/USDT") {
		t.Fatal("a symbol with no ingested trade must not report as delivering — attached-and-silent " +
			"is precisely the state the boot gate exists to detect")
	}

	feedOneTrade(t, h, "binance", "future", "ETH/USDT")

	if !h.Delivering("binance", "future", "ETH/USDT") {
		t.Error("after a trade was admitted the stream is delivering; reporting otherwise would make " +
			"the boot gate wait out its whole grace on a healthy symbol")
	}
}

// TestDeliveringIsScopedAndPerSymbol guards the key shape. A prefix collision would let one
// scope's traffic vouch for another's, and the gate would then seed a silent stream believing
// it was live — the original bug wearing a new costume.
func TestDeliveringIsScopedAndPerSymbol(t *testing.T) {
	t.Parallel()

	h := deliveryTestHandler()
	feedOneTrade(t, h, "binance", "future", "ETH/USDT")

	if h.Delivering("binance", "spot", "ETH/USDT") {
		t.Error("a future-scope trade must not vouch for the spot scope")
	}
	if h.Delivering("binance", "future", "BTC/USDT") {
		t.Error("one symbol's traffic must not vouch for another's")
	}

	symbols := []string{"ETH/USDT", "BTC/USDT", "SOL/USDT"}
	if got := h.DeliveringCount("binance", "future", symbols); got != 1 {
		t.Errorf("DeliveringCount = %d, want 1 — this count is what the boot publishes as its "+
			"stitched-vs-blind split", got)
	}
	if got := h.DeliveringCount("binance", "future", nil); got != 0 {
		t.Errorf("DeliveringCount(nil) = %d, want 0", got)
	}
}

// TestPurgeSymbolClearsDelivery keeps the signal from outliving the subscription it describes:
// a deranked symbol that later comes back must prove delivery again rather than inherit it.
func TestPurgeSymbolClearsDelivery(t *testing.T) {
	t.Parallel()

	h := deliveryTestHandler()
	feedOneTrade(t, h, "binance", "future", "ETH/USDT")

	h.PurgeSymbol("binance:future:ETH/USDT")

	if h.Delivering("binance", "future", "ETH/USDT") {
		t.Error("delivery state must be purged with the subscription")
	}
}
