package ingestion

import (
	"context"
	"testing"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/trade"
)

type capturingStore struct {
	updates    []redisstore.CandleUpdate
	lastTrades int
}

func (s *capturingStore) AddStreamingCandlesBatch(_ context.Context, updates []redisstore.CandleUpdate) error {
	s.updates = append(s.updates, updates...)
	return nil
}

func (s *capturingStore) SetLastTrade(_ context.Context, _, _, _ string, _ candle.Trade) error {
	s.lastTrades++
	return nil
}

// TestProcessCandleTrades_PreservesTakerFields locks the BucketedCandle→Candle
// conversion: every persisted CandleUpdate must carry the taker-side aggregates
// produced by candle.BuildOHLCVC. Regression guard for the bug where the
// conversion only copied OHLCV and silently dropped taker_buy/taker_sell/cvd
// before reaching redis_store.
func TestProcessCandleTrades_PreservesTakerFields(t *testing.T) {
	tfReg := candle.NewTimeframeRegistry([]string{"1m"})
	store := &capturingStore{}

	handler := NewStreamTradeHandler(store, "binance", "future", tfReg)

	baseTs := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	trades := []candle.Trade{
		{ID: "t1", Timestamp: baseTs + 1000, Price: 100, Amount: 2, Side: "buy"},
		{ID: "t2", Timestamp: baseTs + 2000, Price: 101, Amount: 1, Side: "sell"},
		{ID: "t3", Timestamp: baseTs + 3000, Price: 102, Amount: 3, Side: "buy"},
	}

	if err := handler.processCandleTrades("binance", "future", "BTC/USDT", trades, tradeGap{}); err != nil {
		t.Fatalf("processCandleTrades returned error: %v", err)
	}

	if len(store.updates) == 0 {
		t.Fatalf("expected at least one persisted candle update, got none")
	}

	for _, u := range store.updates {
		c := u.Candle
		// Expected from trades: taker_buy = 2+3 = 5; taker_sell = 1; trade_count = 3
		if c.TakerBuyVolume != 5 {
			t.Fatalf("TakerBuyVolume not persisted: got %.4f, want 5", c.TakerBuyVolume)
		}
		if c.TakerSellVolume != 1 {
			t.Fatalf("TakerSellVolume not persisted: got %.4f, want 1", c.TakerSellVolume)
		}
		// The derived values (delta, aggression) are NOT published any more — the consumer
		// computes them from the pair above. What must survive the live path is the
		// MEASUREMENT, and buy+sell > 0 is what marks the bar as measured at all.
		if c.TakerBuyVolume+c.TakerSellVolume <= 0 {
			t.Fatalf("bar must read as MEASURED (buy+sell > 0), got %.4f", c.TakerBuyVolume+c.TakerSellVolume)
		}
		if c.SideUnknownCount != 0 {
			t.Fatalf("SideUnknownCount expected 0, got %d", c.SideUnknownCount)
		}
	}
}

// TestVenueOwned_TheTapeStopsWritingCandlesButKeepsEverythingElse pins the boundary of the
// ownership design, which is easy to get wrong in either direction.
//
// Turning the tape off entirely would lose the missing-id verdict, the delivery signal and
// /api/trades. Leaving it writing candles would recreate the two-writer topology this design
// removes. It must do everything EXCEPT the second write.
func TestVenueOwned_TheTapeStopsWritingCandlesButKeepsEverythingElse(t *testing.T) {
	store := &capturingStore{}
	tfReg := candle.NewTimeframeRegistry([]string{"5m", "15m", "1h"})
	h := NewStreamTradeHandler(store, "binance", "future", tfReg)

	trade := func(id string, tsMs int64) []trade.Trade {
		return []trade.Trade{{
			ID: id, Symbol: "ETH/USDT", Price: 100, Amount: 3,
			Side: "buy", Timestamp: tsMs,
		}}
	}

	// Before the declaration the tape is the writer.
	if err := h.HandleTradesWithContext("binance", "future", "ETH/USDT", trade("1", 1_700_000_100_000)); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(store.updates) == 0 {
		t.Fatal("with no venue owner the tape MUST write the candle")
	}
	wroteBefore := len(store.updates)
	lastTradesBefore := store.lastTrades

	h.MarkVenueOwnedCandles("binance", "future", "5m")
	if err := h.HandleTradesWithContext("binance", "future", "ETH/USDT", trade("2", 1_700_000_101_000)); err != nil {
		t.Fatalf("handle: %v", err)
	}

	// The venue owns 5m only, so the tape must STOP writing 5m and KEEP writing everything else.
	var wrote5m, wroteOthers int
	for _, u := range store.updates[wroteBefore:] {
		if u.Timeframe == "5m" {
			wrote5m++
		} else {
			wroteOthers++
		}
	}
	if wrote5m != 0 {
		t.Errorf("the tape wrote %d update(s) for the venue-owned 5m — that is the second writer "+
			"this design exists to remove", wrote5m)
	}
	if wroteOthers == 0 {
		t.Error("the tape stopped writing timeframes the venue does NOT publish — that leaves them " +
			"with no producer at all, which is how 7 of 8 froze in production")
	}
	if !h.Delivering("binance", "future", "ETH/USDT") {
		t.Error("the delivery signal was lost — the tape must still admit and track")
	}
	if store.lastTrades <= lastTradesBefore {
		t.Error("SetLastTrade was skipped — /api/trades depends on it")
	}
}
