package engine

import (
	"testing"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
)

// TestUpdateCandle_TradelessPlaceholderDoesNotPinTheOpen reproduces a bar that opened at a
// price nobody traded and whose low was a price nobody traded.
//
// # A BUCKET WITH NO TRADES CANNOT DICTATE THE PRICES OF ONE THAT HAS THEM
//
// A REST kline for a bucket in which nothing traded carries open=high=low=close all set to the
// PREVIOUS bucket's close, with volume 0. Seeded first, the declared-total branch prefers the
// incoming open — correct for a real total, wrong for a placeholder. Then real trades arrive as
// deltas, the preference flips back to the stored value, and the phantom price is pinned for
// the life of the bucket. minPositive drags `low` down to it as well.
//
// No volume is missing, so every quantity-shaped check passes. What is wrong is a PRICE.
func TestUpdateCandle_TradelessPlaceholderDoesNotPinTheOpen(t *testing.T) {
	t.Parallel()

	const (
		exchange, market, symbol, tf = "binance", "future", "ADBE/USDT", "5m"
		bucket                       = int64(1_785_929_400_000)
	)
	ce := NewCandleEngine(map[string]int{"default": 16})

	// The empty-bucket kline: OHLC all at the previous close, no volume.
	ce.SeedCandles(exchange, market, symbol, tf, []candle.Candle{{
		Timestamp: bucket, Open: 257.72, High: 257.72, Low: 257.72, Close: 257.72,
		Volume: 0, TakerBuyVolume: 0, TakerSellVolume: 0,
		Source: string(redisstore.CandleMergeSourceRESTTotal),
	}})

	// The first REAL trades, arriving as a live delta two minutes into the bucket.
	ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
		Timestamp: bucket, Open: 257.89, High: 258.12, Low: 257.82, Close: 258.00,
		Volume: 4.08, TakerBuyVolume: 3.90, TakerSellVolume: 0.18,
		Source: string(sourceWSDelta),
	})

	got, ok := ce.PeekLatestCandle(exchange, market, symbol, tf)
	if !ok {
		t.Fatal("no candle in the ring")
	}
	if got.Open != 257.89 {
		t.Errorf("open = %v, want 257.89 — the bar opened at %v, which is the PREVIOUS bucket's "+
			"close and a price that never traded in this one. The first real trade owns the open",
			got.Open, got.Open)
	}
	if got.Low != 257.82 {
		t.Errorf("low = %v, want 257.82 — minPositive dragged the low to the placeholder price, "+
			"so the bar reports a range the market never printed", got.Low)
	}
	if got.High != 258.12 {
		t.Errorf("high = %v, want 258.12", got.High)
	}
	if got.Volume != 4.08 {
		t.Errorf("volume = %v, want 4.08 — this fix moves WHICH price is elected, never how "+
			"much was traded", got.Volume)
	}
}
