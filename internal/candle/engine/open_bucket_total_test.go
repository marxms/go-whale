package engine

import (
	"testing"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
)

// TestUpdateCandle_DeclaredTotalIntoOpenBucketDoesNotDoubleCount guards against
// restart-boundary volume inflation.
//
// The seeder writes the OPEN bucket through AddStreamingCandle → UpdateCandle. The payload it
// passes is a REST kline, which carries the bucket TOTAL, not an increment. If UpdateCandle's
// same-bucket branch accumulates unless the bucket was gap-recovered, the total is ADDED on top
// of whatever the live stream has already accumulated. Measured on ETH/USDT 5m, the bucket that
// was open when a deploy restarted the streamer: ours 22,436.96 against Binance's 12,294.02
// (+82.5%), while every other bar matched to 0.0000%.
//
// The inflation is permanent: the seed union MAX-merges, so a too-high stored value survives
// every later repair — candle_seed_merge_total{outcome="stored_exceeded"} counts it.
//
// # WHY THE FIX IS "DECLARE THE SOURCE", NOT "SPECIAL-CASE THE SEEDER"
//
// late_update.go already states the rule this path was missing: branch by the DECLARED
// source, never by magnitude, because "producers on the live path label ws_delta explicitly,
// so the accumulating branch is reached by intent, never by omission". The REST total reached
// it by omission — it carried no source at all.
//
// The guard is `Source != "" && isTotalSource(...)`, NOT bare isTotalSource: that helper
// treats UNLABELLED as a total, and flipping unlabelled candles to MAX here would silently
// under-count the dominant live path.
func TestUpdateCandle_DeclaredTotalIntoOpenBucketDoesNotDoubleCount(t *testing.T) {
	t.Parallel()

	const (
		exchange = "binance"
		market   = "future"
		symbol   = "ETH/USDT"
		tf       = "5m"
	)
	ts := int64(1_700_000_000_000)
	ce := NewCandleEngine(map[string]int{"default": 16})

	// The live stream has accumulated part of the open bucket in THIS process.
	ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
		Timestamp: ts, Open: 100, High: 110, Low: 95, Close: 105,
		Volume: 400, TakerBuyVolume: 250, TakerSellVolume: 150,
		Source: string(redisstore.CandleMergeSourceWSDelta),
	})

	// The seeder delivers the exchange's TOTAL for the same open bucket. It is larger,
	// because it also covers the part of the bucket that traded before this process started.
	ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
		Timestamp: ts, Open: 100, High: 112, Low: 94, Close: 106,
		Volume: 1000, TakerBuyVolume: 600, TakerSellVolume: 400,
		Source: string(redisstore.CandleMergeSourceRESTTotal),
	})

	got, ok := ce.PeekLatestCandle(exchange, market, symbol, tf)
	if !ok {
		t.Fatal("bucket vanished")
	}

	if got.Volume != 1000 {
		t.Errorf("Volume = %v, want 1000 — the exchange's TOTAL was accumulated onto the live "+
			"partial instead of replacing it, inflating the bucket by the part that traded "+
			"before this process started", got.Volume)
	}
	if got.TakerBuyVolume != 600 || got.TakerSellVolume != 400 {
		t.Errorf("taker pair = %v/%v, want 600/400 — same double-count on order flow",
			got.TakerBuyVolume, got.TakerSellVolume)
	}
	// The invariant that made this visible in production.
	if got.TakerBuyVolume+got.TakerSellVolume > got.Volume {
		t.Errorf("buy+sell (%v) exceeds volume (%v) — physically impossible",
			got.TakerBuyVolume+got.TakerSellVolume, got.Volume)
	}
}

// TestUpdateCandle_LiveDeltasStillAccumulate is the other half, and it is what stops the fix
// from becoming a worse bug. Only an EXPLICITLY declared total may replace; a labeled
// ws_delta is an increment and must still sum, or the dominant live path silently
// under-counts every bucket to its single largest delta.
func TestUpdateCandle_LiveDeltasStillAccumulate(t *testing.T) {
	t.Parallel()

	const (
		exchange = "binance"
		market   = "future"
		symbol   = "BTC/USDT"
		tf       = "5m"
	)
	ts := int64(1_700_000_000_000)
	ce := NewCandleEngine(map[string]int{"default": 16})

	for range 3 {
		ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
			Timestamp: ts, Open: 100, High: 101, Low: 99, Close: 100,
			Volume: 10, TakerBuyVolume: 6, TakerSellVolume: 4,
			Source: string(redisstore.CandleMergeSourceWSDelta),
		})
	}

	got, _ := ce.PeekLatestCandle(exchange, market, symbol, tf)
	if got.Volume != 30 {
		t.Errorf("Volume = %v, want 30 — three ws deltas of 10 must accumulate", got.Volume)
	}
	if got.TakerBuyVolume != 18 || got.TakerSellVolume != 12 {
		t.Errorf("taker pair = %v/%v, want 18/12 — deltas must accumulate",
			got.TakerBuyVolume, got.TakerSellVolume)
	}
}

// TestUpdateCandle_UnlabelledCandleStillAccumulates pins the guard on the guard.
//
// isTotalSource() treats an UNLABELLED candle as a total. Reusing it bare here would flip
// every unlabelled producer from accumulate to replace — and an under-count is exactly what
// the asymmetry documented in late_update.go was written to avoid on this path. Only an
// explicit declaration may change the rule.
func TestUpdateCandle_UnlabelledCandleStillAccumulates(t *testing.T) {
	t.Parallel()

	const (
		exchange = "binance"
		market   = "future"
		symbol   = "SOL/USDT"
		tf       = "5m"
	)
	ts := int64(1_700_000_000_000)
	ce := NewCandleEngine(map[string]int{"default": 16})

	for range 2 {
		ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
			Timestamp: ts, Open: 100, High: 101, Low: 99, Close: 100,
			Volume: 10, TakerBuyVolume: 6, TakerSellVolume: 4,
			// no Source at all
		})
	}

	got, _ := ce.PeekLatestCandle(exchange, market, symbol, tf)
	if got.Volume != 20 {
		t.Errorf("Volume = %v, want 20 — an UNLABELLED candle must keep accumulating; only an "+
			"explicit total may replace", got.Volume)
	}
}
