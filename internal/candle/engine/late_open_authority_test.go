package engine

import (
	"testing"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
)

// TestMergeLateIntoClosed_TotalRestatesTheOpen closes a hole the independent verifier found in
// the fix for `Open`: the correction was applied at TWO merge sites and only ONE was defended.
//
// Reverting candle_engine.go turned a test red. Reverting late_update.go — `merged.Open` under
// `total` — left `go test ./...` ENTIRELY GREEN. A correction with no test that fails when it
// is removed is indistinguishable from a comment, and this is the second time in this activity
// that a guard has been decorative in exactly that way.
//
// The two sites answer different questions. candle_engine.go merges into the bucket that is
// still OPEN; this one merges a payload that arrives for a bucket that has already ROLLED.
// mergeCandleValues documents `total=true` as "the payload IS the whole bucket", and every
// monotonic field honors it — Open was silently exempt from its own contract.
func TestMergeLateIntoClosed_TotalRestatesTheOpen(t *testing.T) {
	t.Parallel()

	const (
		exchange = "binance"
		market   = "future"
		symbol   = "ADA/USDT"
		tf       = "5m"
	)
	step := int64(5 * 60 * 1000)
	base := int64(1_700_000_000_000) / step * step
	ce := NewCandleEngine(map[string]int{"default": 16})

	// The bucket accumulates live, then rolls: its Open is the first price this process saw.
	ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
		Timestamp: base, Open: 0.1928, High: 0.1930, Low: 0.1925, Close: 0.1929,
		Volume: 400, TakerBuyVolume: 250, TakerSellVolume: 150,
		Source: string(redisstore.CandleMergeSourceWSDelta),
	})
	ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
		Timestamp: base + step, Open: 0.1929, High: 0.1929, Low: 0.1929, Close: 0.1929,
		Volume: 1, Source: string(redisstore.CandleMergeSourceWSDelta),
	})

	// The exchange's total for the now-CLOSED bucket arrives late. Its Open is the bucket's
	// real open — the first trade of the bucket, before this process existed.
	ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
		Timestamp: base, Open: 0.1901, High: 0.1935, Low: 0.1899, Close: 0.1929,
		Volume: 1000, TakerBuyVolume: 600, TakerSellVolume: 400,
		Source: string(redisstore.CandleMergeSourceRESTTotal),
	})

	got := candleAt(ce, exchange, market, symbol, tf, base)
	if got == nil {
		t.Fatal("bucket vanished")
	}
	if got.Open != 0.1901 {
		t.Errorf("Open = %v, want 0.1901 — a late payload that declares itself the whole bucket "+
			"carries the bucket's open, exactly as it carries every other monotonic field. "+
			"Keeping ours publishes the price at which THIS PROCESS started watching",
			got.Open)
	}
}

// TestMergeLateIntoClosed_DeltaNeverRestatesTheOpen is the other half. A late ws_delta is one
// more trade in a bucket that already has an open; it must not move it.
func TestMergeLateIntoClosed_DeltaNeverRestatesTheOpen(t *testing.T) {
	t.Parallel()

	const (
		exchange = "binance"
		market   = "future"
		symbol   = "BTC/USDT"
		tf       = "5m"
	)
	step := int64(5 * 60 * 1000)
	base := int64(1_700_000_000_000) / step * step
	ce := NewCandleEngine(map[string]int{"default": 16})

	ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
		Timestamp: base, Open: 63000, High: 63010, Low: 62990, Close: 63005, Volume: 10,
		Source: string(redisstore.CandleMergeSourceWSDelta),
	})
	ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
		Timestamp: base + step, Open: 63005, Close: 63005, Volume: 1,
		Source: string(redisstore.CandleMergeSourceWSDelta),
	})
	ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
		Timestamp: base, Open: 62000, High: 63010, Low: 61999, Close: 63006, Volume: 2,
		Source: string(redisstore.CandleMergeSourceWSDelta),
	})

	got := candleAt(ce, exchange, market, symbol, tf, base)
	if got == nil || got.Open != 63000 {
		t.Fatalf("Open = %v, want 63000 — a late INCREMENT is one more trade in a bucket that "+
			"already has an open, and may not restate it", got.Open)
	}
}
