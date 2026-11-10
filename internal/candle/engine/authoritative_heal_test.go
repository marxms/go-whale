package engine

import (
	"testing"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
)

const healStep = int64(5 * 60 * 1000)

// TestSeedCandles_ClosedBucketHealsFromTheExchange is the structural answer to a class of
// defect, not a patch for one instance of it.
//
// # WHY MAX ALONE WAS THE PROBLEM
//
// A seed fold that MAX-merges every field relies on "an under-count is recoverable, an inflation
// is not". But MAX also makes an inflation PERMANENT: every later repair compares the inflated
// stored value against the exchange's correct one and keeps the larger. A restart once
// double-counted an open bucket (ETH/USDT 5m: 22,436.96 against 12,294.02), and the rule meant to
// protect the data stopped it healing.
//
// # THE STRUCTURAL RULE
//
// For a bucket whose window has ELAPSED, the exchange's own kline is not a competing candidate
// to take a maximum against — it IS the record. Our value is a reconstruction of it, and a
// reconstruction can be wrong in BOTH directions: short when we missed trades, long when we
// double-counted. So an authoritative exchange total for a closed bucket REPLACES.
//
// This makes the system self-healing through the paths that already run — the boot seed and
// gap recovery — with no reconciliation job, no parallel sweep and no scheduled repair. A
// bucket that drifts is corrected the next time anything seeds over it.
func TestSeedCandles_ClosedBucketHealsFromTheExchange(t *testing.T) {
	t.Parallel()

	const (
		exchange = "binance"
		market   = "future"
		symbol   = "ETH/USDT"
		tf       = "5m"
	)
	// Two buckets that closed well in the past, so the elapsed-window test is unambiguous.
	base := time.Now().UnixMilli()/healStep*healStep - 20*healStep
	ce := NewCandleEngine(map[string]int{"default": 16})

	// The buffer holds an INFLATED bucket — the shape a restart double-count leaves behind.
	ce.SeedCandles(exchange, market, symbol, tf, []candle.Candle{
		{
			Timestamp: base, Open: 100, High: 130, Low: 90, Close: 120,
			Volume: 22436, TakerBuyVolume: 14000, TakerSellVolume: 8436,
		},
	})

	// Gap recovery delivers the exchange's record for that same, long-closed bucket.
	ce.SeedCandles(exchange, market, symbol, tf, []candle.Candle{
		{
			Timestamp: base, Open: 100, High: 125, Low: 92, Close: 118,
			Volume: 12294, TakerBuyVolume: 7000, TakerSellVolume: 5294,
			Source: string(redisstore.CandleMergeSourceRESTTotal),
			// Fetched well after the bucket settled — see candle.KlineSettleGraceMs. Without a
			// stamp the record is refused authority and falls back to MAX, which is the safe
			// direction but is not what this test is about.
			FetchedAt: time.Now().UnixMilli(),
		},
	})

	got := candleAt(ce, exchange, market, symbol, tf, base)
	if got == nil {
		t.Fatal("bucket vanished")
	}
	if got.Volume != 12294 {
		t.Errorf("Volume = %v, want 12294 — the exchange's record for a CLOSED bucket must "+
			"replace ours. Keeping the larger value is what made a double-count permanent: "+
			"every later repair re-elected the inflated number", got.Volume)
	}
	if got.TakerBuyVolume != 7000 || got.TakerSellVolume != 5294 {
		t.Errorf("taker pair = %v/%v, want 7000/5294 — order flow must heal with the volume",
			got.TakerBuyVolume, got.TakerSellVolume)
	}
	if got.High != 125 || got.Low != 92 {
		t.Errorf("OHLC = high %v low %v, want 125/92 — the exchange's bar is the reference; "+
			"widening it with our reconstruction keeps a wick the exchange never printed",
			got.High, got.Low)
	}
}

// TestSeedCandles_ClosedBucketHealsUpwardToo pins that the rule is AUTHORITY, not "smaller
// wins". A bucket we under-counted (missed trades during a disconnect) must also converge to
// the exchange's number.
func TestSeedCandles_ClosedBucketHealsUpwardToo(t *testing.T) {
	t.Parallel()

	const (
		exchange = "binance"
		market   = "future"
		symbol   = "BTC/USDT"
		tf       = "5m"
	)
	base := time.Now().UnixMilli()/healStep*healStep - 20*healStep
	ce := NewCandleEngine(map[string]int{"default": 16})

	ce.SeedCandles(exchange, market, symbol, tf, []candle.Candle{
		{
			Timestamp: base, Open: 100, High: 101, Low: 99, Close: 100,
			Volume: 50, TakerBuyVolume: 30, TakerSellVolume: 20,
		},
	})
	ce.SeedCandles(exchange, market, symbol, tf, []candle.Candle{
		{
			Timestamp: base, Open: 100, High: 105, Low: 95, Close: 102,
			Volume: 500, TakerBuyVolume: 300, TakerSellVolume: 200,
			Source:    string(redisstore.CandleMergeSourceRESTTotal),
			FetchedAt: time.Now().UnixMilli(),
		},
	})

	got := candleAt(ce, exchange, market, symbol, tf, base)
	if got == nil || got.Volume != 500 {
		t.Fatalf("Volume = %v, want 500 — an under-counted bucket must heal upward too", got.Volume)
	}
}

// TestSeedCandles_OpenBucketIsNotHealed is the guard that keeps the rule from becoming a new
// defect.
//
// An exchange total for a bucket that is STILL OPEN is a partial snapshot by definition — the
// bucket has not stopped receiving trades. Replacing with it would discard everything the
// live stream accumulated after the fetch, on every seed. The elapsed-window test is what
// separates "authoritative record" from "photo taken mid-bar".
func TestSeedCandles_OpenBucketIsNotHealed(t *testing.T) {
	t.Parallel()

	const (
		exchange = "binance"
		market   = "future"
		symbol   = "SOL/USDT"
		tf       = "5m"
	)
	// The CURRENT bucket — its window has not elapsed.
	base := time.Now().UnixMilli() / healStep * healStep
	ce := NewCandleEngine(map[string]int{"default": 16})

	ce.SeedCandles(exchange, market, symbol, tf, []candle.Candle{
		{
			Timestamp: base, Open: 100, High: 101, Low: 99, Close: 100,
			Volume: 900, TakerBuyVolume: 500, TakerSellVolume: 400,
		},
	})
	// A partial snapshot of the same, still-open bucket.
	ce.SeedCandles(exchange, market, symbol, tf, []candle.Candle{
		{
			Timestamp: base, Open: 100, High: 101, Low: 99, Close: 100,
			Volume: 300, TakerBuyVolume: 180, TakerSellVolume: 120,
			Source:    string(redisstore.CandleMergeSourceRESTTotal),
			FetchedAt: time.Now().UnixMilli(),
		},
	})

	got := candleAt(ce, exchange, market, symbol, tf, base)
	if got == nil {
		t.Fatal("bucket vanished")
	}
	if got.Volume != 900 {
		t.Errorf("Volume = %v, want 900 — a still-OPEN bucket must not be replaced by a partial "+
			"snapshot; that would discard every trade seen after the fetch", got.Volume)
	}
}

// TestSeedCandles_UnlabelledBatchDoesNotHeal pins the other half of the gate.
//
// Authority comes from the EXCHANGE, and only a declared exchange total carries it. A warm
// boot replays our OWN persisted candles; treating those as an authoritative correction would
// let a value we already stored overwrite a better one we have in memory, on the strength of
// nothing.
func TestSeedCandles_UnlabelledBatchDoesNotHeal(t *testing.T) {
	t.Parallel()

	const (
		exchange = "binance"
		market   = "future"
		symbol   = "ADA/USDT"
		tf       = "5m"
	)
	base := time.Now().UnixMilli()/healStep*healStep - 20*healStep
	ce := NewCandleEngine(map[string]int{"default": 16})

	ce.SeedCandles(exchange, market, symbol, tf, []candle.Candle{
		{Timestamp: base, Close: 100, Volume: 900, TakerBuyVolume: 500, TakerSellVolume: 400},
	})
	// No Source — provenance unknown, so it cannot claim authority.
	ce.SeedCandles(exchange, market, symbol, tf, []candle.Candle{
		{Timestamp: base, Close: 100, Volume: 300, TakerBuyVolume: 180, TakerSellVolume: 120},
	})

	got := candleAt(ce, exchange, market, symbol, tf, base)
	if got == nil || got.Volume != 900 {
		t.Fatalf("Volume = %v, want 900 — an unlabelled batch has no authority to overwrite", got.Volume)
	}
}
