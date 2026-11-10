package engine

import (
	"testing"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
)

// TestOpen_AuthoritativeTotalSetsTheBucketOpen guards the one field of the merge that must not
// ignore the declared-source rule.
//
// Every monotonic field of the same-bucket merge dispatches on the DECLARED source: a payload
// that says it is the bucket total replaces, a payload that says it is an increment
// accumulates. If Open used `firstNonZero(latest.Open, c.Open)` instead, the value already in
// the ring would win unconditionally, even against an authoritative exchange total.
//
// For the bucket open at boot, the ring's value is the price of the FIRST TRADE THIS PROCESS
// SAW, which is not the bucket's open. The boot deliberately waits for the live stream to
// deliver before requesting the snapshot, so by the time the exchange's total arrives the ring
// is guaranteed to hold a bar whose Open is the boot-instant price.
//
// The known aggTrade limitation moves VOLUME between adjacent buckets. Bit-identical volume
// with a wrong open is not a straddle; it is a different defect, and the parity exemption for
// the boot bucket does not cover `open` at all.
func TestOpen_AuthoritativeTotalSetsTheBucketOpen(t *testing.T) {
	t.Parallel()

	const (
		exchange = "binance"
		market   = "future"
		symbol   = "ADA/USDT"
		tf       = "2h"
	)
	ts := int64(1_700_000_000_000)
	ce := NewCandleEngine(map[string]int{"default": 16})

	// The live stream delivered first — which the boot ordering GUARANTEES — so the ring holds
	// a bar whose Open is the price of the first trade this process saw.
	ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
		Timestamp: ts, Open: 0.1928, High: 0.1930, Low: 0.1925, Close: 0.1929,
		Volume: 400, TakerBuyVolume: 250, TakerSellVolume: 150,
		Source: string(redisstore.CandleMergeSourceWSDelta),
	})

	// The exchange's total for the same, still-open bucket. Its Open is the bucket's REAL
	// open — the first trade of the bucket, which happened before this process existed.
	ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
		Timestamp: ts, Open: 0.1901, High: 0.1935, Low: 0.1899, Close: 0.1929,
		Volume: 1000, TakerBuyVolume: 600, TakerSellVolume: 400,
		Source: string(redisstore.CandleMergeSourceRESTTotal),
	})

	got, ok := ce.PeekLatestCandle(exchange, market, symbol, tf)
	if !ok {
		t.Fatal("bucket vanished")
	}
	if got.Open != 0.1901 {
		t.Errorf("Open = %v, want 0.1901 — the exchange's total carries the bucket's real open, "+
			"and every other field of this merge already defers to a declared total. Keeping "+
			"the live partial's open publishes the price at which THIS PROCESS started "+
			"watching as though it were the price the bucket opened at", got.Open)
	}
	if got.Volume != 1000 {
		t.Errorf("Volume = %v, want 1000 — the volume rule must not regress", got.Volume)
	}
}

// TestOpen_LiveDeltaNeverMovesTheOpen is the other half, and it is what stops the fix from
// becoming a worse bug. Within a bucket the FIRST trade sets the open; every increment after
// it must leave it alone. Only a declared total may restate it.
func TestOpen_LiveDeltaNeverMovesTheOpen(t *testing.T) {
	t.Parallel()

	const (
		exchange = "binance"
		market   = "future"
		symbol   = "BTC/USDT"
		tf       = "5m"
	)
	ts := int64(1_700_000_000_000)
	ce := NewCandleEngine(map[string]int{"default": 16})

	ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
		Timestamp: ts, Open: 63000, High: 63010, Low: 62990, Close: 63005, Volume: 10,
		Source: string(redisstore.CandleMergeSourceWSDelta),
	})
	for _, px := range []float64{63100, 62900} {
		ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
			Timestamp: ts, Open: px, High: px, Low: px, Close: px, Volume: 5,
			Source: string(redisstore.CandleMergeSourceWSDelta),
		})
	}

	got, _ := ce.PeekLatestCandle(exchange, market, symbol, tf)
	if got.Open != 63000 {
		t.Errorf("Open = %v, want 63000 — an increment may never restate the open; within a "+
			"bucket the first trade sets it", got.Open)
	}
}

// TestOpen_UnlabelledPayloadDoesNotRestateTheOpen keeps provenance load-bearing here too. An
// unlabelled candle has unknown origin — a warm boot replays our own persisted data — and may
// not claim to carry the bucket's open.
func TestOpen_UnlabelledPayloadDoesNotRestateTheOpen(t *testing.T) {
	t.Parallel()

	const (
		exchange = "binance"
		market   = "future"
		symbol   = "SOL/USDT"
		tf       = "5m"
	)
	ts := int64(1_700_000_000_000)
	ce := NewCandleEngine(map[string]int{"default": 16})

	ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
		Timestamp: ts, Open: 73.05, High: 73.1, Low: 73.0, Close: 73.08, Volume: 10,
		Source: string(redisstore.CandleMergeSourceWSDelta),
	})
	ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
		Timestamp: ts, Open: 72.50, High: 73.2, Low: 72.4, Close: 73.09, Volume: 20,
	})

	got, _ := ce.PeekLatestCandle(exchange, market, symbol, tf)
	if got.Open != 73.05 {
		t.Errorf("Open = %v, want 73.05 — unknown provenance may not restate the open", got.Open)
	}
}

// TestOpen_LateAuthoritativeTotalCorrectsAClosedBucket covers the other merge site. A bucket
// that already rolled with the wrong open must still converge when the exchange's record
// arrives — otherwise the defect is merely delayed, not fixed, and every repair pass leaves
// the wrong price in place.
func TestOpen_LateAuthoritativeTotalCorrectsAClosedBucket(t *testing.T) {
	t.Parallel()

	const (
		exchange = "binance"
		market   = "future"
		symbol   = "ETH/USDT"
		tf       = "5m"
	)
	step := int64(5 * 60 * 1000)
	base := time.Now().UnixMilli()/step*step - 20*step
	ce := NewCandleEngine(map[string]int{"default": 16})

	ce.SeedCandles(exchange, market, symbol, tf, []candle.Candle{{
		Timestamp: base, Open: 1853.41, High: 1855, Low: 1852, Close: 1854,
		Volume: 400, TakerBuyVolume: 250, TakerSellVolume: 150,
	}})
	ce.SeedCandles(exchange, market, symbol, tf, []candle.Candle{{
		Timestamp: base, Open: 1850.02, High: 1856, Low: 1849, Close: 1854,
		Volume: 1000, TakerBuyVolume: 600, TakerSellVolume: 400,
		Source: string(redisstore.CandleMergeSourceRESTTotal),
	}})

	got := candleAt(ce, exchange, market, symbol, tf, base)
	if got == nil {
		t.Fatal("bucket vanished")
	}
	if got.Open != 1850.02 {
		t.Errorf("Open = %v, want 1850.02 — a closed bucket must converge to the exchange's "+
			"open like every other field, or the wrong price survives every repair", got.Open)
	}
}
