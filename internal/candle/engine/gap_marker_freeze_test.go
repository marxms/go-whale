package engine

import (
	"testing"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
)

// TestSeedCurrentBucket_LiveDeltasKeepAccumulatingAfterASnapshot is the guard for a bucket
// that stops counting the moment gap recovery touches it.
//
// # THE DEFECT
//
// SeedCurrentBucketCandle marks the bucket gap-recovered UNCONDITIONALLY, outside the
// switch, and the marker is only cleared when the NEXT bucket opens. If that marker forced the
// MAX branch for every ws_delta reaching UpdateCandle, a delta — a small increment — would
// contribute exactly nothing. A 5m bucket touched by gap recovery ten seconds in would be frozen
// at the REST snapshot for the remaining four minutes and fifty seconds. A WS reconnect mid-bar
// does it too, and Binance forces one every 24 hours per stream.
//
// # WHY PROVENANCE IS THE RIGHT GATE
//
// The marker exists to stop a bucket TOTAL being re-summed during a gap-recovery retry storm.
// A blanket MAX was the only tool available before the merge branched on declared source. It is
// no longer:
//
//   - a repeated snapshot is already MAX-merged by SeedCurrentBucketCandle's own path;
//   - a REST total arriving through UpdateCandle is already MAX-merged by declaredTotal;
//   - an UNLABELLED candle keeps the marker's conservative MAX, because unknown provenance
//     may not be assumed to be an increment.
//
// What is left is the one case the marker was never meant to cover: a payload that
// EXPLICITLY declares itself an increment. The live path labels every one of them ws_delta,
// and late_update.go already states the governing rule — branch by the declared source,
// never by magnitude. An explicit increment is an increment regardless of what happened to
// the bucket earlier.
func TestSeedCurrentBucket_LiveDeltasKeepAccumulatingAfterASnapshot(t *testing.T) {
	t.Parallel()

	const (
		exchange = "binance"
		market   = "future"
		symbol   = "ETH/USDT"
		tf       = "5m"
	)
	ts := int64(1_700_000_000_000)
	ce := NewCandleEngine(map[string]int{"default": 16})

	// The live stream has already opened the bucket. This matters: on an EMPTY buffer
	// SeedCurrentBucketCandle returns early and never writes the marker at all, so a test
	// that seeds first measures a different code path than the one in question.
	ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
		Timestamp: ts, Open: 100, High: 101, Low: 99, Close: 100,
		Volume: 400, TakerBuyVolume: 250, TakerSellVolume: 150,
		Source: string(redisstore.CandleMergeSourceWSDelta),
	})

	// Gap recovery delivers the exchange's snapshot for the still-open bucket.
	ce.SeedCurrentBucketCandle(exchange, market, symbol, tf, candle.Candle{
		Timestamp: ts, Open: 100, High: 101, Low: 99, Close: 100,
		Volume: 1000, TakerBuyVolume: 600, TakerSellVolume: 400,
		Source: string(redisstore.CandleMergeSourceRESTTotal),
	})

	// The bar keeps trading for the rest of its window. Four explicitly declared increments.
	for range 4 {
		ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
			Timestamp: ts, Open: 100, High: 102, Low: 98, Close: 101,
			Volume: 50, TakerBuyVolume: 30, TakerSellVolume: 20,
			Source: string(redisstore.CandleMergeSourceWSDelta),
		})
	}

	got, ok := ce.PeekLatestCandle(exchange, market, symbol, tf)
	if !ok {
		t.Fatal("bucket vanished")
	}
	if got.Volume != 1200 {
		t.Errorf("Volume = %v, want 1200 (snapshot 1000 + four declared increments of 50) — "+
			"the gap-recovered marker converted every later delta to a MAX, so the bucket "+
			"froze at the snapshot for the rest of its window. A 5m bar touched ten seconds "+
			"in stops counting for four minutes and fifty seconds", got.Volume)
	}
	if got.TakerBuyVolume != 720 || got.TakerSellVolume != 480 {
		t.Errorf("taker pair = %v/%v, want 720/480 — order flow froze with the volume",
			got.TakerBuyVolume, got.TakerSellVolume)
	}
}

// TestSeedCurrentBucket_UnlabelledUpdatesStillMaxAfterASnapshot pins the conservative half.
// A payload with no declared provenance might be a total, and treating it as an increment
// would re-sum a whole bucket onto itself. Unknown provenance keeps the marker's MAX.
func TestSeedCurrentBucket_UnlabelledUpdatesStillMaxAfterASnapshot(t *testing.T) {
	t.Parallel()

	const (
		exchange = "binance"
		market   = "future"
		symbol   = "COPPER/USDT"
		tf       = "1h"
	)
	ts := int64(1_700_000_000_000)
	ce := NewCandleEngine(map[string]int{"default": 16})

	// Populated first, for the same reason as above.
	ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
		Timestamp: ts, Open: 6.5, High: 6.6, Low: 6.5, Close: 6.55, Volume: 60_000,
	})
	ce.SeedCurrentBucketCandle(exchange, market, symbol, tf, candle.Candle{
		Timestamp: ts, Open: 6.5, High: 6.6, Low: 6.5, Close: 6.55, Volume: 102_711,
	})
	for range 10 {
		// No Source at all — this is the shape the storm arrived in.
		ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
			Timestamp: ts, Open: 6.5, High: 6.6, Low: 6.5, Close: 6.55, Volume: 102_711,
		})
	}

	got, _ := ce.PeekLatestCandle(exchange, market, symbol, tf)
	if got.Volume > 102_711*1.5 {
		t.Errorf("Volume = %v — an UNLABELLED payload was accumulated after a snapshot. That "+
			"is the COPPER/USDT 61x inflation: unknown provenance may be a whole bucket "+
			"total, and re-summing it onto itself is exactly what the marker exists to stop",
			got.Volume)
	}
}
