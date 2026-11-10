package engine

import (
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

// TestSeedCurrentBucket_DoesNotEraseTheGapMarker guards the bookkeeping fields through the open
// bucket merge.
//
// SeedCurrentBucketCandle must not rebuild the open bucket from a literal that omits HasGap,
// SideUnknownCount, LateTradeCount and AggregateMethod: every REST snapshot of the open bucket
// would zero them, while the sibling literal in UpdateCandle carries all four.
//
// HasGap is raised by the ingest path when the exchange's trade ids come up short. A bucket that
// lost trades could otherwise be laundered clean by a later snapshot and close claiming to be
// complete — the repair would never fire, and the fix would look implemented while doing nothing.
func TestSeedCurrentBucket_DoesNotEraseTheGapMarker(t *testing.T) {
	t.Parallel()

	ce := NewCandleEngine(map[string]int{"default": 16})
	const ts = int64(1_785_728_700_000)

	// The live path aggregates a bucket that is KNOWN to be short.
	ce.UpdateCandle("binance", "future", "XRP/USDT", "5m", candle.Candle{
		Timestamp: ts, Open: 1, High: 1, Low: 1, Close: 1,
		Volume: 100, TakerBuyVolume: 40, TakerSellVolume: 60,
		SideUnknownCount: 3, HasGap: true, Source: "ws_delta",
	})

	// A REST snapshot of the SAME open bucket arrives — the routine gap-recovery re-read.
	ce.SeedCurrentBucketCandle("binance", "future", "XRP/USDT", "5m", candle.Candle{
		Timestamp: ts, Open: 1, High: 1, Low: 1, Close: 1,
		Volume: 120, TakerBuyVolume: 48, TakerSellVolume: 72, Source: "rest_partial",
	})

	got, ok := ce.PeekLatestCandle("binance", "future", "XRP/USDT", "5m")
	if !ok {
		t.Fatal("no candle in the ring")
	}
	if !got.HasGap {
		t.Error("HasGap was erased by a REST snapshot of the open bucket — the bucket will close " +
			"claiming to be complete and the repair will never be requested")
	}
	if got.SideUnknownCount != 3 {
		t.Errorf("SideUnknownCount = %d, want 3 — the only evidence of volume seen without an "+
			"attributable side, destroyed by a payload that has nothing to say about it",
			got.SideUnknownCount)
	}
	if got.Volume != 120 {
		t.Errorf("Volume = %v, want 120 — the MAX merge must be unchanged", got.Volume)
	}
}
