package redisstore

import (
	"context"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

// TestMergeClosedCandleBySource_AuthoritativeTotalSettlesDownward is the Redis half of a
// rule the ring buffer already enforces, and without it the correction only reached one tier.
//
// # THE HALF-SETTLEMENT
//
// mergeClosedCandleBySource replaced EVERY field with the incoming record — open, close,
// taker pair, source, counters — except one:
//
//	Volume: maxFloatV2(current.Volume, incoming.Volume)
//
// So a closed bucket adopted the exchange's order flow while keeping OUR volume whenever
// ours was larger. That is the internally impossible bar the ring's own comment warns about:
// authority is per-record, not per-field, and settling half a bucket is worse than settling
// none of it. It also makes an inflation PERMANENT in the tier that /batch-prices reads —
// every later repair re-elected the larger stored number, which is precisely the ratchet
// that made a restart double-count survive on ETH/USDT.
//
// # WHY THE EXCHANGE'S NUMBER WINS, EVEN WHEN IT IS SMALLER
//
// For a bucket whose window has elapsed, the exchange's kline is not a competing candidate
// to take a maximum against — it IS the record. Our value is a reconstruction of it, and a
// reconstruction can be wrong in BOTH directions: short when we missed trades during a
// disconnect, long when we double-counted across a restart. Only replacement converges.
func TestMergeClosedCandleBySource_AuthoritativeTotalSettlesDownward(t *testing.T) {
	t.Parallel()

	now := int64(9_000_000)
	// What we hold: inflated by a restart double-count.
	current := candle.Candle{
		Timestamp: 1000, Open: 100, High: 130, Low: 90, Close: 120,
		Volume: 22436, TakerBuyVolume: 14000, TakerSellVolume: 8436,
	}
	// What the exchange says the bucket actually was.
	incoming := candle.Candle{
		Timestamp: 1000, Open: 100, High: 125, Low: 92, Close: 118,
		Volume: 12294, TakerBuyVolume: 7000, TakerSellVolume: 5294,
		Source: string(CandleMergeSourceRESTTotal),
	}

	merged := mergeClosedCandleBySource(current, incoming, CandleMergeSourceRESTTotal, now)

	if merged.Volume != 12294 {
		t.Errorf("Volume = %v, want 12294 — the exchange's record for a CLOSED bucket must "+
			"replace ours. Keeping the larger value adopts the exchange's order flow while "+
			"holding our volume, which is an internally inconsistent bar, and it makes a "+
			"double-count permanent because every later repair re-elects the inflated number",
			merged.Volume)
	}
	if merged.TakerBuyVolume+merged.TakerSellVolume > merged.Volume {
		t.Errorf("buy+sell (%v) exceeds volume (%v) — physically impossible, and it is the "+
			"half-settlement that produces it",
			merged.TakerBuyVolume+merged.TakerSellVolume, merged.Volume)
	}
}

// TestMergeClosedCandleBySource_DegradedTotalKeepsMax pins the guard on the guard, and it is
// what keeps the existing MAX behavior correct where it belongs.
//
// The seeder degrades to plain FetchOHLCV under rate limits, and that endpoint has no taker
// fields. Such a record claims a volume it cannot account for. Letting it settle would drop
// our volume to its number while leaving our measured pair in place — and if the volume
// moves down far enough, buy + sell > volume.
//
// A record that did not measure the order flow may not settle the volume either. It falls
// back to the union, which is conservative and consistent.
func TestMergeClosedCandleBySource_DegradedTotalKeepsMax(t *testing.T) {
	t.Parallel()

	now := int64(9_000_000)
	current := candle.Candle{
		Timestamp: 1000, Close: 105, Volume: 500,
		TakerBuyVolume: 300, TakerSellVolume: 200,
	}
	degraded := candle.Candle{
		Timestamp: 1000, Close: 106, Volume: 120,
		// no taker split at all — the rate-limited fallback shape
		Source: string(CandleMergeSourceRESTTotal),
	}

	merged := mergeClosedCandleBySource(current, degraded, CandleMergeSourceRESTTotal, now)

	if merged.Volume != 500 {
		t.Errorf("Volume = %v, want 500 — a total with no order flow cannot settle the bucket; "+
			"replacing here would strand our measured pair against a smaller volume", merged.Volume)
	}
	if merged.TakerBuyVolume != 300 || merged.TakerSellVolume != 200 {
		t.Errorf("taker pair = %v/%v, want 300/200 preserved — the degraded record measured "+
			"nothing to replace it with", merged.TakerBuyVolume, merged.TakerSellVolume)
	}
}

// TestMergeClosedCandleBySource_WSDeltaNeverSettles keeps provenance load-bearing. A
// ws_delta is an increment, never a bucket total, and it may not claim authority no matter
// what it carries.
func TestMergeClosedCandleBySource_WSDeltaNeverSettles(t *testing.T) {
	t.Parallel()

	now := int64(9_000_000)
	current := candle.Candle{Timestamp: 1000, Volume: 500, TakerBuyVolume: 300, TakerSellVolume: 200}
	incoming := candle.Candle{
		Timestamp: 1000, Volume: 120, TakerBuyVolume: 70, TakerSellVolume: 50,
		Source: string(CandleMergeSourceWSDelta),
	}

	merged := mergeClosedCandleBySource(current, incoming, CandleMergeSourceWSDelta, now)

	if merged.Volume != 500 {
		t.Errorf("Volume = %v, want 500 — a ws_delta is an increment and cannot settle a bucket",
			merged.Volume)
	}
}

// TestSeedCandlesBatch_ClosedBucketSettlesDownward is the same rule at the OTHER site that
// implements it. SeedCandlesBatch does not call mergeClosedCandleBySource — it has its own
// inline merge — so fixing only the shared helper would leave the seed path, which is the
// path that actually carries the exchange's repair batches, still ratcheting.
func TestSeedCandlesBatch_ClosedBucketSettlesDownward(t *testing.T) {
	t.Parallel()

	store, _ := newTestRedisStore(t)
	ctx := context.Background()
	ex, mt, sym, tf := "binance", "future", "ETH/USDT", "5m"
	ts := closed5mBucketTs()

	// What we hold after a restart double-count. Unlabelled, because it is our own
	// reconstruction, not a claim about the exchange.
	inflated := candle.Candle{
		Timestamp: ts, Open: 100, High: 130, Low: 90, Close: 120,
		Volume: 22436, TakerBuyVolume: 14000, TakerSellVolume: 8436,
	}
	if err := store.SeedCandlesBatch(ctx, ex, mt, sym, tf, []candle.Candle{inflated}); err != nil {
		t.Fatalf("seed inflated: %v", err)
	}

	// The exchange's own record for the same, long-closed bucket.
	authoritative := candle.Candle{
		Timestamp: ts, Open: 100, High: 125, Low: 92, Close: 118,
		Volume: 12294, TakerBuyVolume: 7000, TakerSellVolume: 5294,
		Source: string(CandleMergeSourceRESTTotal),
	}
	if err := store.SeedCandlesBatch(ctx, ex, mt, sym, tf, []candle.Candle{authoritative}); err != nil {
		t.Fatalf("seed authoritative: %v", err)
	}

	rows, err := store.GetCandlesDesc(ctx, ex, mt, sym, tf, 1)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 candle, got %d", len(rows))
	}
	got := rows[0]
	if got.Volume != 12294 {
		t.Errorf("Volume = %v, want 12294 — the seed path is where the exchange's repair "+
			"batches arrive; leaving MAX here means a bucket that drifted upward can never be "+
			"corrected in the tier /batch-prices reads", got.Volume)
	}
}
