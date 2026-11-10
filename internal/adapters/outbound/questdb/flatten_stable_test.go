package questdb

import (
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

// TestFlattenSorted_KeepsEnqueueOrderWithinABucket guards the last few meters of the
// late-correction path, where an unstable sort could silently undo it.
//
// # THE DEFECT
//
// A late trade arriving after a bucket rolled makes the engine repair the bucket and publish
// a revision. Both the original close and that revision are enqueued for the SAME key and
// the SAME timestamp, and when they land in the same sweep the pipeline flattens them into
// one row list ordered by timestamp:
//
//	sort.Slice(rows, func(i, j int) bool { return rows[i].candle.Timestamp < rows[j].candle.Timestamp })
//
// sort.Slice is NOT stable. For two rows comparing equal — which close and revision always
// do, having the same timestamp — the resulting order is unspecified. The table dedups on
// the key, so whichever is written LAST wins. Half the time that is the stale close, and the
// correction the engine worked out is thrown away at the final hop.
//
// This is the same loss as the correction that never left RAM, one layer further down: the
// engine is right, the revision is published, the sink receives it, and the row still ends
// up holding the pre-correction number.
//
// # WHY STABILITY IS THE WHOLE FIX
//
// Enqueue order already IS the correct order — a revision is enqueued after the close it
// corrects, by construction, because it cannot exist before it. A stable sort preserves that
// for equal timestamps while still giving QuestDB the ascending order it needs to take the
// cheap append path instead of an O3 partition merge.
func TestFlattenSorted_KeepsEnqueueOrderWithinABucket(t *testing.T) {
	t.Parallel()

	key := mustSeriesKey("binance", "future", "BTC/USDT", "5m")
	const ts = int64(1_700_000_000_000)

	// The shape production measured on BTC/USDT 5m bucket 01:05: the close published at roll
	// time, then the repair once the late trade arrived.
	closed := candle.Candle{
		Timestamp: ts, Close: 100, Volume: 279.931,
		TakerBuyVolume: 187.849, TakerSellVolume: 92.082,
	}
	revised := candle.Candle{
		Timestamp: ts, Close: 100, Volume: 279.983,
		TakerBuyVolume: 187.849, TakerSellVolume: 92.134,
	}

	// THE SHAPE PRODUCTION ACTUALLY PRODUCES, and the reason this is not a theoretical
	// concern. A sweep spans several buckets, and at each boundary EVERY symbol closes at
	// the same instant — so the list holds a few hundred rows per timestamp across a handful
	// of distinct timestamps. That is the distribution pdqsort partitions, and measured on
	// it the close/revision pair inverts in every bucket, every run. (With a single
	// timestamp it does not: the all-equal case short-circuits, which is why a smaller
	// fixture proves nothing here.)
	groups := map[candle.SeriesKey][]candle.Candle{key: {closed, revised}}
	for i := range 300 {
		other := mustSeriesKey("binance", "future", string(rune('A'+i%26))+string(rune('A'+i/26))+"/USDT", "5m")
		batch := make([]candle.Candle, 0, 5)
		for b := range 5 {
			batch = append(batch, candle.Candle{Timestamp: ts + int64(b)*300_000, Volume: float64(i)})
		}
		groups[other] = batch
	}

	rows := flattenSorted(groups)

	var seenClose bool
	for _, r := range rows {
		if r.candle.Timestamp != ts {
			continue
		}
		switch r.candle.Volume {
		case closed.Volume:
			if seenClose {
				t.Fatal("the close appeared twice")
			}
			seenClose = true
		case revised.Volume:
			if !seenClose {
				t.Error("the REVISION was ordered before the close it corrects. QuestDB dedups " +
					"on the key, so the last write wins — writing them in this order lets the " +
					"stale close overwrite the correction, and the repaired value is lost at " +
					"the final hop after surviving every earlier one")
			}
		}
	}
	if !seenClose {
		t.Fatal("neither row for the bucket reached the output")
	}
}

// TestFlattenSorted_StillAscendingAcrossKeys keeps the property the sort exists for. Rows
// from every group must come out timestamp-ascending, or QuestDB answers the zig-zag with an
// O3 partition merge instead of the cheap append.
func TestFlattenSorted_StillAscendingAcrossKeys(t *testing.T) {
	t.Parallel()

	const base = int64(1_700_000_000_000)
	groups := map[candle.SeriesKey][]candle.Candle{
		mustSeriesKey("binance", "future", "BTC/USDT", "5m"): {
			{Timestamp: base}, {Timestamp: base + 600_000}, {Timestamp: base + 1_200_000},
		},
		mustSeriesKey("binance", "future", "ETH/USDT", "5m"): {
			{Timestamp: base + 300_000}, {Timestamp: base + 900_000},
		},
	}

	rows := flattenSorted(groups)
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}
	for i := 1; i < len(rows); i++ {
		if rows[i].candle.Timestamp < rows[i-1].candle.Timestamp {
			t.Fatalf("row %d (ts=%d) precedes ts=%d — the sweep must be ascending or QuestDB "+
				"rewrites the partition", i, rows[i].candle.Timestamp, rows[i-1].candle.Timestamp)
		}
	}
}
