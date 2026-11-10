package engine

import (
	"testing"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
)

// The boot open bucket is assembled from TWO sources and the engine stitches them with a MAX
// on a declared bucket total. These tests state the arithmetic that stitch depends on, because
// the boot ordering fix (app.awaitSymbolDelivery) is only correct while it holds.
//
// One 5m bucket, one unit of volume per second, so the exchange's truth for the closed bucket
// is 300 and every deficit below reads directly as seconds of missing trades.
const (
	stitchBucketTs = int64(1_700_000_100_000)
	stitchTruth    = 300.0
)

func liveDelta(volume float64) candle.Candle {
	return candle.Candle{
		Timestamp: stitchBucketTs,
		Open:      100, High: 100, Low: 100, Close: 100,
		Volume:         volume,
		TakerBuyVolume: volume,
		Source:         string(redisstore.CandleMergeSourceWSDelta),
	}
}

func restTotal(volume float64) candle.Candle {
	return candle.Candle{
		Timestamp: stitchBucketTs,
		Open:      100, High: 100, Low: 100, Close: 100,
		Volume:         volume,
		TakerBuyVolume: volume,
		Source:         string(redisstore.CandleMergeSourceRESTTotal),
	}
}

func openVolume(t *testing.T, ce *CandleEngine) float64 {
	t.Helper()
	c, ok := ce.PeekLatestCandle("binance", "future", "ETH/USDT", "5m")
	if !ok {
		t.Fatal("no open bucket in the buffer")
	}
	return c.Volume
}

// TestSnapshotBeforeDeliveryLeavesAWindowNoSourceCovers reproduces the production defect in
// arithmetic, so the reason for the ordering gate is written down where it can be re-run.
//
// The REST snapshot covers [bucket start, T_fetch]; the live stream covers [T_deliver, ...].
// Seeding before the stream delivers puts T_fetch BEFORE T_deliver, so the two windows are
// DISJOINT. MAX cannot invent the trades between them, and the accumulation that follows
// starts from a floor that never contained them.
//
// Here: the snapshot lands at second 100 (volume 100) and the stream starts delivering at
// second 180. Seconds 100–180 are in neither source. The bucket closes at 220 against a truth
// of 300 — 26.7% short.
func TestSnapshotBeforeDeliveryLeavesAWindowNoSourceCovers(t *testing.T) {
	t.Parallel()

	ce := NewCandleEngine(map[string]int{"default": 10})

	// Seed first: the exchange's total as of second 100.
	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", restTotal(100))
	// The stream only starts delivering at second 180; seconds 180..300 arrive as deltas.
	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", liveDelta(120))

	got := openVolume(t, ce)
	if got != 220 {
		t.Fatalf("volume = %v, want 220 — this test documents the DEFECT; if the number moved, "+
			"the merge semantics the boot gate relies on changed and the gate must be re-derived", got)
	}
	if deficit := stitchTruth - got; deficit != 80 {
		t.Fatalf("deficit = %v, want 80 = the 80 seconds between the snapshot and first delivery", deficit)
	}
}

// TestDeliveryBeforeSnapshotStitchesWithoutAHole is the fixed ordering, and it is what
// app.awaitSymbolDelivery buys.
//
// Delivery starts at second 180, so by the time the snapshot is taken at second 200 the live
// partial already holds [180,200). The REST total for [0,200) OVERLAPS it, the MAX elects the
// total (which subsumes the overlap rather than adding it), and the deltas that follow
// accumulate on top. Nothing is lost and nothing is double-counted: 300, exactly the
// exchange's truth.
func TestDeliveryBeforeSnapshotStitchesWithoutAHole(t *testing.T) {
	t.Parallel()

	ce := NewCandleEngine(map[string]int{"default": 10})

	// Stream delivering from second 180; 20 seconds accumulate before the snapshot is applied.
	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", liveDelta(20))
	// Snapshot taken at second 200 — it covers the whole bucket so far, INCLUDING the overlap.
	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", restTotal(200))
	// The rest of the bucket arrives live.
	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", liveDelta(100))

	if got := openVolume(t, ce); got != stitchTruth {
		t.Fatalf("volume = %v, want %v — with overlapping windows the stitch must be exact: "+
			"a MAX that under-counts would leave the boot hole open, and one that accumulated "+
			"the overlap would inflate the bucket permanently (the +82,5%% ETH regression)",
			got, stitchTruth)
	}
}

// TestOverlapIsNeverDoubleCounted pins the other side of the same rule. The declared total must
// REPLACE-by-MAX rather than accumulate; if it ever accumulates, the fixed ordering turns from
// a fix into the inflation defect, because after the gate the overlap is no longer empty.
func TestOverlapIsNeverDoubleCounted(t *testing.T) {
	t.Parallel()

	ce := NewCandleEngine(map[string]int{"default": 10})

	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", liveDelta(190))
	ce.UpdateCandle("binance", "future", "ETH/USDT", "5m", restTotal(200))

	if got := openVolume(t, ce); got != 200 {
		t.Fatalf("volume = %v, want 200 — the total subsumes the live partial it overlaps; "+
			"390 would mean the ordering fix now double-counts the overlap it deliberately creates", got)
	}
}
