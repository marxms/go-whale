package ingestion

import (
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

// TestBuildOHLCVCPooled_ConsumesResult proves the pooled wrapper yields the same
// aggregation as the allocating candle.BuildOHLCVC for the consumer callback.
func TestBuildOHLCVCPooled_ConsumesResult(t *testing.T) {
	trades := []candle.Trade{
		{Timestamp: 60_000, Price: 100, Amount: 1, Side: "buy"},
		{Timestamp: 65_000, Price: 110, Amount: 2, Side: "sell"},
	}
	want := candle.BuildOHLCVC(trades, "1m")

	got := make(map[int64]candle.BucketedCandle)
	newOHLCVCPool().build(trades, "1m", func(result map[int64]candle.BucketedCandle) {
		for ts, c := range result {
			got[ts] = c // copy out before the pool reclaims the map
		}
	})

	if len(got) != len(want) {
		t.Fatalf("bucket count: got %d want %d", len(got), len(want))
	}
	for ts, w := range want {
		if got[ts] != w {
			t.Fatalf("bucket %d: got %+v want %+v", ts, got[ts], w)
		}
	}
}

// TestBuildOHLCVCPooled_PanicDiscardsDirtyMap proves the recover guard: when the
// consumer panics, the panic propagates AND the dirty (populated) maps are NOT
// returned to the pool. A subsequent call therefore starts from a clean result
// map — no contamination from the aborted call survives.
func TestBuildOHLCVCPooled_PanicDiscardsDirtyMap(t *testing.T) {
	dirty := []candle.Trade{
		{Timestamp: 60_000, Price: 100, Amount: 7, Side: "buy"},
		{Timestamp: 600_000, Price: 100, Amount: 7, Side: "sell"}, // second bucket
	}

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic to propagate from the consumer")
			}
		}()
		newOHLCVCPool().build(dirty, "1m", func(result map[int64]candle.BucketedCandle) {
			if len(result) == 0 {
				t.Fatal("expected a populated result before panic")
			}
			panic("consumer blew up mid-flush")
		})
	}()

	// Next call on a single-bucket trade set must see exactly one bucket — if the
	// dirty two-bucket map had been recycled without clearing, we'd see stale keys.
	clean := []candle.Trade{
		{Timestamp: 120_000, Price: 200, Amount: 3, Side: "buy"},
	}
	seen := 0
	newOHLCVCPool().build(clean, "1m", func(result map[int64]candle.BucketedCandle) {
		seen = len(result)
	})
	if seen != 1 {
		t.Fatalf("post-panic call saw %d buckets, want 1 (dirty map leaked into pool)", seen)
	}
}
