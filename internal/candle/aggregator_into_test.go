package candle

import (
	"reflect"
	"testing"
)

// d4Timeframes are the bucket sizes exercised for the BuildOHLCVCInto parity
// and pool-reuse checks.
func d4Timeframes() []string {
	return []string{"1m", "5m", "15m", "1h"}
}

// d4MixedTrades spans multiple buckets and all three side classifications so
// the parity check covers buy/sell/unknown aggregation paths.
func d4MixedTrades() []Trade {
	return []Trade{
		{Timestamp: 60_000, Price: 100, Amount: 1, Side: "buy"},
		{Timestamp: 65_000, Price: 110, Amount: 2, Side: "sell"},
		{Timestamp: 70_000, Price: 95, Amount: 1, Side: ""},          // unknown
		{Timestamp: 305_000, Price: 105, Amount: 0.5, Side: "weird"}, // unknown, later bucket
		{Timestamp: 905_000, Price: 102, Amount: 3, Side: "BUY"},
		{Timestamp: 3_605_000, Price: 99, Amount: 4, Side: "Sell"},
	}
}

// TestBuildOHLCVCInto_MatchesBuildOHLCVC proves neutrality: writing into
// caller-provided maps yields a result identical to the allocating
// BuildOHLCVC, across all timeframes and buy/sell/unknown sides.
func TestBuildOHLCVCInto_MatchesBuildOHLCVC(t *testing.T) {
	trades := d4MixedTrades()

	for _, tf := range d4Timeframes() {
		tf := tf
		t.Run(tf, func(t *testing.T) {
			want := BuildOHLCVC(trades, tf)

			buckets := make(map[int64][]Trade)
			got := make(map[int64]BucketedCandle)
			BuildOHLCVCInto(trades, tf, buckets, got)

			if !reflect.DeepEqual(got, want) {
				t.Fatalf("BuildOHLCVCInto != BuildOHLCVC for %s:\n got =%+v\n want=%+v", tf, got, want)
			}
		})
	}
}

// TestBuildOHLCVCInto_PoolReuseNoCrossCallState proves the pool cycle is safe:
// after clear()-ing both maps (the production return-to-pool step), a second
// BuildOHLCVCInto on a DIFFERENT trade set produces exactly the same result as
// a fresh BuildOHLCVC, i.e. no entries from the first call leak into the second.
func TestBuildOHLCVCInto_PoolReuseNoCrossCallState(t *testing.T) {
	const tf = "1m"

	first := []Trade{
		{Timestamp: 60_000, Price: 100, Amount: 1, Side: "buy"},
		{Timestamp: 65_000, Price: 110, Amount: 2, Side: "sell"},
	}
	second := []Trade{
		{Timestamp: 600_000, Price: 200, Amount: 5, Side: "sell"},
	}

	buckets := make(map[int64][]Trade)
	result := make(map[int64]BucketedCandle)

	// First use.
	BuildOHLCVCInto(first, tf, buckets, result)
	wantFirst := BuildOHLCVC(first, tf)
	if !reflect.DeepEqual(result, wantFirst) {
		t.Fatalf("first use mismatch:\n got =%+v\n want=%+v", result, wantFirst)
	}

	// Return-to-pool: clear (NOT realloc) both maps.
	clear(buckets)
	clear(result)
	if len(buckets) != 0 || len(result) != 0 {
		t.Fatalf("clear() left state: buckets=%d result=%d", len(buckets), len(result))
	}

	// Second use on the SAME (reused) maps must not see first-call entries.
	BuildOHLCVCInto(second, tf, buckets, result)
	wantSecond := BuildOHLCVC(second, tf)
	if !reflect.DeepEqual(result, wantSecond) {
		t.Fatalf("reuse contaminated result:\n got =%+v\n want=%+v", result, wantSecond)
	}
}

// BenchmarkBuildOHLCVC documents the allocation cost: the allocating BuildOHLCVC
// builds two maps per call, whereas BuildOHLCVCInto reuses pooled maps and so
// allocates only the per-bucket trade slices (which append into reused storage
// across calls). The Into variant should report fewer allocs/op once warmed.
func BenchmarkBuildOHLCVC(b *testing.B) {
	trades := d4MixedTrades()
	const tf = "1m"

	b.Run("BuildOHLCVC_allocating", func(b *testing.B) {
		b.ReportAllocs()
		var sink map[int64]BucketedCandle
		for i := 0; i < b.N; i++ {
			sink = BuildOHLCVC(trades, tf)
		}
		_ = sink
	})

	b.Run("BuildOHLCVCInto_pooled", func(b *testing.B) {
		b.ReportAllocs()
		buckets := make(map[int64][]Trade)
		result := make(map[int64]BucketedCandle)
		for i := 0; i < b.N; i++ {
			BuildOHLCVCInto(trades, tf, buckets, result)
			clear(buckets)
			clear(result)
		}
	})
}
