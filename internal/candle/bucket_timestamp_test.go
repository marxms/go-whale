package candle

import "testing"

// TestBucketTimestamp locks the canonical bucket-boundary contract that the live
// aggregator uses, so gap-recovery / seeder / gapfiller can delegate to it instead
// of their own `(t/tfMs)*tfMs` math (Thursday-epoch-aligned), which misclassifies the
// 1w bucket by 72h and would leak the OPEN weekly candle into the cold tier as a
// partial "closed" one.
//
// Reference values (UTC): 2024-01-01 00:00:00 is a Monday = 1704067200000 ms.
func TestBucketTimestamp(t *testing.T) {
	const (
		mon0101 = int64(1704067200000) // 2024-01-01 00:00:00 Mon (week + day + hour aligned)
		mon0108 = int64(1704672000000) // 2024-01-08 00:00:00 Mon (next week)
	)
	tests := []struct {
		name string
		ts   int64
		tf   string
		want int64
	}{
		{"5m floors to bucket start", 1704067400000 /*00:03:20*/, "5m", mon0101},
		{"5m exact boundary is itself", mon0101, "5m", mon0101},
		{"15m floors", 1704067200000 + 800000 /*00:13:20*/, "15m", mon0101},
		{"1h floors", 1704069000000 /*00:30:00*/, "1h", mon0101},
		{"1d floors", 1704117200000 /*~13:53*/, "1d", mon0101},
		{"1w Monday maps to itself", mon0101, "1w", mon0101},
		{"1w Wednesday maps back to Monday", 1704283200000 /*Wed 12:00*/, "1w", mon0101},
		{"1w Sunday end-of-week maps to Monday", 1704671999000 /*Sun 23:59:59*/, "1w", mon0101},
		{"1w next Monday rolls forward", mon0108, "1w", mon0108},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BucketTimestamp(tt.ts, tt.tf)
			if got != tt.want {
				t.Fatalf("BucketTimestamp(%d, %q) = %d, want %d (Δ=%d ms)", tt.ts, tt.tf, got, tt.want, got-tt.want)
			}
		})
	}
}
