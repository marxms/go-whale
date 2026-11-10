package redisstore

import (
	"fmt"
	"strconv"
	"testing"
)

// TestFormatTimestampScoreEqualsSprintf proves the substitution is neutral:
// strconv.FormatInt(ts, 10) produces the exact same Redis score/key string as
// fmt.Sprintf("%d", ts) for every representative candle timestamp.
// Candle timestamps are non-negative ms-since-epoch; 0, a typical recent ms
// value, and a far-future large value bracket the range.
func TestFormatTimestampScoreEqualsSprintf(t *testing.T) {
	cases := []struct {
		name string
		ts   int64
	}{
		{name: "zero", ts: 0},
		{name: "small", ts: 60000},
		{name: "typical_recent_ms", ts: 1_749_800_000_000},
		{name: "large", ts: 9_223_372_036_854_775_807}, // math.MaxInt64
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := fmt.Sprintf("%d", tc.ts)
			got := strconv.FormatInt(tc.ts, 10)
			if got != want {
				t.Fatalf("strconv.FormatInt(%d) = %q, want %q", tc.ts, got, want)
			}
		})
	}
}

// BenchmarkTimestampScoreFormat documents the alloc win on the per-candle
// write path: fmt.Sprintf("%d", ts) uses reflection and allocates, while
// strconv.FormatInt does not (small ints hit the runtime's static string set).
func BenchmarkTimestampScoreFormat(b *testing.B) {
	const ts int64 = 1_749_800_000_000

	b.Run("fmt_Sprintf", func(b *testing.B) {
		b.ReportAllocs()
		var sink string
		for i := 0; i < b.N; i++ {
			sink = fmt.Sprintf("%d", ts)
		}
		_ = sink
	})

	b.Run("strconv_FormatInt", func(b *testing.B) {
		b.ReportAllocs()
		var sink string
		for i := 0; i < b.N; i++ {
			sink = strconv.FormatInt(ts, 10)
		}
		_ = sink
	})
}
