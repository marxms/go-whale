package candle

import "testing"

// 1d AND 1w BUCKET BOUNDARIES MUST BE BINANCE'S OWN.
//
// Normal feed observation cannot grade these two timeframes: no 1d bucket closes quickly, and 1w
// needs a week of uptime. If our boundary differs from the venue's, EVERY 1d/1w candle is wrong,
// and the mirror would reveal it only after that week elapsed. Calendar time hides this class, so
// it is pinned arithmetically.
//
// Expected values are Binance USDⓈ-M futures openTime, read from
// fapi.binance.com/fapi/v1/klines.
func TestBucketTimestampMatchesBinanceBoundaries(t *testing.T) {
	// A moment well inside Thursday 2026-08-06 UTC.
	const thursday = int64(1786005000000) // 2026-08-06 08:30:00 UTC

	cases := []struct {
		tf   string
		want int64
		note string
	}{
		{"1d", 1785974400000, "2026-08-06 00:00 UTC Thu — Binance daily opens at UTC midnight"},
		{"1w", 1785715200000, "2026-08-03 00:00 UTC Mon — Binance weekly opens Monday, and the epoch was a Thursday"},
	}
	for _, c := range cases {
		if got := BucketTimestamp(thursday, c.tf); got != c.want {
			t.Errorf("BucketTimestamp(%d, %q) = %d, want %d (%s)", thursday, c.tf, got, c.want, c.note)
		}
	}

	// The Monday boundary itself must be a fixed point, and one millisecond before it must fall
	// into the PREVIOUS week. An off-by-one here shifts every weekly candle by seven days.
	const monday = int64(1785715200000)
	if got := BucketTimestamp(monday, "1w"); got != monday {
		t.Errorf("the weekly open is not a fixed point: %d -> %d", monday, got)
	}
	if got := BucketTimestamp(monday-1, "1w"); got != 1785110400000 {
		t.Errorf("1ms before the weekly open = %d, want the previous Monday 1785110400000", got)
	}
	// Same for the daily open.
	const midnight = int64(1785974400000)
	if got := BucketTimestamp(midnight, "1d"); got != midnight {
		t.Errorf("the daily open is not a fixed point: %d -> %d", midnight, got)
	}
	if got := BucketTimestamp(midnight-1, "1d"); got != 1785888000000 {
		t.Errorf("1ms before the daily open = %d, want the previous day 1785888000000", got)
	}
}
