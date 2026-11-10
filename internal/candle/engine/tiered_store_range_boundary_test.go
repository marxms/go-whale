package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// THE EXCLUSIVE HOT/COLD BOUNDARY ALSO APPLIES TO RANGE READS.
//
// `GetCandlesRange` receives the caller's ENTIRE range (`from, to`) together with
// `limit - len(filteredHot)` rows. The cold query is
// `timestamp >= from AND timestamp <= to ORDER BY timestamp DESC LIMIT n`, so the `n` rows it
// returns are the NEWEST in the range — exactly the region the hot tier already covers.
// `mergeCandles` deduplicates with the hot tier winning and the `n` rows die entirely: a range
// crossing the boundary does not lose one candle, it loses ALL the ones fetched from the cold tier.
//
// # THE CASE GEOMETRY IS PRODUCTION'S
//
// The cold tier holds the SAME buckets as the hot tier plus the older ones — both tiers receive the
// same candle on write. A cold fake with only buckets below the hot tier would hide the
// defect, because then there would be nothing to duplicate. The overlap is the condition for the
// defect and therefore it is the case's condition.
//
// The fake is `inclusiveColdStore`, faithful to the real SQL's `<=`.
func TestGetCandlesRange_ServesTheRequestedLimitAcrossTheHotColdBoundary(t *testing.T) {
	ctx := context.Background()

	// EXTERNAL LITERALS: the bucket width comes from the definition of `1h` and the epoch from the
	// calendar (2026-08-20T00:00Z), not from any function under test.
	const tfMs int64 = 3_600_000
	const oldestHotMs int64 = 1_787_184_000_000
	const hotCap = 800
	const coldDepth = 300 // buckets the cold tier holds BELOW the oldest hot bucket

	if got := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC).UnixMilli(); got != oldestHotMs {
		t.Fatalf("the bucket literal left the calendar: %d != %d", got, oldestHotMs)
	}
	if got := time.Hour.Milliseconds(); got != tfMs {
		t.Fatalf("the 1h bucket literal left the definition: %d != %d", got, tfMs)
	}

	// Bucket indices relative to `oldestHotMs`: the hot tier occupies [0, 799], the cold tier
	// [-300, 799].
	bucket := func(idx int64) time.Time { return time.UnixMilli(oldestHotMs + idx*tfMs) }

	for _, tc := range []struct {
		name    string
		fromIdx int64
		toIdx   int64
		limit   int
		want    int
	}{
		// CROSSES THE BOUNDARY. The hot tier covers the top of the range and the cold tier must complete it
		// BELOW; this is where the rows paid for from the cold tier died in the merge.
		{name: "crosses/ceiling-at-top-of-hot", fromIdx: -200, toIdx: 799, limit: 1000, want: 1000},
		// The ceiling INSIDE the hot tier: the hot tier enters partially and the cold boundary stops
		// being the tier's oldest candle and becomes the oldest of the SERVED RANGE.
		{name: "crosses/ceiling-inside-hot", fromIdx: -300, toIdx: 400, limit: 600, want: 600},

		// DOES NOT CROSS. Controls: the fix must not affect either one.
		{name: "does-not-cross/entirely-hot", fromIdx: 700, toIdx: 799, limit: 100, want: 100},
		{name: "does-not-cross/entirely-cold", fromIdx: -300, toIdx: -1, limit: 100, want: 100},
	} {
		t.Run(fmt.Sprintf("%s/limit=%d", tc.name, tc.limit), func(t *testing.T) {
			ts := newBoundaryTieredStore(t, hotCap)
			defer ts.cancel()

			const exchange, mt, sym, tf = "binance", "future", "BTC/USDT", "1h"

			for i := range hotCap {
				bar := candle.Candle{
					Timestamp: oldestHotMs + int64(i)*tfMs,
					Open:      100, High: 110, Low: 90, Close: 105, Volume: 1,
				}
				if err := ts.AddStreamingCandle(ctx, exchange, mt, sym, tf, bar); err != nil {
					t.Fatalf("AddStreamingCandle[%d]: %v", i, err)
				}
			}

			// The cold tier holds the SAME window as the hot tier plus `coldDepth` buckets below: it is the
			// state the write to both tiers produces, and the overlap is what makes the
			// duplicate exist.
			coldRows := make([]candle.Candle, 0, coldDepth+hotCap)
			for k := -int64(coldDepth); k < hotCap; k++ {
				coldRows = append(coldRows, candle.Candle{
					Timestamp: oldestHotMs + k*tfMs,
					Open:      100, High: 110, Low: 90, Close: 105, Volume: 1,
				})
			}
			ts.SetColdStore(&inclusiveColdStore{candles: coldRows})

			got, err := ts.GetCandlesRange(ctx, exchange, mt, sym, tf, bucket(tc.fromIdx), bucket(tc.toIdx), tc.limit)
			if err != nil {
				t.Fatalf("GetCandlesRange: %v", err)
			}
			if len(got) != tc.want {
				t.Fatalf("range [%d, %d] with limit=%d served %d candles, want %d",
					tc.fromIdx, tc.toIdx, tc.limit, len(got), tc.want)
			}

			// AND WHAT COMES BACK IS A SERIES, not a padding: no repeated bucket,
			// strictly decreasing and entirely inside the requested range. A fix
			// that merely asked the cold tier for more rows would get the count right while returning
			// duplicates.
			seen := make(map[int64]struct{}, len(got))
			for i, c := range got {
				if _, dup := seen[c.Timestamp]; dup {
					t.Fatalf("bucket %d repeated at position %d", c.Timestamp, i)
				}
				seen[c.Timestamp] = struct{}{}
				if i > 0 && got[i-1].Timestamp <= c.Timestamp {
					t.Fatalf("order broken at %d: %d is not greater than %d", i, got[i-1].Timestamp, c.Timestamp)
				}
				if c.Timestamp < bucket(tc.fromIdx).UnixMilli() || c.Timestamp > bucket(tc.toIdx).UnixMilli() {
					t.Fatalf("bucket %d at position %d is outside the range [%d, %d]",
						c.Timestamp, i, bucket(tc.fromIdx).UnixMilli(), bucket(tc.toIdx).UnixMilli())
				}
			}
		})
	}
}
