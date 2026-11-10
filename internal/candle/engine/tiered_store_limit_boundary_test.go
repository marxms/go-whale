package engine

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"
)

// THE CANDLE ROUTE SERVES THE REQUESTED `limit` ACROSS THE WHOLE RANGE.
//
// The hot tier holds 800 candles. Above that `GetCandlesDesc` completes from the cold tier, and if the
// edge between the two is INCLUSIVE on both sides, every request above the cap comes back N-1:
//
//  1. `to` receives the timestamp of the OLDEST hot candle;
//  2. the cold query is `timestamp <= to` — inclusive;
//  3. so the FIRST row the cold tier returns is always the bucket the hot tier already has;
//  4. `mergeCandles` deduplicates by timestamp with the hot tier winning, and that row is
//     discarded.
//
// Of the `needed` rows paid for from the cold tier, one always dies in the merge.
//
// A shared fake that implements `to` as EXCLUSIVE models the opposite of production exactly at
// the edge where the defect lives, and the duplicate never appears. The local fake below pins
// fidelity AT THIS CASE'S DEPENDENCY POINT, so a refactor of the shared fake cannot silently erase
// the only thing that makes this test discriminate.
type inclusiveColdStore struct {
	candles []candle.Candle
}

func (m *inclusiveColdStore) WriteBatches(context.Context, map[candle.SeriesKey][]candle.Candle) error {
	return nil
}

// QueryRange returns [from, to] with BOTH bounds inclusive, ordered DESC and cut at
// `limit` — the semantics of `coldRangeQuery`.
func (m *inclusiveColdStore) QueryRange(_ context.Context, _, _, _, _ string, from, to time.Time, limit int) ([]candle.Candle, error) {
	inside := make([]candle.Candle, 0, len(m.candles))
	for _, c := range m.candles {
		ts := time.UnixMilli(c.Timestamp)
		if !from.IsZero() && ts.Before(from) {
			continue
		}
		if !to.IsZero() && ts.After(to) {
			continue
		}
		inside = append(inside, c)
	}
	sort.Slice(inside, func(i, j int) bool { return inside[i].Timestamp > inside[j].Timestamp })
	if limit > 0 && limit < len(inside) {
		inside = inside[:limit]
	}
	return inside, nil
}

func (m *inclusiveColdStore) GetOldestTimestamp(context.Context, string, string, string, string) (time.Time, error) {
	return time.Time{}, nil
}

func (m *inclusiveColdStore) Close() error                 { return nil }
func (m *inclusiveColdStore) Stats() candle.ColdStoreStats { return candle.ColdStoreStats{} }

func TestGetCandlesDesc_ServesTheRequestedLimitAcrossTheHotColdBoundary(t *testing.T) {
	ctx := context.Background()

	// EXTERNAL LITERALS: the bucket width comes from the definition of `1h` and the epoch from the
	// calendar (2026-08-20T00:00Z), not from any function under test.
	const tfMs int64 = 3_600_000
	const oldestHotMs int64 = 1_787_184_000_000
	const hotCap = 800

	if got := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC).UnixMilli(); got != oldestHotMs {
		t.Fatalf("the bucket literal left the calendar: %d != %d", got, oldestHotMs)
	}
	if got := time.Hour.Milliseconds(); got != tfMs {
		t.Fatalf("the 1h bucket literal left the definition: %d != %d", got, tfMs)
	}

	// THE SECOND DIMENSION KILLS THE WRONG FIX. Compensating by asking the cold tier for
	// `needed+1` rows gets the count right while the cold tier HAS the boundary bucket — and serves one candle
	// TOO MANY when it does not, which is the state of a lagging cold tier. Both columns require
	// exactly `limit` in both states, and only the exclusive boundary satisfies both.
	for _, tc := range []struct {
		limit           int
		coldHasBoundary bool
	}{
		// The hot cap edge: 800 is served whole, 801 must not come back as 800.
		{limit: 800, coldHasBoundary: true},
		{limit: 801, coldHasBoundary: true},
		{limit: 801, coldHasBoundary: false},
		// And well above it, where the cold tier serves hundreds.
		{limit: 999, coldHasBoundary: true},
		{limit: 999, coldHasBoundary: false},
		{limit: 1000, coldHasBoundary: true},
		{limit: 1000, coldHasBoundary: false},
	} {
		t.Run(fmt.Sprintf("limit=%d/boundary-in-cold=%v", tc.limit, tc.coldHasBoundary), func(t *testing.T) {
			ts := newBoundaryTieredStore(t, hotCap)
			defer ts.cancel()

			const exchange, mt, sym, tf = "binance", "future", "BTC/USDT", "1h"

			// The hot tier receives exactly `hotCap` buckets, the oldest at oldestHotMs.
			for i := range hotCap {
				bar := candle.Candle{
					Timestamp: oldestHotMs + int64(i)*tfMs,
					Open:      100, High: 110, Low: 90, Close: 105, Volume: 1,
				}
				if err := ts.AddStreamingCandle(ctx, exchange, mt, sym, tf, bar); err != nil {
					t.Fatalf("AddStreamingCandle[%d]: %v", i, err)
				}
			}

			// The cold tier holds 300 buckets BELOW the hot tier. The BOUNDARY bucket is present or
			// not depending on the case: in production it is there, because both tiers write the
			// same bucket; a lagging cold tier is the case where it is not.
			firstK := 1
			if tc.coldHasBoundary {
				firstK = 0
			}
			coldRows := make([]candle.Candle, 0, 301)
			for k := firstK; k <= 300; k++ {
				coldRows = append(coldRows, candle.Candle{
					Timestamp: oldestHotMs - int64(k)*tfMs,
					Open:      100, High: 110, Low: 90, Close: 105, Volume: 1,
				})
			}
			ts.SetColdStore(&inclusiveColdStore{candles: coldRows})

			got, err := ts.GetCandlesDesc(ctx, exchange, mt, sym, tf, tc.limit)
			if err != nil {
				t.Fatalf("GetCandlesDesc: %v", err)
			}
			// EXACTLY THE REQUEST: neither one fewer (the inclusive boundary ate one) nor one
			// more (the `needed+1` compensation is left over when the cold tier lacks the boundary).
			if len(got) != tc.limit {
				t.Fatalf("limit=%d served %d candles (boundary in cold=%v)",
					tc.limit, len(got), tc.coldHasBoundary)
			}

			// AND WHAT COMES BACK IS A SERIES, not a padding: no repeated bucket and
			// strictly decreasing. A fix that merely added one row to the cold request
			// would return the duplicate and pass the count.
			seen := make(map[int64]struct{}, len(got))
			for i, c := range got {
				if _, dup := seen[c.Timestamp]; dup {
					t.Fatalf("bucket %d repeated at position %d", c.Timestamp, i)
				}
				seen[c.Timestamp] = struct{}{}
				if i > 0 && got[i-1].Timestamp <= c.Timestamp {
					t.Fatalf("order broken at %d: %d is not greater than %d", i, got[i-1].Timestamp, c.Timestamp)
				}
			}
		})
	}
}

// newBoundaryTieredStore assembles a TieredStore with the hot-tier cap the case needs.
// `newTestTieredStore` fixes 500 and the measured edge is 800.
func newBoundaryTieredStore(t *testing.T, hotCap int) *TieredStore {
	t.Helper()
	cfg := config.CandleEngineConfig{Enabled: true, RedisWriteThrough: false}
	ctx, cancel := context.WithCancel(context.Background())
	return &TieredStore{
		engine:       NewCandleEngine(map[string]int{"default": hotCap}),
		redis:        nil,
		writeThrough: cfg.RedisWriteThrough,
		snapshotTick: 0,
		ctx:          ctx,
		cancel:       cancel,
	}
}
