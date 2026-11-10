package engine

import (
	"context"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// A RANGE WITHOUT A CEILING IS OPEN, NOT EMPTY.
//
// `GetCandlesRange` does `coldTo := to` and only replaces that value when the hot tier has a
// candle inside the range. With `?from=` ALONE the handler passes `to = time.Time{}`; if nothing
// in the hot tier matches — symbol/TF outside the engine, or boot before seeding — `coldTo` stays
// ZERO and arrives that way at the cold query. Production has no "no ceiling": a zero time
// serializes to `'0001-01-01T00:00:00.000000Z'`, so the SQL becomes
// `timestamp <= '0001-01-01…'` and returns ZERO ROWS with the cold tier full.
//
// `GetCandlesDesc` does not have the defect: it has the explicit `else { to = time.Now() }`
// branch for the empty-hot case. `GetCandlesRange` inherits the CALLER's ceiling and needs the
// same branch — only when the caller did not provide a ceiling, because here the caller's `to`
// can be a real value that must be respected.
//
// # WHY TWO FAKES, AND NOT ONE
//
// Each fake is faithful on one axis. The EMPTY-hot case measures the ZERO VALUE axis: only
// `mockColdStore` is faithful there, because it models zero as "no limit". `inclusiveColdStore`
// would return the whole range and hide the defect. The POPULATED-hot case measures the RECENCY
// axis (`ORDER BY timestamp DESC LIMIT n`), and there the faithful one is `inclusiveColdStore`;
// on that path `coldTo` is never zero.
func TestGetCandlesRange_OpenEndedRangeReachesTheColdTier(t *testing.T) {
	ctx := context.Background()

	// EXTERNAL LITERALS: the bucket width comes from the definition of `1h` and the epoch from the
	// calendar (2026-07-01T00:00Z), not from any function under test.
	//
	// THE EPOCH IS OLDER THAN THE SIBLING CASES' ON PURPOSE. The replacement ceiling is
	// `time.Now()`, so the ENTIRE window — hot and cold — must be in the past; with a later
	// epoch the 800 hot buckets would end in the future and the case would measure the machine
	// clock instead of the fix.
	const tfMs int64 = 3_600_000
	const oldestHotMs int64 = 1_782_864_000_000
	const hotCap = 800
	const coldDepth = 300 // buckets the cold tier holds BELOW the oldest hot bucket

	if got := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC).UnixMilli(); got != oldestHotMs {
		t.Fatalf("the bucket literal left the calendar: %d != %d", got, oldestHotMs)
	}
	if got := time.Hour.Milliseconds(); got != tfMs {
		t.Fatalf("the 1h bucket literal left the definition: %d != %d", got, tfMs)
	}

	// Bucket indices relative to `oldestHotMs`: the hot tier occupies [0, 799], the cold tier [-300, 799].
	bucket := func(idx int64) time.Time { return time.UnixMilli(oldestHotMs + idx*tfMs) }
	barAt := func(idx int64) candle.Candle {
		return candle.Candle{
			Timestamp: oldestHotMs + idx*tfMs,
			Open:      100, High: 110, Low: 90, Close: 105, Volume: 1,
		}
	}

	const exchange, mt, sym, tf = "binance", "future", "BTC/USDT", "1h"

	// THE REPLACEMENT CEILING IS `time.Now()`, so the case only has an effect if the fixture is in
	// the past. Without this guard, a lagging clock would silently make the case VACUOUS.
	if now := time.Now(); !bucket(hotCap - 1).Before(now) {
		t.Fatalf("the fixture is not in the past (newest=%s, now=%s): the case would lose its meaning",
			bucket(hotCap-1), now)
	}

	t.Run("hot-empty/serves-cold-instead-of-nothing", func(t *testing.T) {
		ts := newBoundaryTieredStore(t, hotCap)
		defer ts.cancel()

		// NOTHING in the hot tier for this key: it is the symbol/TF outside the engine, or boot before
		// seeding — the two states the route encounters in production.
		coldRows := make([]candle.Candle, 0, coldDepth+1)
		for k := -int64(coldDepth); k < 0; k++ {
			coldRows = append(coldRows, barAt(k))
		}

		// AND ONE ROW AHEAD OF THE CLOCK, which is what separates "open UP TO THE CURRENT INSTANT"
		// (what the contract asks for, and what the sibling `GetCandlesDesc` does) from "open with
		// no ceiling". A fix that swapped zero for a distant-future sentinel would serve this row
		// and satisfy "never empty" without satisfying the contract; here it fails.
		const aheadOfClock = 301
		coldRows = append(coldRows, candle.Candle{
			Timestamp: time.Now().Add(time.Hour).UnixMilli(),
			Open:      100, High: 110, Low: 90, Close: 105, Volume: 1,
		})
		if len(coldRows) != aheadOfClock {
			t.Fatalf("the cold fixture has %d rows, want %d", len(coldRows), aheadOfClock)
		}

		cold := &mockColdStore{candles: coldRows}
		ts.SetColdStore(cold)

		// THE FAKE'S FIDELITY, PINNED AT THE POINT WHERE THE CASE DEPENDS ON IT. If someone
		// gives `mockColdStore` a `!to.IsZero()` guard again, this case would turn green by
		// vacuity instead of failing. Here it fails naming the cause.
		zeroCeiling, err := cold.QueryRange(ctx, exchange, mt, sym, tf, bucket(-coldDepth), time.Time{}, len(coldRows))
		if err != nil {
			t.Fatalf("fake probe: %v", err)
		}
		if len(zeroCeiling) != 0 {
			t.Fatalf("the fake returned %d rows with a ZERO ceiling; production returns 0 (`timestamp <= '0001-01-01…'`) and the case below would stop discriminating",
				len(zeroCeiling))
		}

		// THE ROUTE CALL: a lone `?from=` becomes `to = time.Time{}` in the handler.
		got, err := ts.GetCandlesRange(ctx, exchange, mt, sym, tf, bucket(-coldDepth), time.Time{}, hotCap)
		if err != nil {
			t.Fatalf("GetCandlesRange: %v", err)
		}

		if len(got) != coldDepth {
			t.Fatalf("open-ended range with empty hot served %d candles, want the %d from cold (%d fixture rows minus the one ahead of the clock)",
				len(got), coldDepth, aheadOfClock)
		}

		// AND WHAT COMES BACK IS THE COLD TIER, not just any padding: a DESC series, without repetition,
		// from the newest to the oldest the cold tier holds — and the newest is the last one in the
		// PAST, not the one ahead of the clock.
		if got[0].Timestamp != barAt(-1).Timestamp {
			t.Fatalf("the newest served is %d, want %d", got[0].Timestamp, barAt(-1).Timestamp)
		}
		if got[len(got)-1].Timestamp != barAt(-coldDepth).Timestamp {
			t.Fatalf("the oldest served is %d, want %d",
				got[len(got)-1].Timestamp, barAt(-coldDepth).Timestamp)
		}
		for i := 1; i < len(got); i++ {
			if got[i-1].Timestamp <= got[i].Timestamp {
				t.Fatalf("order broken at %d: %d is not greater than %d", i, got[i-1].Timestamp, got[i].Timestamp)
			}
		}
	})

	t.Run("hot-populated/exclusive-boundary-preserved", func(t *testing.T) {
		ts := newBoundaryTieredStore(t, hotCap)
		defer ts.cancel()

		for i := range hotCap {
			if err := ts.AddStreamingCandle(ctx, exchange, mt, sym, tf, barAt(int64(i))); err != nil {
				t.Fatalf("AddStreamingCandle[%d]: %v", i, err)
			}
		}

		// The cold tier holds the SAME window as the hot tier plus `coldDepth` buckets below — the
		// overlap that the write to both tiers produces, and without which there would be no
		// duplicate for the exclusive boundary to avoid.
		coldRows := make([]candle.Candle, 0, coldDepth+hotCap)
		for k := -int64(coldDepth); k < hotCap; k++ {
			coldRows = append(coldRows, barAt(k))
		}
		ts.SetColdStore(&inclusiveColdStore{candles: coldRows})

		const limit = 1000
		const fromIdx = -200
		got, err := ts.GetCandlesRange(ctx, exchange, mt, sym, tf, bucket(fromIdx), time.Time{}, limit)
		if err != nil {
			t.Fatalf("GetCandlesRange: %v", err)
		}

		// THE FIX MUST NOT EAT THE SIBLING BRANCH: with the hot tier populated, the cold ceiling
		// remains `oldest in the range − 1ms`. Returning the boundary bucket duplicated would make
		// it serve 999 here.
		if len(got) != limit {
			t.Fatalf("open-ended range crossing the boundary served %d candles, want %d", len(got), limit)
		}

		seen := make(map[int64]struct{}, len(got))
		for i, c := range got {
			if _, dup := seen[c.Timestamp]; dup {
				t.Fatalf("bucket %d repeated at position %d", c.Timestamp, i)
			}
			seen[c.Timestamp] = struct{}{}
			if i > 0 && got[i-1].Timestamp <= c.Timestamp {
				t.Fatalf("order broken at %d: %d is not greater than %d", i, got[i-1].Timestamp, c.Timestamp)
			}
			if c.Timestamp < bucket(fromIdx).UnixMilli() {
				t.Fatalf("bucket %d at position %d is below the floor %d",
					c.Timestamp, i, bucket(fromIdx).UnixMilli())
			}
		}
	})
}
