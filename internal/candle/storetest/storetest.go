// Package storetest is the shared contract suite for candle persistence adapters.
//
// The suites are ordinary Go code, not test files: an adapter imports this package
// from its own _test.go and runs the SAME cases against its real implementation.
// The fake in this package is the extensibility proof — a minimal in-memory
// implementation that satisfies the ports and passes the suite. It is not a
// production adapter and must never become one.
package storetest

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// HotStore is the candle port set a hot-tier adapter must satisfy.
type HotStore interface {
	candle.Writer
	candle.Reader
	candle.SeedWriter
}

// ColdStore is the durable candle port set a cold-tier adapter must satisfy.
type ColdStore interface {
	candle.ColdStore
}

// HotOptions declares what the hot suite may assume about the store under test.
type HotOptions struct {
	// Cap is the effective resident cap. Zero skips the cap case, for an
	// adapter that does not apply one (or a factory that did not configure it).
	Cap int
}

// RunHot runs the hot-tier contract suite. Each case gets a fresh store from
// newStore; the factory owns isolation (a fresh miniredis, a flushed DB, ...).
func RunHot(t *testing.T, newStore func(t *testing.T) HotStore, opts HotOptions) {
	t.Helper()

	t.Run("write_read_roundtrip", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		symbol, tf := "ROUND/TRIP", "1h"
		bar := candle.Candle{
			Timestamp:       time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC).UnixMilli(),
			Open:            100.5,
			High:            101.25,
			Low:             99.75,
			Close:           100.125,
			Volume:          42.5,
			TakerBuyVolume:  30.25,
			TakerSellVolume: 12.25,
			Source:          "venue",
		}
		if err := store.AddStreamingCandle(ctx, "binance", "future", symbol, tf, bar); err != nil {
			t.Fatalf("AddStreamingCandle: %v", err)
		}

		got, err := store.GetCandleByTimestamp(ctx, "binance", "future", symbol, tf, bar.Timestamp)
		if err != nil {
			t.Fatalf("GetCandleByTimestamp: %v", err)
		}
		if got == nil {
			t.Fatal("the written candle is not reachable by the read")
		}
		assertCandleFields(t, *got, bar)
	})

	t.Run("absence_is_not_zero", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()

		got, err := store.GetCandleByTimestamp(ctx, "binance", "future", "ABSENT/USDT", "1h", 1)
		if err != nil {
			t.Fatalf("GetCandleByTimestamp for an absent key returned an error: %v", err)
		}
		if got != nil {
			t.Fatalf("absent key returned a candle: %+v", got)
		}
		rows, err := store.GetCandlesDesc(ctx, "binance", "future", "ABSENT/USDT", "1h", 10)
		if err != nil {
			t.Fatalf("GetCandlesDesc for an absent key returned an error: %v", err)
		}
		if len(rows) != 0 {
			t.Fatalf("absent key returned %d row(s)", len(rows))
		}
		n, err := store.GetCandlesCount(ctx, "binance", "future", "ABSENT/USDT", "1h")
		if err != nil {
			t.Fatalf("GetCandlesCount for an absent key returned an error: %v", err)
		}
		if n != 0 {
			t.Fatalf("absent-key count = %d, want 0", n)
		}
	})

	t.Run("zero_order_flow_is_a_reading", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		symbol := "ZERO/FLOW"
		bar := candle.Candle{
			Timestamp:       time.Date(2026, 1, 2, 4, 0, 0, 0, time.UTC).UnixMilli(),
			Open:            10,
			High:            11,
			Low:             9,
			Close:           10,
			Volume:          100,
			TakerBuyVolume:  0,
			TakerSellVolume: 0,
			Source:          "venue",
		}
		if err := store.AddStreamingCandle(ctx, "binance", "future", symbol, "1h", bar); err != nil {
			t.Fatalf("AddStreamingCandle: %v", err)
		}
		got, err := store.GetCandleByTimestamp(ctx, "binance", "future", symbol, "1h", bar.Timestamp)
		if err != nil || got == nil {
			t.Fatalf("the candle with zero flow disappeared: got=%v err=%v", got, err)
		}
		if got.TakerBuyVolume != 0 || got.TakerSellVolume != 0 {
			t.Fatalf("zero flow came back as %v/%v", got.TakerBuyVolume, got.TakerSellVolume)
		}
	})

	t.Run("read_orders_and_limits", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		symbol := "ORDER/USDT"
		base := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
		for i := 0; i < 5; i++ {
			bar := candle.Candle{
				Timestamp: base.Add(time.Duration(i) * time.Hour).UnixMilli(),
				Open:      float64(i),
				High:      float64(i) + 1,
				Low:       float64(i) - 1,
				Close:     float64(i),
				Volume:    1,
			}
			if err := store.ReplaceCandle(ctx, "binance", "future", symbol, "1h", bar); err != nil {
				t.Fatalf("ReplaceCandle[%d]: %v", i, err)
			}
		}

		desc, err := store.GetCandlesDesc(ctx, "binance", "future", symbol, "1h", 3)
		if err != nil {
			t.Fatalf("GetCandlesDesc: %v", err)
		}
		if len(desc) != 3 {
			t.Fatalf("GetCandlesDesc returned %d row(s), want 3", len(desc))
		}
		if desc[0].Timestamp <= desc[1].Timestamp || desc[1].Timestamp <= desc[2].Timestamp {
			t.Fatalf("GetCandlesDesc is not in descending order: %v", timestamps(desc))
		}

		asc, err := store.GetCandlesAsc(ctx, "binance", "future", symbol, "1h", 3)
		if err != nil {
			t.Fatalf("GetCandlesAsc: %v", err)
		}
		if len(asc) != 3 {
			t.Fatalf("GetCandlesAsc returned %d row(s), want 3", len(asc))
		}
		if asc[0].Timestamp >= asc[1].Timestamp || asc[1].Timestamp >= asc[2].Timestamp {
			t.Fatalf("GetCandlesAsc is not in ascending order: %v", timestamps(asc))
		}
	})

	t.Run("score_range_is_inclusive_and_ascending", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		symbol := "RANGE/USDT"
		base := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
		var stamps []int64
		for i := 0; i < 5; i++ {
			bar := candle.Candle{
				Timestamp: base.Add(time.Duration(i) * time.Hour).UnixMilli(),
				Open:      float64(i), High: float64(i), Low: float64(i), Close: float64(i),
				Volume: 1,
			}
			stamps = append(stamps, bar.Timestamp)
			if err := store.ReplaceCandle(ctx, "binance", "future", symbol, "1h", bar); err != nil {
				t.Fatalf("ReplaceCandle[%d]: %v", i, err)
			}
		}

		got, err := store.GetCandlesByScoreRange(ctx, "binance", "future", symbol, "1h", stamps[1], stamps[3])
		if err != nil {
			t.Fatalf("GetCandlesByScoreRange: %v", err)
		}
		if want := []int64{stamps[1], stamps[2], stamps[3]}; !equalInt64(timestamps(got), want) {
			t.Fatalf("range = %v, want %v — the interval is inclusive and ascending", timestamps(got), want)
		}
	})

	t.Run("batch_read_returns_requested_windows", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		base := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
		for i, symbol := range []string{"BATCH/ONE", "BATCH/TWO"} {
			for j := 0; j < 2; j++ {
				bar := candle.Candle{
					Timestamp: base.Add(time.Duration(j) * time.Hour).UnixMilli(),
					Open:      float64(i), High: float64(i), Low: float64(i), Close: float64(i),
					Volume: 1,
				}
				if err := store.ReplaceCandle(ctx, "binance", "future", symbol, "1h", bar); err != nil {
					t.Fatalf("ReplaceCandle %s[%d]: %v", symbol, j, err)
				}
			}
		}

		reqs := []candle.CandleReq{
			{Symbol: "BATCH/ONE", Timeframe: "1h", Limit: 5},
			{Symbol: "BATCH/TWO", Timeframe: "1h", Limit: 5},
			{Symbol: "BATCH/ABSENT", Timeframe: "1h", Limit: 5},
		}
		got := store.GetBatchCandlesDesc(ctx, "binance", "future", reqs)
		if len(got[reqs[0]]) != 2 || len(got[reqs[1]]) != 2 {
			t.Fatalf("returned windows = %d/%d, want 2/2", len(got[reqs[0]]), len(got[reqs[1]]))
		}
		// Absence may be OMITTED or arrive as an empty slice; the contract is "no rows",
		// not the shape of the key in the map. Requiring omission would turn a driver
		// difference into a contract failure.
		if len(got[reqs[2]]) != 0 {
			t.Fatalf("an absent request returned %d row(s)", len(got[reqs[2]]))
		}
	})

	t.Run("replace_revises_without_duplicating", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		symbol := "REVISE/USDT"
		ts := time.Date(2026, 1, 2, 5, 0, 0, 0, time.UTC).UnixMilli()

		first := candle.Candle{Timestamp: ts, Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 10}
		second := candle.Candle{Timestamp: ts, Open: 1, High: 3, Low: 0.5, Close: 2.5, Volume: 11}
		if err := store.ReplaceCandle(ctx, "binance", "future", symbol, "1h", first); err != nil {
			t.Fatalf("initial ReplaceCandle: %v", err)
		}
		if err := store.ReplaceCandle(ctx, "binance", "future", symbol, "1h", second); err != nil {
			t.Fatalf("ReplaceCandle revision: %v", err)
		}

		got, err := store.GetCandleByTimestamp(ctx, "binance", "future", symbol, "1h", ts)
		if err != nil || got == nil {
			t.Fatalf("read after revision: got=%v err=%v", got, err)
		}
		if got.Close != second.Close || got.High != second.High || got.Volume != second.Volume {
			t.Fatalf("the revision did not replace the row: %+v", *got)
		}
		n, err := store.GetCandlesCount(ctx, "binance", "future", symbol, "1h")
		if err != nil {
			t.Fatalf("GetCandlesCount: %v", err)
		}
		if n != 1 {
			t.Fatalf("the revision duplicated the row: count=%d, want 1", n)
		}
	})

	t.Run("seed_is_idempotent", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		symbol := "SEED/USDT"
		base := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
		batch := []candle.Candle{
			{Timestamp: base.UnixMilli(), Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 10},
			{Timestamp: base.Add(time.Hour).UnixMilli(), Open: 1.5, High: 3, Low: 1, Close: 2, Volume: 20},
		}
		if err := store.SeedCandlesBatch(ctx, "binance", "future", symbol, "1h", batch); err != nil {
			t.Fatalf("SeedCandlesBatch: %v", err)
		}
		if err := store.SeedCandlesBatch(ctx, "binance", "future", symbol, "1h", batch); err != nil {
			t.Fatalf("repeated SeedCandlesBatch: %v", err)
		}
		n, err := store.GetCandlesCount(ctx, "binance", "future", symbol, "1h")
		if err != nil {
			t.Fatalf("GetCandlesCount: %v", err)
		}
		if n != int64(len(batch)) {
			t.Fatalf("repeated seed count=%d, want %d", n, len(batch))
		}

		current := candle.Candle{
			Timestamp: base.Add(2 * time.Hour).UnixMilli(),
			Open:      2, High: 4, Low: 1.5, Close: 3, Volume: 30,
		}
		if err := store.SeedCurrentBucketCandle(ctx, "binance", "future", symbol, "1h", current); err != nil {
			t.Fatalf("SeedCurrentBucketCandle: %v", err)
		}
		got, err := store.GetCandleByTimestamp(ctx, "binance", "future", symbol, "1h", current.Timestamp)
		if err != nil || got == nil {
			t.Fatalf("the seeded current bucket is not readable: got=%v err=%v", got, err)
		}
	})

	if opts.Cap > 0 {
		t.Run("cap_keeps_the_newest", func(t *testing.T) {
			store := newStore(t)
			ctx := context.Background()
			symbol := "CAP/USDT"
			base := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
			total := opts.Cap + 3
			for i := 0; i < total; i++ {
				bar := candle.Candle{
					Timestamp: base.Add(time.Duration(i) * time.Hour).UnixMilli(),
					Open:      float64(i), High: float64(i), Low: float64(i), Close: float64(i),
					Volume: 1,
				}
				if err := store.AddStreamingCandle(ctx, "binance", "future", symbol, "1h", bar); err != nil {
					t.Fatalf("AddStreamingCandle[%d]: %v", i, err)
				}
			}
			n, err := store.GetCandlesCount(ctx, "binance", "future", symbol, "1h")
			if err != nil {
				t.Fatalf("GetCandlesCount: %v", err)
			}
			if n != int64(opts.Cap) {
				t.Fatalf("count=%d, want cap=%d", n, opts.Cap)
			}
			oldest := base.UnixMilli()
			got, err := store.GetCandleByTimestamp(ctx, "binance", "future", symbol, "1h", oldest)
			if err != nil {
				t.Fatalf("GetCandleByTimestamp: %v", err)
			}
			if got != nil {
				t.Fatalf("the cap preserved the oldest candle: %+v", got)
			}
			newest := base.Add(time.Duration(total-1) * time.Hour).UnixMilli()
			got, err = store.GetCandleByTimestamp(ctx, "binance", "future", symbol, "1h", newest)
			if err != nil || got == nil {
				t.Fatalf("the cap dropped the newest candle: got=%v err=%v", got, err)
			}
		})
	}

	t.Run("cancel_refuses_the_operation", func(t *testing.T) {
		store := newStore(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		bar := candle.Candle{Timestamp: 1, Open: 1, High: 1, Low: 1, Close: 1, Volume: 1}
		if err := store.AddStreamingCandle(ctx, "binance", "future", "CANCEL/USDT", "1h", bar); err == nil {
			t.Error("AddStreamingCandle accepted a canceled context")
		}
		if err := store.SeedCandlesBatch(ctx, "binance", "future", "CANCEL/USDT", "1h", []candle.Candle{bar}); err == nil {
			t.Error("SeedCandlesBatch accepted a canceled context")
		}
		if _, err := store.GetCandlesDesc(ctx, "binance", "future", "CANCEL/USDT", "1h", 1); err == nil {
			t.Error("GetCandlesDesc accepted a canceled context")
		}
	})
}

// RunCold runs that part of the candle contract which a durable adapter owns.
//
// QueryRange is the only range contract a cold adapter must satisfy: it returns
// rows DESCENDING by timestamp and honors from/to inclusively and limit. The
// hot Reader.GetCandlesRange is a merged-tier operation and is deliberately not
// part of this suite.
func RunCold(t *testing.T, newStore func(t *testing.T) ColdStore) {
	t.Helper()

	key := func(symbol string) candle.SeriesKey {
		k, err := candle.SeriesKeyOf("binance", "future", symbol, "1h")
		if err != nil {
			t.Fatalf("SeriesKeyOf: %v", err)
		}
		return k
	}

	t.Run("write_read_roundtrip", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		base := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
		bars := []candle.Candle{
			{Timestamp: base.UnixMilli(), Open: 100.5, High: 101.25, Low: 99.75, Close: 100.125, Volume: 42.5, TakerBuyVolume: 30.25, TakerSellVolume: 12.25},
			{Timestamp: base.Add(time.Hour).UnixMilli(), Open: 100, High: 102, Low: 99, Close: 101, Volume: 10, TakerBuyVolume: 6, TakerSellVolume: 4},
			{Timestamp: base.Add(2 * time.Hour).UnixMilli(), Open: 101, High: 103, Low: 100, Close: 102, Volume: 11, TakerBuyVolume: 7, TakerSellVolume: 4},
		}
		if err := store.WriteBatches(ctx, map[candle.SeriesKey][]candle.Candle{key("COLD/RT"): bars}); err != nil {
			t.Fatalf("WriteBatches: %v", err)
		}

		got, err := store.QueryRange(ctx, "binance", "future", "COLD/RT", "1h",
			base.Add(-time.Hour), base.Add(3*time.Hour), 10)
		if err != nil {
			t.Fatalf("QueryRange: %v", err)
		}
		if len(got) != len(bars) {
			t.Fatalf("%d row(s) read, want %d", len(got), len(bars))
		}
		for i := range got {
			want := bars[len(bars)-1-i]
			if got[i].Timestamp != want.Timestamp {
				t.Fatalf("order = %v, want descending", timestamps(got))
			}
			assertCandleFields(t, got[i], want)
		}

		oldest, err := store.GetOldestTimestamp(ctx, "binance", "future", "COLD/RT", "1h")
		if err != nil {
			t.Fatalf("GetOldestTimestamp: %v", err)
		}
		if !oldest.Equal(base) {
			t.Fatalf("oldest = %s, want %s", oldest, base)
		}
	})

	t.Run("absence_is_an_empty_result_and_a_zero_oldest", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		got, err := store.QueryRange(ctx, "binance", "future", "COLD/ABSENT", "1h",
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), 10)
		if err != nil {
			t.Fatalf("QueryRange for an absent series returned an error: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("absent series returned %d row(s)", len(got))
		}
		oldest, err := store.GetOldestTimestamp(ctx, "binance", "future", "COLD/ABSENT", "1h")
		if err != nil {
			t.Fatalf("GetOldestTimestamp for an absent series returned an error: %v", err)
		}
		if !oldest.IsZero() {
			t.Fatalf("oldest of an absent series = %s, want zero", oldest)
		}
	})

	t.Run("range_is_inclusive_and_limited", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		base := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
		bars := make([]candle.Candle, 0, 5)
		for i := 0; i < 5; i++ {
			bars = append(bars, candle.Candle{
				Timestamp: base.Add(time.Duration(i) * time.Hour).UnixMilli(),
				Open:      float64(i), High: float64(i), Low: float64(i), Close: float64(i), Volume: 1,
			})
		}
		if err := store.WriteBatches(ctx, map[candle.SeriesKey][]candle.Candle{key("COLD/RANGE"): bars}); err != nil {
			t.Fatalf("WriteBatches: %v", err)
		}

		got, err := store.QueryRange(ctx, "binance", "future", "COLD/RANGE", "1h",
			base.Add(time.Hour), base.Add(3*time.Hour), 2)
		if err != nil {
			t.Fatalf("QueryRange: %v", err)
		}
		if want := []int64{bars[3].Timestamp, bars[2].Timestamp}; !equalInt64(timestamps(got), want) {
			t.Fatalf("range+limit = %v, want %v", timestamps(got), want)
		}
	})

	t.Run("dedup_keeps_one_row_and_the_last_write", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		base := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
		first := candle.Candle{Timestamp: base.UnixMilli(), Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 10}
		second := candle.Candle{Timestamp: base.UnixMilli(), Open: 1, High: 3, Low: 0.5, Close: 2.5, Volume: 11}
		if err := store.WriteBatches(ctx, map[candle.SeriesKey][]candle.Candle{key("COLD/DEDUP"): {first}}); err != nil {
			t.Fatalf("initial WriteBatches: %v", err)
		}
		if err := store.WriteBatches(ctx, map[candle.SeriesKey][]candle.Candle{key("COLD/DEDUP"): {second}}); err != nil {
			t.Fatalf("WriteBatches revision: %v", err)
		}

		got, err := store.QueryRange(ctx, "binance", "future", "COLD/DEDUP", "1h",
			base.Add(-time.Hour), base.Add(time.Hour), 10)
		if err != nil {
			t.Fatalf("QueryRange: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("DEDUP left %d row(s), want 1", len(got))
		}
		if got[0].Close != second.Close || got[0].Volume != second.Volume {
			t.Fatalf("the last write did not win: %+v", got[0])
		}
	})

	t.Run("series_are_isolated_by_identity", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		base := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
		one := candle.Candle{Timestamp: base.UnixMilli(), Open: 1, High: 1, Low: 1, Close: 1, Volume: 1}
		two := candle.Candle{Timestamp: base.UnixMilli(), Open: 2, High: 2, Low: 2, Close: 2, Volume: 2}
		groups := map[candle.SeriesKey][]candle.Candle{
			key("COLD/ONE"): {one},
			key("COLD/TWO"): {two},
		}
		if err := store.WriteBatches(ctx, groups); err != nil {
			t.Fatalf("WriteBatches: %v", err)
		}
		gotOne, err := store.QueryRange(ctx, "binance", "future", "COLD/ONE", "1h", base.Add(-time.Hour), base.Add(time.Hour), 10)
		if err != nil {
			t.Fatalf("QueryRange ONE: %v", err)
		}
		gotTwo, err := store.QueryRange(ctx, "binance", "future", "COLD/TWO", "1h", base.Add(-time.Hour), base.Add(time.Hour), 10)
		if err != nil {
			t.Fatalf("QueryRange TWO: %v", err)
		}
		if len(gotOne) != 1 || gotOne[0].Close != 1 {
			t.Fatalf("series ONE contaminated: %+v", gotOne)
		}
		if len(gotTwo) != 1 || gotTwo[0].Close != 2 {
			t.Fatalf("series TWO contaminated: %+v", gotTwo)
		}
	})

	t.Run("cancel_refuses_the_write", func(t *testing.T) {
		store := newStore(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		base := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
		bar := candle.Candle{Timestamp: base.UnixMilli(), Open: 1, High: 1, Low: 1, Close: 1, Volume: 1}
		if err := store.WriteBatches(ctx, map[candle.SeriesKey][]candle.Candle{key("COLD/CANCEL"): {bar}}); err == nil {
			t.Error("WriteBatches accepted a canceled context")
		}
	})
}

// FakeHot is the minimal in-memory HotStore. It is the suite's extensibility
// proof: a new adapter is a type with these methods, not a new package.
type FakeHot struct {
	mu     sync.Mutex
	series map[string]map[int64]candle.Candle
	cap    int
}

// NewFakeHot builds the fake with the resident cap the suite was told about.
func NewFakeHot(capacity int) *FakeHot {
	return &FakeHot{series: map[string]map[int64]candle.Candle{}, cap: capacity}
}

func fakeKey(exchange, marketType, symbol, timeframe string) string {
	return fmt.Sprintf("%s:%s:%s:%s", exchange, marketType, symbol, timeframe)
}

func (f *FakeHot) upsert(applyCap bool, exchange, marketType, symbol, timeframe string, bar candle.Candle) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := fakeKey(exchange, marketType, symbol, timeframe)
	rows := f.series[key]
	if rows == nil {
		rows = map[int64]candle.Candle{}
		f.series[key] = rows
	}
	rows[bar.Timestamp] = bar
	if applyCap && f.cap > 0 && len(rows) > f.cap {
		ordered := make([]int64, 0, len(rows))
		for ts := range rows {
			ordered = append(ordered, ts)
		}
		sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
		for _, ts := range ordered[:len(ordered)-f.cap] {
			delete(rows, ts)
		}
	}
}

// AddStreamingCandle upserts one live candle into the fake hot tier.
func (f *FakeHot) AddStreamingCandle(ctx context.Context, exchange, marketType, symbol, timeframe string, bar candle.Candle) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.upsert(true, exchange, marketType, symbol, timeframe, bar)
	return nil
}

// AddStreamingCandlesBatch applies each update through AddStreamingCandle.
func (f *FakeHot) AddStreamingCandlesBatch(ctx context.Context, updates []candle.CandleUpdate) error {
	for _, u := range updates {
		if err := f.AddStreamingCandle(ctx, u.Exchange, u.MarketType, u.Symbol, u.Timeframe, u.Candle); err != nil {
			return err
		}
	}
	return nil
}

// ReplaceCandle overwrites the bar at its timestamp.
func (f *FakeHot) ReplaceCandle(ctx context.Context, exchange, marketType, symbol, timeframe string, bar candle.Candle) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.upsert(false, exchange, marketType, symbol, timeframe, bar)
	return nil
}

func (f *FakeHot) rows(exchange, marketType, symbol, timeframe string) []candle.Candle {
	f.mu.Lock()
	defer f.mu.Unlock()
	rows := f.series[fakeKey(exchange, marketType, symbol, timeframe)]
	out := make([]candle.Candle, 0, len(rows))
	for _, c := range rows {
		out = append(out, c)
	}
	return out
}

// GetCandleByTimestamp returns the stored bar at one timestamp, or nil.
func (f *FakeHot) GetCandleByTimestamp(ctx context.Context, exchange, marketType, symbol, timeframe string, timestamp int64) (*candle.Candle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	rows := f.series[fakeKey(exchange, marketType, symbol, timeframe)]
	c, ok := rows[timestamp]
	if !ok {
		return nil, nil
	}
	return &c, nil
}

// GetCandlesDesc returns up to limit bars newest-first.
func (f *FakeHot) GetCandlesDesc(ctx context.Context, exchange, marketType, symbol, timeframe string, limit int) ([]candle.Candle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rows := f.rows(exchange, marketType, symbol, timeframe)
	sort.Slice(rows, func(i, j int) bool { return rows[i].Timestamp > rows[j].Timestamp })
	return limitRows(rows, limit), nil
}

// GetBatchCandlesDesc returns one newest-first window per request.
func (f *FakeHot) GetBatchCandlesDesc(ctx context.Context, exchange, marketType string, reqs []candle.CandleReq) map[candle.CandleReq][]candle.Candle {
	if ctx.Err() != nil {
		return map[candle.CandleReq][]candle.Candle{}
	}
	out := make(map[candle.CandleReq][]candle.Candle, len(reqs))
	for _, req := range reqs {
		rows := f.rows(exchange, marketType, req.Symbol, req.Timeframe)
		if len(rows) == 0 {
			continue
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].Timestamp > rows[j].Timestamp })
		out[req] = limitRows(rows, req.Limit)
	}
	return out
}

// GetCandlesAsc returns up to limit bars oldest-first.
func (f *FakeHot) GetCandlesAsc(ctx context.Context, exchange, marketType, symbol, timeframe string, limit int) ([]candle.Candle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rows := f.rows(exchange, marketType, symbol, timeframe)
	sort.Slice(rows, func(i, j int) bool { return rows[i].Timestamp < rows[j].Timestamp })
	return limitRows(rows, limit), nil
}

// GetCandlesByScoreRange returns bars whose timestamp falls in the range.
func (f *FakeHot) GetCandlesByScoreRange(ctx context.Context, exchange, marketType, symbol, timeframe string, minScore, maxScore int64) ([]candle.Candle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rows := f.rows(exchange, marketType, symbol, timeframe)
	out := make([]candle.Candle, 0, len(rows))
	for _, c := range rows {
		if c.Timestamp >= minScore && c.Timestamp <= maxScore {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp < out[j].Timestamp })
	return out, nil
}

// GetCandlesCount returns the number of stored bars.
func (f *FakeHot) GetCandlesCount(_ context.Context, exchange, marketType, symbol, timeframe string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return int64(len(f.series[fakeKey(exchange, marketType, symbol, timeframe)])), nil
}

// GetCandlesRange reports that range reads belong to the merged tier.
func (f *FakeHot) GetCandlesRange(_ context.Context, _, _, _, _ string, _, _ time.Time, _ int) ([]candle.Candle, error) {
	return nil, fmt.Errorf("GetCandlesRange is a merged-tier operation, not a hot-store contract")
}

// SeedCandlesBatch inserts a historical run through AddStreamingCandle.
func (f *FakeHot) SeedCandlesBatch(ctx context.Context, exchange, marketType, symbol, timeframe string, bars []candle.Candle) error {
	for _, bar := range bars {
		if err := f.AddStreamingCandle(ctx, exchange, marketType, symbol, timeframe, bar); err != nil {
			return err
		}
	}
	return nil
}

// SeedCurrentBucketCandle upserts the open bucket candle.
func (f *FakeHot) SeedCurrentBucketCandle(ctx context.Context, exchange, marketType, symbol, timeframe string, bar candle.Candle) error {
	return f.AddStreamingCandle(ctx, exchange, marketType, symbol, timeframe, bar)
}

// FakeCold is the minimal in-memory ColdStore.
type FakeCold struct {
	mu     sync.Mutex
	series map[string]map[int64]candle.Candle
}

// NewFakeCold builds the durable fake.
func NewFakeCold() *FakeCold {
	return &FakeCold{series: map[string]map[int64]candle.Candle{}}
}

// WriteBatches stores each series' candles in the fake cold tier.
func (f *FakeCold) WriteBatches(ctx context.Context, groups map[candle.SeriesKey][]candle.Candle) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for key, bars := range groups {
		seriesKey := fmt.Sprintf("%s:%s:%s:%s", key.Scope.Exchange, key.Scope.MarketType, key.Scope.Symbol, key.Timeframe)
		rows := f.series[seriesKey]
		if rows == nil {
			rows = map[int64]candle.Candle{}
			f.series[seriesKey] = rows
		}
		for _, bar := range bars {
			rows[bar.Timestamp] = bar
		}
	}
	return nil
}

// QueryRange returns the stored bars intersecting the requested window.
func (f *FakeCold) QueryRange(_ context.Context, exchange, marketType, symbol, timeframe string, from, to time.Time, limit int) ([]candle.Candle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rows := f.series[fmt.Sprintf("%s:%s:%s:%s", exchange, marketType, symbol, timeframe)]
	out := make([]candle.Candle, 0, len(rows))
	for _, c := range rows {
		ts := time.UnixMilli(c.Timestamp).UTC()
		if (ts.Equal(from) || ts.After(from)) && (ts.Equal(to) || ts.Before(to)) {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp > out[j].Timestamp })
	return limitRows(out, limit), nil
}

// GetOldestTimestamp returns the oldest stored bucket, or the zero time.
func (f *FakeCold) GetOldestTimestamp(_ context.Context, exchange, marketType, symbol, timeframe string) (time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rows := f.series[fmt.Sprintf("%s:%s:%s:%s", exchange, marketType, symbol, timeframe)]
	if len(rows) == 0 {
		return time.Time{}, nil
	}
	var oldest int64
	first := true
	for ts := range rows {
		if first || ts < oldest {
			oldest = ts
			first = false
		}
	}
	return time.UnixMilli(oldest).UTC(), nil
}

// Close is a no-op; the fake owns no resources.
func (f *FakeCold) Close() error { return nil }

// Stats reports the fake as connected.
func (f *FakeCold) Stats() candle.ColdStoreStats { return candle.ColdStoreStats{Connected: true} }

func limitRows(rows []candle.Candle, limit int) []candle.Candle {
	if limit > 0 && len(rows) > limit {
		return rows[:limit]
	}
	return rows
}

func timestamps(rows []candle.Candle) []int64 {
	out := make([]int64, len(rows))
	for i, c := range rows {
		out[i] = c.Timestamp
	}
	return out
}

func equalInt64(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func assertCandleFields(t *testing.T, got, want candle.Candle) {
	t.Helper()
	if got.Timestamp != want.Timestamp {
		t.Errorf("timestamp = %d, want %d", got.Timestamp, want.Timestamp)
	}
	fields := []struct {
		name      string
		got, want float64
	}{
		{"open", got.Open, want.Open},
		{"high", got.High, want.High},
		{"low", got.Low, want.Low},
		{"close", got.Close, want.Close},
		{"volume", got.Volume, want.Volume},
		{"taker_buy_volume", got.TakerBuyVolume, want.TakerBuyVolume},
		{"taker_sell_volume", got.TakerSellVolume, want.TakerSellVolume},
	}
	for _, f := range fields {
		if f.got != f.want {
			t.Errorf("%s = %v, want %v", f.name, f.got, f.want)
		}
	}
}
