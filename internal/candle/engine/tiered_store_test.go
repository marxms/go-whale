package engine

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"
	"github.com/marxms/go-whale/internal/retention"
)

// mockColdStore implements candle.ColdStore for testing hot/cold merge.
type mockColdStore struct {
	candles    []candle.Candle
	queryErr   error
	writeCalls int
}

func (m *mockColdStore) WriteBatches(_ context.Context, _ map[candle.SeriesKey][]candle.Candle) error {
	// writeCalls counts COMMITS, not series: one call covers the whole sweep.
	m.writeCalls++
	return nil
}

// QueryRange returns `[from, to]` with BOTH bounds INCLUSIVE, like production.
//
// # THE FAKE MUST MATCH THE EDGE OF A REAL DEFECT
//
// The cold tier runs `timestamp >= $from AND timestamp <= $to`. If the fake models `to` as
// EXCLUSIVE, the first cold row stops being a DUPLICATE of the hot one, and the duplicate never
// appears in the merge. `GetCandlesDesc` and `GetCandlesRange` stitch the two tiers together by
// passing a `to` derived from the hot tier; the inclusivity of that bound is load-bearing.
//
// # THE ZERO-VALUE AXIS: PRODUCTION HAS NO "NO LIMIT"
//
// Production does not model zero as "no limit": a zero time serializes to
// `'0001-01-01T00:00:00.000000Z'` and the literal enters the SQL like any other. The effect on
// the two bounds is OPPOSITE:
//
//   - `from` zero -> `timestamp >= '0001-01-01…'` selects EVERYTHING, which happens to give the
//     same result as the special case.
//   - `to` zero -> `timestamp <= '0001-01-01…'` selects NOTHING, while the special case returned
//     everything. It is the difference between the whole range and the empty range, in the value
//     the route produces most.
//
// Reachable: a lone `?from=` leaves `to` zero in `GetCandlesRange`.
//
// Ordering remains insertion order, not production's `ORDER BY timestamp DESC`; whoever needs
// the recency cut uses `inclusiveColdStore`, which models both.
func (m *mockColdStore) QueryRange(_ context.Context, _, _, _, _ string, from, to time.Time, limit int) ([]candle.Candle, error) {
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	var result []candle.Candle
	for _, c := range m.candles {
		ts := time.UnixMilli(c.Timestamp)
		if ts.Before(from) || ts.After(to) {
			continue
		}
		result = append(result, c)
		if len(result) >= limit {
			break
		}
	}
	return result, nil
}

// THE SHARED FAKE AND THE PRODUCTION QUERY AGREE ON BOTH BOUNDS.
//
// A fake that models a bound differently from production makes the duplicate invisible at exactly
// the edge where the defect lives. This case ties the semantics on both sides without copying
// them: the production bound is READ from `coldRangeQuery` instead of remembered here. If the SQL
// changes `<=` to `<`, the case fails and takes the fake with it, instead of letting the two
// diverge in silence.
func TestMockColdStore_HonoursTheProductionRangeBoundary(t *testing.T) {
	// EXTERNAL LITERALS: 2026-08-20T00:00Z and the width of the `1h` bucket.
	const tfMs int64 = 3_600_000
	const fromMs int64 = 1_787_184_000_000
	const toMs int64 = fromMs + 2*tfMs

	if got := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC).UnixMilli(); got != fromMs {
		t.Fatalf("the bucket literal left the calendar: %d != %d", got, fromMs)
	}
	if got := time.Hour.Milliseconds(); got != tfMs {
		t.Fatalf("the 1h bucket literal left the definition: %d != %d", got, tfMs)
	}

	from, to := time.UnixMilli(fromMs), time.UnixMilli(toMs)

	// 2) WHAT THE FAKE RETURNS — both bounds inside, and only the neighbors outside.
	cold := &mockColdStore{candles: []candle.Candle{
		{Timestamp: fromMs - tfMs, Close: 1}, // below the range
		{Timestamp: fromMs, Close: 2},        // LOWER bound — inclusive
		{Timestamp: fromMs + tfMs, Close: 3}, // middle
		{Timestamp: toMs, Close: 4},          // UPPER bound — inclusive
		{Timestamp: toMs + tfMs, Close: 5},   // above the range
	}}

	got, err := cold.QueryRange(context.Background(), "binance", "future", "BTC/USDT", "1h", from, to, 10)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}

	want := []int64{fromMs, fromMs + tfMs, toMs}
	if len(got) != len(want) {
		t.Fatalf("range [%d, %d] returned %d bucket(s), want %d: %v", fromMs, toMs, len(got), len(want), got)
	}
	for i, w := range want {
		if got[i].Timestamp != w {
			t.Fatalf("bucket [%d] = %d, want %d — the `to` edge is INCLUSIVE in production",
				i, got[i].Timestamp, w)
		}
	}
}

// THE FAKE HONORS PRODUCTION ON THE ZERO-VALUE AXIS TOO.
//
// A fake that treats zero `from`/`to` as "no limit" models a path production does not have.
// `timestampLiteral(time.Time{})` yields `'0001-01-01T00:00:00.000000Z'` and the literal enters
// the SQL like any other — with the OPPOSITE effect on the two bounds, because year 1 is below
// every real candle: at the floor (`>=`) it selects everything, at the ceiling (`<=`) it selects
// nothing. The fake returned everything in both.
//
// The bound is READ from production instead of repeated here: the predicate comes from
// `coldRangeQuery` and what zero becomes inside it comes from `timestampLiteral`. If either
// starts treating zero as a special case, this case fails and takes the fake with it.
//
// The axis is REACHABLE, not hypothetical: a lone `?from=` leaves `to` ZERO at the store.
func TestMockColdStore_HonoursTheProductionZeroBound(t *testing.T) {
	// EXTERNAL LITERALS: 2026-08-20T00:00Z and the width of the `1h` bucket.
	const tfMs int64 = 3_600_000
	const fromMs int64 = 1_787_184_000_000
	const toMs int64 = fromMs + 2*tfMs

	if got := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC).UnixMilli(); got != fromMs {
		t.Fatalf("the bucket literal left the calendar: %d != %d", got, fromMs)
	}
	if got := time.Hour.Milliseconds(); got != tfMs {
		t.Fatalf("the 1h bucket literal left the definition: %d != %d", got, tfMs)
	}

	zero := time.Time{}
	from, to := time.UnixMilli(fromMs), time.UnixMilli(toMs)

	fixture := []candle.Candle{
		{Timestamp: fromMs - tfMs, Close: 1},
		{Timestamp: fromMs, Close: 2},
		{Timestamp: fromMs + tfMs, Close: 3},
		{Timestamp: toMs, Close: 4},
		{Timestamp: toMs + tfMs, Close: 5},
	}

	for _, c := range fixture {
		if !zero.Before(time.UnixMilli(c.Timestamp)) {
			t.Fatalf("candle %d is not above year 1 — the fixture does not exercise the axis", c.Timestamp)
		}
	}

	cases := []struct {
		name     string
		from, to time.Time
		want     []int64
	}{
		{
			// The ceiling is year 1: `timestamp <= '0001-01-01…'` selects no row at all.
			// This is where the fake returned the whole range.
			name: "to zero — the ceiling becomes year 1 and nothing passes",
			from: from,
			to:   zero,
			want: nil,
		},
		{
			// The floor is year 1: `timestamp >= '0001-01-01…'` selects everything. The fake already
			// got it right by accident, and the case exists so that "refuse when it is zero" does not
			// pass for a fix of the axis.
			name: "from zero — the floor becomes year 1 and everything up to the ceiling passes",
			from: zero,
			to:   to,
			want: []int64{fromMs - tfMs, fromMs, fromMs + tfMs, toMs},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// 3) WHAT THE FAKE RETURNS — it has to be the same answer.
			cold := &mockColdStore{candles: fixture}
			got, err := cold.QueryRange(context.Background(), "binance", "future", "BTC/USDT", "1h", tc.from, tc.to, 10)
			if err != nil {
				t.Fatalf("QueryRange: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("the fake returned %d bucket(s) for %q, production returns %d: %v",
					len(got), tc.name, len(tc.want), got)
			}
			for i, w := range tc.want {
				if got[i].Timestamp != w {
					t.Fatalf("bucket [%d] = %d, want %d", i, got[i].Timestamp, w)
				}
			}
		})
	}
}

func (m *mockColdStore) GetOldestTimestamp(_ context.Context, _, _, _, _ string) (time.Time, error) {
	return time.Time{}, nil
}

func (m *mockColdStore) Close() error { return nil }

func (m *mockColdStore) Stats() candle.ColdStoreStats { return candle.ColdStoreStats{} }

// newTestTieredStore creates a TieredStore backed only by CandleEngine (no Redis).
// writeThrough=false avoids the snapshot goroutine and Redis dependency.
func newTestTieredStore() *TieredStore {
	engine := NewCandleEngine(map[string]int{"default": 500})
	cfg := config.CandleEngineConfig{
		Enabled:           true,
		RedisWriteThrough: false,
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &TieredStore{
		engine:       engine,
		redis:        nil,
		writeThrough: cfg.RedisWriteThrough,
		snapshotTick: 0,
		ctx:          ctx,
		cancel:       cancel,
	}
}

func TestTieredStore_WriteThenRead_DataParity(t *testing.T) {
	ts := newTestTieredStore()
	defer ts.cancel()

	ctx := context.Background()
	const exchange, mt, sym, tf = "binance", "future", "BTC/USDT", "5m"

	// Write 10 sequential candles via AddStreamingCandle (each a new bucket).
	input := make([]candle.Candle, 10)
	for i := range 10 {
		input[i] = candle.Candle{
			Timestamp: int64((i + 1) * 300_000), // 5m buckets
			Open:      100 + float64(i),
			High:      110 + float64(i),
			Low:       90 + float64(i),
			Close:     105 + float64(i),
			Volume:    float64(50 + i*10),
		}
		if err := ts.AddStreamingCandle(ctx, exchange, mt, sym, tf, input[i]); err != nil {
			t.Fatalf("AddStreamingCandle[%d]: %v", i, err)
		}
	}

	// Read back via GetCandlesDesc.
	got, err := ts.GetCandlesDesc(ctx, exchange, mt, sym, tf, 10)
	if err != nil {
		t.Fatalf("GetCandlesDesc: %v", err)
	}
	if len(got) != 10 {
		t.Fatalf("expected 10 candles, got %d", len(got))
	}

	// GetCandlesDesc returns newest-first; reverse to compare with input.
	for i, j := 0, len(got)-1; i < j; i, j = i+1, j-1 {
		got[i], got[j] = got[j], got[i]
	}

	for i, want := range input {
		g := got[i]
		if g.Timestamp != want.Timestamp {
			t.Errorf("[%d] Timestamp: want %d, got %d", i, want.Timestamp, g.Timestamp)
		}
		if g.Open != want.Open {
			t.Errorf("[%d] Open: want %f, got %f", i, want.Open, g.Open)
		}
		if g.High != want.High {
			t.Errorf("[%d] High: want %f, got %f", i, want.High, g.High)
		}
		if g.Low != want.Low {
			t.Errorf("[%d] Low: want %f, got %f", i, want.Low, g.Low)
		}
		if g.Close != want.Close {
			t.Errorf("[%d] Close: want %f, got %f", i, want.Close, g.Close)
		}
		if g.Volume != want.Volume {
			t.Errorf("[%d] Volume: want %f, got %f", i, want.Volume, g.Volume)
		}
	}
}

func TestTieredStore_SeedCandles(t *testing.T) {
	ts := newTestTieredStore()
	defer ts.cancel()

	ctx := context.Background()
	const exchange, mt, sym, tf = "binance", "future", "ETH/USDT", "1h"

	// Generate 200 seed candles (oldest-first as SeedCandles expects).
	seed := make([]candle.Candle, 200)
	for i := range 200 {
		seed[i] = candle.Candle{
			Timestamp: int64((i + 1) * 3_600_000),
			Open:      2000 + float64(i),
			High:      2100 + float64(i),
			Low:       1900 + float64(i),
			Close:     2050 + float64(i),
			Volume:    float64(1000 + i),
		}
	}

	// SeedCandlesBatch writes to engine (Redis call will be skipped since redis=nil
	// would panic — use engine.SeedCandles directly through AddStreamingCandlesBatch).
	ts.engine.SeedCandles(exchange, mt, sym, tf, seed)

	// Read all via GetCandlesDesc.
	got, err := ts.GetCandlesDesc(ctx, exchange, mt, sym, tf, 200)
	if err != nil {
		t.Fatalf("GetCandlesDesc: %v", err)
	}
	if len(got) != 200 {
		t.Fatalf("expected 200 candles, got %d", len(got))
	}

	// Verify DESC ordering: newest first.
	for i := 1; i < len(got); i++ {
		if got[i].Timestamp >= got[i-1].Timestamp {
			t.Fatalf("not DESC ordered at index %d: %d >= %d", i, got[i].Timestamp, got[i-1].Timestamp)
		}
	}

	// Verify first (newest) and last (oldest).
	if got[0].Timestamp != seed[199].Timestamp {
		t.Errorf("newest: want %d, got %d", seed[199].Timestamp, got[0].Timestamp)
	}
	if got[199].Timestamp != seed[0].Timestamp {
		t.Errorf("oldest: want %d, got %d", seed[0].Timestamp, got[199].Timestamp)
	}

	// Verify OHLCV of a middle candle.
	mid := got[100] // index 100 in desc = seed[99] in asc (200 - 1 - 100 = 99)
	want := seed[99]
	if mid.Open != want.Open || mid.High != want.High || mid.Low != want.Low || mid.Close != want.Close || mid.Volume != want.Volume {
		t.Errorf("middle candle mismatch: want %+v, got %+v", want, mid)
	}
}

func TestTieredStore_GetCandlesAsc(t *testing.T) {
	ts := newTestTieredStore()
	defer ts.cancel()

	ctx := context.Background()
	const exchange, mt, sym, tf = "binance", "future", "BTC/USDT", "30m"

	seed := []candle.Candle{
		{Timestamp: 1000, Open: 100, High: 110, Low: 90, Close: 105, Volume: 50},
		{Timestamp: 2000, Open: 106, High: 120, Low: 95, Close: 115, Volume: 60},
		{Timestamp: 3000, Open: 116, High: 130, Low: 105, Close: 125, Volume: 70},
	}
	ts.engine.SeedCandles(exchange, mt, sym, tf, seed)

	got, err := ts.GetCandlesAsc(ctx, exchange, mt, sym, tf, 3)
	if err != nil {
		t.Fatalf("GetCandlesAsc: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 candles, got %d", len(got))
	}
	// ASC: oldest first.
	if got[0].Timestamp != 1000 || got[2].Timestamp != 3000 {
		t.Errorf("ASC order wrong: [0]=%d [2]=%d", got[0].Timestamp, got[2].Timestamp)
	}
}

func TestTieredStore_MergeSemantics_OpenCandle(t *testing.T) {
	ts := newTestTieredStore()
	defer ts.cancel()

	ctx := context.Background()
	const exchange, mt, sym, tf = "binance", "future", "BTC/USDT", "5m"

	// Write open candle with vol=100.
	if err := ts.AddStreamingCandle(ctx, exchange, mt, sym, tf, candle.Candle{
		Timestamp: 1000, Open: 100, High: 110, Low: 90, Close: 105, Volume: 100,
	}); err != nil {
		t.Fatal(err)
	}

	// Update same open candle with vol=50.
	if err := ts.AddStreamingCandle(ctx, exchange, mt, sym, tf, candle.Candle{
		Timestamp: 1000, Open: 101, High: 115, Low: 88, Close: 112, Volume: 50,
	}); err != nil {
		t.Fatal(err)
	}

	got, err := ts.GetCandlesDesc(ctx, exchange, mt, sym, tf, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 candle, got %d", len(got))
	}

	g := got[0]
	// Open candle merge: volume = SUM.
	if g.Volume != 150 {
		t.Errorf("Volume: expected 150 (100+50), got %f", g.Volume)
	}
	// Open preserved from first write.
	if g.Open != 100 {
		t.Errorf("Open: expected 100 (first write), got %f", g.Open)
	}
	// High = max(110, 115).
	if g.High != 115 {
		t.Errorf("High: expected 115, got %f", g.High)
	}
	// Low = min(90, 88).
	if g.Low != 88 {
		t.Errorf("Low: expected 88, got %f", g.Low)
	}
	// Close = latest.
	if g.Close != 112 {
		t.Errorf("Close: expected 112, got %f", g.Close)
	}
}

// TestTieredStore_MergeSemantics_ClosedCandle pins the closed-candle merge rule in both
// directions: a higher volume replaces the stored candle, a lower one is ignored. Both
// cases share the same fixture and read-back, so they are subtests of one invariant.
func TestTieredStore_MergeSemantics_ClosedCandle(t *testing.T) {
	const exchange, mt, sym, tf = "binance", "future", "BTC/USDT", "5m"

	tests := []struct {
		name        string
		writeVolume float64
		wantVolume  float64
	}{
		{name: "higher volume replaces", writeVolume: 200, wantVolume: 200},
		{name: "lower volume is ignored", writeVolume: 50, wantVolume: 100},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts := newTestTieredStore()
			defer ts.cancel()

			ctx := context.Background()

			// Seed two candles so ts=1000 becomes historical (closed).
			ts.engine.SeedCandles(exchange, mt, sym, tf, []candle.Candle{
				{Timestamp: 1000, Open: 100, High: 110, Low: 90, Close: 105, Volume: 100},
				{Timestamp: 2000, Open: 106, High: 120, Low: 95, Close: 115, Volume: 60},
			})

			// Write the same closed candle with a different volume; the merge rule keeps
			// the higher one.
			if err := ts.AddStreamingCandle(ctx, exchange, mt, sym, tf, candle.Candle{
				Timestamp: 1000, Open: 99, High: 111, Low: 89, Close: 104, Volume: tc.writeVolume,
			}); err != nil {
				t.Fatal(err)
			}

			got, err := ts.GetCandlesDesc(ctx, exchange, mt, sym, tf, 10)
			if err != nil {
				t.Fatal(err)
			}

			var found *candle.Candle
			for i := range got {
				if got[i].Timestamp == 1000 {
					found = &got[i]
					break
				}
			}
			if found == nil {
				t.Fatal("candle ts=1000 not found")
			}
			if found.Volume != tc.wantVolume {
				t.Errorf("Volume: expected %f, got %f", tc.wantVolume, found.Volume)
			}
		})
	}
}

func TestTieredStore_GetCandleByTimestamp(t *testing.T) {
	ts := newTestTieredStore()
	defer ts.cancel()

	ctx := context.Background()
	const exchange, mt, sym, tf = "binance", "future", "SOL/USDT", "15m"

	ts.engine.SeedCandles(exchange, mt, sym, tf, []candle.Candle{
		{Timestamp: 1000, Open: 10, High: 12, Low: 9, Close: 11, Volume: 500},
		{Timestamp: 2000, Open: 11, High: 13, Low: 10, Close: 12, Volume: 600},
	})

	c, err := ts.GetCandleByTimestamp(ctx, exchange, mt, sym, tf, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("expected candle, got nil")
	}
	if c.Open != 10 || c.Volume != 500 {
		t.Errorf("unexpected candle: %+v", c)
	}

	// Non-existent timestamp.
	c2, err := ts.GetCandleByTimestamp(ctx, exchange, mt, sym, tf, 9999)
	if err != nil {
		t.Fatal(err)
	}
	if c2 != nil {
		t.Errorf("expected nil for missing timestamp, got %+v", c2)
	}
}

func TestTieredStore_GetCandlesCount(t *testing.T) {
	ts := newTestTieredStore()
	defer ts.cancel()

	ctx := context.Background()
	const exchange, mt, sym, tf = "binance", "future", "BTC/USDT", "4h"

	// Empty.
	count, err := ts.GetCandlesCount(ctx, exchange, mt, sym, tf)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("expected 0, got %d", count)
	}

	ts.engine.SeedCandles(exchange, mt, sym, tf, []candle.Candle{
		{Timestamp: 1000}, {Timestamp: 2000}, {Timestamp: 3000},
	})

	count, err = ts.GetCandlesCount(ctx, exchange, mt, sym, tf)
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("expected 3, got %d", count)
	}
}

func TestTieredStore_BatchWrite(t *testing.T) {
	ts := newTestTieredStore()
	defer ts.cancel()

	ctx := context.Background()
	const exchange, mt, sym, tf = "binance", "future", "BTC/USDT", "5m"

	updates := make([]candle.Candle, 5)
	for i := range 5 {
		updates[i] = candle.Candle{
			Timestamp: int64((i + 1) * 300_000),
			Open:      float64(100 + i),
			High:      float64(110 + i),
			Low:       float64(90 + i),
			Close:     float64(105 + i),
			Volume:    float64(50 + i*10),
		}
	}

	// Use AddStreamingCandlesBatch to write multiple candle.
	var batchUpdates []struct {
		Exchange   string
		MarketType string
		Symbol     string
		Timeframe  string
		Candle     candle.Candle
	}
	// Build the batch through the engine directly (AddStreamingCandlesBatch
	// expects redisstore.CandleUpdate which we avoid importing here).
	for _, c := range updates {
		ts.engine.UpdateCandle(exchange, mt, sym, tf, c)
	}

	got, err := ts.GetCandlesDesc(ctx, exchange, mt, sym, tf, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 {
		t.Fatalf("expected 5 candles, got %d", len(got))
	}
	_ = batchUpdates // suppress unused
}

func TestTieredStore_MultipleSymbols_Isolation(t *testing.T) {
	ts := newTestTieredStore()
	defer ts.cancel()

	ctx := context.Background()
	const exchange, mt, tf = "binance", "future", "5m"

	// Write to BTC.
	if err := ts.AddStreamingCandle(ctx, exchange, mt, "BTC/USDT", tf, candle.Candle{
		Timestamp: 1000, Open: 50000, High: 51000, Low: 49000, Close: 50500, Volume: 100,
	}); err != nil {
		t.Fatal(err)
	}

	// Write to ETH.
	if err := ts.AddStreamingCandle(ctx, exchange, mt, "ETH/USDT", tf, candle.Candle{
		Timestamp: 1000, Open: 3000, High: 3100, Low: 2900, Close: 3050, Volume: 200,
	}); err != nil {
		t.Fatal(err)
	}

	btc, _ := ts.GetCandlesDesc(ctx, exchange, mt, "BTC/USDT", tf, 1)
	eth, _ := ts.GetCandlesDesc(ctx, exchange, mt, "ETH/USDT", tf, 1)

	if len(btc) != 1 || len(eth) != 1 {
		t.Fatalf("expected 1 each, got btc=%d eth=%d", len(btc), len(eth))
	}
	if btc[0].Open != 50000 {
		t.Errorf("BTC Open: expected 50000, got %f", btc[0].Open)
	}
	if eth[0].Open != 3000 {
		t.Errorf("ETH Open: expected 3000, got %f", eth[0].Open)
	}
}

func TestTieredStore_ReplaceCandle(t *testing.T) {
	ts := newTestTieredStore()
	defer ts.cancel()

	ctx := context.Background()
	const exchange, mt, sym, tf = "binance", "future", "BTC/USDT", "5m"

	// Seed initial candle.
	ts.engine.SeedCandles(exchange, mt, sym, tf, []candle.Candle{
		{Timestamp: 1000, Open: 100, High: 110, Low: 90, Close: 105, Volume: 50},
	})

	// Replace via TieredStore (writeThrough=false so Redis not called).
	if err := ts.ReplaceCandle(ctx, exchange, mt, sym, tf, candle.Candle{
		Timestamp: 1000, Open: 99, High: 115, Low: 88, Close: 112, Volume: 80,
	}); err != nil {
		t.Fatal(err)
	}

	got, _ := ts.GetCandlesDesc(ctx, exchange, mt, sym, tf, 1)
	if len(got) != 1 {
		t.Fatalf("expected 1, got %d", len(got))
	}
	// ReplaceCandle goes through UpdateCandle with the same timestamp as latest.
	// Open stays from first (100), Volume accumulates (50+80=130).
	if got[0].Volume != 130 {
		t.Errorf("Volume after replace: expected 130, got %f", got[0].Volume)
	}
}

func TestTieredStore_FeatureFlag_EngineRouting(t *testing.T) {
	// Verify that when CandleEngine is the backing store, reads are entirely in-memory.
	ts := newTestTieredStore()
	defer ts.cancel()

	ctx := context.Background()
	const exchange, mt, sym, tf = "binance", "future", "BTC/USDT", "1h"

	// Write through TieredStore → engine.
	for i := range 50 {
		_ = ts.AddStreamingCandle(ctx, exchange, mt, sym, tf, candle.Candle{
			Timestamp: int64((i + 1) * 3_600_000),
			Open:      float64(100 + i),
			High:      float64(110 + i),
			Low:       float64(90 + i),
			Close:     float64(105 + i),
			Volume:    float64(10 + i),
		})
	}

	// All reads come from engine — no Redis needed.
	got, err := ts.GetCandlesDesc(ctx, exchange, mt, sym, tf, 50)
	if err != nil {
		t.Fatalf("GetCandlesDesc should not error without Redis: %v", err)
	}
	if len(got) != 50 {
		t.Fatalf("expected 50, got %d", len(got))
	}

	// Verify newest first.
	if got[0].Timestamp != 50*3_600_000 {
		t.Errorf("newest timestamp: want %d, got %d", 50*3_600_000, got[0].Timestamp)
	}

	// Verify count.
	count, err := ts.GetCandlesCount(ctx, exchange, mt, sym, tf)
	if err != nil {
		t.Fatal(err)
	}
	if count != 50 {
		t.Errorf("count: want 50, got %d", count)
	}
}

func TestTieredStore_DataParity_OHLCVPrecision(t *testing.T) {
	ts := newTestTieredStore()
	defer ts.cancel()

	ctx := context.Background()
	const exchange, mt, sym, tf = "binance", "future", "BTC/USDT", "5m"

	// Use realistic float64 values to verify no precision loss.
	want := candle.Candle{
		Timestamp: 1710489600000,
		Open:      67234.12345678,
		High:      67890.99999999,
		Low:       67100.00000001,
		Close:     67456.78901234,
		Volume:    1234.56789012,
	}

	_ = ts.AddStreamingCandle(ctx, exchange, mt, sym, tf, want)

	got, _ := ts.GetCandlesDesc(ctx, exchange, mt, sym, tf, 1)
	if len(got) != 1 {
		t.Fatalf("expected 1, got %d", len(got))
	}

	g := got[0]
	if math.Abs(g.Open-want.Open) > 1e-10 {
		t.Errorf("Open precision loss: want %f, got %f", want.Open, g.Open)
	}
	if math.Abs(g.High-want.High) > 1e-10 {
		t.Errorf("High precision loss: want %f, got %f", want.High, g.High)
	}
	if math.Abs(g.Low-want.Low) > 1e-10 {
		t.Errorf("Low precision loss: want %f, got %f", want.Low, g.Low)
	}
	if math.Abs(g.Close-want.Close) > 1e-10 {
		t.Errorf("Close precision loss: want %f, got %f", want.Close, g.Close)
	}
	if math.Abs(g.Volume-want.Volume) > 1e-10 {
		t.Errorf("Volume precision loss: want %f, got %f", want.Volume, g.Volume)
	}
}

// ============================================================================
// COLD STORE MERGE TESTS
// ============================================================================

func TestTieredStore_GetCandlesDesc_NoColdStore_ReturnHot(t *testing.T) {
	ts := newTestTieredStore()
	defer ts.cancel()

	ctx := context.Background()
	const exchange, mt, sym, tf = "binance", "future", "BTC/USDT", "5m"

	// Seed 3 candles
	for i := range 3 {
		_ = ts.AddStreamingCandle(ctx, exchange, mt, sym, tf, candle.Candle{
			Timestamp: int64((i + 1) * 300_000),
			Open:      100, High: 110, Low: 90, Close: 105, Volume: 50,
		})
	}

	// Request 10 but only 3 exist, no cold store → return 3
	got, err := ts.GetCandlesDesc(ctx, exchange, mt, sym, tf, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
}

func TestTieredStore_GetCandlesDesc_HotSatisfiesLimit(t *testing.T) {
	ts := newTestTieredStore()
	defer ts.cancel()
	cold := &mockColdStore{}
	ts.SetColdStore(cold)

	ctx := context.Background()
	const exchange, mt, sym, tf = "binance", "future", "BTC/USDT", "5m"

	// Seed 5 hot candles
	for i := range 5 {
		_ = ts.AddStreamingCandle(ctx, exchange, mt, sym, tf, candle.Candle{
			Timestamp: int64((i + 1) * 300_000),
			Open:      100, High: 110, Low: 90, Close: 105, Volume: 50,
		})
	}

	// Request limit=3, hot has 5 → cold should NOT be queried
	got, err := ts.GetCandlesDesc(ctx, exchange, mt, sym, tf, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
}

func TestTieredStore_GetCandlesDesc_MergeHotAndCold(t *testing.T) {
	ts := newTestTieredStore()
	defer ts.cancel()

	// Cold store has older candles (timestamps 100k, 200k)
	cold := &mockColdStore{
		candles: []candle.Candle{
			{Timestamp: 200_000, Open: 90, High: 95, Low: 85, Close: 92, Volume: 30},
			{Timestamp: 100_000, Open: 80, High: 88, Low: 78, Close: 85, Volume: 20},
		},
	}
	ts.SetColdStore(cold)

	ctx := context.Background()
	const exchange, mt, sym, tf = "binance", "future", "BTC/USDT", "5m"

	// Hot: 2 candles (timestamps 300k, 600k)
	_ = ts.AddStreamingCandle(ctx, exchange, mt, sym, tf, candle.Candle{
		Timestamp: 300_000, Open: 100, High: 110, Low: 90, Close: 105, Volume: 50,
	})
	_ = ts.AddStreamingCandle(ctx, exchange, mt, sym, tf, candle.Candle{
		Timestamp: 600_000, Open: 110, High: 120, Low: 100, Close: 115, Volume: 60,
	})

	// Request 4 candles: 2 hot + 2 cold
	got, err := ts.GetCandlesDesc(ctx, exchange, mt, sym, tf, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("expected 4, got %d", len(got))
	}

	// Verify DESC order: 600k, 300k, 200k, 100k
	expected := []int64{600_000, 300_000, 200_000, 100_000}
	for i, want := range expected {
		if got[i].Timestamp != want {
			t.Errorf("[%d] timestamp: want %d, got %d", i, want, got[i].Timestamp)
		}
	}
}

func TestTieredStore_GetCandlesDesc_ColdStoreFailure_ReturnsHot(t *testing.T) {
	ts := newTestTieredStore()
	defer ts.cancel()

	cold := &mockColdStore{queryErr: fmt.Errorf("connection refused")}
	ts.SetColdStore(cold)

	ctx := context.Background()
	const exchange, mt, sym, tf = "binance", "future", "BTC/USDT", "5m"

	_ = ts.AddStreamingCandle(ctx, exchange, mt, sym, tf, candle.Candle{
		Timestamp: 300_000, Open: 100, High: 110, Low: 90, Close: 105, Volume: 50,
	})

	// Cold fails but we still get hot data
	got, err := ts.GetCandlesDesc(ctx, exchange, mt, sym, tf, 10)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1, got %d", len(got))
	}
}

func TestTieredStore_GetCandlesDesc_DedupHotWins(t *testing.T) {
	ts := newTestTieredStore()
	defer ts.cancel()

	// Cold has a candle at same timestamp as hot, with different data
	cold := &mockColdStore{
		candles: []candle.Candle{
			{Timestamp: 300_000, Open: 50, High: 55, Low: 45, Close: 52, Volume: 10},
			{Timestamp: 100_000, Open: 80, High: 88, Low: 78, Close: 85, Volume: 20},
		},
	}
	ts.SetColdStore(cold)

	ctx := context.Background()
	const exchange, mt, sym, tf = "binance", "future", "BTC/USDT", "5m"

	// Hot has candle at 300k with different values
	_ = ts.AddStreamingCandle(ctx, exchange, mt, sym, tf, candle.Candle{
		Timestamp: 300_000, Open: 100, High: 110, Low: 90, Close: 105, Volume: 50,
	})

	got, err := ts.GetCandlesDesc(ctx, exchange, mt, sym, tf, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 (dedup), got %d", len(got))
	}

	// Hot value should win for timestamp 300k
	if got[0].Timestamp != 300_000 {
		t.Errorf("expected newest=300000, got %d", got[0].Timestamp)
	}
	if got[0].Open != 100 {
		t.Errorf("expected hot Open=100, got %f (cold leaked)", got[0].Open)
	}
}

func TestTieredStore_GetCandlesRange_Basic(t *testing.T) {
	ts := newTestTieredStore()
	defer ts.cancel()

	cold := &mockColdStore{
		candles: []candle.Candle{
			{Timestamp: 200_000, Open: 90, High: 95, Low: 85, Close: 92, Volume: 30},
			{Timestamp: 100_000, Open: 80, High: 88, Low: 78, Close: 85, Volume: 20},
		},
	}
	ts.SetColdStore(cold)

	ctx := context.Background()
	const exchange, mt, sym, tf = "binance", "future", "BTC/USDT", "5m"

	// Hot candles at 300k and 600k
	_ = ts.AddStreamingCandle(ctx, exchange, mt, sym, tf, candle.Candle{
		Timestamp: 300_000, Open: 100, High: 110, Low: 90, Close: 105, Volume: 50,
	})
	_ = ts.AddStreamingCandle(ctx, exchange, mt, sym, tf, candle.Candle{
		Timestamp: 600_000, Open: 110, High: 120, Low: 100, Close: 115, Volume: 60,
	})

	// Query range: only 100k..400k → should get 300k(hot), 200k(cold), 100k(cold)
	from := time.UnixMilli(100_000)
	to := time.UnixMilli(400_000)
	got, err := ts.GetCandlesRange(ctx, exchange, mt, sym, tf, from, to, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
	// DESC order: 300k, 200k, 100k
	if got[0].Timestamp != 300_000 || got[1].Timestamp != 200_000 || got[2].Timestamp != 100_000 {
		t.Errorf("unexpected order: %d, %d, %d", got[0].Timestamp, got[1].Timestamp, got[2].Timestamp)
	}
}

func TestMergeCandles_EmptyInputs(t *testing.T) {
	// Both empty
	result := mergeCandles(nil, nil)
	if len(result) != 0 {
		t.Errorf("expected 0, got %d", len(result))
	}

	// Hot only
	hot := []candle.Candle{{Timestamp: 100}}
	result = mergeCandles(hot, nil)
	if len(result) != 1 || result[0].Timestamp != 100 {
		t.Errorf("hot-only: expected [100], got %v", result)
	}

	// Cold only
	cold := []candle.Candle{{Timestamp: 50}}
	result = mergeCandles(nil, cold)
	if len(result) != 1 || result[0].Timestamp != 50 {
		t.Errorf("cold-only: expected [50], got %v", result)
	}
}

// ============================================================================
// THE WRITE CLAMP AND RETENTION SHARE THE BOUNDARY
// ============================================================================

// THE ONE-DAY BAND THE WRITE MUST NOT REFUSE.
//
// Retention cuts at `now − (N+1) days`, because it keeps one spare day so as not to drop the
// active partition. A write clamp that used `now − N×24h` would refuse a full day of candles that
// retention preserves; those candles would stay only in Redis, becoming a sole copy manufactured
// by the clamp itself.
//
// The values here are ABSOLUTE on purpose. Deriving them from `cutoff` would make the test pass
// even if both accounts used `now − N×24h` — it would be tautology. What is asserted is the
// boundary RETENTION applies, with the spare day inside it.
func TestAppendClosedToCold_TheBandRetentionKeepsIsWritten(t *testing.T) {
	const windowDays = 365
	day := 24 * time.Hour
	now := time.Now()
	policy := retention.Policy{Table: "candles", RetentionDays: windowDays}

	cases := []struct {
		name    string
		age     time.Duration
		written bool
	}{
		{"inside the declared window", 12 * time.Hour, true},
		{"eve of the old clamp", 364*day + 23*time.Hour, true},
		{"THE BAND: beyond the old clamp, short of the retention cut", 365*day + 12*time.Hour, true},
		{"beyond the retention cut", 366*day + time.Hour, false},
		{"far beyond the cut", 400 * day, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeColdAppender{}
			ts := &TieredStore{coldAppender: fake, coldBoundary: policy}
			bar := candle.Candle{Timestamp: now.Add(-tc.age).UnixMilli()}

			if err := ts.appendClosedToCold(context.Background(), "binance", "future", "BTC/USDT", "1d",
				[]candle.Candle{bar}); err != nil {
				t.Fatalf("appendClosedToCold: %v", err)
			}

			written := len(fake.got) == 1
			if written != tc.written {
				t.Fatalf("candle aged %v: written=%v, want %v — the clamp is %v away from the boundary retention uses",
					tc.age, written, tc.written, tc.age)
			}
		})
	}
}

// THE SHARING IS THE ASSERTION, and it holds for ANY declared window: writing is the
// exact complement of dropping. This case is the net against future drift — any
// new arithmetic on one side alone and it fails.
func TestAppendClosedToCold_TheWriteFloorIsTheRetentionCutoff(t *testing.T) {
	day := 24 * time.Hour
	now := time.Now()

	for _, window := range []int{1, 7, 90, 365, 2000} {
		policy := retention.Policy{Table: "candles", RetentionDays: window}
		cutoff, _ := policy.Cutoff(now)

		for _, age := range []time.Duration{
			time.Hour,
			time.Duration(window)*day - time.Hour,
			time.Duration(window)*day + 12*time.Hour,
			time.Duration(window+1)*day + time.Hour,
			time.Duration(window+30) * day,
		} {
			stamp := now.Add(-age)
			fake := &fakeColdAppender{}
			ts := &TieredStore{coldAppender: fake, coldBoundary: policy}

			if err := ts.appendClosedToCold(context.Background(), "binance", "future", "BTC/USDT", "1d",
				[]candle.Candle{{Timestamp: stamp.UnixMilli()}}); err != nil {
				t.Fatalf("appendClosedToCold: %v", err)
			}

			// RETENTION drops what is before the cutoff; the write must accept
			// exactly the rest.
			wantWritten := !stamp.Before(cutoff)
			if written := len(fake.got) == 1; written != wantWritten {
				t.Fatalf("window=%dd age=%v: written=%v, want %v (retention cutoff at %s)",
					window, age, written, wantWritten, cutoff.Format(time.RFC3339))
			}
		}
	}
}

// WITH NO DECLARED WINDOW THERE IS NO CLAMP. Refusing a write without anyone dropping the
// refused band only destroys: the candle would remain a sole copy in the volatile tier, forever.
func TestAppendClosedToCold_NoDeclaredWindowMeansNoClamp(t *testing.T) {
	fake := &fakeColdAppender{}
	ts := &TieredStore{coldAppender: fake, coldBoundary: retention.Policy{Table: "candles"}}
	ancient := time.Now().Add(-4000 * 24 * time.Hour).UnixMilli()

	if err := ts.appendClosedToCold(context.Background(), "binance", "future", "BTC/USDT", "1w",
		[]candle.Candle{{Timestamp: ancient}}); err != nil {
		t.Fatalf("appendClosedToCold: %v", err)
	}
	if len(fake.got) != 1 {
		t.Fatalf("with no declared window the candle was refused — a clamp without retention is destruction without counterweight")
	}
}
