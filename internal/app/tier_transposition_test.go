package app

import (
	"context"
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/questdb"
	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/candle/engine"
	"github.com/marxms/go-whale/internal/platform/config"
	"github.com/marxms/go-whale/internal/retention"

	"github.com/alicebob/miniredis/v2"
)

// TIER TRANSPOSITION — the invariant the Binance parity gate structurally cannot see.
//
// # WHY A SECOND GATE EXISTS AT ALL
//
// internal/parity compares /api/timeseries against Binance. That endpoint is a MERGED read:
// TieredStore.GetCandlesDesc unions hot and cold and mergeCandles resolves every duplicate
// timestamp in favor of HOT. A cold row that is wrong is therefore SHADOWED by a correct hot
// row, and the comparison passes. The measured case: BTC/USDT 5m bucket 01:05 held engine
// 279,98300 and QuestDB 279,93100 — a bucket the engine had repaired in RAM and told nobody
// about — and the whole live harness was green for as long as it existed.
//
// So this gate never looks at the merged view. It reads the THREE tiers SEPARATELY and
// requires them to agree bucket by bucket:
//
//	ring (CandleEngine)  ==  Redis (hot, durable)  ==  QuestDB (cold, durable)
//
// A defect that corrects one tier and not the others has nowhere to hide in that formulation,
// which is precisely what "the engine fixed it and told nobody" is.
//
// # WHY IT IS ORGANIC AND NOT A RECONCILIATION JOB
//
// It asserts a property of paths that ALREADY run — the close callback, the revision
// callback, the seed fold. It introduces no goroutine, no sweep, no schedule. The production
// code is untouched; only the wiring is instantiated here, with fakes at the two edges that
// need a server (miniredis for the hot tier, an in-memory ColdStore for the cold one), so the
// gate runs in `go test ./...` with no stack alive.
//
// # WHAT IS COMPARED, AND WHAT IS DELIBERATELY NOT
//
// Only the seven fields all three tiers actually carry: OHLC, volume and the taker pair. The
// cold tier is columnar and stores no local bookkeeping (CreatedAt, LastUpdate, Source,
// LateTradeCount), so requiring those to match would be requiring the tiers to store
// something they do not. They are the fields a consumer reads, and they are the fields the
// drift showed up in.
//
// # WHICH BUCKETS ARE SUBJECT TO IT
//
// Every bucket whose window HAS ELAPSED. Not "every bucket except the newest": an elapsed
// bucket that the ring holds and the durable tiers do not is the same defect wearing a
// different hat — the engine knows something it never published.

// ============================================================================
// FAKES
// ============================================================================

// fakeColdStore is an in-memory candle.ColdStore with QuestDB's DEDUP UPSERT semantics: a
// row written twice for the same (key, timestamp) is REPLACED, not duplicated. That is the
// property every cold write in this system relies on, so a fake that appended instead would
// make idempotent re-writes look like corruption.
//
// It stores the PROJECTION of the candle the cold table actually has columns for. A fake that
// round-tripped the whole struct would let a comparison of, say, Source pass here and fail in
// production, where the column does not exist.
func appSeriesKey(exchange, marketType, symbol, timeframe string) candle.SeriesKey {
	key, err := candle.SeriesKeyOf(exchange, marketType, symbol, timeframe)
	if err != nil {
		panic(err)
	}
	return key
}

type fakeColdStore struct {
	rows map[candle.SeriesKey]map[int64]candle.Candle
}

func newFakeColdStore() *fakeColdStore {
	return &fakeColdStore{rows: make(map[candle.SeriesKey]map[int64]candle.Candle)}
}

func (f *fakeColdStore) WriteBatches(_ context.Context, groups map[candle.SeriesKey][]candle.Candle) error {
	for key, batch := range groups {
		if f.rows[key] == nil {
			f.rows[key] = make(map[int64]candle.Candle)
		}
		for _, c := range batch {
			f.rows[key][c.Timestamp] = coldProjection(c)
		}
	}
	return nil
}

func (f *fakeColdStore) QueryRange(_ context.Context, exchange, marketType, symbol, timeframe string, from, to time.Time, limit int) ([]candle.Candle, error) {
	key := appSeriesKey(exchange, marketType, symbol, timeframe)
	var out []candle.Candle
	for _, c := range f.rows[key] {
		ts := time.UnixMilli(c.Timestamp)
		if !from.IsZero() && ts.Before(from) {
			continue
		}
		if !to.IsZero() && ts.After(to) {
			continue
		}
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp > out[j].Timestamp }) // DESC, like QuestDB
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeColdStore) GetOldestTimestamp(_ context.Context, _, _, _, _ string) (time.Time, error) {
	return time.Time{}, nil
}

func (f *fakeColdStore) Close() error                 { return nil }
func (f *fakeColdStore) Stats() candle.ColdStoreStats { return candle.ColdStoreStats{} }

// coldProjection keeps only what the cold table has a column for.
func coldProjection(c candle.Candle) candle.Candle {
	return candle.Candle{
		Timestamp: c.Timestamp, Open: c.Open, High: c.High, Low: c.Low, Close: c.Close,
		Volume: c.Volume, TakerBuyVolume: c.TakerBuyVolume, TakerSellVolume: c.TakerSellVolume,
	}
}

// ============================================================================
// HARNESS
// ============================================================================

// transpositionRig is the production wiring with fakes only at the two edges that need a
// server. It registers the SAME callbacks NewApp registers (app.go:204-205); registering
// anything else here would test a system that does not exist.
type transpositionRig struct {
	engine  *engine.CandleEngine
	redis   *redisstore.RedisStore
	tiered  *engine.TieredStore
	cold    *fakeColdStore
	pipe    *questdb.Pipeline
	stopped bool
}

func newTranspositionRig(t *testing.T) *transpositionRig {
	t.Helper()

	mr := miniredis.RunT(t)
	cfg := config.Config{
		RedisAddr:          mr.Addr(),
		RedisPoolSize:      10,
		RedisDialTimeoutS:  1,
		RedisReadTimeoutS:  1,
		RedisWriteTimeoutS: 1,
		RedisPoolTimeoutS:  1,
		RedisMaxRetries:    3,
	}
	tfReg := candle.NewTimeframeRegistry([]string{"1m", "5m", "15m", "1h"})
	redisStore, err := redisstore.NewRedisStore(t.Context(), cfg, 500, tfReg)
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}

	ce := engine.NewCandleEngine(map[string]int{"default": 500})
	tiered := engine.NewTieredStore(ce, redisStore, config.CandleEngineConfig{
		Enabled:           true,
		RedisWriteThrough: true, // WriteClosedToHot is gated on it — this is the production value
		// Long enough that the periodic snapshot never fires during a test. It is inert anyway
		// (EnableSnapshots is deliberately not called), but a test must not depend on a timer.
		SnapshotIntervalMS: int(time.Hour / time.Millisecond),
	})

	cold := newFakeColdStore()
	// A real pipeline over a fake store: the batching, grouping and drain-on-Stop are the
	// production ones, so a regression in flushBatches is visible here too.
	pipe := questdb.NewPipeline(cold, 4096, time.Hour)
	pipe.Start()
	tiered.SetColdStore(cold)
	tiered.SetColdAppender(pipe, retention.Policy{})

	app := &App{
		tieredStore:  tiered,
		candleEngine: ce,
		qdbPipeline:  pipe,
		redisStore:   redisStore,
		// realtimeProducer / hub / gapRecovery stay nil: onCandleClose nil-checks each one and
		// they publish nothing durable. The three tiers are the whole subject here.
	}
	ce.OnCandleClose(app.onCandleClose)
	ce.OnCandleRevised(app.onCandleRevised)
	ce.OnSeedCandleRevised(app.onSeedCandleRevised)

	rig := &transpositionRig{engine: ce, redis: redisStore, tiered: tiered, cold: cold, pipe: pipe}
	t.Cleanup(rig.stop)
	return rig
}

func TestSeedAncestorRepairAdmittedOnceAndReachesHotAndCold(t *testing.T) {
	r := newTranspositionRig(t)
	const ex, market, symbol = "binance", "future", "XPL/USDT"
	r.engine.RegisterDerivedScope(ex, market, "5m")
	base := candle.BucketTimestamp(time.Now().Add(-4*time.Hour).UnixMilli(), "30m")
	old := candle.Candle{Timestamp: base, Open: 1, High: 1, Low: 1, Close: 1, Volume: 10, TakerBuyVolume: 6, TakerSellVolume: 4, Source: string(redisstore.CandleMergeSourceWSKline)}
	r.engine.LoadStoredCandles(ex, market, symbol, "30m", []candle.Candle{old})
	tail := old
	tail.Timestamp += 15 * 60 * 1000
	r.engine.UpdateCandle(ex, market, symbol, "15m", tail)
	early := old
	early.Source, early.FetchedAt = string(redisstore.CandleMergeSourceRESTTotal), time.Now().UnixMilli()
	if err := r.tiered.SeedCandlesBatch(context.Background(), ex, market, symbol, "15m", []candle.Candle{early}); err != nil {
		t.Fatal(err)
	}
	r.settle()
	key := appSeriesKey(ex, market, symbol, "30m")
	hot, err := r.redis.GetCandlesDesc(context.Background(), ex, market, symbol, "30m", 10)
	if err != nil || len(hot) != 1 || hot[0].Volume != 20 {
		t.Fatalf("hot parent: %v err=%v", hot, err)
	}
	if c, ok := r.cold.rows[key][base]; !ok || c.Volume != 20 {
		t.Fatalf("cold parent: %+v present=%v", c, ok)
	}
	if got := r.pipe.Stats().Flushed; got != 2 {
		t.Fatalf("expected one parent and one child admission, got %d", got)
	}
}

// settle drains the cold pipeline so the assertion reads a quiesced system. Stop() flushes
// whatever is buffered, which is what makes this deterministic without a single sleep.
func (r *transpositionRig) settle() {
	if !r.stopped {
		r.pipe.Stop(context.Background())
		r.stopped = true
	}
}

func (r *transpositionRig) stop() {
	r.settle()
	_ = r.tiered.Close()
}

// ============================================================================
// THE ASSERTION
// ============================================================================

type tierValues struct {
	ring, hot, cold *candle.Candle
}

// assertTiersTransposed is the gate. For EVERY key the engine holds and EVERY bucket whose
// window has elapsed, the three tiers must carry the same seven values.
func assertTiersTransposed(t *testing.T, r *transpositionRig, nowMs int64) {
	t.Helper()
	r.settle()

	ctx := context.Background()
	failures := 0

	for _, key := range r.engine.BufferKeys() {
		tfMs, ok := candle.ParseTimeframeMs(key.Timeframe)
		if !ok {
			t.Fatalf("unparseable timeframe %q on key %+v — the gate cannot decide which buckets elapsed", key.Timeframe, key)
		}

		byTS := map[int64]*tierValues{}
		coldKey := appSeriesKey(key.Exchange, key.MarketType, key.Symbol, key.Timeframe)

		ring := r.engine.GetCandlesAsc(key.Exchange, key.MarketType, key.Symbol, key.Timeframe, 0)
		for i := range ring {
			c := ring[i]
			byTS[c.Timestamp] = &tierValues{ring: &c}
		}

		hot, err := r.redis.GetCandlesDesc(ctx, key.Exchange, key.MarketType, key.Symbol, key.Timeframe, 1000)
		if err != nil {
			t.Fatalf("hot read %+v: %v", key, err)
		}
		for i := range hot {
			c := hot[i]
			slot := byTS[c.Timestamp]
			if slot == nil {
				slot = &tierValues{}
				byTS[c.Timestamp] = slot
			}
			slot.hot = &c
		}

		for ts, c := range r.cold.rows[coldKey] {
			slot := byTS[ts]
			if slot == nil {
				slot = &tierValues{}
				byTS[ts] = slot
			}
			cc := c
			slot.cold = &cc
		}

		timestamps := make([]int64, 0, len(byTS))
		for ts := range byTS {
			timestamps = append(timestamps, ts)
		}
		sort.Slice(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] })

		elapsed := 0
		for _, ts := range timestamps {
			if ts+tfMs > nowMs {
				continue // still open: the tiers are allowed to disagree while trades keep arriving
			}
			elapsed++
			v := byTS[ts]
			bucket := fmt.Sprintf("%s:%s:%s:%s bucket %s",
				key.Exchange, key.MarketType, key.Symbol, key.Timeframe,
				time.UnixMilli(ts).UTC().Format(time.RFC3339))

			if v.ring == nil || v.hot == nil || v.cold == nil {
				failures++
				t.Errorf("%s PRESENCE: ring=%v hot=%v cold=%v — an elapsed bucket present in one "+
					"tier and absent from another is the same defect as a divergent value: something "+
					"knows about this bucket and never published it",
					bucket, v.ring != nil, v.hot != nil, v.cold != nil)
				continue
			}

			for _, f := range comparedFields(*v.ring, *v.hot, *v.cold) {
				if f.ring == f.hot && f.hot == f.cold {
					continue
				}
				failures++
				t.Errorf("%s FIELD %s: ring=%.8f hot=%.8f cold=%.8f (hot-ring=%+.8f cold-ring=%+.8f) — "+
					"the tiers disagree on a CLOSED bucket. The merged read (/api/timeseries) resolves "+
					"duplicates in favor of hot, so this is invisible to the Binance parity gate",
					bucket, f.name, f.ring, f.hot, f.cold, f.hot-f.ring, f.cold-f.ring)
			}
		}

		if elapsed == 0 {
			t.Errorf("%+v: ZERO elapsed buckets compared — an empty sample is a harness failure, "+
				"not a pass", key)
		}
	}

	if len(r.engine.BufferKeys()) == 0 {
		t.Fatal("engine holds no keys — the gate compared nothing")
	}
	if failures > 0 {
		t.Errorf("TRANSPOSITION FAILED: %d field/presence divergence(s) across the three tiers", failures)
	}
}

type fieldTriple struct {
	name            string
	ring, hot, cold float64
}

// comparedFields is the contract surface: the values every tier stores and every consumer
// reads. Exact equality, not a tolerance — these are the SAME float64 traveling through
// three encodings, not three independent measurements. Go's JSON encoding of float64
// round-trips exactly, and the cold projection is a struct copy, so any difference here is a
// real divergence, never noise.
func comparedFields(ring, hot, cold candle.Candle) []fieldTriple {
	return []fieldTriple{
		{"open", ring.Open, hot.Open, cold.Open},
		{"high", ring.High, hot.High, cold.High},
		{"low", ring.Low, hot.Low, cold.Low},
		{"close", ring.Close, hot.Close, cold.Close},
		{"volume", ring.Volume, hot.Volume, cold.Volume},
		{"takerBuyVolume", ring.TakerBuyVolume, hot.TakerBuyVolume, cold.TakerBuyVolume},
		{"takerSellVolume", ring.TakerSellVolume, hot.TakerSellVolume, cold.TakerSellVolume},
	}
}

// ============================================================================
// FIXTURE
// ============================================================================

const (
	rigExchange   = "binance"
	rigMarketType = "future"
	rigSymbol     = "BTC/USDT"
	rigTimeframe  = "5m"
)

// liveWindow drives the REAL live path: one ws_delta per bucket, in order, so each new
// timestamp closes the previous bucket through OnCandleClose exactly as a trade stream does.
//
// It always leaves the CURRENT bucket open, which is what a running process looks like: every
// elapsed bucket has therefore been through the close fan-out, and any elapsed bucket the
// durable tiers lack is a genuine defect rather than an artifact of stopping mid-bar.
//
// Returns the aligned bucket timestamps, oldest first, INCLUDING the still-open last one.
func liveWindow(t *testing.T, r *transpositionRig, nowMs int64, closedBuckets int) []int64 {
	t.Helper()
	tfMs, _ := candle.ParseTimeframeMs(rigTimeframe)
	current := (nowMs / tfMs) * tfMs

	ts := make([]int64, 0, closedBuckets+1)
	for i := closedBuckets; i >= 0; i-- {
		ts = append(ts, current-int64(i)*tfMs)
	}

	ctx := context.Background()
	for i, bucketTS := range ts {
		price := 100.0 + float64(i)
		c := candle.Candle{
			Timestamp:       bucketTS,
			Open:            price,
			High:            price + 2,
			Low:             price - 1,
			Close:           price + 1,
			Volume:          10 + float64(i),
			TakerBuyVolume:  6 + float64(i)/2,
			TakerSellVolume: 4 + float64(i)/2,
			Source:          string(redisstore.CandleMergeSourceWSDelta),
		}
		if err := r.tiered.AddStreamingCandle(ctx, rigExchange, rigMarketType, rigSymbol, rigTimeframe, c); err != nil {
			t.Fatalf("AddStreamingCandle: %v", err)
		}
	}
	return ts
}

// ============================================================================
// SCENARIOS
// ============================================================================

// TestTiersTransposed_LiveCloseStream is the baseline: nothing exotic, just buckets rolling.
// If this fails, the close fan-out itself is broken and everything below is noise.
func TestTiersTransposed_LiveCloseStream(t *testing.T) {
	r := newTranspositionRig(t)
	now := time.Now().UnixMilli()

	liveWindow(t, r, now, 6)

	assertTiersTransposed(t, r, now)
}

// TestTiersTransposed_LateTradeCorrectionReachesEveryTier is the one that matters, and it is
// the exact shape of the defect that survived the entire harness.
//
// A trade arriving after its bucket rolled is merged by mergeHistorical into the ring — 98
// occurrences in one production window. The ring becomes correct. Before OnCandleRevised
// existed, that was the end of it: the periodic snapshot only ever writes the NEWEST bucket
// per key and nothing else revisits a closed one, so Redis and QuestDB kept the
// pre-correction value forever (measured BTC/USDT 5m 01:05: engine 279,98300 / cold
// 279,93100), and the Binance gate passed anyway because the merged read served the ring.
//
// Unregister app.onCandleRevised and this test fails on volume and the taker pair. That is
// the whole point: it is the only formulation in the suite that can see it.
func TestTiersTransposed_LateTradeCorrectionReachesEveryTier(t *testing.T) {
	r := newTranspositionRig(t)
	now := time.Now().UnixMilli()

	buckets := liveWindow(t, r, now, 6)
	// A bucket that closed a while ago — not the one that just rolled.
	target := buckets[2]

	ctx := context.Background()
	late := candle.Candle{
		Timestamp:       target,
		Close:           123.75,
		Volume:          0.052,
		TakerBuyVolume:  0.031,
		TakerSellVolume: 0.021,
		Source:          string(redisstore.CandleMergeSourceWSDelta), // an increment, declared as one
	}
	if err := r.tiered.AddStreamingCandle(ctx, rigExchange, rigMarketType, rigSymbol, rigTimeframe, late); err != nil {
		t.Fatalf("late AddStreamingCandle: %v", err)
	}

	// The ring must actually have moved, otherwise the test would pass by comparing three
	// copies of an uncorrected value.
	got := r.engine.GetCandleByTimestamp(rigExchange, rigMarketType, rigSymbol, rigTimeframe, target)
	if got == nil {
		t.Fatalf("bucket %d vanished from the ring", target)
	}
	if math.Abs(got.Volume-(10+2+0.052)) > 1e-9 {
		t.Fatalf("the ring did not absorb the late trade (volume=%v) — the fixture, not the gate, is wrong", got.Volume)
	}

	assertTiersTransposed(t, r, now)
}

// TestTiersTransposed_AuthoritativeRepairSettlesEveryTier covers the other direction: a
// bucket that drifted UPWARD and is corrected DOWNWARD by the exchange's own kline.
//
// This is the seed/gap-recovery path (TieredStore.SeedCandlesBatch), and it is where the
// tiers are deliberately handed DIFFERENT slices — cold gets the merged view, Redis gets the
// raw batch, each for a documented reason (PersistSeedBatch). Two different inputs are
// allowed; two different RESULTS are not. Nothing else in the suite checks that the two
// merge implementations — engine.unionSeedInto and RedisStore's inline closed-bucket merge —
// actually land on the same number.
func TestTiersTransposed_AuthoritativeRepairSettlesEveryTier(t *testing.T) {
	r := newTranspositionRig(t)
	now := time.Now().UnixMilli()

	buckets := liveWindow(t, r, now, 6)
	target := buckets[1]

	ctx := context.Background()
	// The exchange's final record for that bucket: a declared total, carrying order flow,
	// for a window that has elapsed, and FETCHED after the exchange finished writing that
	// bucket — the four credentials isAuthoritativeClosedTotal requires before a record is
	// allowed to SETTLE rather than compete in a MAX.
	//
	// The fetch instant is the newest of the four. Without it, "the bucket closed" was taken to
	// mean "the exchange has a final record", and a kline read seconds after the close (still
	// settling, for up to ~5s) replaced a correct live reconstruction with a short one.
	authoritative := candle.Candle{
		Timestamp: target,
		Open:      101, High: 103.5, Low: 99.5, Close: 102.25,
		Volume:          8.5, // strictly BELOW what the ring accumulated: the drift heals downward
		TakerBuyVolume:  5.0,
		TakerSellVolume: 3.5,
		Source:          string(redisstore.CandleMergeSourceRESTTotal),
		FetchedAt:       target + 5*60*1000 + 2*candle.KlineSettleGraceMs,
	}
	if err := r.tiered.SeedCandlesBatch(ctx, rigExchange, rigMarketType, rigSymbol, rigTimeframe,
		[]candle.Candle{authoritative}); err != nil {
		t.Fatalf("SeedCandlesBatch: %v", err)
	}

	got := r.engine.GetCandleByTimestamp(rigExchange, rigMarketType, rigSymbol, rigTimeframe, target)
	if got == nil {
		t.Fatalf("bucket %d vanished from the ring", target)
	}
	if got.Volume != 8.5 {
		t.Fatalf("the exchange's record did not settle the ring (volume=%v, want 8.5) — the ratchet "+
			"is back and every later repair will re-elect the inflated number", got.Volume)
	}

	assertTiersTransposed(t, r, now)
}

// TestTiersTransposed_SeedFillsAHoleInEveryTier: the repair inserts a bucket the live stream
// never delivered. A hole filled in the ring alone is invisible to a merged read for exactly
// the same reason a wrong value is.
func TestTiersTransposed_SeedFillsAHoleInEveryTier(t *testing.T) {
	r := newTranspositionRig(t)
	now := time.Now().UnixMilli()
	tfMs, _ := candle.ParseTimeframeMs(rigTimeframe)

	buckets := liveWindow(t, r, now, 6)
	// A bucket OLDER than everything the live stream produced, so the fold inserts rather
	// than merges.
	missing := buckets[0] - tfMs

	ctx := context.Background()
	if err := r.tiered.SeedCandlesBatch(ctx, rigExchange, rigMarketType, rigSymbol, rigTimeframe,
		[]candle.Candle{{
			Timestamp: missing,
			Open:      99, High: 100, Low: 98, Close: 99.5,
			Volume: 7.25, TakerBuyVolume: 4.25, TakerSellVolume: 3,
			Source: string(redisstore.CandleMergeSourceRESTTotal),
		}}); err != nil {
		t.Fatalf("SeedCandlesBatch: %v", err)
	}

	if got := r.engine.GetCandleByTimestamp(rigExchange, rigMarketType, rigSymbol, rigTimeframe, missing); got == nil {
		t.Fatalf("the seed did not insert bucket %d into the ring — fixture failure", missing)
	}

	assertTiersTransposed(t, r, now)
}
