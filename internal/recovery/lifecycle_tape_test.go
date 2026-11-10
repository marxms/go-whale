package recovery

// LIFECYCLE TAPE — one deterministic recording of a streamer's whole life, asserted at the
// end against the trades that were injected into it.
//
// # WHY THIS FILE EXISTS
//
// Every other test in this repository starts at a clean NewCandleEngine and ends before the
// process would have died. Five of the eleven defects found on this branch were not inside
// any single component — both halves were individually correct — they were in the SEAM at a
// LIFECYCLE EVENT: a restart, a warm boot, a REST seed landing on a ring the live stream had
// already touched, a WS reconnect, a bucket rolling. A unit assertion cannot see a seam,
// because a seam only exists once two correct things are asked to hand over to each other.
//
// So this test is not a unit test with more steps. It is a TAPE: a fixed, seeded recording of
// trades, played through the REAL production objects across a REAL restart, with ONE terminal
// invariant.
//
// # THE TERMINAL INVARIANT
//
//	for every CLOSED bucket, in EVERY tier (engine ring, Redis hot, cold):
//	    volume          == the sum of the amounts of the trades the tape generated for it
//	    takerBuyVolume  == the sum of the BUY amounts
//	    takerSellVolume == the sum of the SELL amounts
//	    takerBuy + takerSell == volume        (the pair survives, and it partitions the bar)
//
// Note what it is NOT: it is never a per-bar delta against another system. Reconstructing a
// kline from aggTrade cannot match an exchange bar-for-bar — one aggTrade carries ONE
// timestamp for a group of fills and the group straddling a boundary is split by the kline,
// not by us (~4.2% of boundaries, adjacent pairs cancel, zero bias). Here the tape IS the
// exchange: it generates the trades, so it also knows the true bucket totals, and equality is
// exact rather than approximate. See "WHY THE NUMBERS ARE EXACT" below.
//
// # WHAT PLAYS THE TAPE
//
// Real, in-process, no network:
//
//	engine.CandleEngine       the ring and every merge branch
//	engine.TieredStore        FoldSeedBatch / PersistSeedBatch / WriteClosedToHot / cold routing
//	engine.WarmCandleEngine   the warm boot itself
//	redisstore.RedisStore        over miniredis — the real CAS/merge/snapshot code
//	redisstore.CombinedStore     the exact wrapper production passes to the seeder
//	ingestion.StreamTradeHandler   real dedup window + BuildOHLCVC + bucket ordering
//	seed.applySeedWindow      the real three-step seed order (this file is in package recovery_test
//	                          precisely so that unexported seam is reachable)
//
// Substituted, and why:
//
//	cold store        tapeCold — an in-memory stand-in for QuestDB that models the two
//	                  properties the seam depends on: DEDUP UPSERT (last write for a bucket
//	                  wins) and the COLUMN SET (only OHLC + volume + the taker pair survive a
//	                  round trip). Modeling the column set is what makes a degraded record
//	                  erasing order flow VISIBLE instead of theoretical.
//	cold appender     tapeAppender — writes through synchronously instead of via
//	                  QuestDBPipeline's flush goroutine. The pipeline's batching, retry and
//	                  single-writer behavior are already pinned by questdb_batch_test.go and
//	                  cold_singlewriter_test.go; what this tape needs from it is ORDER, and a
//	                  background flusher would trade that for a sleep.
//	close fan-out     onCandleClose/onCandleRevised below mirror app/candle_sinks.go. The App
//	                  composition root cannot be built from here; the fan-out itself is three
//	                  lines and app/candle_sinks_test.go pins the wiring.
//	gap recovery      the two calls gaprecover/manager.go makes (SeedCandlesBatch for the
//	                  closed history, then SeedCurrentBucketCandle for the open bucket, both
//	                  labeled rest_total) are issued directly. Manager.process needs a live
//	                  venue client pool; the seam under test is the two store calls and their
//	                  provenance, not the worker queue.
//
// # WHY THE NUMBERS ARE EXACT
//
// Every trade amount is a whole number of EIGHTHS and every price a whole number of QUARTERS.
// Binary fractions with small denominators are exactly representable in float64, and every
// partial sum here stays far below 2^53, so addition is associative for this data: the ring's
// running total, the tape's ledger and the REST total agree BIT FOR BIT no matter what order
// they were accumulated in. That is deliberate. An epsilon would be a place for a small,
// systematic loss to hide, and a small systematic loss is exactly what this branch spent two
// adversarial rounds finding.
//
// # WHY EVERY BUCKET IS IN THE WALL-CLOCK PAST
//
// Two production rules consult the clock: engine.isAuthoritativeClosedTotal (an exchange
// total only settles a bucket whose window has ELAPSED) and RedisStore.isCandleClosed. The
// tape anchors its buckets to whole hours BEFORE the current one so both rules see closed
// buckets, which is the state the settle/heal path exists for. The OPEN/CLOSED distinction the
// tape actually exercises is the engine's own — ring-head ordering — which is where the seam
// defects lived. Because nothing depends on the tape finishing inside one wall-clock hour,
// crossing an hour boundary mid-run cannot flake it: the asserted buckets are hours old either
// way, and the one bucket whose classification could move (the newest) is deliberately not
// asserted.

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/candle/engine"
	"github.com/marxms/go-whale/internal/ingestion"
	"github.com/marxms/go-whale/internal/platform/config"
	"github.com/marxms/go-whale/internal/retention"
	"github.com/marxms/go-whale/internal/trade"
)

const (
	tapeExchangeID = "binance"
	tapeMarketType = "future"
	tapeSymbol     = "ETH/USDT"
	tapeTF         = "1h"
	tapeTFMs       = int64(3_600_000)

	// FIXED SEED. Nothing in this file may consult an unseeded rand or a wall clock for the
	// DATA it generates; the clock is read exactly once, to anchor the bucket grid.
	tapeRandSeed = int64(0x5EEDCA57)

	// tapeBuckets are indices into the grid. b8 is the newest and is deliberately never
	// asserted: it is the bucket left open when the tape stops.
	tapeAssertedBuckets = 8 // b0..b7
	tapeGridBuckets     = 9 // b0..b8
)

// ============================================================================
// THE LEDGER — the tape's only source of truth
// ============================================================================

// tapeSegment is a named run of trades inside one bucket. Segments exist because the tape
// needs to say WHICH trades a given process saw: the whole point of a lifecycle test is that
// some trades are delivered by the live stream, some only by REST after an outage, and some
// by both. The ledger totals count a segment exactly once regardless.
type tapeSegment struct {
	name   string
	bucket int
	trades []candle.Trade
}

type tapeTotals struct{ volume, buy, sell float64 }

func (t *tapeTotals) add(tr candle.Trade) {
	t.volume += tr.Amount
	if tr.Side == "buy" {
		t.buy += tr.Amount
	} else {
		t.sell += tr.Amount
	}
}

func totalsOf(segs ...*tapeSegment) tapeTotals {
	var out tapeTotals
	for _, s := range segs {
		for i := range s.trades {
			out.add(s.trades[i])
		}
	}
	return out
}

// tapeLedger generates the trades and remembers the grid.
type tapeLedger struct {
	rng    *rand.Rand
	base   int64 // timestamp of bucket 0
	nextID int64
}

func newTapeLedger() *tapeLedger {
	// The ONLY clock read in the whole tape. Bucket 8 is the hour that is currently open, so
	// every asserted bucket (0..7) is at least one full hour in the past.
	openBucket := candle.BucketTimestamp(time.Now().UnixMilli(), tapeTF)
	return &tapeLedger{
		rng:    rand.New(rand.NewSource(tapeRandSeed)), //nolint:gosec // G404: deterministic test sampling, not cryptographic randomness.
		base:   openBucket - int64(tapeGridBuckets-1)*tapeTFMs,
		nextID: 1_000_000,
	}
}

func (l *tapeLedger) bucketTS(i int) int64 { return l.base + int64(i)*tapeTFMs }

// seg builds a segment of n trades inside bucket i, the first at startOff ms into the bucket
// and each subsequent one step ms later. blankLastID drops the id of the final trade, which is
// the shape the trade handler passes through UNDEDUPLICATED (trade_handler.go:159) — used once,
// on purpose, to manufacture the only drift in this tape from real production behavior rather
// than by writing a wrong number into a fixture.
func (l *tapeLedger) seg(name string, i, n int, startOff, step int64, blankLastID bool) *tapeSegment {
	s := &tapeSegment{name: name, bucket: i, trades: make([]candle.Trade, 0, n)}
	start := l.bucketTS(i)
	for j := 0; j < n; j++ {
		ts := start + startOff + int64(j)*step
		if ts >= start+tapeTFMs {
			panic(fmt.Sprintf("tape segment %q overflows its bucket", name))
		}
		side := "buy"
		if l.rng.Intn(2) == 0 {
			side = "sell"
		}
		id := fmt.Sprintf("%d", l.nextID)
		l.nextID++
		if blankLastID && j == n-1 {
			id = ""
		}
		s.trades = append(s.trades, candle.Trade{
			ID:        id,
			Timestamp: ts,
			// Quarters and eighths: exactly representable, so every sum is exact.
			Price:  100 + float64(1+l.rng.Intn(8000))/4,
			Amount: float64(1+l.rng.Intn(4000)) / 8,
			Side:   trade.ParseSide(side),
		})
	}
	return s
}

// klines turns segments into the exchange's own record for a bucket: a TOTAL, with the taker
// split it measured. withTaker=false models the documented degradation — the seeder falls back
// to plain FetchOHLCV under rate limits and that endpoint has no taker fields at all.
func (l *tapeLedger) kline(i int, withTaker bool, segs ...*tapeSegment) candle.Candle {
	tot := totalsOf(segs...)
	var high, low, open, closePrice float64
	low = math.Inf(1)
	var firstTS, lastTS int64
	for _, s := range segs {
		for _, tr := range s.trades {
			if tr.Price > high {
				high = tr.Price
			}
			if tr.Price < low {
				low = tr.Price
			}
			if firstTS == 0 || tr.Timestamp < firstTS {
				firstTS, open = tr.Timestamp, tr.Price
			}
			if tr.Timestamp >= lastTS {
				lastTS, closePrice = tr.Timestamp, tr.Price
			}
		}
	}
	c := candle.Candle{
		Timestamp: l.bucketTS(i),
		Open:      open, High: high, Low: low, Close: closePrice,
		Volume: tot.volume,
	}
	if withTaker {
		c.TakerBuyVolume, c.TakerSellVolume = tot.buy, tot.sell
	}
	return c
}

// ============================================================================
// COLD TIER STAND-IN
// ============================================================================

// tapeCold models the two properties of the cold tier the lifecycle seam depends on:
//
//	DEDUP UPSERT   a second write for the same (key, timestamp) REPLACES the first. This is
//	               what makes a late repair converge — and what makes a degraded record
//	               destructive rather than merely useless.
//	COLUMN SET     only the columns questdb_store.go actually writes survive. Persisting the
//	               whole struct would let a taker pair the cold tier never stored appear to
//	               survive a round trip, which is precisely the defect class this models.
func tapeKey(ex, mt, sym, tf string) candle.SeriesKey {
	key, err := candle.SeriesKeyOf(ex, mt, sym, tf)
	if err != nil {
		panic(err)
	}
	return key
}

type tapeCold struct {
	mu   sync.Mutex
	rows map[candle.SeriesKey]map[int64]candle.Candle
}

func newTapeCold() *tapeCold {
	return &tapeCold{rows: make(map[candle.SeriesKey]map[int64]candle.Candle)}
}

func (c *tapeCold) WriteBatches(_ context.Context, groups map[candle.SeriesKey][]candle.Candle) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, batch := range groups {
		if c.rows[key] == nil {
			c.rows[key] = make(map[int64]candle.Candle)
		}
		for _, in := range batch {
			c.rows[key][in.Timestamp] = candle.Candle{
				Timestamp: in.Timestamp,
				Open:      in.Open, High: in.High, Low: in.Low, Close: in.Close,
				Volume:          in.Volume,
				TakerBuyVolume:  in.TakerBuyVolume,
				TakerSellVolume: in.TakerSellVolume,
			}
		}
	}
	return nil
}

func (c *tapeCold) QueryRange(_ context.Context, ex, mt, sym, tf string, from, to time.Time, limit int) ([]candle.Candle, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := tapeKey(ex, mt, sym, tf)
	var out []candle.Candle
	for _, c := range c.rows[key] {
		ts := time.UnixMilli(c.Timestamp)
		if !from.IsZero() && ts.Before(from) {
			continue
		}
		if !to.IsZero() && ts.After(to) {
			continue
		}
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp > out[j].Timestamp }) // newest first
	if limit > 0 && limit < len(out) {
		out = out[:limit]
	}
	return out, nil
}

func (c *tapeCold) GetOldestTimestamp(_ context.Context, ex, mt, sym, tf string) (time.Time, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := tapeKey(ex, mt, sym, tf)
	var oldest int64
	for ts := range c.rows[key] {
		if oldest == 0 || ts < oldest {
			oldest = ts
		}
	}
	if oldest == 0 {
		return time.Time{}, nil
	}
	return time.UnixMilli(oldest), nil
}

func (c *tapeCold) Close() error                 { return nil }
func (c *tapeCold) Stats() candle.ColdStoreStats { return candle.ColdStoreStats{Connected: true} }
func (c *tapeCold) row(ts int64) (candle.Candle, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	got, ok := c.rows[tapeKey(tapeExchangeID, tapeMarketType, tapeSymbol, tapeTF)][ts]
	return got, ok
}

var _ candle.ColdStore = (*tapeCold)(nil)

// tapeAppender is the ColdAppender the TieredStore routes seed/backfill closed candles
// through, and the same path the close fan-out uses.
//
// `during` is the tape's stand-in for time passing while the durable write blocks. Persisting
// a seed batch pushes ~200 rows through the cold pipeline and the live stream does not pause
// for it; running the hook on the first candle of the batch reproduces that deterministically,
// with no sleep and no second goroutine.
type tapeAppender struct {
	cold   *tapeCold
	during func()
}

func (a *tapeAppender) EnqueueClosedBlocking(ctx context.Context, ex, mt, sym, tf string, c candle.Candle) error {
	if a.during != nil {
		f := a.during
		a.during = nil // once per arming
		f()
	}
	return a.cold.WriteBatches(ctx, map[candle.SeriesKey][]candle.Candle{
		tapeKey(ex, mt, sym, tf): {c},
	})
}

var _ candle.ColdAppender = (*tapeAppender)(nil)

// ============================================================================
// THE RIG — one streamer process, replaceable
// ============================================================================

type tapeSinkEvent struct {
	kind string // "close" | "revised"
	ts   int64
	vol  float64
}

type tapeRig struct {
	t    *testing.T
	mr   *miniredis.Miniredis
	cold *tapeCold
	app  *tapeAppender

	// Per-PROCESS state. Everything below is discarded and rebuilt by restart(), which is what
	// makes this a lifecycle test rather than a long unit test: the ring, the dedup window and
	// the Redis client do not survive, and Redis + cold do.
	eng     *engine.CandleEngine
	tiered  *engine.TieredStore
	redis   *redisstore.RedisStore
	store   *redisstore.CombinedStore
	handler *ingestion.StreamTradeHandler

	mu     sync.Mutex
	events []tapeSinkEvent
}

func tapeConfig(addr string) config.Config {
	return config.Config{
		RedisAddr:          addr,
		RedisPoolSize:      10,
		RedisDB:            0,
		RedisDialTimeoutS:  2,
		RedisReadTimeoutS:  2,
		RedisWriteTimeoutS: 2,
		RedisPoolTimeoutS:  2,
		RedisMaxRetries:    3,
		CandleEngine: config.CandleEngineConfig{
			Enabled:           true,
			Timeframes:        []string{tapeTF},
			BufferSizes:       map[string]int{"default": 500},
			RedisWriteThrough: true,
			// One hour: the periodic snapshot must NEVER fire during the tape. It is the
			// mechanism that would MASK a missing closed-bucket write, and the defect that
			// motivated WriteClosedToHot is precisely a bucket rolling BETWEEN two sweeps.
			// Leaving it off makes the close path the only way a closed bucket can reach the
			// hot tier — which is the production case that under-reported by 8.85%.
			SnapshotIntervalMS: 3_600_000,
		},
	}
}

// boot brings a streamer process up against the SURVIVING Redis and cold tiers.
// warm=false is a cold boot (nothing persisted yet); warm=true runs the real WarmCandleEngine.
func (r *tapeRig) boot(warm bool) {
	r.t.Helper()

	cfg := tapeConfig(r.mr.Addr())
	tfReg := candle.NewTimeframeRegistry([]string{tapeTF})

	rs, err := redisstore.NewRedisStore(r.t.Context(), cfg, 500, tfReg)
	if err != nil {
		r.t.Fatalf("NewRedisStore: %v", err)
	}
	eng := engine.NewCandleEngine(cfg.CandleEngine.BufferSizes)
	tiered := engine.NewTieredStore(eng, rs, cfg.CandleEngine)
	tiered.SetColdStore(r.cold)
	tiered.SetColdAppender(r.app, retention.Policy{}) // zero policy = no clamp; the tape grid spans hours
	eng.SetRedisStore(rs)

	r.eng, r.tiered, r.redis = eng, tiered, rs
	// Registered BEFORE the warm boot and read at CALL time, exactly as the composition root
	// does — see app/candle_sinks.go on why these must not be captured or nested inside the
	// cold-tier branch.
	eng.OnCandleClose(r.onCandleClose)
	eng.OnCandleRevised(r.onCandleRevised)
	tiered.EnableSnapshots()

	// The exact wrapper production hands the seeder. Its fields are the candle-owned PORTS,
	// and the splitter assertion runs against the embedded SeedWriter — the forwarding path
	// the tape exercises here is the production one.
	r.store = &redisstore.CombinedStore{
		Writer:     tiered,
		Reader:     tiered,
		Catalog:    tiered,
		SeedWriter: tiered,
		Redis:      rs,
	}
	r.handler = ingestion.NewStreamTradeHandler(r.store, tapeExchangeID, tapeMarketType, tfReg)

	if warm {
		engine.WarmCandleEngine(r.t.Context(), eng, r.cold, rs, cfg)
	}
}

// shutdown is the graceful stop: a final snapshot flush, the snapshot goroutine stopped, the
// Redis client closed. Whatever is not durable at this instant is gone.
func (r *tapeRig) shutdown() {
	r.t.Helper()
	if err := r.tiered.Close(); err != nil {
		r.t.Fatalf("TieredStore.Close: %v", err)
	}
}

// onCandleClose mirrors app.onCandleClose (app/candle_sinks.go:33) for the two sinks that carry
// data: cold durability and the hot-tier closed-bucket write. The realtime bus and the WS hub
// are pure fan-out with no effect on stored state.
func (r *tapeRig) onCandleClose(key engine.CompositeKey, c candle.Candle) {
	r.record("close", c)
	if err := r.app.EnqueueClosedBlocking(context.Background(), key.Exchange, key.MarketType, key.Symbol, key.Timeframe, c); err != nil {
		r.t.Errorf("cold enqueue on close: %v", err)
	}
	r.tiered.WriteClosedToHot(key, c)
}

// onCandleRevised mirrors app.onCandleRevised. Without it a correction to an already-closed
// bucket lives in RAM and nowhere else, which is the measured engine-279.983 / cold-279.931
// divergence.
func (r *tapeRig) onCandleRevised(key engine.CompositeKey, c candle.Candle) {
	r.record("revised", c)
	if err := r.app.EnqueueClosedBlocking(context.Background(), key.Exchange, key.MarketType, key.Symbol, key.Timeframe, c); err != nil {
		r.t.Errorf("cold enqueue on revision: %v", err)
	}
	r.tiered.WriteClosedToHot(key, c)
}

func (r *tapeRig) record(kind string, c candle.Candle) {
	r.mu.Lock()
	r.events = append(r.events, tapeSinkEvent{kind: kind, ts: c.Timestamp, vol: c.Volume})
	r.mu.Unlock()
}

func (r *tapeRig) sinkEvents(kind string) []tapeSinkEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []tapeSinkEvent
	for _, e := range r.events {
		if e.kind == kind {
			out = append(out, e)
		}
	}
	return out
}

// stream plays segments through the REAL trade handler in ONE batch, which is how the
// websocket delivers them. Passing several segments in one call is how the tape builds a batch
// that STRADDLES a bucket boundary.
func (r *tapeRig) stream(segs ...*tapeSegment) {
	r.t.Helper()
	var batch []trade.Trade
	for _, s := range segs {
		for _, tr := range s.trades {
			batch = append(batch, trade.Trade{
				ID:        tr.ID,
				Symbol:    tapeSymbol,
				Price:     tr.Price,
				Amount:    tr.Amount,
				Side:      tr.Side,
				Timestamp: tr.Timestamp,
			})
		}
	}
	if err := r.handler.HandleTradesWithContext(tapeExchangeID, tapeMarketType, tapeSymbol, batch); err != nil {
		r.t.Fatalf("HandleTradesWithContext: %v", err)
	}
}

// seedREST plays the real seeder seam: applySeedWindow, on the real CombinedStore.
// tapeBucketOf returns the newest bucket the seed carries, so the fixture can stamp a fetch
// instant that is past THAT bucket's settle grace. Anchoring on the wall clock does not work:
// this tape sits near the present, so time.Now() can precede the bucket's own close.
func tapeBucketOf(historical []candle.Candle, current *candle.Candle) int64 {
	newest := int64(0)
	for _, c := range historical {
		if c.Timestamp > newest {
			newest = c.Timestamp
		}
	}
	if current != nil && current.Timestamp > newest {
		newest = current.Timestamp
	}
	return newest
}

func (r *tapeRig) seedREST(historical []candle.Candle, current *candle.Candle) {
	r.t.Helper()
	// fetchedAt mirrors production: fetchSeedWindow stamps the instant the RESPONSE was parsed,
	// and applySeedWindow copies it onto every total it labels. Omitting it here would leave the
	// window unstamped, the records would be refused authority, and the test would fail for a
	// reason unrelated to the lifecycle it exercises.
	w := &seedWindow{
		historical: historical,
		current:    current,
		fetchedAt:  time.UnixMilli(tapeBucketOf(historical, current) + tapeTFMs + 2*candle.KlineSettleGraceMs),
	}
	if err := applySeedWindow(context.Background(), Metrics{}, r.store, tapeExchangeID, tapeMarketType, tapeSymbol, tapeTF, w); err != nil {
		r.t.Fatalf("applySeedWindow: %v", err)
	}
}

// gapRecover issues the two store calls gaprecover/manager.go:523-555 makes, in that order and
// with that provenance: the closed history first so the series grows backwards into place, the
// open bucket second.
func (r *tapeRig) gapRecover(historical []candle.Candle, current *candle.Candle) {
	r.t.Helper()
	ctx := context.Background()
	for i := range historical {
		historical[i].Source = string(redisstore.CandleMergeSourceRESTTotal)
		// Stamped as production stamps it: the instant the RESPONSE was parsed. A total with no
		// stamp cannot settle a bucket, because "the bucket closed" and "the exchange finished
		// writing the bucket" are different facts and only this field separates them.
		historical[i].FetchedAt = historical[i].Timestamp + tapeTFMs + 2*candle.KlineSettleGraceMs
	}
	if len(historical) > 0 {
		if err := r.store.SeedCandlesBatch(ctx, tapeExchangeID, tapeMarketType, tapeSymbol, tapeTF, historical); err != nil {
			r.t.Fatalf("gap-recovery SeedCandlesBatch: %v", err)
		}
	}
	if current != nil {
		c := *current
		c.Source = string(redisstore.CandleMergeSourceRESTTotal)
		c.FetchedAt = c.Timestamp + tapeTFMs + 2*candle.KlineSettleGraceMs
		if err := r.store.SeedCurrentBucketCandle(ctx, tapeExchangeID, tapeMarketType, tapeSymbol, tapeTF, c); err != nil {
			r.t.Fatalf("gap-recovery SeedCurrentBucketCandle: %v", err)
		}
	}
}

// ============================================================================
// THE TAPE
// ============================================================================

func TestLifecycleTape_EveryClosedBucketMatchesItsTradesInEveryTier(t *testing.T) {
	l := newTapeLedger()

	// ---- the recording -----------------------------------------------------
	//
	// Bucket by bucket, and what each one is FOR. Nothing here is decoration: every segment
	// exists because removing it removes a seam.
	//
	//	b0,b1  pre-history. Only the cold-boot REST seed ever supplies them.
	//	b2     streamed live, and INFLATED by a re-sent id-less trade — the handler passes
	//	       un-deduplicable trades through by design (trade_handler.go:159), so this is real
	//	       ingestion behavior, not a doctored number. The exchange's later authoritative
	//	       record must SETTLE it back down; a MAX-only union ratchets it permanently.
	//	b3     a HOLE. The live stream never delivers it, and the warm boot cannot restore what
	//	       was never persisted. Only a seed that UNIONS (rather than keeps or replaces) puts
	//	       it back into the ring's interior.
	//	b4     streamed live and rich; the REST record for it is DEGRADED (no taker split). The
	//	       union must keep the measured pair, and the cold tier must receive the union — a
	//	       DEDUP table has no NULL, so a 0/0 write is indistinguishable from a real zero.
	//	b5     the RESTART bucket, split four ways across the process boundary.
	//	b6     the WS-OUTAGE bucket: closed while the socket was dead, supplied only by
	//	       gap-recovery, and therefore never fired through onCandleClose at all.
	//	b7     the bucket gap-recovery hands over live: a REST total lands mid-bucket and the
	//	       stream must keep ACCUMULATING onto it afterwards.
	//	b8     opened by the rollover and never asserted.

	b0 := l.seg("b0", 0, 12, 10_000, 250_000, false)
	b1 := l.seg("b1", 1, 12, 10_000, 250_000, false)

	b2live := l.seg("b2.live", 2, 20, 10_000, 170_000, true /* last trade has no id */)
	b2resend := &tapeSegment{name: "b2.resend", bucket: 2, trades: []candle.Trade{b2live.trades[len(b2live.trades)-1]}}

	b3 := l.seg("b3", 3, 15, 10_000, 220_000, false)
	b4 := l.seg("b4", 4, 18, 10_000, 190_000, false)

	b5p1 := l.seg("b5.p1", 5, 10, 10_000, 90_000, false)     // process #1, before the restart
	b5down := l.seg("b5.down", 5, 8, 950_000, 90_000, false) // traded while the process was down
	b5pre := l.seg("b5.pre", 5, 5, 1_700_000, 60_000, false) // process #2's WS, before the seed lands
	b5mid := l.seg("b5.mid", 5, 6, 2_100_000, 60_000, false) // arrives WHILE the seed's durable write blocks
	b5post := l.seg("b5.post", 5, 6, 2_500_000, 60_000, false)

	b6 := l.seg("b6", 6, 14, 10_000, 230_000, false) // whole bucket lost to the WS outage

	b7out := l.seg("b7.out", 7, 9, 10_000, 60_000, false)      // traded during the outage
	b7l1 := l.seg("b7.l1", 7, 4, 700_000, 60_000, false)       // first deltas after the reconnect
	b7l2 := l.seg("b7.l2", 7, 7, 1_200_000, 60_000, false)     // after gap-recovery wrote the total
	b7roll := l.seg("b7.roll", 7, 3, 2_400_000, 60_000, false) // shares one batch with b8

	b8 := l.seg("b8.roll", 8, 2, 1_000, 500, false)
	// A trade for b7 delivered AFTER b8 opened the next bucket — inside the handler's 60s
	// tolerance, which is the only shape a late trade can actually have in production
	// (trade_handler.go:153 discards anything older). 98 of these were measured in one window.
	b7late := &tapeSegment{name: "b7.late", bucket: 7, trades: []candle.Trade{{
		ID:        "9999999",
		Timestamp: l.bucketTS(8) - 5_000,
		Price:     1234.75,
		Amount:    321.5,
		Side:      "buy",
	}}}

	// ---- the truth ---------------------------------------------------------
	truth := map[int64]tapeTotals{
		l.bucketTS(0): totalsOf(b0),
		l.bucketTS(1): totalsOf(b1),
		// b2.resend is NOT truth. The exchange saw that trade once; we ingested it twice.
		l.bucketTS(2): totalsOf(b2live),
		l.bucketTS(3): totalsOf(b3),
		l.bucketTS(4): totalsOf(b4),
		l.bucketTS(5): totalsOf(b5p1, b5down, b5pre, b5mid, b5post),
		l.bucketTS(6): totalsOf(b6),
		l.bucketTS(7): totalsOf(b7out, b7l1, b7l2, b7roll, b7late),
	}

	// ---- the rig -----------------------------------------------------------
	mr := miniredis.RunT(t)
	cold := newTapeCold()
	rig := &tapeRig{t: t, mr: mr, cold: cold, app: &tapeAppender{cold: cold}}

	// =========================== COLD BOOT ==================================
	rig.boot(false)

	// The boot seed: the exchange's history for the buckets that closed before this process
	// existed. Authoritative — it carries the taker split.
	rig.seedREST([]candle.Candle{
		l.kline(0, true, b0),
		l.kline(1, true, b1),
	}, nil)

	// =========================== STREAM =====================================
	rig.stream(b2live)
	rig.stream(b2resend) // the exchange re-sent it; no id, so nothing can dedup it
	// b3 is never streamed. That is the hole.
	rig.stream(b4)   // rolls b2 -> onCandleClose(b2) with the INFLATED value
	rig.stream(b5p1) // rolls b4 -> onCandleClose(b4) with the rich, correct value

	assertEngineBucket(t, rig, l.bucketTS(2), totalsOf(b2live, b2resend),
		"the tape must actually have produced the drift it later asks the system to heal; "+
			"if this fails the heal assertion below would be vacuous")

	// ================== RESTART, MID-BUCKET (b5 is open) ====================
	//
	// This is the event no other test in the repository crosses. The ring, the trade handler's
	// dedup window and the Redis client all die here. b5 has NEVER been closed, so it exists
	// only in whatever the hot tier happened to hold; the cold tier has never seen it.
	rig.shutdown()
	rig.boot(true)

	// The WS comes back before the seeder finishes — it always does, the two run concurrently —
	// so live deltas land on a ring that the REST total has not reached yet.
	rig.stream(b5pre)

	// The seeder's REST fetch happens AFTER those deltas, so the exchange's total for the open
	// bucket already contains them. That ordering is what makes MAX the correct rule and SUM a
	// +82.5% inflation.
	b5RESTtotal := l.kline(5, true, b5p1, b5down, b5pre)

	// Arm the "the live stream does not pause while the durable write blocks" hook.
	rig.app.during = func() { rig.stream(b5mid) }

	rig.seedREST([]candle.Candle{
		l.kline(0, true, b0),
		l.kline(1, true, b1),
		l.kline(2, true, b2live), // authoritative: settles the drift instead of competing with it
		l.kline(3, true, b3),     // the hole — the ring has never held this bucket
		l.kline(4, false, b4),    // DEGRADED: volume only, no taker split
	}, &b5RESTtotal)

	if rig.app.during != nil {
		t.Fatal("the during-persist hook never fired — the seed's durable write did not reach " +
			"the cold appender, so the ordering this tape exists to check was not exercised")
	}
	rig.stream(b5post)

	assertEngineBucket(t, rig, l.bucketTS(3), truth[l.bucketTS(3)],
		"a bucket the live stream never delivered must be INSERTED into the ring's interior by the "+
			"seed union; keeping the buffer drops it while the same batch still flows to cold and Redis")
	assertEngineBucket(t, rig, l.bucketTS(2), truth[l.bucketTS(2)],
		"an authoritative closed exchange total must SETTLE a drifted bucket; a MAX-only union "+
			"re-elects the inflated number on every later repair, forever")
	assertEngineBucket(t, rig, l.bucketTS(4), truth[l.bucketTS(4)],
		"a REST record that did not measure the order flow may not erase the pair that did")

	// ================== WS DISCONNECT, THEN RECONNECT =======================
	//
	// b6 was lost entirely. b7 is partly lost. The reconnect delivers the current bucket first
	// (rolling b5 and closing it), and gap-recovery follows a moment later.
	rig.stream(b7l1)

	b7RESTtotal := l.kline(7, true, b7out, b7l1)
	rig.gapRecover([]candle.Candle{l.kline(6, true, b6)}, &b7RESTtotal)

	// The bucket gap-recovery just wrote a TOTAL into keeps trading. Those deltas declare
	// themselves increments and MUST accumulate: MAX-ing them froze a recovered 5m bar for the
	// remaining 4m50s of its window.
	rig.stream(b7l2)

	// ============================ ROLLOVER ==================================
	//
	// ONE batch straddling the b7/b8 boundary. The handler must emit b7's buckets before b8's,
	// or it closes b7 early and then has to publish a revision to undo its own close.
	rig.stream(b7roll, b8)
	// And the genuinely late trade, after the roll.
	rig.stream(b7late)

	// ======================= TERMINAL INVARIANT =============================
	for i := 0; i < tapeAssertedBuckets; i++ {
		ts := l.bucketTS(i)
		want := truth[ts]
		assertEngineBucket(t, rig, ts, want, "engine ring")
		assertRedisBucket(t, rig, ts, want, "hot tier")
		assertColdBucket(t, cold, ts, want, "cold tier")
	}

	// Exactly ONE revision, for exactly the one deliberately-late trade. A second revision means
	// a bucket was closed before the batch that was still filling it had been drained — the
	// map-iteration ordering defect, which converges on value but publishes a close no consumer
	// should ever have been sent.
	revs := rig.sinkEvents("revised")
	if len(revs) != 1 || revs[0].ts != l.bucketTS(7) {
		t.Errorf("revisions = %+v, want exactly one for bucket 7 (ts=%d). More than one means a "+
			"bucket rolled while its own trades were still queued in the same batch; zero means a "+
			"late correction stayed in RAM and never reached the durable tiers",
			revs, l.bucketTS(7))
	}

	// The close stream must never go BACKWARDS. checkOpenTimeContinuity records every close as
	// the new baseline before deciding whether it is a gap, so an older bucket closing after a
	// newer one drags that baseline back and makes the next genuine close read as a jump — the
	// reason a correction is published as candle.revised and not as a second close.
	//
	// A REPEAT of the same timestamp is expected exactly once here and is not a violation: the
	// warm ring boots with a closed bucket at its head, and the first live delta of the new
	// process rolls it again. Every sink on the other end is idempotent by construction (cold
	// dedups on the key, the hot write is a snapshot-total replace), which is why this is
	// tolerable rather than merely tolerated.
	var prev int64 = -1
	for _, e := range rig.sinkEvents("close") {
		if e.ts < prev {
			t.Errorf("close stream went BACKWARDS: bucket %d closed after %d", e.ts, prev)
		}
		prev = e.ts
	}

	// ======================= GRACEFUL SHUTDOWN ==============================
	//
	// And then assert the hot tier AGAIN through a brand-new client. What a restart can read is
	// the only definition of durable that matters.
	rig.shutdown()

	cfg := tapeConfig(mr.Addr())
	after, err := redisstore.NewRedisStore(t.Context(), cfg, 500, candle.NewTimeframeRegistry([]string{tapeTF}))
	if err != nil {
		t.Fatalf("post-shutdown NewRedisStore: %v", err)
	}
	defer func() { _ = after.Client().Close() }()
	rig.redis = after
	for i := 0; i < tapeAssertedBuckets; i++ {
		ts := l.bucketTS(i)
		assertRedisBucket(t, rig, ts, truth[ts], "hot tier, re-read after shutdown")
	}
}

// ============================================================================
// ASSERTIONS
// ============================================================================

func assertTotals(t *testing.T, tier string, ts int64, got candle.Candle, want tapeTotals, why string) {
	t.Helper()
	if got.Volume != want.volume {
		t.Errorf("[%s] bucket %d volume = %v, want %v (delta %+v) — %s",
			tier, ts, got.Volume, want.volume, got.Volume-want.volume, why)
	}
	if got.TakerBuyVolume != want.buy || got.TakerSellVolume != want.sell {
		t.Errorf("[%s] bucket %d taker pair = (%v, %v), want (%v, %v) — %s",
			tier, ts, got.TakerBuyVolume, got.TakerSellVolume, want.buy, want.sell, why)
	}
	// The pair must PARTITION the bar. buy+sell > volume is physically impossible and is the
	// signature of a total being summed onto a bucket that already held it; buy+sell < volume
	// with both non-zero is the signature of a total replacing only half a bucket.
	if sum := got.TakerBuyVolume + got.TakerSellVolume; sum != got.Volume {
		t.Errorf("[%s] bucket %d taker pair does not partition the bar: buy+sell=%v vs volume=%v",
			tier, ts, sum, got.Volume)
	}
}

func assertEngineBucket(t *testing.T, r *tapeRig, ts int64, want tapeTotals, why string) {
	t.Helper()
	got := r.eng.GetCandleByTimestamp(tapeExchangeID, tapeMarketType, tapeSymbol, tapeTF, ts)
	if got == nil {
		t.Fatalf("[engine] bucket %d ABSENT from the ring — %s", ts, why)
	}
	assertTotals(t, "engine", ts, *got, want, why)
}

func assertRedisBucket(t *testing.T, r *tapeRig, ts int64, want tapeTotals, why string) {
	t.Helper()
	rows, err := r.redis.GetCandlesDesc(context.Background(), tapeExchangeID, tapeMarketType, tapeSymbol, tapeTF, 500)
	if err != nil {
		t.Fatalf("[redis] read: %v", err)
	}
	for _, c := range rows {
		if c.Timestamp == ts {
			assertTotals(t, "redis", ts, c, want, why)
			return
		}
	}
	t.Fatalf("[redis] bucket %d ABSENT from the hot tier — %s", ts, why)
}

func assertColdBucket(t *testing.T, cold *tapeCold, ts int64, want tapeTotals, why string) {
	t.Helper()
	got, ok := cold.row(ts)
	if !ok {
		t.Fatalf("[cold] bucket %d ABSENT from the durable tier — %s", ts, why)
	}
	assertTotals(t, "cold", ts, got, want, why)
}
