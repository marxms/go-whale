package recovery

import (
	"context"
	"testing"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
)

// recordingStore records the ORDER in which a seed window reaches the store.
//
// It implements only the two methods the seeder's MultiExchangeStore port declares, so
// the fake cannot silently satisfy a wider surface than production passes.
type recordingStore struct {
	calls []string
	// duringBatch runs while SeedCandlesBatch is executing, standing in for the live WS
	// deltas that keep arriving while ~200 rows are pushed to the cold tier.
	duringBatch func()
}

func (r *recordingStore) AddStreamingCandle(_ context.Context, _, _, _, _ string, _ candle.Candle) error {
	r.calls = append(r.calls, "current")
	return nil
}

func (r *recordingStore) SeedCandlesBatch(_ context.Context, _, _, _, _ string, _ []candle.Candle) error {
	r.calls = append(r.calls, "historical")
	if r.duringBatch != nil {
		r.duringBatch()
	}
	return nil
}

// splittingStore implements candle.SeedSplitter. TestProductionStoreCanSplit below is
// what makes this fake meaningful: without it, a fake that splits proves nothing about a
// production store that silently cannot.
type splittingStore struct {
	calls []string
}

func (s *splittingStore) FoldSeedBatch(_, _, _, _ string, cs []candle.Candle) []candle.Candle {
	s.calls = append(s.calls, "fold")
	return cs
}

func (s *splittingStore) PersistSeedBatch(_ context.Context, _, _, _, _ string, _, _ []candle.Candle) error {
	s.calls = append(s.calls, "persist")
	return nil
}

func (s *splittingStore) AddStreamingCandle(_ context.Context, _, _, _, _ string, _ candle.Candle) error {
	s.calls = append(s.calls, "current")
	return nil
}

// SeedCandlesBatch exists only because MultiExchangeStore declares it; the split path must
// use PersistSeedBatch instead, and recording the call as a distinct step makes a fallback
// regression visible in the order assertion.
func (s *splittingStore) SeedCandlesBatch(_ context.Context, _, _, _, _ string, _ []candle.Candle) error {
	s.calls = append(s.calls, "historical-fallback")
	return nil
}

// TestProductionStoreCanSplit is the guard that keeps the three-step order from becoming
// dead code again.
//
// applySeedWindow chooses its path with a type assertion. Asserting on a locally-declared
// interface FAILS when *redisstore.CombinedStore embeds a full candle-store INTERFACE rather
// than the concrete engine: the extra methods are never promoted. The seeder then falls through
// to the fallback on every boot, the fix never runs, and no test notices because every test fake
// implements the interface directly.
//
// A failed assertion is indistinguishable from a store that legitimately cannot split, which
// is why this has to be asserted explicitly rather than inferred from behavior.
func TestProductionStoreCanSplit(t *testing.T) {
	t.Parallel()

	var store MultiExchangeStore = &redisstore.CombinedStore{}
	if _, ok := store.(candle.SeedSplitter); !ok {
		t.Fatal("the production store does not satisfy candle.SeedSplitter — " +
			"applySeedWindow will silently take the fallback path on every boot, and the " +
			"three-step order (fold, open bucket, persist) will never execute")
	}
}

// TestApplySeedWindow_ThreeStepOrder pins the order on the path production takes.
//
// Each position is forced by a defect measured in production:
//
//	fold BEFORE current   — the open bucket is newer than everything in the ring, so applying
//	                        it rolls the ring's newest bucket. After a warm boot that is a
//	                        bucket restored from Redis, truncated at whatever the last
//	                        shutdown persisted: rolling it publishes a stale close AND hands
//	                        checkOpenTimeContinuity an hours-old baseline, so the first real
//	                        live close of every key reads as a gap and enqueues recovery for
//	                        the whole fleet. Folding first puts an authoritative exchange
//	                        record at the head instead.
//	current BEFORE persist — persisting ~200 rows blocks on the cold pipeline while the live
//	                        stream keeps accumulating into the open bucket. The REST total
//	                        then wins the MAX and every delta from the blocking window is
//	                        discarded. Measured on ETH/USDT 5m across a deploy: -517.054.
func TestApplySeedWindow_ThreeStepOrder(t *testing.T) {
	t.Parallel()

	current := candle.Candle{
		Timestamp: 1_700_000_300_000, Open: 100, High: 100, Low: 100, Close: 100, Volume: 1000,
		TakerBuyVolume: 600, TakerSellVolume: 400,
	}
	store := &splittingStore{}

	if err := applySeedWindow(context.Background(), Metrics{}, store, "binance", "future", "ETH/USDT", "5m",
		&seedWindow{
			historical: []candle.Candle{{Timestamp: 1_700_000_000_000, Open: 99, High: 99, Low: 99, Close: 99, Volume: 500}},
			current:    &current,
		}); err != nil {
		t.Fatalf("applySeedWindow: %v", err)
	}

	want := []string{"fold", "current", "persist"}
	if len(store.calls) != len(want) {
		t.Fatalf("calls = %v, want %v", store.calls, want)
	}
	for i := range want {
		if store.calls[i] != want[i] {
			t.Fatalf("calls = %v, want %v — the ring must be folded first, the open bucket "+
				"applied second, and the blocking durable write last", store.calls, want)
		}
	}
}

// TestApplySeedWindow_FallbackKeepsTheOpenBucketBeforeTheBlockingWrite covers a store
// that does NOT implement the splitter: it cannot fold separately, so the best remaining
// order is open-bucket-then-persist. Guard for the
// RESTART-BOUNDARY UNDER-COUNT measured against Binance.
//
// # THE DEFECT
//
// ETH/USDT 5m, the bucket that was open when a deploy restarted the streamer:
//
//	ours 517.054 SHORT of the exchange, on that bucket alone
//	every other bar in the window matched to 0.0000%
//
// The seeder fetched the window, then called SeedCandlesBatch — which pushes ~200 rows
// through the cold pipeline and BLOCKS for as long as that takes — and only afterwards
// applied window.current, the REST total for the open bucket.
//
// Every WS delta that arrived during that block accumulated into the engine's open bucket.
// Then the REST total landed and candle_engine.go took math.Max(live, total): the total is
// larger (it covers the part of the bucket that traded before this process existed), so it
// won, and every delta accumulated during the block was DISCARDED.
//
// # WHY THE FIX IS THE ORDER AND NOT THE MERGE RULE
//
// The merge rule is right. A REST kline for an open bucket is a TOTAL up to the fetch
// instant, and the live deltas that follow it are increments on top of that total. Expressed
// in the correct order the two compose exactly:
//
//	MAX(live_so_far, REST_total) — the total subsumes what live had, so it wins
//	then every later ws_delta SUMs onto it — those trades are genuinely new
//
// Expressed in the wrong order the same two rules destroy data, because the deltas that
// arrived during the block are on the WRONG SIDE of the MAX. Neither MAX nor SUM can
// recover them; only applying the total before the block can.
//
// The residual after this fix is the trades between the exchange's server-side snapshot
// instant and the local application of it — one network round-trip — and that is
// irreducible without a different data source.
func TestApplySeedWindow_FallbackKeepsTheOpenBucketBeforeTheBlockingWrite(t *testing.T) {
	t.Parallel()

	current := candle.Candle{
		Timestamp: 1_700_000_300_000, Open: 100, High: 100, Low: 100, Close: 100, Volume: 1000,
		TakerBuyVolume: 600, TakerSellVolume: 400,
	}
	store := &recordingStore{}

	if err := applySeedWindow(context.Background(), Metrics{}, store, "binance", "future", "ETH/USDT", "5m",
		&seedWindow{
			historical: []candle.Candle{{Timestamp: 1_700_000_000_000, Open: 99, High: 99, Low: 99, Close: 99, Volume: 500}},
			current:    &current,
		}); err != nil {
		t.Fatalf("applySeedWindow: %v", err)
	}

	if len(store.calls) != 2 {
		t.Fatalf("expected both the open bucket and the historical batch to be applied, got %v", store.calls)
	}
	if store.calls[0] != "current" {
		t.Errorf("order = %v, want the OPEN BUCKET first. Applying it after the historical "+
			"batch loses every live delta that arrives while that batch blocks on the cold "+
			"pipeline: the REST total then wins the MAX and those trades are discarded. "+
			"Measured on ETH/USDT 5m: -517.054 against Binance on the restart bucket", store.calls)
	}
}

// TestApplySeedWindow_DeltasDuringTheBatchSurvive states the same guard as a CONSEQUENCE
// rather than as an ordering assertion, so the test keeps its meaning if the seam is ever
// restructured. A delta that arrives while the historical batch is blocked must still be
// present in what the store holds afterwards.
func TestApplySeedWindow_DeltasDuringTheBatchSurvive(t *testing.T) {
	t.Parallel()

	var totalApplied bool
	store := &recordingStore{}
	store.duringBatch = func() {
		// The live stream does not pause while the cold pipeline drains. If the REST total
		// has not landed yet, this delta is about to be thrown away by the MAX.
		totalApplied = len(store.calls) > 0 && store.calls[0] == "current"
	}

	current := candle.Candle{
		Timestamp: 1_700_000_300_000, Open: 100, High: 100, Low: 100, Close: 100, Volume: 1000,
		TakerBuyVolume: 600, TakerSellVolume: 400,
	}
	if err := applySeedWindow(context.Background(), Metrics{}, store, "binance", "future", "ETH/USDT", "5m",
		&seedWindow{
			historical: []candle.Candle{{Timestamp: 1_700_000_000_000, Open: 99, High: 99, Low: 99, Close: 99, Volume: 500}},
			current:    &current,
		}); err != nil {
		t.Fatalf("applySeedWindow: %v", err)
	}

	if !totalApplied {
		t.Error("a live delta arriving during the historical batch would be discarded — the " +
			"REST total for the open bucket had not been applied yet, so the delta sits on " +
			"the losing side of the MAX and no later merge can recover it")
	}
}

// TestApplySeedWindow_DeclaresProvenanceOnBothSides pins the other half of the contract.
// Order is worthless without provenance: an unlabelled REST total is ACCUMULATED by the
// engine's same-bucket branch, which is the +82.5% inflation this branch already fixed.
func TestApplySeedWindow_DeclaresProvenanceOnBothSides(t *testing.T) {
	t.Parallel()

	var gotCurrent candle.Candle
	var gotHistorical []candle.Candle
	store := &provenanceStore{
		onCurrent:    func(c candle.Candle) { gotCurrent = c },
		onHistorical: func(cs []candle.Candle) { gotHistorical = cs },
	}

	current := candle.Candle{Timestamp: 1_700_000_300_000, Open: 100, High: 100, Low: 100, Close: 100, Volume: 1000}
	if err := applySeedWindow(context.Background(), Metrics{}, store, "binance", "future", "ETH/USDT", "5m",
		&seedWindow{
			historical: []candle.Candle{{Timestamp: 1_700_000_000_000, Open: 99, High: 99, Low: 99, Close: 99, Volume: 500}},
			current:    &current,
		}); err != nil {
		t.Fatalf("applySeedWindow: %v", err)
	}

	want := string(redisstore.CandleMergeSourceRESTTotal)
	if gotCurrent.Source != want {
		t.Errorf("open bucket Source = %q, want %q — unlabelled, the engine ACCUMULATES this "+
			"total onto the live partial (+82.5%% measured on ETH/USDT 5m)", gotCurrent.Source, want)
	}
	for i, c := range gotHistorical {
		if c.Source != want {
			t.Errorf("historical[%d] Source = %q, want %q — without it the closed bucket cannot "+
				"be settled by the exchange's record and drift stays permanent", i, c.Source, want)
		}
	}
}

func TestApplySeedWindow_UsesRealRequestTimeForHistoricalCandles(t *testing.T) {
	var got []candle.Candle
	store := &provenanceStore{onHistorical: func(cs []candle.Candle) { got = cs }}
	requested := time.Now().Add(-time.Second)
	fetched := time.Now()
	w := &seedWindow{historical: []candle.Candle{{Timestamp: 1, Open: 10, High: 11, Low: 9, Close: 10, Volume: 1, RequestedAt: 7}}, requestedAt: requested, fetchedAt: fetched}
	if err := applySeedWindow(context.Background(), Metrics{}, store, "binance", "future", "XPL/USDT", "15m", w); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].RequestedAt != requested.UnixMilli() || got[0].FetchedAt != fetched.UnixMilli() {
		t.Fatalf("historical request bracket lost: %+v", got)
	}
	w = &seedWindow{historical: []candle.Candle{{Timestamp: 1, Open: 10, High: 11, Low: 9, Close: 10, Volume: 1, RequestedAt: 7}}}
	if err := applySeedWindow(context.Background(), Metrics{}, store, "binance", "future", "XPL/USDT", "15m", w); err != nil || got[0].RequestedAt != 0 {
		t.Fatalf("unstamped request gained false proof: %+v err=%v", got, err)
	}
}

type provenanceStore struct {
	onCurrent    func(candle.Candle)
	onHistorical func([]candle.Candle)
}

func (p *provenanceStore) AddStreamingCandle(_ context.Context, _, _, _, _ string, c candle.Candle) error {
	p.onCurrent(c)
	return nil
}

func (p *provenanceStore) SeedCandlesBatch(_ context.Context, _, _, _, _ string, cs []candle.Candle) error {
	p.onHistorical(cs)
	return nil
}
