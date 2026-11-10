package collector

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/coinalyze"
)

// THE DEEP SEEDING.
//
// Collecting only the tail resolves the client's complaint in MONTHS of process uptime; the
// complaint is about the regime of months BEFORE.
//
// The design is decided by a measurement against the real API (`BTCUSDT_PERP.A`): the ceiling
// is OF THE RESPONSE, not of the window. Requesting 2000 days, the provider returns ~2000
// points at any interval — 5.5 years daily, 11 months at `4h`, ~3 months at `1h`. Requesting
// more days brings the same points. Therefore: one request per (timeframe, endpoint) harvests
// everything, and the requested window must be exactly 2000 buckets — fewer leaves history on
// the table, more makes the coverage audit report as a gap what the provider never had.

func windowSeconds(t *testing.T, queries []url.Values) int64 {
	t.Helper()
	if len(queries) == 0 {
		t.Fatal("no history request was made")
	}
	from, _ := strconv.ParseInt(queries[0].Get("from"), 10, 64)
	to, _ := strconv.ParseInt(queries[0].Get("to"), 10, 64)
	return to - from
}

// TestSeedAsksForTheProvidersFullReach: the first cycle requests 2000 buckets, the measured
// response ceiling. A cycle that requested the tail would leave 5.5 years of daily data on the
// table.
func TestSeedAsksForTheProvidersFullReach(t *testing.T) {
	for _, tc := range []struct {
		timeframe   string
		wantSeconds int64
	}{
		{"1h", 2000 * 3600},
		{"4h", 2000 * 4 * 3600},
		{"1d", 2000 * 86400},
	} {
		poller, fake, _, _, _ := newCoinalyzeHarness(t, []string{tc.timeframe}, nil)
		if err := poller.PollOnce(context.Background()); err != nil {
			t.Fatalf("%s: PollOnce: %v", tc.timeframe, err)
		}
		got := windowSeconds(t, fake.queriesFor("/open-interest-history"))
		if got != tc.wantSeconds {
			t.Errorf("%s: seeding requested %d s (%d buckets), expected %d s (2000 buckets)",
				tc.timeframe, got, got/(tc.wantSeconds/2000), tc.wantSeconds)
		}
	}
}

// TestSeedRunsOnceThenTails: seeding is the FIRST pass, not every pass. Rewriting 2000 points
// per hour would spend writes confirming what is already there.
func TestSeedRunsOnceThenTails(t *testing.T) {
	poller, fake, _, _, _ := newCoinalyzeHarness(t, []string{"1d"}, nil)

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("first cycle: %v", err)
	}
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("second cycle: %v", err)
	}

	queries := fake.queriesFor("/open-interest-history")
	if len(queries) != 2 {
		t.Fatalf("expected 2 open interest requests, got %d", len(queries))
	}

	primeira := windowSeconds(t, queries[:1])
	segunda := windowSeconds(t, queries[1:])

	// The harness's `Lookback` is 72 h and it SWALLOWED this assertion: the tail rule is
	// `max(4 buckets, lookback)`, and for `1d` both sides exceed two days, so a mutant reducing
	// the tail from 4 to 1 bucket survived. The tail is measured against the BUCKET, not against
	// the configuration floor.
	poller.lookback = time.Hour
	if primeira != 2000*86400 {
		t.Errorf("the first cycle requested %d s, expected 2000 daily buckets", primeira)
	}
	if segunda >= primeira {
		t.Errorf("the second cycle requested %d s, not less than the seeding (%d s) — it is re-seeding every time",
			segunda, primeira)
	}
	// THE TAIL COVERS FOUR BUCKETS: the current one is still filling and the previous may be
	// revised by the provider. A one-bucket tail would never correct the penultimate one.
	if got := poller.windowFor("1d", false); got != 4*24*time.Hour {
		t.Errorf("the `1d` tail is %s, expected 4 buckets (96h)", got)
	}
	if got := poller.windowFor("1h", false); got != 4*time.Hour {
		t.Errorf("the `1h` tail is %s, expected 4 buckets", got)
	}
	// AND THE CONFIGURATION FLOOR still wins when it is LARGER — the tail is a minimum, not a
	// ceiling.
	poller.lookback = 30 * 24 * time.Hour
	if got := poller.windowFor("1d", false); got != 30*24*time.Hour {
		t.Errorf("with a 30-day lookback the tail was %s; the configured floor must win", got)
	}
}

// TestSeedWindowMatchesWhatTheProviderActuallyServes is the guard against the false gap.
//
// The coverage audit compares the REQUESTED interval with the RETURNED one. Requesting five
// years of `1h` when the provider keeps three months would mark the series as partially covered
// on every cycle — and a detector that always accuses, accuses nothing.
func TestSeedWindowMatchesWhatTheProviderActuallyServes(t *testing.T) {
	// Measured: the real reach per interval, in days.
	for _, tc := range []struct {
		timeframe  string
		medidoDias float64
	}{
		{"1d", 2000},  // 2021-03-03 .. today
		{"4h", 333.8}, // 2025-09-23 .. today
		{"1h", 83.3},  // 2026-05-31 .. today
	} {
		poller, fake, _, _, _ := newCoinalyzeHarness(t, []string{tc.timeframe}, nil)
		if err := poller.PollOnce(context.Background()); err != nil {
			t.Fatalf("%s: %v", tc.timeframe, err)
		}
		pedidoDias := float64(windowSeconds(t, fake.queriesFor("/open-interest-history"))) / 86400
		if diff := pedidoDias - tc.medidoDias; diff > 1 || diff < -1 {
			t.Errorf("%s: seeding requests %.1f days and the provider serves %.1f — the difference becomes a false gap on every cycle",
				tc.timeframe, pedidoDias, tc.medidoDias)
		}
	}
}

// THE FLEET IS RESOLVED PER CYCLE, not at construction.
//
// The configuration builds the fleet through DISCOVERY (`max_assets: 30`), which does not
// produce `symbols:`; reading only the static list means the only declared static symbol came
// from another exchange. Long history, which is the whole feature, reached 1 of 30.
func TestFleetIsResolvedPerCycleNotAtConstruction(t *testing.T) {
	poller, fake, _, _, _ := newCoinalyzeHarness(t, []string{"1d"}, nil)

	chamadas := 0
	poller.symbolsFunc = func(context.Context) []string {
		chamadas++
		return []string{"BTC/USDT"}
	}

	for i := 0; i < 3; i++ {
		if err := poller.PollOnce(context.Background()); err != nil {
			t.Fatalf("cycle %d: %v", i, err)
		}
	}
	if chamadas != 3 {
		t.Errorf("the fleet was resolved %d time(s) in 3 cycles; freezing it would collect the boot symbols forever", chamadas)
	}
	if len(fake.queriesFor("/open-interest-history")) != 3 {
		t.Errorf("there were %d open interest requests in 3 cycles", len(fake.queriesFor("/open-interest-history")))
	}
}

// AN EMPTY FLEET FALLS BACK TO THE STATIC LIST. Without it, a momentary registry failure would
// silently erase the entire collection — and "collected nothing" is indistinguishable from
// "there is nothing to collect" when nobody complains.
func TestAnEmptyFleetFallsBackToTheStaticList(t *testing.T) {
	poller, fake, _, _, _ := newCoinalyzeHarness(t, []string{"1d"}, nil)
	poller.symbolsFunc = func(context.Context) []string { return nil }

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}
	if n := len(fake.queriesFor("/open-interest-history")); n != 1 {
		t.Fatalf("an empty fleet produced %d request(s); the static list had to answer", n)
	}
}

// SEEDING IS PER SYMBOL, AND IT HEALS ITSELF.
//
// The first cycle runs immediately at boot, discovery has not populated the fleet yet,
// resolution falls back to a one-symbol static list — and a process-wide boolean stayed set.
// The other 29 assets would only get the deep pass in a next seeding window, which does not
// exist. The screen showed thirty and the table held one.
//
// A symbol that joins later — through discovery or configuration — receives the deep pass in
// the following cycle. Requesting deep for those that already have it is an idempotent rewrite;
// requesting shallow for those that do not is history that never arrives.
func TestSeed_ASymbolThatJoinsLaterIsSeededToo(t *testing.T) {
	// THE CATALOG MUST SERVE BOTH. The seeding identity is the id RESOLVED by the provider, not
	// the internal symbol: an asset the provider does not list cannot be seeded, and marking it
	// would mark what was never attempted.
	poller, fake, _, _, _ := newCoinalyzeHarness(t, []string{"1d"}, map[string]string{
		"/future-markets": `[
			{"symbol":"BTCUSDT_PERP.A","exchange":"A","symbol_on_exchange":"BTCUSDT","oi_lq_vol_denominated_in":"BASE_ASSET"},
			{"symbol":"ETHUSDT_PERP.A","exchange":"A","symbol_on_exchange":"ETHUSDT","oi_lq_vol_denominated_in":"BASE_ASSET"}
		]`,
	})

	frota := []string{"BTC/USDT"}
	poller.symbolsFunc = func(context.Context) []string { return frota }

	// Cycle 1: one-symbol fleet, deep seeding.
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("cycle 1: %v", err)
	}
	// Cycle 2: same fleet, already seeded — tail.
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("cycle 2: %v", err)
	}
	// Cycle 3: discovery brought a NEW symbol.
	frota = []string{"BTC/USDT", "ETH/USDT"}
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("cycle 3: %v", err)
	}

	queries := fake.queriesFor("/open-interest-history")
	// FOUR, NOT THREE. With seeding marked per symbol, a MIXED-fleet cycle emits two requests:
	// depth travels as `from` in the query, so whoever re-seeds and whoever goes on the tail do
	// not fit in the same request. It costs one extra request and no extra symbol in the budget,
	// which is charged per symbol.
	if len(queries) != 4 {
		t.Fatalf("there were %d requests in 3 cycles, expected 4 (cycle 3 has a mixed fleet)", len(queries))
	}
	profunda := int64(2000 * 86400)
	if got := windowSeconds(t, queries[:1]); got != profunda {
		t.Errorf("cycle 1 requested %d s, expected the seeding (%d s)", got, profunda)
	}
	if got := windowSeconds(t, queries[1:2]); got >= profunda {
		t.Errorf("cycle 2 requested %d s; the fleet was already seeded, it had to be a tail", got)
	}
	if got := windowSeconds(t, queries[2:3]); got != profunda {
		t.Errorf("cycle 3 requested %d s; a NEW symbol entered the fleet and did not receive the deep pass", got)
	}
	// AND THE DEEP PASS COVERS ONLY THE NEW ONE — the half that the count alone does not show:
	// with a per-cycle mark, this request would bring both symbols.
	if got := queries[2].Get("symbols"); got != "ETHUSDT_PERP.A" {
		t.Errorf("cycle 3's deep pass covered %q; it belongs to the NEW symbol, alone", got)
	}
	if got := windowSeconds(t, queries[3:4]); got >= profunda {
		t.Errorf("cycle 3's second request asked for %d s; whoever was already seeded goes on the tail", got)
	}
	if got := queries[3].Get("symbols"); got != "BTCUSDT_PERP.A" {
		t.Errorf("cycle 3's tail covered %q; it belongs to whoever was already seeded", got)
	}
}

// AND A FLEET THAT ONLY SHRINKS does not re-seed: leaving the fleet is no reason to rewrite
// 2,000 points of those that stayed.
func TestSeed_AShrinkingFleetDoesNotReseed(t *testing.T) {
	poller, fake, _, _, _ := newCoinalyzeHarness(t, []string{"1d"}, nil)

	frota := []string{"BTC/USDT", "ETH/USDT"}
	poller.symbolsFunc = func(context.Context) []string { return frota }
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("ciclo 1: %v", err)
	}
	frota = []string{"BTC/USDT"}
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("ciclo 2: %v", err)
	}

	queries := fake.queriesFor("/open-interest-history")
	if got := windowSeconds(t, queries[1:2]); got >= int64(2000*86400) {
		t.Errorf("the fleet shrank and the cycle re-seeded (%d s)", got)
	}
}

// THE SAMPLE INSIDE THE BUCKET IS PER SERIES, and it is what keeps both provenances in the same
// bucket.
//
// Both declare `LabelIntervalStart` and both return today's bucket at 00:00Z — the convention is
// right in both. What differs is WHERE INSIDE THE INTERVAL each provider samples, a dimension
// `LabelConvention` does not model: an honest interval-start label still leaves open whether the
// number is from the first instant or the last.
//
// MEASURED against Binance's `/futures/data/openInterestHist`, 30 daily buckets (BTC; ETH
// 0.079% and SOL 0.029% on D-1 with `.c`, hence systematic):
//
//	                  D-1       D        D+1
//	coinalyze `.o`  1.265%   0.018%    1.259%
//	coinalyze `.c`  0.018%   1.232%    1.639%
//
// Funding stays on the close because the measurement says it is already aligned there: `.c`
// against our own venue series gives EXACTLY 9900.000% on D across 60 buckets — the 99 is the
// unit factor the poller converts, and the zero dispersion is perfect alignment.
func TestCoinalyzeFieldChoice_IsPerSeriesNotPerProvider(t *testing.T) {
	poller, _, store, _, _ := newCoinalyzeHarness(t, []string{"1d"}, nil)
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}
	snap := firstWriteAt(t, store, coinalyzeFixtureTsMs)

	// The fixture has DIFFERENT `o` and `c` on both axes, otherwise this case does not
	// discriminate.
	const oiAbertura, oiFechamento = 107640.019, 106117.954
	const frAberturaPct, frFechamentoPct = 0.004964, 0.01

	if snap.OpenInterestBase == nil || *snap.OpenInterestBase != oiAbertura {
		t.Errorf("open interest = %v, expected the OPEN (%v) — the close is the sample at the following instant",
			snap.OpenInterestBase, oiAbertura)
	}
	if snap.OpenInterestBase != nil && *snap.OpenInterestBase == oiFechamento {
		t.Error("open interest wrote the close: the series is one bucket ahead of the venue again")
	}

	// Funding is PERCENT at the source and a fraction in storage — the comparison is over the
	// converted close.
	if snap.FundingRate == nil {
		t.Fatal("funding missing")
	}
	if got := *snap.FundingRate; got != frFechamentoPct/100 {
		t.Errorf("funding = %v, expected the converted CLOSE (%v)", got, frFechamentoPct/100)
	}
	if got := *snap.FundingRate; got == frAberturaPct/100 {
		t.Error("funding wrote the open: the measurement says the close is what is aligned")
	}
}

// A FAILING CYCLE DOES NOT BURN THE SEEDING.
//
// The mark is applied AFTER the cycle. Marking before would let a first cycle in which the
// provider returns 500 at all three endpoints log a WARN, return `nil`, and keep the mark: from
// then on only a four-bucket tail, and the 2000 history points are never requested again until
// the process restarts.
//
// AND SEEDING IS NOT RE-REQUESTED IN THE IMMEDIATELY FOLLOWING CYCLE: the failed cycle puts the
// fleet in backoff, and the next cycle requests the tail. What this case asserts is that the
// seeding was NOT LOST — it returns as soon as the wait advances — which is what distinguishes
// it from a burn:
//
//	cycle 1  provider down   → no collection, one-cycle backoff
//	cycle 2  provider back   → tail (the backoff), and the symbol is NOT taken as seeded
//	cycle 3                  → the deep window returns, whole
func TestSeed_AFailedCycleDoesNotBurnTheSeeding(t *testing.T) {
	falhando := map[string]string{}
	poller, fake, _, _, _ := newCoinalyzeHarness(t, []string{"1d"}, falhando)

	// Cycle 1 with all three history endpoints down.
	fake.mu.Lock()
	for _, path := range []string{"/open-interest-history", "/funding-rate-history", "/long-short-ratio-history"} {
		delete(fake.bodies, path) // absent => 404 => batch error
	}
	fake.mu.Unlock()

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("a cycle with the provider down had to degrade, not abort: %v", err)
	}

	// The provider returns.
	fake.mu.Lock()
	fake.bodies["/open-interest-history"] = coinalyzeOpenInterestBody
	fake.bodies["/funding-rate-history"] = coinalyzeFundingBody
	fake.bodies["/long-short-ratio-history"] = coinalyzeLongShortBody
	fake.mu.Unlock()

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("cycle 2: %v", err)
	}

	queries := fake.queriesFor("/open-interest-history")
	if len(queries) < 2 {
		t.Fatalf("there were %d open interest request(s) in 2 cycles", len(queries))
	}
	// THE CYCLE AFTER THE FAILURE REQUESTS THE TAIL, not the 2000 buckets: that is the backoff —
	// without it, two consecutive cycles emitted the same deep request, indefinitely.
	if got := windowSeconds(t, queries[1:2]); got >= 2000*86400 {
		t.Fatalf("the cycle immediately after the failure requested %d s; repeated failure amplifies "+
			"the request at the same pace", got)
	}

	// AND THE WAIT ADVANCES: the following cycle requests the WHOLE deep window again. This half
	// separates "backoff" from "burn" — the seeding was not lost, it was postponed.
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("cycle 3: %v", err)
	}
	queries = fake.queriesFor("/open-interest-history")
	if got := windowSeconds(t, queries[len(queries)-1:]); got != 2000*86400 {
		t.Fatalf("the third cycle requested %d s; the seeding was burned by a cycle that collected "+
			"nothing — backoff had to postpone it, not lose it", got)
	}
}

// AND A SYMBOL THE CATALOG REFUSES DOES NOT ENTER THE MARK.
//
// A mark covering the CONFIGURED fleet instead of the RESOLVED one lets an asset refused by
// denomination, or not yet listed by the provider, enter it without ever having been collected —
// and when the catalog reloads (every six hours) and starts serving it, it is already recorded as
// seeded. It would keep four tail buckets and no history forever.
//
// The DISCRIMINATING case is that one: refused in the first cycle, listed in the second.
type catalogoQueMuda struct {
	mu      sync.Mutex
	aceitos map[string]string
}

func (c *catalogoQueMuda) Resolve(_ context.Context, symbol string) (coinalyze.CoinalyzeMarket, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	id, ok := c.aceitos[symbol]
	if !ok {
		return coinalyze.CoinalyzeMarket{}, fmt.Errorf("%w: %s", coinalyze.ErrCoinalyzeSymbolNotFound, symbol)
	}
	return coinalyze.CoinalyzeMarket{Symbol: id, DenominatedIn: "BASE_ASSET"}, nil
}

func (c *catalogoQueMuda) passaAAceitar(symbol, id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.aceitos[symbol] = id
}

func TestSeed_ASymbolRefusedNowAndListedLaterIsStillSeeded(t *testing.T) {
	poller, fake, _, _, _ := newCoinalyzeHarness(t, []string{"1d"}, nil)
	poller.symbolsFunc = func(context.Context) []string { return []string{"BTC/USDT", "AINDANAO/USDT"} }

	cat := &catalogoQueMuda{aceitos: map[string]string{"BTC/USDT": "BTCUSDT_PERP.A"}}
	poller.catalog = cat

	// Cycle 1: the provider does not list the second one yet.
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("cycle 1: %v", err)
	}
	// Cycle 2: nothing changed — tail.
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("cycle 2: %v", err)
	}
	// Cycle 3: the catalog reloaded and now lists the second one.
	cat.passaAAceitar("AINDANAO/USDT", "AINDANAOUSDT_PERP.A")
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("cycle 3: %v", err)
	}

	queries := fake.queriesFor("/open-interest-history")
	// FOUR: cycle 3 has a mixed fleet (the newly listed one to seed, BTC already seeded) and
	// per-symbol seeding separates the two requests. See the note in
	// TestSeed_ASymbolThatJoinsLaterIsSeededToo.
	if len(queries) != 4 {
		t.Fatalf("there were %d requests in 3 cycles, expected 4 (cycle 3 has a mixed fleet)", len(queries))
	}
	profunda := int64(2000 * 86400)
	if got := windowSeconds(t, queries[:1]); got != profunda {
		t.Errorf("cycle 1 requested %d s, expected the seeding", got)
	}
	if got := windowSeconds(t, queries[1:2]); got >= profunda {
		t.Errorf("cycle 2 re-seeded (%d s) with nothing having changed", got)
	}
	if got := windowSeconds(t, queries[2:3]); got != profunda {
		t.Errorf("cycle 3 requested %d s; the symbol started being listed and did not receive the deep pass — it was marked as seeded without ever having been collected", got)
	}
	if got := queries[2].Get("symbols"); got != "AINDANAOUSDT_PERP.A" {
		t.Errorf("cycle 3's deep pass covered %q; it belongs to the newly listed symbol, alone", got)
	}
	if got := queries[3].Get("symbols"); got != "BTCUSDT_PERP.A" {
		t.Errorf("cycle 3's tail covered %q; it belongs to whoever was already seeded", got)
	}
}
