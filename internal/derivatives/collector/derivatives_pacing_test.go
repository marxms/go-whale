package collector

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/market"
	"github.com/marxms/go-whale/internal/platform/config"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// ─────────────────────────────────────────────────────────────────────────────
// THE PACED POLL CYCLE
//
// The defect these tests hold: a SEQUENTIAL pollAll, one symbol and three blocking REST
// round trips at a time. Measured in production, the spread of `timestamp` across a third
// of the fleet was 562s against a declared 300s interval, and because the caller is a
// time.Ticker a cycle that outlasts its period does not merely run late — the runtime
// drops the ticks that fall inside it. The 5m series carried the signature: gaps of
// exactly 300s or 600s and nothing between, with 17.9% of the newest 40 points adjacent
// against 97.5-100% of the older ones written by the PARALLEL bootstrap.
//
// Every test here drives the pacing source and the clock itself. Nothing sleeps: the
// admission channel is UNBUFFERED, so a send from the test and the receive inside
// pollAll are a rendezvous, and after it returns the loop is provably parked on the
// next receive. That is what makes "no symbol entered early" an exact claim rather
// than a timing guess.
// ─────────────────────────────────────────────────────────────────────────────

// manualPacer is the admission source under the test's control.
//
// It doubles as the cycle's CLOCK: each tick advances a virtual instant, so a cycle
// that spans 285 virtual seconds costs the test nothing in wall time. advancePerTick
// is normally the interval pollAll asked to be paced at — a cycle keeping its own
// declared pace — and is raised above it to model one that does not.
type manualPacer struct {
	mu             sync.Mutex
	now            time.Time
	askedFor       time.Duration
	advancePerTick time.Duration

	ch   chan time.Time
	stop chan struct{}
}

func newManualPacer() *manualPacer {
	return &manualPacer{
		now:  time.Unix(0, 0),
		ch:   make(chan time.Time),
		stop: make(chan struct{}),
	}
}

// build is what the poller calls to obtain its admission source for a cycle. It
// records the pace the poller derived, which is how a test ties the mechanism to the
// arithmetic in admissionInterval.
func (p *manualPacer) build(d time.Duration) (<-chan time.Time, func()) {
	p.mu.Lock()
	p.askedFor = d
	if p.advancePerTick == 0 {
		p.advancePerTick = d
	}
	p.mu.Unlock()
	return p.ch, func() { close(p.stop) }
}

func (p *manualPacer) clock() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.now
}

func (p *manualPacer) pace() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.askedFor
}

// tick advances the virtual clock and releases exactly one symbol. The send blocks
// until pollAll receives it, so when tick returns the poller has consumed it and no
// further symbol can enter without another call.
func (p *manualPacer) tick(t *testing.T) {
	t.Helper()
	p.mu.Lock()
	p.now = p.now.Add(p.advancePerTick)
	p.mu.Unlock()

	select {
	case p.ch <- p.now:
	case <-time.After(testDeadline):
		t.Fatalf("the poll loop did not take an admission tick within %s — it is not paced by this source", testDeadline)
	}
}

// testDeadline bounds every blocking wait in this file. It is a deadlock detector, not
// a pace: nothing here is supposed to take measurable wall time, so any wait that
// reaches it is a test that would otherwise hang.
const testDeadline = 5 * time.Second

// pacedFakeClient is the venue side of a paced cycle.
//
// It is mutex-guarded because ONE client instance serves the whole fleet — p.clients is
// keyed by exchange, not by symbol, so the fan-out reaches this object concurrently,
// exactly as it reached the real venue client.
type pacedFakeClient struct {
	mu sync.Mutex
	// enteredAt is the virtual instant each symbol's work began, in admission order.
	enteredAt []time.Duration
	// entries announces each entry so a test can wait for one without polling.
	entries chan string

	clock func() time.Time
	// gate, when set, blocks the named symbol until the channel is closed.
	gateSymbol string
	gate       chan struct{}

	// history is what the settlement-interval revalidation reads. Serving it makes
	// every symbol write to the shared fundingProfileCache, which is the shared
	// mutable state the fan-out has to be safe against.
	history []derivatives.FundingRateHistoryPoint
}

func newPacedFakeClient(clock func() time.Time, capacity int) *pacedFakeClient {
	return &pacedFakeClient{
		entries: make(chan string, capacity),
		clock:   clock,
	}
}

func (c *pacedFakeClient) FundingRate(_ context.Context, symbol string) (derivatives.FundingRate, error) {
	c.mu.Lock()
	c.enteredAt = append(c.enteredAt, c.clock().Sub(time.Unix(0, 0)))
	gate := c.gate
	gated := c.gateSymbol == symbol
	c.mu.Unlock()

	c.entries <- symbol
	if gated && gate != nil {
		<-gate
	}
	return derivatives.FundingRate{Rate: fptr(0.0001), MarkPrice: fptr(100)}, nil
}

func (c *pacedFakeClient) OpenInterest(context.Context, string) (derivatives.OpenInterest, error) {
	return derivatives.OpenInterest{Amount: fptr(1000)}, nil
}

func (c *pacedFakeClient) FundingRateHistory(context.Context, string, int64) ([]derivatives.FundingRateHistoryPoint, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.history, nil
}

func (c *pacedFakeClient) LoadMarkets(context.Context) (map[string]market.MarketInfo, error) {
	return nil, errNotWiredInFake
}

func (c *pacedFakeClient) OpenInterestHistory(context.Context, string, string, int64, int64, int64) ([]derivatives.OpenInterestHistoryPoint, error) {
	return nil, errNotWiredInFake
}

func (c *pacedFakeClient) LongShortRatioHistory(context.Context, string, string, int64, int64, int64) ([]derivatives.LongShortRatioPoint, error) {
	return nil, errNotWiredInFake
}

func (c *pacedFakeClient) Close() []error { return nil }

// admissions returns the virtual instant of each symbol's entry, in the order they
// entered.
func (c *pacedFakeClient) admissions() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.enteredAt...)
}

// savingStore counts completed symbols. SaveSnapshot is the LAST thing pollSymbol
// does, so a save is the only honest signal that a symbol finished rather than merely
// started.
type savingStore struct {
	recordingStore
	saves chan string
}

func newSavingStore(capacity int) *savingStore {
	return &savingStore{saves: make(chan string, capacity)}
}

func (s *savingStore) SaveSnapshot(_ context.Context, snapshot *derivatives.DerivativesSnapshot) error {
	s.saves <- snapshot.Symbol
	return nil
}

func (s *savingStore) GetLatestSnapshot(context.Context, string, string, string) (*derivatives.DerivativesSnapshot, error) {
	return nil, nil
}

var _ pollerStore = (*savingStore)(nil)

// pacingPoller wires a poller whose pace, clock and venue are all the test's.
func pacingPoller(pacer *manualPacer, client derivatives.Source, store pollerStore,
	intervalSec, symbols int, logs ...*slog.Logger,
) *DerivativesPoller {
	scopes := make([]SymbolScope, symbols)
	for i := range scopes {
		scopes[i] = SymbolScope{
			ExchangeID: "binance",
			MarketType: "future",
			Symbol:     fmt.Sprintf("SYM%04d/USDT", i),
		}
	}

	return &DerivativesPoller{
		cfg:             config.DerivativesConfig{Enabled: true, PollIntervalSec: intervalSec, MaxSeriesSize: 200},
		store:           store,
		symbols:         scopes,
		clients:         map[string]derivatives.Source{"binance": client},
		fundingProfiles: NewFundingProfileRegistry(),
		now:             pacer.clock,
		newPacer:        pacer.build,
		log:             logger.OrDiscard(logs...),
		// THE CONVENTION THE REAL COLLECTOR DECLARES comes from the TABLE, through this double's
		// venue (`binance`), not from a field the double would have to remember to fill:
		// `pollAll` refuses a feed without a declaration BEFORE the first fetch, and a double
		// pointing at an undeclared venue would measure the refusal instead of what this file
		// measures.
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The arithmetic
// ─────────────────────────────────────────────────────────────────────────────

func TestAdmissionInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cycle       time.Duration
		symbols     int
		want        time.Duration
		wantFloored bool
	}{
		{
			// THE PRODUCTION CASE. 285s of admission window over 500 symbols. Well clear
			// of the 400ms floor, and ~526 long/short requests in any rolling five-minute
			// window against a budget of 1000.
			name:    "the production fleet paces at 570ms, half the venue's long/short budget",
			cycle:   300 * time.Second,
			symbols: 500,
			want:    570 * time.Millisecond,
		},
		{
			// The fleet is discovered, not declared, so the exact count moves. One symbol
			// either side must not flip the floor on.
			name:    "one symbol more does not touch the floor",
			cycle:   300 * time.Second,
			symbols: 501,
			want:    285 * time.Second / 501,
		},
		{
			// The reserve is what makes the cycle fit at all: without it the pace would be
			// 300s/10 = 30s and the last symbol would enter with nothing left.
			name:    "a small fleet keeps the same one-twentieth reserve",
			cycle:   300 * time.Second,
			symbols: 10,
			want:    28500 * time.Millisecond,
		},
		{
			// The misconfiguration the floor exists for: 500 symbols on a 60s interval
			// wants one every 114ms, which is 2632 long/short requests in a rolling
			// five-minute window against a ceiling of 1000.
			name:        "an interval the venue cannot serve is clamped and reported",
			cycle:       60 * time.Second,
			symbols:     500,
			want:        minAdmissionInterval,
			wantFloored: true,
		},
		{
			// An unconfigured interval must not produce a zero-period ticker, which panics.
			name:        "a zero interval falls back to the floor rather than to zero",
			cycle:       0,
			symbols:     10,
			want:        minAdmissionInterval,
			wantFloored: true,
		},
		{
			name:    "an empty fleet has nothing to pace",
			cycle:   300 * time.Second,
			symbols: 0,
			want:    minAdmissionInterval,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, floored := admissionInterval(tt.cycle, tt.symbols)
			if got != tt.want {
				t.Errorf("admissionInterval(%s, %d) = %s, want %s", tt.cycle, tt.symbols, got, tt.want)
			}
			if floored != tt.wantFloored {
				t.Errorf("admissionInterval(%s, %d) floored = %v, want %v",
					tt.cycle, tt.symbols, floored, tt.wantFloored)
			}
			if got < minAdmissionInterval {
				t.Errorf("admissionInterval(%s, %d) = %s, which is faster than the %s the venue's "+
					"long/short pool serves — that is the ban, not a performance choice",
					tt.cycle, tt.symbols, got, minAdmissionInterval)
			}
		})
	}
}

// TestCycleStatsOverrun holds the predicate that decides whether an operator hears
// about a cycle at all.
func TestCycleStatsOverrun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		elapsed       time.Duration
		interval      time.Duration
		wantOverran   bool
		wantLapFactor float64
	}{
		{
			// The measured serial cycle, which nothing reported.
			name:          "the measured serial cycle is an overrun at 1,87x",
			elapsed:       562 * time.Second,
			interval:      300 * time.Second,
			wantOverran:   true,
			wantLapFactor: 562.0 / 300.0,
		},
		{
			name:          "a paced cycle inside its period is not an overrun",
			elapsed:       285*time.Second + 550*time.Millisecond,
			interval:      300 * time.Second,
			wantLapFactor: 285.55 / 300.0,
		},
		{
			// Exactly at the period is not yet a lost tick.
			name:          "a cycle landing exactly on its period is not an overrun",
			elapsed:       300 * time.Second,
			interval:      300 * time.Second,
			wantLapFactor: 1,
		},
		{
			name:        "one millisecond over is an overrun, because that is where ticks start dropping",
			elapsed:     300*time.Second + time.Millisecond,
			interval:    300 * time.Second,
			wantOverran: true,
		},
		{
			name:    "with no configured interval there is nothing to overrun",
			elapsed: time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := cycleStats{elapsed: tt.elapsed, interval: tt.interval}
			if got := s.overran(); got != tt.wantOverran {
				t.Errorf("overran() = %v, want %v (elapsed %s, interval %s)",
					got, tt.wantOverran, tt.elapsed, tt.interval)
			}
			if tt.wantLapFactor != 0 {
				if got := s.lapFactor(); abs(got-tt.wantLapFactor) > 1e-9 {
					t.Errorf("lapFactor() = %v, want %v", got, tt.wantLapFactor)
				}
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The mechanism
// ─────────────────────────────────────────────────────────────────────────────

// TestPollAllAdmitsOneSymbolPerTickAndNeverInABurst is the anti-burst claim, and it is
// the one the venue cares about: 500 requests fired together every 300s put 1000 into a
// single rolling five-minute window, which is the exact ceiling of the long/short pool.
//
// The assertion is exact rather than statistical. The admission channel is unbuffered,
// so once a send returns the loop has consumed that tick and is parked on the next
// receive — no symbol beyond the one just released CAN have entered.
func TestPollAllAdmitsOneSymbolPerTickAndNeverInABurst(t *testing.T) {
	const symbols = 6

	pacer := newManualPacer()
	client := newPacedFakeClient(pacer.clock, symbols)
	store := newSavingStore(symbols)
	poller := pacingPoller(pacer, client, store, 300, symbols)

	done := make(chan cycleStats, 1)
	go func() { done <- poller.pollAll(context.Background()) }()

	for i := 0; i < symbols; i++ {
		// The FIRST symbol enters on its own; every later one needs a tick.
		if i > 0 {
			pacer.tick(t)
		}

		entered := receiveWithin(t, client.entries, "symbol entry")
		if want := poller.symbols[i].Symbol; entered != want {
			t.Fatalf("tick %d admitted %s, want %s — symbols must enter in fleet order", i, entered, want)
		}

		// Nothing else can be waiting. A burst would have queued the rest here.
		if extra := len(client.entries); extra != 0 {
			t.Fatalf("after admitting %d symbol(s), %d more had already entered — the cycle is "+
				"bursting, not pacing", i+1, extra)
		}
	}

	stats := receiveWithin(t, done, "the cycle to finish")
	if stats.ok != symbols {
		t.Errorf("cycle reported %d ok, want %d", stats.ok, symbols)
	}
	if stats.errors != 0 {
		t.Errorf("cycle reported %d errors, want 0", stats.errors)
	}

	// The mechanism is tied to the arithmetic: the pace the loop asked for is the
	// derived one, not something the loop invented.
	wantPace, _ := admissionInterval(300*time.Second, symbols)
	if got := pacer.pace(); got != wantPace {
		t.Errorf("the cycle paced at %s, want the derived %s", got, wantPace)
	}
	if got := stats.admission; got != wantPace {
		t.Errorf("cycle reported admission %s, want %s", got, wantPace)
	}
}

// TestPollAllFitsItsIntervalWhereTheSerialLoopDidNot runs the production shape — 500
// symbols on a 300s interval — over a recorded virtual timeline.
//
// The comparison is against the same fleet's measured serial cost. 562s for ~500
// symbols is ~1,124s per symbol; taken one after another that is 562s against a 300s
// period. Paced, the admissions span 284,43s and the last symbol's own 1,124s lands
// inside the drain reserve.
func TestPollAllFitsItsIntervalWhereTheSerialLoopDidNot(t *testing.T) {
	const (
		symbols     = 500
		intervalSec = 300
	)
	// perSymbolCost is the MEASURED figure, not a convenient one: 562s of serial cycle
	// over ~500 symbols.
	const perSymbolCost = 1124 * time.Millisecond

	interval := time.Duration(intervalSec) * time.Second

	pacer := newManualPacer()
	client := newPacedFakeClient(pacer.clock, symbols)
	store := newSavingStore(symbols)
	poller := pacingPoller(pacer, client, store, intervalSec, symbols)

	done := make(chan cycleStats, 1)
	go func() { done <- poller.pollAll(context.Background()) }()

	for i := 0; i < symbols; i++ {
		if i > 0 {
			pacer.tick(t)
		}
		receiveWithin(t, client.entries, "symbol entry")
	}
	stats := receiveWithin(t, done, "the cycle to finish")

	if stats.ok != symbols {
		t.Fatalf("cycle reported %d ok, want %d", stats.ok, symbols)
	}

	admissions := client.admissions()
	if len(admissions) != symbols {
		t.Fatalf("recorded %d admissions, want %d", len(admissions), symbols)
	}

	// 1. Spread, not burst: the entries occupy the admission window rather than an
	//    instant. The last one is (N-1) paces in.
	pace, floored := admissionInterval(interval, symbols)
	if floored {
		t.Fatalf("the production configuration should not be pacing at the venue floor")
	}
	first, last := admissions[0], admissions[len(admissions)-1]
	if first != 0 {
		t.Errorf("the first symbol entered at %s, want immediately", first)
	}
	if want := time.Duration(symbols-1) * pace; last != want {
		t.Errorf("the last symbol entered at %s, want %s", last, want)
	}

	// 2. The cycle FITS: the last symbol still has its whole cost inside the period.
	cycleEnd := last + perSymbolCost
	if cycleEnd > interval {
		t.Errorf("the cycle ends at %s, past its %s period — the drain reserve does not cover "+
			"the measured %s a symbol costs", cycleEnd, interval, perSymbolCost)
	}

	// 3. And the thing it replaced did not. Stated as an assertion so the comparison
	//    cannot quietly stop being true if the numbers are ever edited.
	if serial := time.Duration(symbols) * perSymbolCost; serial <= interval {
		t.Fatalf("this fleet would not have overrun serially (%s in a %s period), so it does "+
			"not hold the defect", serial, interval)
	}

	if stats.overran() {
		t.Errorf("cycle reported an overrun: took %s of a %s interval", stats.elapsed, stats.interval)
	}
}

// TestPollAllSlowSymbolDoesNotStallTheRest is the whole reason for the pool. Under the
// sequential loop one unresponsive symbol held the entire fleet behind it, and the
// symbols after it in registry order simply were not polled that cycle.
func TestPollAllSlowSymbolDoesNotStallTheRest(t *testing.T) {
	const symbols = 5

	pacer := newManualPacer()
	client := newPacedFakeClient(pacer.clock, symbols)
	store := newSavingStore(symbols)
	poller := pacingPoller(pacer, client, store, 300, symbols)

	// The FIRST symbol hangs — the worst position, because it is the one the serial
	// loop would have blocked on before reaching any other.
	gate := make(chan struct{})
	client.mu.Lock()
	client.gateSymbol = poller.symbols[0].Symbol
	client.gate = gate
	client.mu.Unlock()

	done := make(chan cycleStats, 1)
	go func() { done <- poller.pollAll(context.Background()) }()

	for i := 1; i < symbols; i++ {
		pacer.tick(t)
	}

	// Every other symbol must COMPLETE — a save, not just an entry — while the first
	// one is still hanging.
	finished := make(map[string]bool, symbols-1)
	for i := 1; i < symbols; i++ {
		finished[receiveWithin(t, store.saves, "a symbol to finish while another hangs")] = true
	}
	if len(finished) != symbols-1 {
		t.Fatalf("%d distinct symbols finished, want %d: %v", len(finished), symbols-1, finished)
	}
	if finished[poller.symbols[0].Symbol] {
		t.Fatalf("%s finished, but the test is holding it — the gate is not wired",
			poller.symbols[0].Symbol)
	}

	close(gate)

	if got := receiveWithin(t, store.saves, "the held symbol to finish"); got != poller.symbols[0].Symbol {
		t.Errorf("the released save was %s, want %s", got, poller.symbols[0].Symbol)
	}
	stats := receiveWithin(t, done, "the cycle to finish")
	if stats.ok != symbols {
		t.Errorf("cycle reported %d ok, want %d — a slow symbol must not cost the fleet", stats.ok, symbols)
	}
}

// TestPollAllReportsAnOverrun holds the reporting: a duration line at Debug leaves a
// cycle running at 1.87x its period invisible.
func TestPollAllReportsAnOverrun(t *testing.T) {
	const (
		symbols     = 4
		intervalSec = 300
	)

	pacer := newManualPacer()
	// Each admission costs a whole period of virtual time — a cycle wildly outside its
	// own declared pace, which is what a lapped ticker looks like from inside.
	pacer.advancePerTick = time.Duration(intervalSec) * time.Second

	client := newPacedFakeClient(pacer.clock, symbols)
	store := newSavingStore(symbols)
	poller := pacingPoller(pacer, client, store, intervalSec, symbols)

	done := make(chan cycleStats, 1)
	go func() { done <- poller.pollAll(context.Background()) }()

	for i := 0; i < symbols; i++ {
		if i > 0 {
			pacer.tick(t)
		}
		receiveWithin(t, client.entries, "symbol entry")
	}
	stats := receiveWithin(t, done, "the cycle to finish")

	if !stats.overran() {
		t.Fatalf("a cycle of %s on a %s interval was not reported as an overrun",
			stats.elapsed, stats.interval)
	}
	if want := time.Duration(symbols-1) * pacer.advancePerTick; stats.elapsed != want {
		t.Errorf("cycle elapsed = %s, want %s — the cycle is not measuring itself against its own clock",
			stats.elapsed, want)
	}
	if got, want := stats.lapFactor(), float64(symbols-1); abs(got-want) > 1e-9 {
		t.Errorf("lapFactor = %.2f, want %.2f", got, want)
	}

	// reportCycle is what an operator actually sees. It must not panic on any branch,
	// and the overrun branch is the one exercised here.
	reportCycle(stats)
}

// TestPollAllStopsAdmittingWhenTheCycleIsCancelled. A cycle now spans the whole poll
// interval by design, so a shutdown that is only noticed between cycles would be
// noticed up to 300s late. The scheduler cancels the context; admission must observe it
// from inside the cycle.
func TestPollAllStopsAdmittingWhenTheCycleIsCancelled(t *testing.T) {
	const symbols = 5

	pacer := newManualPacer()
	client := newPacedFakeClient(pacer.clock, symbols)
	poller := pacingPoller(pacer, client, newSavingStore(symbols), 300, symbols)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan cycleStats, 1)
	go func() { done <- poller.pollAll(ctx) }()

	receiveWithin(t, client.entries, "the first symbol")
	cancel()

	stats := receiveWithin(t, done, "the cycle to abandon admission")
	if stats.ok+stats.errors != 1 {
		t.Errorf("cycle processed %d symbols after cancellation, want only the one already admitted",
			stats.ok+stats.errors)
	}
}

// TestPollAllConcurrentSymbolsShareTheFundingCache exists for the race detector.
//
// fundingProfileCache is package-level mutable state that every symbol touches:
// fetchDerivatives READS the measured rung and revalidateFundingInterval WRITES it, and
// on a cold cache revalidationDue is true for every symbol, so all of them write. Under
// the sequential loop the RWMutex was precautionary; under the fan-out it is the only
// thing standing between the fleet and a torn map.
//
// Run under -race this fails loudly if the guard is ever dropped.
func TestPollAllConcurrentSymbolsShareTheFundingCache(t *testing.T) {
	const symbols = 64

	pacer := newManualPacer()
	client := newPacedFakeClient(pacer.clock, symbols)
	// Two settlements four hours apart: enough for measureIntervalHours to return a
	// rung, so every symbol WRITES to the shared cache rather than only reading it.
	client.history = []derivatives.FundingRateHistoryPoint{
		{Timestamp: iptr(1_700_000_000_000)},
		{Timestamp: iptr(1_700_000_000_000 + 4*3_600_000)},
	}

	store := newSavingStore(symbols)
	poller := pacingPoller(pacer, client, store, 300, symbols)

	done := make(chan cycleStats, 1)
	go func() { done <- poller.pollAll(context.Background()) }()

	for i := 0; i < symbols; i++ {
		if i > 0 {
			pacer.tick(t)
		}
		receiveWithin(t, client.entries, "symbol entry")
	}
	stats := receiveWithin(t, done, "the cycle to finish")

	if stats.ok != symbols {
		t.Fatalf("cycle reported %d ok, want %d", stats.ok, symbols)
	}

	// Every symbol deposited the measured rung, and they are all readable afterwards —
	// a map that was written concurrently and survived is only half the claim.
	for _, scope := range poller.symbols {
		cadence, ok := poller.fundingProfiles.measuredFor(scope)
		if !ok || cadence.Hours != 4 {
			t.Fatalf("%s measured rung = %v (present %v), want 4h — the concurrent writes did not "+
				"all land", scope.Symbol, cadence.Hours, ok)
		}
	}
}

// receiveWithin takes one value from a channel or fails the test. Nothing in this file
// is supposed to wait, so reaching the deadline means a deadlock, not slowness.
func receiveWithin[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(testDeadline):
		var zero T
		t.Fatalf("timed out after %s waiting for %s", testDeadline, what)
		return zero
	}
}

func iptr(v int64) *int64 { return &v }
