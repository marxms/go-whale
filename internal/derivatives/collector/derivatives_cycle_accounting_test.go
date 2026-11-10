package collector

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/platform/config"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

var (
	_ pollerStore        = (*accountingStore)(nil)
	_ cycleWriteReporter = (*accountingStore)(nil)
)

// THE PRODUCTION STORE MUST ANSWER FOR ITS DESTINATIONS, and this is a compile-time ratchet,
// not a hope. The partition check is done by type assertion; if `SaveSnapshotForCycle`
// disappeared from the concrete store, the assertion would fall to the usual path in SILENCE
// and the counter would again claim writes that did not happen. Here the package stops
// compiling — the loudest failure mode there is. `a.derivStore` is `*DerivativesRedisStore`,
// with no decorator (app.go).
var _ cycleWriteReporter = (*redisstore.DerivativesRedisStore)(nil)

// ─────────────────────────────────────────────────────────────────────────────
// THE CYCLE PARTITION: WRITES · ERRORS · NAMED ABSENCES
//
// MEASURED ON THE LAN: the cycle reported `101 ok` every 5 min while 7 symbols of the served
// fleet had neither `latest` nor a series in ANY timeframe. `ok` counted the ERROR RETURN of
// `pollSymbol`, and the two things it conflated are distinct:
//
//   · a snapshot that reached no destination produces no error — it produces a warning inside
//     the store (`saveSnapshotOnce` swallowed the series batch refusal and returned `nil`),
//     and the symbol entered `ok`;
//   · the list the cycle walks is resolved ONCE, at construction, while the fleet the read
//     answers is reordered. The two diverged by 7 symbols on each side — checking the list
//     against itself yields zero omitted in both directions.
//
// Each subcase below declares, in its comment, the mutation it rejects.
// ─────────────────────────────────────────────────────────────────────────────

// accountingStore is the test-controlled store on the TWO dimensions the counter must
// distinguish: whether the WRITE happened, and WHICH fleet the read serves.
type accountingStore struct {
	recordingStore
	// notWritten is what SaveSnapshotForCycle returns: the destinations that did NOT receive
	// the snapshot. An empty list means the write happened in full. Omission travels through
	// here, and NOT through SaveSnapshot's error — that contract is deliberate and a dedicated
	// test pins it.
	notWritten []string
	// served is the served fleet, by "exchange/marketType" scope. A scope ABSENT from the map
	// answers with an error; a present and EMPTY scope answers with an empty set — the
	// transient case the caller must treat as an incomplete read.
	served map[string]map[string]struct{}

	// servedLater, when present, is what the SECOND read onwards returns.
	//
	// It exists because the cycle reads the fleet TWICE: at the start, to know what to poll,
	// and at the end, to check the partition. The candle fleet is reordered by a 6 h loop and
	// the cycle lasts ~4m45s, so a symbol that enters IN THE MIDDLE of the cycle is polled in
	// one read and served in the other — and that is the scenario in which "carried and not
	// written" still happens. Without this sequence, the double could not represent a fact of
	// the clock.
	servedLater map[string]map[string]struct{}
	calls       map[string]int
}

func (s *accountingStore) SaveSnapshotForCycle(context.Context, *derivatives.DerivativesSnapshot) ([]string, error) {
	return s.notWritten, nil
}

func (s *accountingStore) ServedSymbols(_ context.Context, exchange, marketType string) (map[string]struct{}, error) {
	key := exchange + "/" + marketType
	if s.calls == nil {
		s.calls = make(map[string]int)
	}
	s.calls[key]++
	if s.calls[key] > 1 && s.servedLater != nil {
		if later, ok := s.servedLater[key]; ok {
			return later, nil
		}
	}
	members, ok := s.served[key]
	if !ok {
		return nil, fmt.Errorf("fixture: scope %s/%s has no registration", exchange, marketType)
	}
	return members, nil
}

// servedFleet builds a served-fleet set.
func servedFleet(symbols ...string) map[string]struct{} {
	out := make(map[string]struct{}, len(symbols))
	for _, s := range symbols {
		out[s] = struct{}{}
	}
	return out
}

// runCycle runs a whole cycle with pacing and clock under test control. It mirrors the pacing
// tests' loop: the admission channel is unbuffered, so each tick is a rendezvous and the
// assertion "no symbol entered early" is exact.
func runCycle(t *testing.T, pacer *manualPacer, poller *DerivativesPoller,
	client *pacedFakeClient, symbols int,
) cycleStats {
	stats, _ := runCycleRecording(t, pacer, poller, client, symbols)
	return stats
}

// runCycleRecording is `runCycle` that ALSO returns the symbols actually polled, in admission
// order.
//
// It exists because "the cycle polled the served fleet" is a statement about the SET of entries
// at the venue, and a cycle counter does not distinguish it: two cycles that poll two symbols
// each have the same `ok`, `symbols` and `elapsed` — and different sets. Asserting the counter
// here would assert what was not measured.
func runCycleRecording(t *testing.T, pacer *manualPacer, poller *DerivativesPoller,
	client *pacedFakeClient, symbols int,
) (cycleStats, []string) {
	t.Helper()
	done := make(chan cycleStats, 1)
	go func() { done <- poller.pollAll(context.Background()) }()

	polled := make([]string, 0, symbols)
	for i := 0; i < symbols; i++ {
		if i > 0 {
			pacer.tick(t)
		}
		polled = append(polled, receiveWithin(t, client.entries, "symbol entry"))
	}
	return receiveWithin(t, done, "the cycle to finish"), polled
}

// TestCycleDoesNotCountASymbolItDidNotWrite — a write that did not happen does not enter `ok`.
//
// The store answers that the snapshot did NOT reach a destination: the venue answered, the
// snapshot is valid, and the series was not written. The symbol must leave the WRITE count and
// appear BY NAME.
//
// MUTATION THIS TEST KILLS: letting `len(notWritten) > 0` fall into `okCount.Add(1)`. `ok`
// becomes 1 and the assertion fails — it is a write the counter claims and did not happen.
func TestCycleDoesNotCountASymbolItDidNotWrite(t *testing.T) {
	const symbols = 1

	store := &accountingStore{
		notWritten: []string{"derivatives:series:binance:future:SYM0000/USDT:5m"},
		served:     map[string]map[string]struct{}{"binance/future": servedFleet("SYM0000/USDT")},
	}

	pacer := newManualPacer()
	client := newPacedFakeClient(pacer.clock, symbols)
	stats := runCycle(t, pacer, pacingPoller(pacer, client, store, 300, symbols), client, symbols)

	if stats.ok != 0 {
		t.Errorf("ok = %d, want 0 — the symbol was NOT written and cannot enter the write count", stats.ok)
	}
	if stats.errors != 0 {
		t.Errorf("errors = %d, want 0 — the venue answered; we are the ones who did not write", stats.errors)
	}
	if stats.omittedN != 1 {
		t.Fatalf("omittedN = %d, want 1 — the missing write must enter the partition", stats.omittedN)
	}
	if len(stats.omitted) != 1 || !strings.Contains(stats.omitted[0], "SYM0000/USDT") {
		t.Errorf("omitted = %v, want the symbol named", stats.omitted)
	}
}

// TestCycleNamesAServedSymbolItDidNotCarry — the fleet the read serves contains a symbol the
// cycle does not carry, and it cannot disappear from the report.
//
// The write of what the cycle carries happens normally (`ok`), and STILL the cycle must name
// the served symbol it did not write.
//
// THE FLEET GROWS DURING THE CYCLE, and that is the only way the case exists at all. The cycle
// reads the fleet at the start (to know what to poll, see
// `TestCyclePollsTheFleetTheReadServes`) and again at the end (to check the partition). Because
// the fleet is reordered by a 6 h loop and the cycle lasts ~4m45s, a symbol that enters IN THE
// MIDDLE of the cycle is served on the second read and was not in the first — and that is
// exactly where "served and not written" is still possible.
//
// MUTATION THIS TEST KILLS: checking the partition against `p.symbols` instead of against the
// served fleet — the cycle ends with `omittedN == 0` and the assertion fails.
func TestCycleNamesAServedSymbolItDidNotCarry(t *testing.T) {
	const symbols = 1

	store := &accountingStore{
		served: map[string]map[string]struct{}{
			"binance/future": servedFleet("SYM0000/USDT"),
		},
		servedLater: map[string]map[string]struct{}{
			"binance/future": servedFleet("SYM0000/USDT", "GHOST/USDT"),
		},
	}

	pacer := newManualPacer()
	client := newPacedFakeClient(pacer.clock, symbols)
	stats := runCycle(t, pacer, pacingPoller(pacer, client, store, 300, symbols), client, symbols)

	if stats.ok != symbols {
		t.Fatalf("ok = %d, want %d — the carried symbol's write happened", stats.ok, symbols)
	}
	if stats.omittedN != 1 {
		t.Fatalf("omittedN = %d, want 1 — the served-fleet symbol the cycle does not carry must come out named",
			stats.omittedN)
	}
	if len(stats.omitted) != 1 || !strings.Contains(stats.omitted[0], "GHOST/USDT") {
		t.Errorf("omitted = %v, want GHOST/USDT", stats.omitted)
	}
}

// TestCyclePollsTheFleetTheReadServes — the cycle polls the fleet the READ serves, not the list
// construction resolved.
//
// # THE MEASURED DEFECT
//
// The list was resolved ONCE, in `NewDerivativesPoller` -> `buildFutureSymbolScopes`, and the
// fleet the read serves is reordered every 6 h. Measured on the LAN, read-only: 32 served symbols
// with NO series at all and 32 others polled OUTSIDE the fleet, against 7 and 7 before. In
// class: 32/32 of the outside-the-fleet ones had a fresh `derivatives:snapshot:latest` (TRX
// written 30 s before the read) and 0/32 of the served-without-series ones had any derivatives
// key.
//
// # WHAT THIS TEST ASSERTS
//
// The built list has THREE symbols (`SYM0000`–`SYM0002`); the served fleet has FOUR, and they
// are OTHERS (`LIVE0`–`LIVE3`). The cycle must poll the FLEET — and the polled set is read from
// the client, symbol by symbol, so that the assertion is about the EFFECT and not a counter.
//
// THE SIZES DIVERGE ON PURPOSE (3 against 4). If they were equal, `stats.symbols` would be
// indistinguishable between `len(fleet)` and `len(p.symbols)` — the mutation
// `stats.symbols = len(p.symbols)` would survive. A counter that cannot be distinguished from
// the wrong list asserts what it did not measure. Besides, a fleet of FOUR symbols makes the
// ORDER assertion discriminating: without sorting, map order is random, and getting it right by
// chance costs 1 in 24.
//
// MUTATIONS THIS TEST KILLS, each by ASSERTION and never by expired wait:
//
//	· `p.fleetFor(ctx)` -> `p.symbols` — the cycle would poll the `SYM` symbols (3 entries) and
//	  the set fails;
//	· `stats.symbols = len(p.symbols)` — 3 against 4 fails;
//	· removing `sort.Slice` — admission order stops being stable and the order assertion fails;
//	· inverting the `sort` — the order assertion always fails;
//	· removing the `seen[feed]` dedupe — `ServedSymbols` starts being called once per SCOPE
//	  instead of per feed, and the polled set becomes 3 × 4 = 12 entries; the count fails.
func TestCyclePollsTheFleetTheReadServes(t *testing.T) {
	const declarados, servidos = 3, 4

	store := &accountingStore{
		served: map[string]map[string]struct{}{
			"binance/future": servedFleet("LIVE0/USDT", "LIVE1/USDT", "LIVE2/USDT", "LIVE3/USDT"),
		},
	}

	pacer := newManualPacer()
	client := newPacedFakeClient(pacer.clock, readyPacerTicks)

	poller := pacingPoller(pacer, client, store, 300, declarados)
	poller.newPacer = readyPacer()
	stats := poller.pollAll(context.Background())

	// DECLARED GAP: ORDER IS NOT ASSERTED HERE.
	//
	// ADMISSION order is what `fleetFor`'s `sort` fixes. The available observation point is the
	// order of ENTRIES at the client — and it does NOT carry admission order: the cycle runs
	// symbols in concurrent goroutines (`pollAdmissionConcurrency`), so entries arrive in the
	// order the scheduler releases them. Measured: with the fleet sorted, `drainEntries`
	// returned `[LIVE3/USDT LIVE1/USDT LIVE0/USDT LIVE2/USDT]`. An order assertion here would
	// assert a property the observation point does not have.
	//
	// The mutation that INVERTS the `sort` survives the suite. The clause's hard restriction
	// still holds (stable order), but it has NO instrument: observing it would require a point
	// that exposes the cycle's iteration, or a rendezvous pacer — and the second makes the
	// mutant that changes the QUANTITY hang instead of failing. It stays declared, not hidden
	// behind a fragile assertion.
	polled := drainEntries(client)

	got := append([]string(nil), polled...)
	sort.Strings(got)
	want := []string{"LIVE0/USDT", "LIVE1/USDT", "LIVE2/USDT", "LIVE3/USDT"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("polled symbols = %v, want %v — the cycle must poll the fleet the READ serves, "+
			"not the list resolved at construction", got, want)
	}
	if stats.symbols != servidos {
		t.Errorf("stats.symbols = %d, want %d (the served fleet) and NOT %d (the declared list) — "+
			"the cycle counts what it polled", stats.symbols, servidos, declarados)
	}
	if stats.omittedN != 0 {
		t.Errorf("omittedN = %d, want 0 — the served fleet IS the polled set, so nothing was left out",
			stats.omittedN)
	}
}

// readyPacerTicks is how many tokens the pacer pre-loads. It is DELIBERATELY LARGER than any
// fixture fleet in this file.
//
// The size is not slack: a mutant that INCREASES the polled set (for example removing the
// per-feed dedupe, which multiplies the fleet by the number of scopes) needs to EMPTY the channel
// in order to hang — and a channel that never empties turns that mutant from "timeout" into
// "assertion".
const readyPacerTicks = 256

// readyPacer returns a pacer that RELEASES IMMEDIATELY, with tokens already in the channel.
//
// It exists for tests whose outcome under test is NOT the pace, and where the assertion is about
// the SET of what was polled. With the manual pacer, a mutant that changes the QUANTITY (up or
// down) would leave the cycle waiting for a tick nobody gives, and the test would hang until the
// deadline instead of failing — a defect behind a timeout. The pre-loaded channel removes the
// wait from the equation and lets the defect appear as an assertion, with the suite finishing.
func readyPacer() func(time.Duration) (<-chan time.Time, func()) {
	released := make(chan time.Time, readyPacerTicks)
	for i := 0; i < readyPacerTicks; i++ {
		released <- time.Unix(0, 0)
	}
	return func(time.Duration) (<-chan time.Time, func()) { return released, func() {} }
}

// drainEntries collects what reached the venue without waiting for anything else.
//
// It exists for tests that do NOT wait for N entries: when the outcome under test is "the cycle
// polled zero", waiting for an entry would hang instead of failing — and a suite that hangs hides
// the defect behind a timeout. Here the channel is drained and absence becomes an assertion.
func drainEntries(client *pacedFakeClient) []string {
	var out []string
	for {
		select {
		case s := <-client.entries:
			out = append(out, s)
		default:
			return out
		}
	}
}

// TestCycleDeclaresTheFallbackItTook — a fleet that could not be read returns the declared list,
// and the substitution comes out NAMED in the log.
//
// # WHY THIS NEEDS ITS OWN TEST
//
// The mutation "ignore the UNREADABLE fleet error and continue" **survives** unless the log is
// read: with a single feed, the error leaves `resolved` empty and the EMPTY-fleet guard performs
// the same fallback — behavior stays the same and only the LOG LINE changes. Both lines can be
// built and emitted with **no test reading them**. It is the same class as a field written and
// never printed: an instrument that declares what it did not measure, on the log side.
//
// # WHAT THIS TEST ASSERTS
//
// In both outcomes — UNREADABLE fleet and EMPTY fleet — the cycle polls the DECLARED list (the
// built symbol, not zero symbols), counts that size, and the log names the substitution **and the
// scope**. The cycle is called directly, without waiting for any entry: the outcome under test
// here is "the cycle polled the declared list", and its absence must be an assertion, not a
// timeout.
//
// MUTATIONS THIS TEST KILLS:
//
//	· removing the EMPTY-fleet fallback (returning empty `resolved`) — `stats.symbols` becomes 0
//	  and the polled set becomes empty, and both assertions fail WITHOUT an expired wait;
//	· ignoring the UNREADABLE fleet error (`continue` instead of `return p.symbols`) — the log
//	  starts saying EMPTY and the UNREADABLE assertion fails.
func TestCycleDeclaresTheFallbackItTook(t *testing.T) {
	t.Run("unreadable fleet", func(t *testing.T) {
		// A scope ABSENT from the map makes the double answer with an error — the fleet that did
		// not answer.
		store := &accountingStore{}

		pacer := newManualPacer()
		client := newPacedFakeClient(pacer.clock, readyPacerTicks)

		log, sink := logger.NewCapture(logger.LevelDebug)
		stats := pacingPoller(pacer, client, store, 300, 1, log).pollAll(context.Background())

		line := lineContaining(t, sink, "served fleet unreadable")
		if !strings.Contains(line, "binance/future") {
			t.Errorf("the substitution does not name the SCOPE of the fleet that did not answer: %s", line)
		}
		if !strings.Contains(line, "declared list") {
			t.Errorf("the line does not say the declared list took over: %s", line)
		}
		if stats.symbols != 1 {
			t.Errorf("stats.symbols = %d, want 1 — the DECLARED list must hold when the fleet is "+
				"unreadable, and zero symbols is not the same as the previous list", stats.symbols)
		}
		if got := drainEntries(client); !reflect.DeepEqual(got, []string{"SYM0000/USDT"}) {
			t.Errorf("polled symbols = %v, want [SYM0000/USDT] — the declared list was not polled", got)
		}
	})

	t.Run("empty fleet", func(t *testing.T) {
		// Present and EMPTY scope: the registry answered, and it sustains no fleet.
		store := &accountingStore{
			served: map[string]map[string]struct{}{"binance/future": {}},
		}

		pacer := newManualPacer()
		client := newPacedFakeClient(pacer.clock, readyPacerTicks)

		log, sink := logger.NewCapture(logger.LevelDebug)
		stats := pacingPoller(pacer, client, store, 300, 1, log).pollAll(context.Background())

		line := lineContaining(t, sink, "served fleet empty")
		if !strings.Contains(line, "declared list") {
			t.Errorf("the line does not say the declared list took over: %s", line)
		}
		if stats.symbols != 1 {
			t.Errorf("stats.symbols = %d, want 1 — an empty registry sustains no fleet, and the "+
				"declared list must hold", stats.symbols)
		}
		if got := drainEntries(client); !reflect.DeepEqual(got, []string{"SYM0000/USDT"}) {
			t.Errorf("polled symbols = %v, want [SYM0000/USDT] — the declared list was not polled", got)
		}
	})

	t.Run("one unreadable feed drops the WHOLE cycle to the declared list", func(t *testing.T) {
		// TWO feeds, and the UNREADABLE one is the SECOND on purpose.
		//
		// With a SINGLE feed, "ignore the error and continue" falls into the EMPTY-fleet guard and
		// produces the SAME outcome — the mutation survives and the only thing that changes is
		// the CAUSE the log names. With two, the outcomes DIVERGE: ignoring the error would keep
		// the RESOLVED fleet of the first feed (`LIVE0/USDT`) while the clause requires the whole
		// DECLARED list.
		scopes := []SymbolScope{
			{ExchangeID: "binance", MarketType: "future", Symbol: "SYM0000/USDT"},
			{ExchangeID: "bingx", MarketType: "future", Symbol: "BX0000/USDT"},
		}
		store := &accountingStore{
			served: map[string]map[string]struct{}{
				// Readable, and DIFFERENT from the declared one: if it survived, the polled set
				// denounces it.
				"binance/future": servedFleet("LIVE0/USDT"),
				// "bingx/future" ABSENT from the map: the double answers with an error — the fleet
				// that did not answer.
			},
		}

		pacer := newManualPacer()
		client := newPacedFakeClient(pacer.clock, readyPacerTicks)

		// THE PACER THAT RELEASES IMMEDIATELY is the difference between failing and hanging, here
		// and in the main test: the outcome under test is not the pace, and a wait for a tick
		// would make the mutant that changes the polled QUANTITY appear as a timeout instead of an
		// assertion.
		poller := &DerivativesPoller{
			cfg:             config.DerivativesConfig{Enabled: true, PollIntervalSec: 300, MaxSeriesSize: 200},
			store:           store,
			symbols:         scopes,
			clients:         map[string]derivatives.Source{"binance": client, "bingx": client},
			fundingProfiles: NewFundingProfileRegistry(),
			now:             pacer.clock,
			newPacer:        readyPacer(),
		}
		log, sink := logger.NewCapture(logger.LevelDebug)
		poller.log = log
		stats := poller.pollAll(context.Background())

		line := lineContaining(t, sink, "served fleet unreadable")
		if !strings.Contains(line, "bingx/future") {
			t.Errorf("the fallback line does not name the feed that did NOT answer: %s", line)
		}
		if stats.symbols != len(scopes) {
			t.Errorf("stats.symbols = %d, want %d — an unreadable feed drops the WHOLE cycle to the "+
				"declared list", stats.symbols, len(scopes))
		}
		// THE DISCRIMINATING SIGNAL is the ABSENCE of the resolved symbol, not literal set
		// equality: the fake client reports the symbol in the VENUE's form (`BX0000-USDT`, with a
		// hyphen, is BingX's form), and comparing against the internal form would tie this
		// assertion to each exchange's formatter. What the clause forbids is `LIVE0` resolving and
		// surviving.
		got := drainEntries(client)
		for _, s := range got {
			if strings.Contains(s, "LIVE0/USDT") {
				t.Errorf("the cycle polled %q, which is the RESOLVED fleet of the feed that answered — "+
					"an unreadable feed drops the WHOLE cycle to the declared list, and keeping the "+
					"resolved fleet is the mutation that swaps `return p.symbols` for `continue`", s)
			}
		}
		if len(got) != len(scopes) {
			t.Errorf("polled symbols = %d (%v), want %d — the whole DECLARED list", len(got), got, len(scopes))
		}
	})
}

// TestCycleDeclaresAPartitionItCouldNotTake — an empty or unavailable served fleet is an
// INCOMPLETE read, never "zero omitted".
//
// Absence of a read is not absence of a symbol: a `SMEMBERS` that answered empty — a registry not
// yet seeded, or a scope that does not exist — would mark the whole fleet as unwritten if read as
// "the fleet is truly empty", and would mark ZERO omitted if read as "checked and clean". Both
// outcomes assert what was not measured.
//
// MUTATION THIS TEST KILLS: removing the caller's empty-set guard. `fleetErr` becomes nil and the
// assertion fails — the cycle declares a partition it did not take.
func TestCycleDeclaresAPartitionItCouldNotTake(t *testing.T) {
	t.Run("empty served fleet", func(t *testing.T) {
		const symbols = 1
		store := &accountingStore{
			served: map[string]map[string]struct{}{"binance/future": {}},
		}

		pacer := newManualPacer()
		client := newPacedFakeClient(pacer.clock, symbols)
		stats := runCycle(t, pacer, pacingPoller(pacer, client, store, 300, symbols), client, symbols)

		if stats.fleetErr == nil {
			t.Error("EMPTY served fleet with no diagnosis — the cycle asserts a partition it did not take")
		}
		if stats.omittedN != 0 {
			t.Errorf("omittedN = %d, want 0 — an empty registry does not sustain accusing the fleet of not writing", stats.omittedN)
		}
	})

	t.Run("unavailable fleet read", func(t *testing.T) {
		const symbols = 1
		// The scope is not in the map: ServedSymbols returns an error.
		store := &accountingStore{served: map[string]map[string]struct{}{}}

		pacer := newManualPacer()
		client := newPacedFakeClient(pacer.clock, symbols)
		stats := runCycle(t, pacer, pacingPoller(pacer, client, store, 300, symbols), client, symbols)

		if stats.fleetErr == nil {
			t.Error("unavailable fleet read with no diagnosis — 'none omitted' would be indistinguishable from 'I did not look'")
		}
		if stats.omittedN != 0 {
			t.Errorf("omittedN = %d, want 0 — without a read there is no accusation to make", stats.omittedN)
		}
	})
}

// TestRefusedCycleNamesItsCauseAndTheFleetItLeftUnwritten — feed refusal carries the REASON all
// the way to the report, and the whole served fleet enters the partition as absence.
//
// `cycleStats.refused` existed and `reportCycle` NEVER printed it: the operator saw "0 ok" with
// not one word of reason — the same class of defect this partition closes, one line above. And
// nothing was attempted, so the whole served fleet was left unwritten.
//
// MUTATIONS THIS TEST KILLS:
//   - removing `stats.refusal = err`: the cause disappears and the reason assertion fails;
//   - skipping the partition on the refusal path: `omittedN` stays 0 and the assertion fails.
func TestRefusedCycleNamesItsCauseAndTheFleetItLeftUnwritten(t *testing.T) {
	store := &accountingStore{
		served: map[string]map[string]struct{}{"coinalyze/future": servedFleet("BTC/USDT")},
	}
	// `coinalyze` is not a declared venue: `admitFeed` refuses before the first fetch.
	poller := &DerivativesPoller{
		cfg:     config.DerivativesConfig{Enabled: true, PollIntervalSec: 60},
		store:   store,
		symbols: []SymbolScope{{ExchangeID: "coinalyze", MarketType: "future", Symbol: "BTC/USDT"}},
	}

	stats := poller.pollAll(context.Background())

	if !stats.refused {
		t.Fatal("the cycle should have been refused")
	}
	if stats.refusal == nil {
		t.Error("the refusal carries no reason — the operator reads '0 ok' without knowing why")
	}
	if stats.ok != 0 || stats.errors != 0 {
		t.Errorf("nothing was attempted: ok=%d errors=%d, want 0 and 0", stats.ok, stats.errors)
	}
	if stats.omittedN != 1 {
		t.Errorf("omittedN = %d, want 1 — the served fleet was left unwritten, and that is an absence", stats.omittedN)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// THE REPORT, not just the field.
//
// Writing `refusal` into the field while `reportCycle` NEVER reads it leaves the clause "the
// refusal carries the cause to the report" unclosed, and a test asserting the FIELD blind to it.
// A field nobody reads passes any assertion; what the operator reads is the line. These three
// subcases assert the LINE.
//
// MUTATION THESE TESTS KILL: deleting any of the three `reportCycle` emissions — the refusal,
// the names, or the unchecked-partition declaration. A field written and never printed leaves
// the suite green, which is why the test goes down to the log.
// ─────────────────────────────────────────────────────────────────────────────

// lineContaining returns the FIRST captured line containing the marker.
func lineContaining(t *testing.T, sink *logger.Sink, marker string) string {
	t.Helper()
	for _, r := range sink.Records() {
		if strings.Contains(r.Text(), marker) {
			return r.Text()
		}
	}
	t.Fatalf("no report line contains %q; output: %v", marker, sink.Records())
	return ""
}

func TestReportCyclePutsThePartitionWhereTheOperatorReads(t *testing.T) {
	t.Run("the refusal carries the cause", func(t *testing.T) {
		log, sink := logger.NewCapture(logger.LevelDebug)
		reportCycle(cycleStats{
			refused:  true,
			refusal:  errors.New(`feed "coinalyze" has label convention unspecified`),
			symbols:  3,
			interval: time.Minute,
		}, log)

		line := lineContaining(t, sink, "refused at admission")
		if !strings.Contains(line, "label convention unspecified") {
			t.Errorf("the report refuses without naming the reason: %s", line)
		}
	})

	t.Run("the absence comes out named, and the total survives the cap", func(t *testing.T) {
		names := make([]string, 0, omittedNameCap+5)
		for i := 0; i < omittedNameCap+5; i++ {
			names = append(names, fmt.Sprintf("binance/future/S%02d/USDT", i))
		}

		log, sink := logger.NewCapture(logger.LevelDebug)
		reportCycle(cycleStats{omitted: names, omittedN: len(names), interval: time.Minute}, log)

		line := lineContaining(t, sink, "had no write")
		if !strings.Contains(line, fmt.Sprintf("omitted=%d", len(names))) {
			t.Errorf("the omitted total did not survive the name cap: %s", line)
		}
		if !strings.Contains(line, fmt.Sprintf("S%02d/USDT", omittedNameCap-1)) {
			t.Errorf("the last name within the cap did not come out: %s", line)
		}
		if strings.Contains(line, fmt.Sprintf("S%02d/USDT", omittedNameCap)) {
			t.Errorf("the cap of %d names did not truncate: %s", omittedNameCap, line)
		}
		if !strings.Contains(line, "(+5 not listed)") {
			t.Errorf("the truncation was not declared: %s", line)
		}
	})

	t.Run("the unchecked partition is declared", func(t *testing.T) {
		log, sink := logger.NewCapture(logger.LevelDebug)
		reportCycle(cycleStats{
			fleetErr: errors.New("served fleet of binance/future unavailable in this cycle"),
			interval: time.Minute,
		}, log)

		line := lineContaining(t, sink, "omitted-symbol count is unknown")
		if !strings.Contains(line, "unavailable in this cycle") {
			t.Errorf("the unchecked partition does not name the reason: %s", line)
		}
	})
}
