package collector

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/coinalyze"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// ─────────────────────────────────────────────────────────────────────────────
// DURATION, SPEND AND REFUSALS BECOME A MEASURED SIGNAL.
//
// The 40/min constant describes the KEY's CEILING, and the key is shared: production and the LAN
// carry the same `sha256` (`0ec152beb58edff8`) and both emit — 991 and 623 refusals in 24 h. So no
// quantity derived from the constant describes this process; only measurement does.
// ─────────────────────────────────────────────────────────────────────────────

// cincoSimbolos is a batch small enough to fit one request's burst ceiling, so the case measures
// ACCOUNTING and not the wait.
func cincoSimbolos() []string {
	out := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		out = append(out, fmt.Sprintf("SYM%02d_PERP.A", i))
	}
	return out
}

// contabilidadeDeclarada is the budget accounting with a hand-set value.
type contabilidadeDeclarada struct {
	tally coinalyze.CoinalyzeBudgetTally
}

func (c *contabilidadeDeclarada) Tally() coinalyze.CoinalyzeBudgetTally { return c.tally }

// THE CYCLE'S SPEND IS THE DIFFERENCE, NEVER THE ACCUMULATED VALUE.
//
// The budget accounting is monotonic on purpose — zeroing it would make two concurrent readings
// steal counts from each other. The meter turns accumulated into per-cycle, and reporting the
// accumulated value would make the second cycle look twice the first.
func TestCoinalyzeSpendMeter_ReportsTheCycleAndNotTheRunningTotal(t *testing.T) {
	fonte := &contabilidadeDeclarada{}
	meter := NewCoinalyzeSpendMeter(fonte)

	fonte.tally = coinalyze.CoinalyzeBudgetTally{CallsSpent: 120, RateRefusals: 3}
	primeiro := meter.Cycle(2 * time.Minute)
	if primeiro.CallsSpent != 120 || primeiro.RateRefusals != 3 {
		t.Fatalf("first cycle: spent %d and received %d, expected 120 and 3",
			primeiro.CallsSpent, primeiro.RateRefusals)
	}

	// The accumulated value rises to 200/5: the SECOND cycle spent 80 and received 2.
	fonte.tally = coinalyze.CoinalyzeBudgetTally{CallsSpent: 200, RateRefusals: 5}
	segundo := meter.Cycle(time.Minute)
	if segundo.CallsSpent != 80 {
		t.Fatalf("the second cycle reported %d calls, expected 80 (the difference) — it is reporting the accumulated value",
			segundo.CallsSpent)
	}
	if segundo.RateRefusals != 2 {
		t.Fatalf("the second cycle reported %d refusals, expected 2 (the difference)", segundo.RateRefusals)
	}
}

// THE REPORTED RATE COMES FROM THE MEASURED DURATION, NOT THE DECLARED CEILING.
//
// This is the discriminating case: 120 calls in 2 minutes are 60/min, a value the 40/min constant
// would not produce at all.
func TestCoinalyzeSpendMeter_TheRateComesFromTheMeasuredDurationNotTheConstant(t *testing.T) {
	sinal := CoinalyzeCycleSignal{Elapsed: 2 * time.Minute, CallsSpent: 120}

	if got := sinal.ObservedCallsPerMinute(); got != 60 {
		t.Fatalf("the measured rate was %.1f/min, expected 60 (120 calls in 2 min)", got)
	}
	if got := sinal.ObservedCallsPerMinute(); got == float64(coinalyze.CoinalyzeCallsPerMinute) {
		t.Fatalf("the reported rate is the declared constant (%d/min) instead of the measurement",
			coinalyze.CoinalyzeCallsPerMinute)
	}

	// AND A SATURATED CYCLE REPORTS THE LOW RATE IT SUSTAINED. A cycle that spent the whole hour
	// waiting for a penalty spent little, and that is what must appear — the constant would say
	// 40/min forever, including here.
	saturado := CoinalyzeCycleSignal{Elapsed: time.Hour, CallsSpent: 60, RateRefusals: 900}
	if got := saturado.ObservedCallsPerMinute(); got != 1 {
		t.Fatalf("the saturated cycle reported %.1f/min, expected 1 (60 calls in 60 min)", got)
	}
}

// THE THREE QUANTITIES COME OUT ON THE SAME LINE, because the question is the relation between them.
//
// "I spent 120 calls" says nothing. "I spent 120 calls in 2 minutes and received 37 refusals" says
// the window is being consumed by another process.
func TestCoinalyzeSpendMeter_TheThreeQuantitiesAreReportedTogether(t *testing.T) {
	fonte := &contabilidadeDeclarada{tally: coinalyze.CoinalyzeBudgetTally{CallsSpent: 120, RateRefusals: 37}}
	log, sink := logger.NewCapture(logger.LevelDebug)
	meter := NewCoinalyzeSpendMeter(fonte, log)

	meter.Cycle(2 * time.Minute)
	records := sink.Records()

	var linha string
	for _, record := range records {
		if strings.Contains(record.Text(), "coinalyze cycle spent calls") {
			linha = record.Text()
			break
		}
	}
	if linha == "" {
		t.Fatalf("the cycle spend was not reported: %v", records)
	}
	for _, want := range []string{
		"2m0s", // measured duration
		"120",  // calls spent
		"60.0", // MEASURED rate, in calls per minute
		"37",   // rate refusals received
	} {
		if !strings.Contains(linha, want) {
			t.Fatalf("the report does not carry %q: %s", want, linha)
		}
	}
}

// WITHOUT A BUDGET THERE IS NO METER, and a missing meter neither takes down the cycle nor reports
// zero.
//
// Reporting zero would be worse than not reporting: a spend of zero calls with the collector
// enabled is indistinguishable from a process that stopped emitting.
func TestCoinalyzeSpendMeter_ANilMeterMeasuresNothingAndSaysNothing(t *testing.T) {
	if meter := NewCoinalyzeSpendMeter(nil); meter != nil {
		t.Fatal("a nil source must return a nil meter")
	}

	sinal := (*CoinalyzeSpendMeter)(nil).Cycle(time.Minute)

	if sinal != (CoinalyzeCycleSignal{}) {
		t.Fatalf("a nil meter measured %+v", sinal)
	}
}

// AND THE BUDGET ACCOUNTING COUNTS WHAT WAS DEBITED — per symbol AND per attempt.
//
// Counting the REQUEST instead of the debit would lose the two things that make the measurement
// worth anything: a multi-symbol request costs one call per symbol, and resending a 429 is charged
// again because the refused request reached the provider.
func TestCoinalyzeBudgetTally_CountsWhatWasDebitedAndWhatCameBackRefused(t *testing.T) {
	relogio := &relogioDeTeste{t: time.Unix(1787465684, 0).UTC()}
	budget := coinalyze.NewCoinalyzeBudget(coinalyze.CoinalyzeBudgetOptions{
		CallsPerMinute: coinalyze.CoinalyzeCallsPerMinute,
		Now:            relogio.Now,
		Wait: func(_ context.Context, d time.Duration) error {
			relogio.advance(d)
			return nil
		},
	})

	if inicial := budget.Tally(); inicial.CallsSpent != 0 || inicial.RateRefusals != 0 {
		t.Fatalf("a new budget already accounted for %+v", inicial)
	}

	recusa := &coinalyze.CoinalyzeError{Status: 429, Message: "Too Many Requests", RetryAfter: time.Second}
	_ = budget.Do(context.Background(), cincoSimbolos(), func(context.Context) error { return recusa })

	tally := budget.Tally()
	// Three attempts × 5 symbols = 15 calls debited, and 3 refusals received.
	if tally.CallsSpent != 3*5 {
		t.Fatalf("accounted for %d calls, expected 15 (3 attempts × 5 symbols)", tally.CallsSpent)
	}
	if tally.RateRefusals != coinalyze.DefaultCoinalyzeAttempts {
		t.Fatalf("accounted for %d refusals, expected %d", tally.RateRefusals, coinalyze.DefaultCoinalyzeAttempts)
	}
}
