package engine

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"testing"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/derivatives"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// ============================================================================
// COVERAGE CHECKED AGAINST ROWS **AND** READABILITY
// ============================================================================
//
// The two checkers that already existed pass with `funding_rate_8h` at ZERO ROWS: one
// compares the declaration with the struct, the other compares the declaration with the columns the
// write EMITS — and emitting is having the `if p.FundingRate8h != nil`, it is not the pointer ever
// having been non-nil.

// fakeFillCounter returns the declared counts per timeframe.
type fakeFillCounter struct {
	rows map[string]map[string]int64
	err  error
	// asked stores the requested columns, to prove that the question is driven by the
	// declaration and not by a fixed list inside the checker.
	asked []string
}

func (c *fakeFillCounter) CountDerivativesColumnRows(_ context.Context, timeframe string, columns []string) (map[string]int64, error) {
	c.asked = append([]string(nil), columns...)
	if c.err != nil {
		return nil, c.err
	}
	out := map[string]int64{}
	for _, column := range columns {
		out[column] = c.rows[timeframe][column]
	}
	return out, nil
}

// todasAsColunasCheias assembles the count of a healthy table.
func todasAsColunasCheias(timeframe string, n int64) map[string]map[string]int64 {
	linhas := map[string]int64{}
	for _, coluna := range redisstore.DerivativesDurableCoverage() {
		if coluna == "" {
			continue
		}
		linhas[coluna] = n
	}
	return map[string]map[string]int64{timeframe: linhas}
}

func acharAchado(t *testing.T, achados []DurableFillFinding, campo string) DurableFillFinding {
	t.Helper()
	for _, f := range achados {
		if f.Field == campo {
			return f
		}
	}
	t.Fatalf("no finding for field %q among %d finding(s)", campo, len(achados))
	return DurableFillFinding{}
}

func probeCom(counter derivativesFillCounter, elapsed time.Duration, logs ...*slog.Logger) *DurableFillProbe {
	return NewDurableFillProbe(DurableFillProbeOptions{
		Counter:    counter,
		Timeframes: []string{"1d"},
		CycleFloor: time.Hour,
		Elapsed:    func() time.Duration { return elapsed },
		Log:        logger.OrDiscard(logs...),
	})
}

// TestDurableFillProbe_ADeclaredFieldWithZeroRowsIsNotApproved ⭐ is the third checker, and
// kills the mutant that approves a field declared covered with ZERO ROWS.
//
// It is the defect this entire feature exists to fix: `funding_rate_8h` was
// declared, had a write branch and had zero rows, and the two existing checkers
// gave green.
func TestDurableFillProbe_ADeclaredFieldWithZeroRowsIsNotApproved(t *testing.T) {
	rows := todasAsColunasCheias("1d", 2000)
	rows["1d"]["funding_rate_8h"] = 0

	// Grace period EXPIRED: two daily buckets is 48h, and the process already has 100.
	achados := probeCom(&fakeFillCounter{rows: rows}, 100*time.Hour).Check(context.Background())

	vazio := acharAchado(t, achados, "fundingRate8h")
	if vazio.State != FillEmpty {
		t.Fatalf("fundingRate8h with zero rows came out %q, want %q", vazio.State, FillEmpty)
	}
	if vazio.Rows != 0 {
		t.Fatalf("Rows=%d", vazio.Rows)
	}
	// And the rest of the table is NOT accused along with it: the finding is per field, because the consequence
	// (capping the cell) is per field.
	cheio := acharAchado(t, achados, "fundingRate")
	if cheio.State != FillFilled {
		t.Fatalf("fundingRate with 2000 rows came out %q, want %q", cheio.State, FillFilled)
	}
}

// TestDurableFillProbe_AWrittenColumnTheReadNeverAsksIsNotCoverage ⭐ is the fourth checker,
// and kills the mutant that approves a written field the route does not return.
//
// There is no `SELECT *` in this module. A column with a million rows that the query does not
// request reaches no consumer at all — and declaring it covered is written permission to
// cap the field's hot window and destroy the only readable copy.
func TestDurableFillProbe_AWrittenColumnTheReadNeverAsksIsNotCoverage(t *testing.T) {
	rows := todasAsColunasCheias("1d", 1_000_000)

	// The column the read does NOT request: none, by construction — so the case proves the
	// RULE over the set actually requested, and it is the mutant that inverts it.
	achados := probeCom(&fakeFillCounter{rows: rows}, 100*time.Hour).Check(context.Background())

	pedidas := map[string]bool{}
	for _, coluna := range derivatives.DurableReadColumns() {
		pedidas[coluna] = true
	}
	var conferidos int
	for _, f := range achados {
		conferidos++
		if pedidas[f.Column] && f.State == FillUnreadable {
			t.Errorf("%s is requested by the read and came out %q", f.Column, f.State)
		}
		if !pedidas[f.Column] && f.State != FillUnreadable {
			t.Errorf("%s is NOT requested by the read and came out %q with %d row(s) — rows are not "+
				"coverage if the read does not reach the column", f.Column, f.State, f.Rows)
		}
	}
	if conferidos == 0 {
		t.Fatal("no field was checked — the comparison would pass vacuously")
	}
}

// TestFillStateOf_RowsDoNotRedeemAnUnreachableColumn is the same rule in the isolated decision, with
// a case ordinary production data does not produce: a full and unreachable column.
//
// THE ORDER OF THE QUESTIONS IS THE DECISION — readability before count —, and without this case a
// mutant that inverted them would approve the column for having rows.
func TestFillStateOf_RowsDoNotRedeemAnUnreachableColumn(t *testing.T) {
	got := fillStateOf(1_000_000, false, 100*time.Hour, 48*time.Hour)
	if got != FillUnreadable {
		t.Fatalf("column with 1,000,000 rows that the read does not request came out %q, want %q",
			got, FillUnreadable)
	}
	// And within the grace period it is not absolved either: readability does not depend on time.
	if got := fillStateOf(0, false, 0, 48*time.Hour); got != FillUnreadable {
		t.Fatalf("unreachable column within grace came out %q, want %q", got, FillUnreadable)
	}
}

// TestDurableFillProbe_TheFirstBootIsWithinTheDeclaredGrace ⭐ is the contract's edge case, and
// kills the mutant that charges the column before a cycle that would write it exists.
//
// On this feature's first boot the new columns have ZERO ROWS BY DEFINITION — they
// have just been born. A checker without a grace period would accuse the deployment that installs the feature
// itself.
func TestDurableFillProbe_TheFirstBootIsWithinTheDeclaredGrace(t *testing.T) {
	rows := todasAsColunasCheias("1d", 0)

	achados := probeCom(&fakeFillCounter{rows: rows}, 0).Check(context.Background())

	if len(achados) == 0 {
		t.Fatal("no finding on the first boot")
	}
	for _, f := range achados {
		if f.State != FillWithinGrace {
			t.Errorf("%s came out %q at startup, want %q", f.Column, f.State, FillWithinGrace)
		}
		if severityOf(f.State) != fillOK {
			t.Errorf("%s becomes a finding at startup — the premature verdict the grace period prevents", f.Column)
		}
	}
}

// TestDurableFillGrace_IsPerTimeframeWithTheCycleAsFloor — the grace period belongs to the TIMEFRAME because
// the time to fill belongs to the timeframe: a daily bucket takes a day to close.
//
// Charging every timeframe on the same window would flag the daily one every day for not being hourly.
func TestDurableFillGrace_IsPerTimeframeWithTheCycleAsFloor(t *testing.T) {
	const ciclo = time.Hour

	if got, want := durableFillGrace("1d", ciclo), 48*time.Hour; got != want {
		t.Fatalf("1d grace = %v, want %v (two daily buckets)", got, want)
	}
	if got, want := durableFillGrace("4h", ciclo), 8*time.Hour; got != want {
		t.Fatalf("4h grace = %v, want %v", got, want)
	}
	// THE FLOOR BITES where two buckets are shorter than a collection cycle: no column
	// can be charged before a cycle that would write it exists.
	if got, want := durableFillGrace("15m", ciclo), ciclo; got != want {
		t.Fatalf("15m grace = %v, want the %v floor — two 15m buckets are 30 minutes, less than "+
			"the cycle that would write them", got, want)
	}
}

// TestDurableFillProbe_NoStateEverBlocksTheBoot ⭐ kills the mutant that makes the verdict fatal
// and takes down the first boot.
//
// The repository's doctrine reserves FATAL for the gate whose condition is the comparison of two
// read numbers. Zero rows is not that: it admits a write regression AND "has not warmed up yet",
// and this feature's first boot is the second case by definition.
//
// The ratchet walks ALL states, not only the ones ordinary data produces: a new state promoted to
// blocking fails here.
func TestDurableFillProbe_NoStateEverBlocksTheBoot(t *testing.T) {
	todos := []DurableFillState{FillFilled, FillWithinGrace, FillEmpty, FillUnreadable}
	for _, state := range todos {
		// The comparison is against the CEILING, not against a named step: a new step
		// promoted above `fillWarn` fails here under whatever name it has.
		if severityOf(state) > fillWarn {
			t.Errorf("state %q escalates above warning — the rule is WARN, and a fatal gate "+
				"here would take down the deploy that installs the feature itself", state)
		}
	}
	// And the worst state is still a finding: warning without taking down is not silencing.
	if severityOf(FillEmpty) == fillOK {
		t.Fatalf("state %q passed as routine — the warning must be loud", FillEmpty)
	}

	// THE OTHER HALF OF THE PROMISE, checked mechanically: `Report` returns NOTHING, so
	// there is no way for the caller to turn this verdict into a boot refusal. Adding
	// an `error` return is the natural path to making the gate fatal, and it is what this
	// case kills.
	if n := reflect.TypeOf((*DurableFillProbe).Report).NumOut(); n != 0 {
		t.Fatalf("Report returns %d value(s) — a return is the path by which the verdict becomes "+
			"a boot refusal, and that is forbidden", n)
	}
}

// TestDurableFillProbe_TheQuestionIsDrivenByTheDeclaration — the checker asks about the
// columns the DECLARATION names, not about its own list.
//
// A second list here would be the divergence it exists to catch, written inside the
// checker itself.
func TestDurableFillProbe_TheQuestionIsDrivenByTheDeclaration(t *testing.T) {
	counter := &fakeFillCounter{rows: todasAsColunasCheias("1d", 1)}
	probeCom(counter, time.Hour).Check(context.Background())

	pedidas := map[string]bool{}
	for _, coluna := range counter.asked {
		pedidas[coluna] = true
	}
	var declaradas int
	for campo, coluna := range redisstore.DerivativesDurableCoverage() {
		if coluna == "" {
			continue
		}
		declaradas++
		if !pedidas[coluna] {
			t.Errorf("%s is declared covered by column %q and the checker did not ask about it",
				campo, coluna)
		}
	}
	if declaradas == 0 {
		t.Fatal("no declared column — the comparison would pass vacuously")
	}
	if len(counter.asked) != declaradas {
		t.Fatalf("the checker asked about %d column(s) and the declaration has %d: %v",
			len(counter.asked), declaradas, counter.asked)
	}
}

// TestDurableFillProbe_AnUnreadableCountIsNotAnEmptyColumn — not knowing is not "it is empty".
//
// A query that did not answer would accuse SIX healthy columns at once, and the operator
// would look for the defect in the wrong place.
func TestDurableFillProbe_AnUnreadableCountIsNotAnEmptyColumn(t *testing.T) {
	counter := &fakeFillCounter{err: errors.New("questdb is down")}

	achados := probeCom(counter, 100*time.Hour).Check(context.Background())

	if len(achados) != 0 {
		t.Fatalf("%d finding(s) from a count that did not answer: %+v", len(achados), achados)
	}
}

// TestNewDurableFillProbe_WithoutADurableTierThereIsNothingToCheck — without a durable tier there
// is no question to ask, and a checker that warned about a disabled table would be
// an alarm about the configuration itself.
func TestNewDurableFillProbe_WithoutADurableTierThereIsNothingToCheck(t *testing.T) {
	if p := NewDurableFillProbe(DurableFillProbeOptions{Timeframes: []string{"1d"}}); p != nil {
		t.Fatal("checker built without a counter")
	}
	if p := NewDurableFillProbe(DurableFillProbeOptions{Counter: &fakeFillCounter{}}); p != nil {
		t.Fatal("checker built without a timeframe")
	}
	// And the nil receiver is a no-op at both ends, instead of crashing the process through the
	// back door.
	var nulo *DurableFillProbe
	if got := nulo.Check(context.Background()); got != nil {
		t.Fatalf("Check on a nil checker returned %v", got)
	}
	nulo.Report(context.Background())
}

// ============================================================================
// INTEGRITY IN FIVE DIMENSIONS
//
// THE MUTANT THESE CASES KILL: implementing some of the dimensions and keeping the label of
// all of them. It is the defect of a monitor named for more than its mechanism measures.
// ============================================================================

// fakeSpanReader returns the declared window per timeframe.
type fakeSpanReader struct {
	spans map[string]derivatives.DerivativesTimeframeSpan
	err   error
}

func (r *fakeSpanReader) DerivativesTimeframeSpan(_ context.Context, timeframe string) (derivatives.DerivativesTimeframeSpan, error) {
	if r.err != nil {
		return derivatives.DerivativesTimeframeSpan{}, r.err
	}
	return r.spans[timeframe], nil
}

// diariosContinuos assembles `n` consecutive daily buckets, with no hole, with `porBucket` rows
// in each — the shape of a healthy fleet table.
func diariosContinuos(n, porBucket int64) derivatives.DerivativesTimeframeSpan {
	fim := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	return derivatives.DerivativesTimeframeSpan{
		Rows:            n * porBucket,
		DistinctBuckets: n,
		Oldest:          fim.AddDate(0, 0, -int(n-1)),
		Newest:          fim,
	}
}

func probeComJanela(counter derivativesFillCounter, spans derivativesSpanReader, elapsed time.Duration, logs ...*slog.Logger) *DurableFillProbe {
	return NewDurableFillProbe(DurableFillProbeOptions{
		Counter:    counter,
		Spans:      spans,
		Timeframes: []string{"1d"},
		CycleFloor: time.Hour,
		Elapsed:    func() time.Duration { return elapsed },
		Log:        logger.OrDiscard(logs...),
	})
}

// fakeCoherenceReader answers how many rows of each timeframe contradict their own step.
type fakeCoherenceReader struct {
	incoerentes map[string]int64
	err         error
}

func (r *fakeCoherenceReader) CountDerivativesIncoherentFunding(_ context.Context, timeframe string) (int64, error) {
	if r.err != nil {
		return 0, r.err
	}
	return r.incoerentes[timeframe], nil
}

// probeCompleto assembles the checker with ALL mechanisms wired.
//
// It exists separately from `probeComJanela` because the inverse half — the label shrinking with the
// mechanism — needs a checker that is MISSING something. A single helper that wired
// everything would leave the label ratchet without a control.
func probeCompleto(counter derivativesFillCounter, spans derivativesSpanReader,
	coherence derivativesCoherenceReader, elapsed time.Duration, logs ...*slog.Logger,
) *DurableFillProbe {
	return NewDurableFillProbe(DurableFillProbeOptions{
		Counter:    counter,
		Spans:      spans,
		Coherence:  coherence,
		Timeframes: []string{"1d"},
		CycleFloor: time.Hour,
		Elapsed:    func() time.Duration { return elapsed },
		Log:        logger.OrDiscard(logs...),
	})
}

// ⭐ THE CENTRAL CASE: the label is DERIVED from the findings, and the ratchet compares it with the
// declaration IN BOTH DIRECTIONS.
//
// A mechanism that measured three dimensions and kept the text of all four would pass
// any assertion about the text. Here there is no text to assert: the list comes from
// `MeasuredDimensions`, which only includes the dimension that produced a finding.
func TestDurableIntegrity_TheLabelNamesExactlyTheDimensionsMeasured(t *testing.T) {
	counter := &fakeFillCounter{rows: todasAsColunasCheias("1d", 1000)}
	spans := &fakeSpanReader{spans: map[string]derivatives.DerivativesTimeframeSpan{"1d": diariosContinuos(2000, 30)}}

	coherence := &fakeCoherenceReader{}

	report := probeCompleto(counter, spans, coherence, 72*time.Hour).Integrity(context.Background())
	medidas := report.MeasuredDimensions()

	if len(medidas) != len(declaredIntegrityDimensions()) {
		t.Fatalf("measured %v, and the contract requires %v", medidas, declaredIntegrityDimensions())
	}
	presentes := map[DurableIntegrityDimension]bool{}
	for _, d := range medidas {
		presentes[d] = true
	}
	for _, exigida := range declaredIntegrityDimensions() {
		if !presentes[exigida] {
			t.Errorf("dimension %q is required and was not measured", exigida)
		}
	}
	for _, medida := range medidas {
		declarada := false
		for _, exigida := range declaredIntegrityDimensions() {
			if medida == exigida {
				declarada = true
			}
		}
		if !declarada {
			t.Errorf("dimension %q was measured and is not among the required ones", medida)
		}
	}
	if len(report.UnmeasuredDimensions()) != 0 {
		t.Errorf("with everything measured, a dimension was left unanswered: %v", report.UnmeasuredDimensions())
	}
}

// AND THE INVERSE HALF: when the mechanism CANNOT measure the others, the label SHRINKS and the
// missing ones are named. Without this case, a fixed label would pass the one above.
func TestDurableIntegrity_ALabelShrinksWhenTheMechanismCannotMeasure(t *testing.T) {
	counter := &fakeFillCounter{rows: todasAsColunasCheias("1d", 1000)}

	report := probeComJanela(counter, nil, 72*time.Hour).Integrity(context.Background())
	medidas := report.MeasuredDimensions()

	if len(medidas) != 1 || medidas[0] != DimensionColumnFill {
		t.Fatalf("without a span reader the label should name only per-column fill, it named %v", medidas)
	}
	faltando := report.UnmeasuredDimensions()
	if len(faltando) != len(declaredIntegrityDimensions())-1 {
		t.Fatalf("dimensions without a mechanism were not named: %v", faltando)
	}
	for _, esperada := range []DurableIntegrityDimension{
		DimensionRowCount, DimensionTimeContinuity, DimensionTimeframeCoverage, DimensionFundingCoherence,
	} {
		encontrada := false
		for _, d := range faltando {
			if d == esperada {
				encontrada = true
			}
		}
		if !encontrada {
			t.Errorf("%q was left without a mechanism and without mention — exactly the silence the label rule forbids", esperada)
		}
	}
}

// TIME CONTINUITY IS MEASURED IN DISTINCT BUCKETS, not in rows.
//
// The table is a fleet one: a daily bucket carries one row per symbol. `count()` over a
// series with a hole stays high, and that is why the per-column count never saw it.
func TestDurableIntegrity_AHoleIsFoundEvenWithEveryColumnFull(t *testing.T) {
	counter := &fakeFillCounter{rows: todasAsColunasCheias("1d", 60000)}
	// 2,000 days of window, 1,980 buckets with a row: 20 days vanished in the middle, and ALL
	// columns remain full.
	comBuraco := diariosContinuos(2000, 30)
	comBuraco.DistinctBuckets = 1980
	spans := &fakeSpanReader{spans: map[string]derivatives.DerivativesTimeframeSpan{"1d": comBuraco}}

	report := probeComJanela(counter, spans, 72*time.Hour).Integrity(context.Background())

	if len(report.Continuity) != 1 {
		t.Fatalf("no continuity verdict: %+v", report.Continuity)
	}
	c := report.Continuity[0]
	if c.MissingBuckets != 20 {
		t.Errorf("missing buckets = %d, want 20 (2,000 expected, 1,980 observed)", c.MissingBuckets)
	}
	if c.ExpectedBuckets != 2000 {
		t.Errorf("expected buckets = %d, want 2000", c.ExpectedBuckets)
	}

	// AND THE PER-COLUMN FILL REMAINS GREEN — which is the proof that the new dimension
	// is not the old one under another name.
	for _, f := range report.Fill {
		if f.State != FillFilled {
			t.Errorf("%s.%s=%s — the table has every column full", f.Timeframe, f.Column, f.State)
		}
	}
}

// A CONTINUOUS SERIES DOES NOT ACCUSE. Without this control, a calculation that returned "buckets missing"
// every time would pass the case above.
func TestDurableIntegrity_AContinuousSeriesReportsNoHole(t *testing.T) {
	counter := &fakeFillCounter{rows: todasAsColunasCheias("1d", 60000)}
	spans := &fakeSpanReader{spans: map[string]derivatives.DerivativesTimeframeSpan{"1d": diariosContinuos(2000, 30)}}

	report := probeComJanela(counter, spans, 72*time.Hour).Integrity(context.Background())
	if report.Continuity[0].MissingBuckets != 0 {
		t.Errorf("a series with no hole reported %d missing", report.Continuity[0].MissingBuckets)
	}
}

// TIMEFRAME COVERAGE IS THE DIMENSION THE PER-COLUMN COUNT CANNOT HAVE: it
// asks about the columns of a timeframe, and a newly covered timeframe with NO row
// answers zero in all of them — without saying that the problem is the entire timeframe.
func TestDurableIntegrity_ATimeframeWithNoRowsAtAllIsReportedAsUncovered(t *testing.T) {
	counter := &fakeFillCounter{rows: todasAsColunasCheias("1d", 0)}
	spans := &fakeSpanReader{spans: map[string]derivatives.DerivativesTimeframeSpan{"1d": {}}}

	// After the grace period: 1d has a grace period of two daily buckets = 48h.
	report := probeComJanela(counter, spans, 72*time.Hour).Integrity(context.Background())
	if len(report.Coverage) != 1 {
		t.Fatalf("no coverage verdict: %+v", report.Coverage)
	}
	if report.Coverage[0].Covered {
		t.Error("a timeframe with zero rows came out covered")
	}
	if report.Coverage[0].WithinGrace {
		t.Error("after grace the timeframe was still being spared")
	}

	// AND WITHIN THE GRACE PERIOD IT IS SPARED — otherwise the first boot would accuse every timeframe.
	cedo := probeComJanela(counter, spans, time.Hour).Integrity(context.Background())
	if !cedo.Coverage[0].WithinGrace {
		t.Error("within grace the timeframe was charged — the first boot would accuse everything")
	}
}

// THE ROW COUNT ARRIVES WITH THE NUMBER, and not as a "has rows" boolean.
func TestDurableIntegrity_RowCountCarriesTheMeasuredNumber(t *testing.T) {
	counter := &fakeFillCounter{rows: todasAsColunasCheias("1d", 45161)}
	spans := &fakeSpanReader{spans: map[string]derivatives.DerivativesTimeframeSpan{
		"1d": {
			Rows: 45161, DistinctBuckets: 2002,
			Oldest: time.Date(2021, 3, 3, 0, 0, 0, 0, time.UTC),
			Newest: time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC),
		},
	}}

	report := probeComJanela(counter, spans, 72*time.Hour).Integrity(context.Background())
	if got := report.Rows["1d"]; got != 45161 {
		t.Errorf("rows for 1d = %d, want 45161 — the number measured on the LAN", got)
	}
}

// AN UNREADABLE WINDOW DOES NOT BECOME ZERO. A read that failed does not authorize declaring the series
// whole or empty — the three dimensions disappear from the label, which is the honest answer.
func TestDurableIntegrity_AnUnreadableSpanIsNotAnIntegrityVerdict(t *testing.T) {
	counter := &fakeFillCounter{rows: todasAsColunasCheias("1d", 1000)}
	spans := &fakeSpanReader{err: errors.New("connection refused")}

	report := probeComJanela(counter, spans, 72*time.Hour).Integrity(context.Background())
	if len(report.Continuity) != 0 || len(report.Coverage) != 0 || len(report.Rows) != 0 {
		t.Errorf("a failed read produced a verdict: %+v", report)
	}
	if len(report.UnmeasuredDimensions()) != len(declaredIntegrityDimensions())-1 {
		t.Errorf("dimensions without an answer were not named: %v", report.UnmeasuredDimensions())
	}
}

// AND THE REPORT TAKES NOTHING DOWN — the same promise as `Report`, with more dimensions.
// `ReportIntegrity` does not return an error, and the arity is checked by reflection so that a
// return added later — the natural path to turning a warning into a refusal — fails here.
func TestDurableIntegrity_ReportingNeverBlocksTheBoot(t *testing.T) {
	counter := &fakeFillCounter{rows: todasAsColunasCheias("1d", 0)}
	spans := &fakeSpanReader{spans: map[string]derivatives.DerivativesTimeframeSpan{"1d": {}}}
	probe := probeComJanela(counter, spans, 72*time.Hour)

	probe.ReportIntegrity(context.Background())

	if got := reflect.TypeOf(probe.ReportIntegrity).NumOut(); got != 0 {
		t.Errorf("ReportIntegrity started returning %d value(s) — the natural path for the caller to turn the warning into a refusal", got)
	}
}

// TestDurableIntegrity_TheGroupCoherenceDimensionSeesWhatColumnFillREWARDS ⭐ is the reason the
// fifth dimension exists, stated as a property and not as prose.
//
// The other four ask about ONE column or about the INSTANT. The contradiction in the funding
// group is between TWO columns of the same row, and no per-column projection reaches it.
//
// WORSE THAN INVISIBLE: the incoherent row carries `funding_rate_8h`, so it ADDS to the
// fill of the column whose zero opened this feature. Here the table comes out with the column
// declared COVERED (gauge = 1), the timeframe COVERED, the series CONTINUOUS and the count FULL —
// and even so there are 137 rows asserting an eight-hour equivalent without a step that
// authorizes it.
func TestDurableIntegrity_TheGroupCoherenceDimensionSeesWhatColumnFillREWARDS(t *testing.T) {
	counter := &fakeFillCounter{rows: todasAsColunasCheias("1d", 60000)}
	spans := &fakeSpanReader{spans: map[string]derivatives.DerivativesTimeframeSpan{"1d": diariosContinuos(2000, 30)}}
	coherence := &fakeCoherenceReader{incoerentes: map[string]int64{"1d": 137}}

	report := probeCompleto(counter, spans, coherence, 72*time.Hour).Integrity(context.Background())

	// THE OTHER FOUR READ HEALTHY.
	for _, f := range report.Fill {
		if f.Column == "funding_rate_8h" && f.State != FillFilled {
			t.Fatalf("funding_rate_8h came out as %q — the scenario requires the column FULL, "+
				"otherwise the case proves nothing about rewarding the defect", f.State)
		}
	}
	for _, c := range report.Continuity {
		if c.MissingBuckets != 0 {
			t.Fatalf("continuity reported %d bucket(s) — the scenario requires a continuous series", c.MissingBuckets)
		}
	}
	for _, c := range report.Coverage {
		if !c.Covered {
			t.Fatal("timeframe coverage reported — the scenario requires the timeframe covered")
		}
	}

	// AND THE FIFTH FINDS THE DEFECT.
	if len(report.Coherence) != 1 {
		t.Fatalf("%d coherence finding(s), want 1", len(report.Coherence))
	}
	if got := report.Coherence[0].IncoherentRows; got != 137 {
		t.Fatalf("incoherent rows = %d, want 137 — the dimension that sees the contradiction "+
			"between two columns of the same row", got)
	}
	if report.Coherence[0].Timeframe != "1d" {
		t.Fatalf("finding attributed to %q", report.Coherence[0].Timeframe)
	}
}

// UNREADABLE COHERENCE DOES NOT BECOME ZERO, by the same rule as the window: a query that failed does not
// authorize declaring the table coherent. The dimension disappears from the label and is named.
func TestDurableIntegrity_AnUnreadableCoherenceIsNotAVerdict(t *testing.T) {
	counter := &fakeFillCounter{rows: todasAsColunasCheias("1d", 1000)}
	spans := &fakeSpanReader{spans: map[string]derivatives.DerivativesTimeframeSpan{"1d": diariosContinuos(2000, 30)}}
	coherence := &fakeCoherenceReader{err: errors.New("connection refused")}

	report := probeCompleto(counter, spans, coherence, 72*time.Hour).Integrity(context.Background())

	if len(report.Coherence) != 0 {
		t.Fatalf("a failed query produced a coherence verdict: %+v", report.Coherence)
	}
	faltando := report.UnmeasuredDimensions()
	if len(faltando) != 1 || faltando[0] != DimensionFundingCoherence {
		t.Fatalf("unanswered dimensions = %v, want only the funding group coherence", faltando)
	}
}

// TestDurableFillProbe_BlindnessIsNotSilent ⭐ closes the silent path: when NO
// count answers, `Report` cannot return without saying anything.
//
// A zero-findings path cannot make the blind checker come out in the log the same as a satisfied
// checker: the absence of a verdict must not pass for a verdict.
func TestDurableFillProbe_BlindnessIsNotSilent(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	counter := &fakeFillCounter{err: errors.New("questdb is down")}
	probeCom(counter, 100*time.Hour, log).Report(context.Background())

	level, found := sink.LevelOf("nothing was checked")
	if !found {
		t.Fatalf("the blind checker said nothing; captured: %v", sink.Records())
	}
	if level != logger.LevelWarn {
		t.Errorf("blindness came out at %s; below WARN it does not appear at the process's default level", level)
	}
	// AND NOT the per-field accusation: without a count there is no finding, and accusing here would send the
	// operator to look for the defect in twelve fields that answered.
	if _, acusou := sink.LevelOf("declared and not delivered"); acusou {
		t.Errorf("a count that did not answer became a per-field accusation: %v", sink.Records())
	}
}

// NEGATIVE CONTROL: a checker that answered does NOT emit the blindness line — without this
// case, an unconditional `Warn` would pass the one above and the log would gain a line per cycle.
func TestDurableFillProbe_ACountedCycleIsNotReportedAsBlind(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	counter := &fakeFillCounter{rows: todasAsColunasCheias("1d", 2000)}
	probeCom(counter, 100*time.Hour, log).Report(context.Background())

	if _, cego := sink.LevelOf("nothing was checked"); cego {
		t.Errorf("a counted cycle was reported as blind: %v", sink.Records())
	}
}
