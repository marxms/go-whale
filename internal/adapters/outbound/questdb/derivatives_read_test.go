package questdb

import (
	"context"
	"math"
	"reflect"
	"testing"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/derivatives"
)

// derivScope names the durable series these read tests exercise.
func derivScope() derivatives.DerivativesScope {
	return derivatives.DerivativesScope{
		Source: "coinalyze", Exchange: "binance", MarketType: "future",
		Symbol: "BTC/USDT", Timeframe: "1d",
	}
}

func TestDerivativesRangeQuery_TheIntervalIsALiteralAndTheScopeIsBound(t *testing.T) {
	from := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 8, 23, 0, 0, 0, 0, time.UTC)

	query := derivativesRangeQuery(from, to)

	for _, want := range []string{
		// The two unit columns enter the SAME `SELECT` as the others: without them the row
		// comes back with the number and without its unit.
		"funding_interval_hours",
		"n_step",
		"timestamp >= '2026-08-20T00:00:00.000000Z'",
		"timestamp <= '2026-08-23T00:00:00.000000Z'",
		"source = $1",
		"symbol = $4",
		"ORDER BY timestamp ASC",
		"FROM derivatives",
	} {
		if !contains(query, want) {
			t.Fatalf("query does not contain %q:\n%s", want, query)
		}
	}
	// The scope NEVER becomes SQL text: it is what comes from the exchange.
	for _, forbidden := range []string{"coinalyze", "BTC/USDT", "binance"} {
		if contains(query, forbidden) {
			t.Fatalf("query interpolates %q instead of binding it:\n%s", forbidden, query)
		}
	}
}

func contains(haystack, needle string) bool {
	return len(needle) > 0 && len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}

// THE STRAIGHT-LINE DEFECT, PINNED DOWN.
//
// The panel showed `OI coverage 81%` and a 156-day STRAIGHT LINE in open interest between two
// dates. In that stretch the ZSET has the bucket and does NOT have `openInterestBase`; the
// durable tier has both. Overwriting the whole point swapped a value for a null, and the chart
// joined the ends with a straight segment — worse than a hole, because a hole looks like
// missing data and a straight line looks like data.
func TestPresentFloat_TreatsNaNAsAbsent(t *testing.T) {
	nan := math.NaN()
	if got := presentFloat(&nan); got != nil {
		t.Errorf("NaN crossed as %v; it would pass every filter without triggering an error", *got)
	}
	if got := presentFloat(nil); got != nil {
		t.Errorf("nil became %v", *got)
	}

	// AND A NORMAL NUMBER PASSES — without this, "always returns nil" would satisfy the two
	// cases above and the durable read would serve an entire service of absences.
	for _, v := range []float64{0, -1.5, 1234.5} {
		valor := v
		got := presentFloat(&valor)
		if got == nil {
			t.Errorf("%v was treated as absent", v)
			continue
		}
		if *got != valor {
			t.Errorf("%v became %v", valor, *got)
		}
	}
}

// INFINITY IS THE SAME DEFECT ONE VALUE UP: `±Inf` is greater (or smaller) than everything,
// crosses every filter and triggers no error.
//
// `presentFloat` must filter it here: `derivatives.LogIncrement` rejects `<= 0` and `NaN`, so
// `+Inf` would survive all the way to the axis calculation, and this is the last point where
// it can be barred.
func TestPresentFloat_TreatsInfinityAsAbsent(t *testing.T) {
	for _, v := range []float64{math.Inf(1), math.Inf(-1)} {
		valor := v
		if got := presentFloat(&valor); got != nil {
			t.Errorf("%v crossed as %v; it is greater (or smaller) than every filter and triggers no error", v, *got)
		}
	}

	// CONTROL: the greatest finite value on both sides PASSES. Without it, "returns nil for
	// everything large" would pass the two cases above and the durable read would lose real
	// tail data.
	for _, v := range []float64{math.MaxFloat64, -math.MaxFloat64} {
		valor := v
		if got := presentFloat(&valor); got == nil || *got != valor {
			t.Errorf("the finite %v did not cross: %v", v, got)
		}
	}
}

// THE GUARD'S COVERAGE, ENUMERATED BY THE COLUMNS THE DURABLE READ REQUESTS.
//
// The path is the query's LIST (`derivativesReadColumns`), which is already checked against
// the durable coverage declaration by
// `TestDerivativesReadColumns_EveryDeclaredDurableColumnIsAsked` — so a new column enters
// here by itself. There is no second hand-written field list: the enumeration comes from the
// SCAN TARGETS (`scanTargets`), which is what the driver fills.
//
// What is asserted, per column and per non-finite value: the value does NOT cross into the
// snapshot. The snapshot-side check is by reflection because `DerivativesSnapshot` lives in
// another package — its fields are exported —, and the assertion is about ANY field with a
// non-finite value, which is stronger than checking the equivalently named field and avoids
// the name map that would diverge silently.
func TestDerivativesRow_NoNonFiniteColumnCrossesTheReadWhole(t *testing.T) {
	naoFinitos := []float64{math.Inf(1), math.Inf(-1), math.NaN()}

	linha := &derivativesRow{}
	colunas := derivativesReadColumns()

	var colunasExercitadas int
	for i, alvo := range linha.scanTargets() {
		ponteiro, ok := alvo.(**float64)
		if !ok {
			continue // timestamp, n_step and funding_at_anchor are not quantities
		}
		colunasExercitadas++

		for _, naoFinito := range naoFinitos {
			// Through the SAME pointer the Scan uses: that is how the value enters the row,
			// and the pointer points at its field.
			valor := naoFinito
			*ponteiro = &valor

			atravessou := camposNaoFinitosDoSnapshot(linha.snapshot(derivScope()))
			if len(atravessou) > 0 {
				t.Errorf("column %q (%v) crossed the read in %v", colunas[i], naoFinito, atravessou)
			}
			*ponteiro = nil
		}
	}
	if colunasExercitadas == 0 {
		t.Fatal("no numeric column enumerated by the Scan targets — the check would pass vacuously")
	}

	// POSITIVE CONTROL of the enumeration: with FINITE values in every numeric column, the
	// snapshot carries ALL of them. Without this case, a read that returned everything absent
	// would pass the loops above.
	cheia := &derivativesRow{}
	for _, alvo := range cheia.scanTargets() {
		if ponteiro, ok := alvo.(**float64); ok {
			finito := 1234.5
			*ponteiro = &finito
		}
	}
	if presentes := camposFloatPresentes(cheia.snapshot(derivScope())); presentes != colunasExercitadas {
		t.Fatalf("the snapshot carried %d of %d finite quantities — the read is discarding what has a reading",
			presentes, colunasExercitadas)
	}
}

// camposNaoFinitosDoSnapshot names every float field of the snapshot that carries a value
// WITHOUT A READING. The check is by reflection because `DerivativesSnapshot` lives in
// another package and an equivalent name map would diverge silently — the same reason a
// second field list is avoided elsewhere.
func camposNaoFinitosDoSnapshot(snap *derivatives.DerivativesSnapshot) []string {
	var out []string
	v := reflect.ValueOf(snap).Elem()
	for i := 0; i < v.NumField(); i++ {
		ptr, ok := v.Field(i).Interface().(*float64)
		if !ok || ptr == nil {
			continue
		}
		if math.IsNaN(*ptr) || math.IsInf(*ptr, 0) {
			out = append(out, v.Type().Field(i).Name)
		}
	}
	return out
}

// camposFloatPresentes counts the float quantities the snapshot carries — the positive
// control of the case above, without needing each one's name.
func camposFloatPresentes(snap *derivatives.DerivativesSnapshot) int {
	presentes := 0
	v := reflect.ValueOf(snap).Elem()
	for i := 0; i < v.NumField(); i++ {
		if ptr, ok := v.Field(i).Interface().(*float64); ok && ptr != nil {
			presentes++
		}
	}
	return presentes
}

// ============================================================================
// THE DURABLE READ REACHES THE NEW COLUMNS
// ============================================================================
//
// THERE IS NO `SELECT *` IN THIS MODULE. The read lists the columns one by one and does the
// `Scan` by position, so a column added to the table and to the write is SILENTLY
// UNREACHABLE: nothing fails, nothing warns, and the only evidence is a null field in the
// response.
//
// The chain this closes against us: column written → declaration says covered → cap released
// → the only readable copy (the ZSET) is destroyed, and the durable column that would replace
// it never reaches the client.

// TestDerivativesReadColumns_EveryDeclaredDurableColumnIsAsked is the "readable" half of that
// chain, and it is the only mechanical checker between the write and the route.
//
// The comparison is against the SAME declaration the boot gate and the write checker consume:
// every column the durable coverage claims to carry must be in the list the read requests. A
// column written and not requested is coverage that never reaches anyone.
func TestDerivativesReadColumns_EveryDeclaredDurableColumnIsAsked(t *testing.T) {
	pedidas := map[string]bool{}
	for _, coluna := range derivativesReadColumns() {
		pedidas[coluna] = true
	}

	var declaradas int
	for campo, coluna := range redisstore.DerivativesDurableCoverage() {
		if coluna == "" {
			continue
		}
		declaradas++
		if !pedidas[coluna] {
			t.Errorf("%s is declared covered by column %q, which the durable read does NOT request — "+
				"the column is written and silently unreachable", campo, coluna)
		}
	}
	if declaradas == 0 {
		t.Fatal("no column declared covered — the comparison would pass vacuously")
	}
}

// TestDerivativesRow_TheScanTargetsMatchTheColumnsAsked — the `Scan` is POSITIONAL, and a
// column added to the list without a matching target is an error on every row read.
//
// Arity is checked here so it fails in CI instead of on the first deep-window `GET`.
func TestDerivativesRow_TheScanTargetsMatchTheColumnsAsked(t *testing.T) {
	var row derivativesRow
	if got, want := len(row.scanTargets()), len(derivativesReadColumns()); got != want {
		t.Fatalf("%d Scan target(s) for %d requested column(s): %v",
			got, want, derivativesReadColumns())
	}
}

// TestDerivativesRow_TheFundingGroupCrossesTheReadWhole kills the mutant that populates
// `fundingRate8h` from the cold tier without bringing the `n_step` from the same row.
//
// An eight-hour equivalent without the rung that produced it is a number that asserts
// evidence without carrying it: the consumer cannot distinguish a measured `N` from an
// inherited one, and it is exactly that distinction that keeps a 2021 bucket normalized at
// today's cadence from entering a cost comparison.
func TestDerivativesRow_TheFundingGroupCrossesTheReadWhole(t *testing.T) {
	hours := 4.0
	step := string(derivatives.NFromMeasuredSpacing)
	rate, rate8h := -8.547e-05, -1.7094e-04

	row := derivativesRow{
		ts:                   time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC),
		fundingRate:          &rate,
		fundingRate8h:        &rate8h,
		fundingIntervalHours: &hours,
		nStep:                &step,
	}

	snap := row.snapshot(derivScope())

	if snap.FundingRate8h == nil || *snap.FundingRate8h != rate8h {
		t.Fatalf("FundingRate8h=%v, wanted %v", snap.FundingRate8h, rate8h)
	}
	if snap.FundingIntervalHours == nil || *snap.FundingIntervalHours != 4 {
		t.Fatalf("FundingIntervalHours=%v, wanted 4 — the N that produced the number above",
			snap.FundingIntervalHours)
	}
	if snap.NStep != derivatives.NFromMeasuredSpacing {
		t.Fatalf("NStep=%q, wanted %q — without the rung, the 8h asserts evidence it does not carry",
			snap.NStep, derivatives.NFromMeasuredSpacing)
	}
}

// TestDerivativesRow_AnAbsentRungIsTheEmptyStepAndNotAPanic — the `n_step` column is NULL on
// every row written before it existed, and a read that assumes otherwise takes down the
// entire deep window.
//
// The empty rung is the zero value, and it does NOT normalize — which is the right answer for
// a row nobody classified.
func TestDerivativesRow_AnAbsentRungIsTheEmptyStepAndNotAPanic(t *testing.T) {
	oi := 106117.954
	row := derivativesRow{
		ts:               time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC),
		openInterestBase: &oi,
	}

	snap := row.snapshot(derivScope())

	if snap.NStep != "" {
		t.Fatalf("NStep=%q on a row without the column, wanted the empty rung", snap.NStep)
	}
	if snap.NStep.Normalizes() {
		t.Fatal("the empty rung normalized — a row nobody classified carries no evidence")
	}
	if snap.OpenInterestBase == nil || *snap.OpenInterestBase != oi {
		t.Fatalf("OpenInterestBase=%v, wanted %v", snap.OpenInterestBase, oi)
	}
}

// ============================================================================
// THE FUNDING GROUP MOVES AS A UNIT, AND THE SEAM DECLARES ITSELF
// ============================================================================

func TestDerivativesReadColumns_EveryWrittenColumnIsAsked(t *testing.T) {
	sender := newFakeLineSender()
	writer := derivatives.NewDerivativesTieredWriter(newFakeColdStore(sender))
	if err := writer.WriteBatch(context.Background(), fullyPopulatedBatch()); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if len(sender.lines) != 1 {
		t.Fatalf("%d line(s) emitted, wanted 1", len(sender.lines))
	}

	pedidas := map[string]bool{}
	for _, coluna := range derivativesReadColumns() {
		pedidas[coluna] = true
	}

	emitidas := colunasDeMedicaoEmitidas(sender.lines[0])
	if len(emitidas) == 0 {
		t.Fatal("the write emitted no measurement column at all — the comparison would pass vacuously")
	}
	for _, coluna := range emitidas {
		if !pedidas[coluna] {
			t.Errorf("the durable write emits %q and the read does NOT request it — the column is silently unreachable", coluna)
		}
	}
}

// THE READ ROW CARRIES THE SIX NEW COLUMNS ALL THE WAY TO THE SNAPSHOT.
//
// The requested list, the `Scan` targets and the translation are three places, and all three
// must agree. Requesting the column and losing it in translation is the most expensive form
// of lying "covered": the query pays the cost and the client receives null.
//
// `fundingAtAnchor` is asserted as a PREDICATE — `true` arrives as `true`, and the case below
// guarantees `false` does not become absence.
func TestDerivativesRow_TheSixNewQuantitiesCrossTheRead(t *testing.T) {
	oi, next, ir := 5_432_100_000.0, 0.00012, 0.0001
	long, short := 48.86, 51.14
	anchor := true

	row := derivativesRow{
		ts:              time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC),
		openInterest:    &oi,
		nextFundingRate: &next,
		interestRate:    &ir,
		fundingAtAnchor: &anchor,
		lsLongPct:       &long,
		lsShortPct:      &short,
	}
	got := row.snapshot(derivatives.DerivativesScope{Source: "coinalyze", Symbol: "BTC/USDT"})

	for _, caso := range []struct {
		nome string
		got  *float64
		want float64
	}{
		{"openInterest", got.OpenInterest, oi},
		{"nextFundingRate", got.NextFundingRate, next},
		{"interestRate", got.InterestRate, ir},
		{"lsLongPct", got.LSLongPct, long},
		{"lsShortPct", got.LSShortPct, short},
	} {
		if caso.got == nil {
			t.Errorf("%s died in the translation of the read row", caso.nome)
			continue
		}
		if *caso.got != caso.want {
			t.Errorf("%s = %v, wanted %v", caso.nome, *caso.got, caso.want)
		}
	}
	if got.FundingAtAnchor == nil || *got.FundingAtAnchor != true {
		t.Errorf("fundingAtAnchor = %v, wanted true", got.FundingAtAnchor)
	}
}

// `false` IS NOT ABSENCE. The predicate has three states — pinned, not pinned, not measured —
// and collapsing the last two would publish "funding is not at the anchor" about a row that
// measured nothing.
func TestDerivativesRow_AFalseAnchorIsNotAnAbsentOne(t *testing.T) {
	naoPreso := false
	row := derivativesRow{ts: time.Now(), fundingAtAnchor: &naoPreso}

	got := row.snapshot(derivatives.DerivativesScope{Source: "coinalyze"})
	if got.FundingAtAnchor == nil {
		t.Fatal("a read `false` became absence — not measured and measured-as-false became the same state")
	}
	if *got.FundingAtAnchor {
		t.Fatalf("fundingAtAnchor = true over a read `false`")
	}

	semLeitura := derivativesRow{ts: time.Now()}
	if semLeitura.snapshot(derivatives.DerivativesScope{}).FundingAtAnchor != nil {
		t.Fatal("a NULL column became a predicate")
	}
}

// THE COMPANIONS TRAVEL WITH THE PRINCIPAL, and the reason is the seam.
//
// Notional and base come from the SAME open-interest read; both shares come from the SAME
// long/short read. Letting each one choose its tier would stitch, in a single point, numbers
// from two providers that did not measure the same instant.
