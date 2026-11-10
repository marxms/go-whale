package collector

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/marxms/go-whale/internal/derivatives"
)

// ─── THE ADDITIVE FALSIFIER OF THE LABEL CONVENTION ──────────────────────────
//
// THE PROBLEM IT SOLVES. Comparing snapshots does NOT separate start from end: for a
// point-sample series the value tied to boundary `T` is the observation at `T` under BOTH
// conventions — what changes is which interval the bucket represents, not the published number.
// The retracted measurement that tried to decide this way found that "`t = today 00:00Z` does
// not discriminate between them".
//
// WHAT SEPARATES THEM. A field SUMMED over the interval. The bar labeled `T` of an additive feed
// must equal the sum of the sub-buckets of the interval the DECLARATION claims — if the
// declaration is inverted, the bar is compared against the sum of ANOTHER window, and the
// equality disappears. Method measured against the venue snapshots described below.
//
// WHAT THIS FILE does **NOT** CLAIM, and it is the hard caveat of the measurement: the measured
// result is from `takerlongshortRatio`, which is **not one of the three endpoints this collector
// ingests** (the `derivativesLabelDeclarations` table declares funding, OI and LSR — all point
// samples or an event). It confirms the primary doc FOR THE TAKER and **does not transfer** to
// OI/LSR: for those, what was measured is the opposite — the two conventions are
// indistinguishable. No line here supports a transfer.
//
// UNIT vs INTEGRATION. The arithmetic below runs on the VERSIONED FIXTURE, with no network. The
// live call — which exists so the venue can change behavior — lives in
// `label_convention_falsifier_integration_test.go`, behind the `integration` tag, because a
// fixture does not fail when the venue changes and a unit test cannot touch the network.

// ─── VERSIONED FIXTURE — the measurement ─────────────────────────────────────
//
// Provenance: a recorded venue snapshot. Endpoint
// `/futures/data/takerlongshortRatio`, symbol BTCUSDT, Binance USDⓈ-M exchange, field `buyVol`,
// 288 `5m` sub-buckets covering the UTC day 2026-10-06 (a full day: 24h × 12).
//
// WHAT THE FIXTURE CARRIES, AND WHY ONLY THAT. The measurement published the SUM and the `1d`
// bar; the 288 individual values were NOT published, and writing them here would fabricate the
// evidence this file exists to check. The comparable unit, therefore, is the aggregation of an
// interval — which is also what the source publishes in the aggregate bar.
const (
	// fixtureDayLabelMs is the label of the measured `1d` bar — and the START of the interval it
	// covers under the declared convention. `1791244800000` = 2026-10-06T00:00:00Z, resolved by
	// epoch arithmetic and not inherited from the sample table, whose UTC column has a one-day
	// off-by-one on some rows.
	fixtureDayLabelMs int64 = 1791244800000
	// fixtureDayWidthMs is 24h — the width of the interval the `1d` bar covers.
	fixtureDayWidthMs int64 = 86400000
	// fixtureSubBucketCount is the number of `5m` sub-buckets in a full day. Without this count,
	// an empty fixture would "confirm" anything by summing to zero.
	fixtureSubBucketCount int = 288
	// fixtureSubBucketSum is the SUM of the `buyVol` of the 288 sub-buckets of the interval that
	// STARTS at `fixtureDayLabelMs`.
	fixtureSubBucketSum float64 = 53140.53
	// fixtureBarBuyVol is the `buyVol` of the `1d` bar labeled at `fixtureDayLabelMs`.
	fixtureBarBuyVol float64 = 53199.80
	// fixtureResidual is the measured deviation between the two quantities above: 0.111%. It
	// enters as an ASSERTED number, so a digit swap in the fixture fails.
	fixtureResidual float64 = 0.0011141019327141093
)

// additiveFalsifierTolerance is DECLARED, not derived: the fixture measures 0.111% and no larger
// measurement exists. A tight limit below the measured value would fail the venue on its own
// arithmetic; too wide a limit stops separating the inverted declaration. This number is the
// calibration a reviewer must be able to contest, which is why it is named instead of embedded
// in the comparison.
const additiveFalsifierTolerance = 0.005

// additiveSubBucketFalsifierName is the METHOD's name, not a description: it is what an additive
// declaration must name in order not to be refused in `admitDeclaration`. It exists as a
// constant so the declaration string and the machine backing it cannot diverge silently.
const additiveSubBucketFalsifierName = "sum of the 5m sub-buckets equals the 1d bar labeled T"

// subBucketAggregate is the sum of the sub-buckets of ONE interval, with the window it covers.
type subBucketAggregate struct {
	windowStartMs int64
	windowEndMs   int64
	count         int
	sum           float64
}

// additiveBar is an AGGREGATED bar, labeled at `labelMs`.
type additiveBar struct {
	labelMs int64
	value   float64
}

// declaredIntervalStartMs returns the START of the interval the declaration CLAIMS the label
// names, and says whether the declaration is usable.
//
// The nature decides the rest, and this is where the difference stops being rhetorical: for an
// ADDITIVE feed the interval is what it aggregates, so an end convention pushes the start one
// bucket back. For a POINT SAMPLE the value belongs to the label instant and the bucket is the
// one containing it under both conventions — measured,  — so both return the same start and
// the misalignment cannot come from there.
func declaredIntervalStartMs(nature feedNature, decl derivatives.LabelConvention, labelMs, widthMs int64) (int64, bool) {
	if !decl.Valid() {
		return 0, false
	}
	switch nature {
	case feedNatureAdditive:
		if decl == derivatives.LabelIntervalEnd {
			return labelMs - widthMs, true
		}
		return labelMs, true
	case feedNaturePointSample:
		return labelMs, true
	default:
		return 0, false
	}
}

// additiveResidual compares the aggregated bar with the sum of the sub-buckets of the interval
// the DECLARATION claims, and says whether the measurement CONFIRMS it.
//
// NOT CONFIRMING IS THE HONEST RESULT FOR A DECLARATION THE MEASUREMENT DOES NOT REACH: if the
// claimed window is not the window the sub-buckets cover, there is no sum to compare against,
// and inventing one is the guess the declaration exists to prevent.
//
// AND IT IS THE ARITHMETIC THAT REFUSES THE REST — what remains here is what CHANGES THE
// VERDICT, nothing more. An empty aggregate against a zero bar gives 0/0, that is NaN; a
// non-finite bar gives NaN or +Inf; and none of the three is `<=` a finite tolerance. Explicit
// guards for those cases were REMOVED by measurement, not by taste: the discrimination sensor
// showed removing them failed no case, because the verdict was already the same. What replaces
// them are cases that assert the verdict — see the three refusal tests below, in particular the
// non-finite bar one, which exists because the INVERTED comparison
// (`!(residual > tol)`) would confirm NaN.
func additiveResidual(
	decl derivatives.LabelConvention,
	bar additiveBar,
	sub subBucketAggregate,
	widthMs int64,
) (residual float64, confirmed bool) {
	claimedStart, ok := declaredIntervalStartMs(feedNatureAdditive, decl, bar.labelMs, widthMs)
	if !ok || sub.windowStartMs != claimedStart {
		return 0, false
	}
	residual = math.Abs(sub.sum-bar.value) / math.Abs(bar.value)
	return residual, residual <= additiveFalsifierTolerance
}

// ─── THE CONFIRMATION, AND ITS DISCRIMINATING HALF ───────────────────────────

// THE MEASURED CASE, asserted with the two numbers the measurement published and with the
// deviation they produce. The fixture window STARTS at the label — if it started one day
// earlier, this case is what would fail.
func TestAdditiveFalsifierConfirmsTheDeclaredStartInterval(t *testing.T) {
	sub := subBucketAggregate{
		windowStartMs: fixtureDayLabelMs,
		windowEndMs:   fixtureDayLabelMs + fixtureDayWidthMs,
		count:         fixtureSubBucketCount,
		sum:           fixtureSubBucketSum,
	}
	bar := additiveBar{labelMs: fixtureDayLabelMs, value: fixtureBarBuyVol}

	residual, confirmed := additiveResidual(derivatives.LabelIntervalStart, bar, sub, fixtureDayWidthMs)
	if !confirmed {
		t.Fatalf("the measurement did not confirm the start declaration: deviation %.9f", residual)
	}
	if math.Abs(residual-fixtureResidual) > 1e-12 {
		t.Fatalf("the measured deviation is %.9f and the fixture asserts %.9f — a swapped digit in the fixture cannot pass",
			residual, fixtureResidual)
	}
	if residual > additiveFalsifierTolerance {
		t.Fatalf("the measured deviation (%.9f) is OUTSIDE the declared tolerance (%.9f)", residual, additiveFalsifierTolerance)
	}

	// THE COMPARISON IS NOT SELECTOR VACUITY: the same pair, under the OTHER declaration, is
	// compared against another window. This is the discriminating half.
	otherStart, ok := declaredIntervalStartMs(feedNatureAdditive, derivatives.LabelIntervalEnd, bar.labelMs, fixtureDayWidthMs)
	if !ok {
		t.Fatal("the end convention produced no window")
	}
	if otherStart != fixtureDayLabelMs-fixtureDayWidthMs {
		t.Fatalf("the end convention claims window [%d,%d), and the start convention claims [%d,%d)",
			otherStart, otherStart+fixtureDayWidthMs, fixtureDayLabelMs, fixtureDayLabelMs+fixtureDayWidthMs)
	}
}

// THE OPPOSITE DECLARATION IS NOT CONFIRMED BY THE SAME MEASUREMENT. The fixture covers the
// interval the START declaration claims; under the end one, the claimed window is the previous
// one, which this measurement does not carry — and "not measured" must come out as NOT
// confirmed, never as confirmed by omission.
func TestAdditiveFalsifierRefusesTheOppositeDeclaration(t *testing.T) {
	sub := subBucketAggregate{
		windowStartMs: fixtureDayLabelMs,
		windowEndMs:   fixtureDayLabelMs + fixtureDayWidthMs,
		count:         fixtureSubBucketCount,
		sum:           fixtureSubBucketSum,
	}
	bar := additiveBar{labelMs: fixtureDayLabelMs, value: fixtureBarBuyVol}

	if _, confirmed := additiveResidual(derivatives.LabelIntervalStart, bar, sub, fixtureDayWidthMs); !confirmed {
		t.Fatal("the start declaration had to be confirmed — without this, the negative case below is vacuous")
	}
	if _, confirmed := additiveResidual(derivatives.LabelIntervalEnd, bar, sub, fixtureDayWidthMs); confirmed {
		t.Fatal("the END convention was confirmed by the measurement of the interval that STARTS at the label — the falsifier does not discriminate")
	}
}

// WITHOUT A DECLARATION THERE IS NO CONFIRMATION, and the missing convention is refused instead
// of assumed to be start — the same doctrine as the admission door.
func TestAdditiveFalsifierRefusesAnUndeclaredConvention(t *testing.T) {
	sub := subBucketAggregate{
		windowStartMs: fixtureDayLabelMs,
		count:         fixtureSubBucketCount,
		sum:           fixtureSubBucketSum,
	}
	bar := additiveBar{labelMs: fixtureDayLabelMs, value: fixtureBarBuyVol}

	if _, ok := declaredIntervalStartMs(feedNatureAdditive, derivatives.LabelUnspecified, bar.labelMs, fixtureDayWidthMs); ok {
		t.Fatal("an undeclared convention has no claimed window")
	}
	if _, confirmed := additiveResidual(derivatives.LabelUnspecified, bar, sub, fixtureDayWidthMs); confirmed {
		t.Fatal("without a declaration the falsifier confirmed — the missing value became the guess it exists to prevent")
	}
	if _, ok := declaredIntervalStartMs(feedNatureUnspecified, derivatives.LabelIntervalStart, bar.labelMs, fixtureDayWidthMs); ok {
		t.Fatal("an undeclared nature does not know which interval to claim")
	}
}

// AND A DEVIATION LARGER THAN THE TOLERANCE IS REFUSED, with the RIGHT window — otherwise the
// tolerance decides nothing: a confirmation that accepts any number is indistinguishable from a
// function returning `true`.
//
// The bar here is a COUNTERFACTUAL over the measured fixture, and is declared as such: the only
// purpose is to prove the limit bites. Nothing on this line claims anything about the market.
func TestAdditiveFalsifierRefusesADeviationBeyondTheTolerance(t *testing.T) {
	sub := subBucketAggregate{
		windowStartMs: fixtureDayLabelMs,
		windowEndMs:   fixtureDayLabelMs + fixtureDayWidthMs,
		count:         fixtureSubBucketCount,
		sum:           fixtureSubBucketSum,
	}
	bar := additiveBar{labelMs: fixtureDayLabelMs, value: fixtureSubBucketSum * 1.2}

	residual, confirmed := additiveResidual(derivatives.LabelIntervalStart, bar, sub, fixtureDayWidthMs)
	if confirmed {
		t.Fatalf("a deviation of %.6f was confirmed with the tolerance at %.6f", residual, additiveFalsifierTolerance)
	}
	if residual <= additiveFalsifierTolerance {
		t.Fatalf("the counterfactual did not exceed the tolerance: %.6f against %.6f", residual, additiveFalsifierTolerance)
	}
}

// AN EMPTY AGGREGATE DOES NOT CONFIRM, and the verdict is what is asserted — not the mechanism.
// Zero sum against a zero bar gives 0/0, and NaN is not `<=` any tolerance: an incomplete day is
// the real case of this class, and "confirmed by vacuity" is worse than "not measured".
func TestAdditiveFalsifierRefusesAnEmptyAggregate(t *testing.T) {
	sub := subBucketAggregate{
		windowStartMs: fixtureDayLabelMs,
		count:         0,
		sum:           0,
	}
	bar := additiveBar{labelMs: fixtureDayLabelMs, value: 0}

	residual, confirmed := additiveResidual(derivatives.LabelIntervalStart, bar, sub, fixtureDayWidthMs)
	if confirmed {
		t.Fatalf("an aggregation with no sub-buckets confirmed the declaration (deviation %v)", residual)
	}
	if !math.IsNaN(residual) {
		t.Fatalf("0/0 had to give NaN and gave %v — the refusal came from somewhere other than the arithmetic", residual)
	}
}

// A NON-FINITE BAR NEVER CONFIRMS — the invariant applied to the falsifier itself.
//
// This case exists because of a CONCRETE, not hypothetical, implementation error: the inverted
// comparison (`!(residual > tolerance)`) confirms NaN, because every comparison with NaN is
// false. It is the same mechanism that approved `NaN` in `validateShareSum`, which is why it is
// asserted instead of trusted.
func TestAdditiveFalsifierRefusesANonFiniteBar(t *testing.T) {
	sub := subBucketAggregate{
		windowStartMs: fixtureDayLabelMs,
		windowEndMs:   fixtureDayLabelMs + fixtureDayWidthMs,
		count:         fixtureSubBucketCount,
		sum:           fixtureSubBucketSum,
	}

	for _, valor := range []float64{math.Inf(1), math.Inf(-1), math.NaN()} {
		bar := additiveBar{labelMs: fixtureDayLabelMs, value: valor}
		if _, confirmed := additiveResidual(derivatives.LabelIntervalStart, bar, sub, fixtureDayWidthMs); confirmed {
			t.Errorf("a bar %v confirmed the declaration", valor)
		}
	}
}

// THE POINT SAMPLE DOES NOT MOVE WITH THE CONVENTION — and that is what the  measurement says,
// with the numbers from  and : the `1d` bar labeled at `1791331200000`
// (2026-10-07T00:00Z) is 1.1142, which is EXACTLY the `5m` bucket of the same timestamp, while
// the previous `5m` bucket (23:55Z on 2026-10-06) is 1.1128. The value belongs to the label
// instant.
func TestTheMeasuredPointSampleDoesNotMoveWithTheConvention(t *testing.T) {
	const (
		dayLabelMs    int64   = 1791331200000 // 2026-10-07T00:00:00Z
		barDaily      float64 = 1.1142
		sameInstant5m float64 = 1.1142
		previous5m    float64 = 1.1128
	)
	if barDaily != sameInstant5m {
		t.Fatalf("the 1d bar (%v) had to equal the 5m bucket of the SAME instant (%v)", barDaily, sameInstant5m)
	}
	if previous5m == barDaily {
		t.Fatal("the previous 5m bucket equaled the bar — the two instants would be indistinguishable and the case would assert nothing")
	}

	for _, decl := range []derivatives.LabelConvention{derivatives.LabelIntervalStart, derivatives.LabelIntervalEnd} {
		start, ok := declaredIntervalStartMs(feedNaturePointSample, decl, dayLabelMs, fixtureDayWidthMs)
		if !ok {
			t.Fatalf("%s produced no window for the point sample", decl)
		}
		if start != dayLabelMs {
			t.Fatalf("%s pushed the point sample to %d, and the value belongs to the label instant (%d)",
				decl, start, dayLabelMs)
		}
	}

	// THE MISALIGNMENT, THEREFORE, DOES NOT COME FROM THE POINT SAMPLE: both conventions put the
	// reading in the same bucket. It comes from declaring the ADDITIVE feed with the wrong
	// convention.
}

// ─── THE OFFSET BETWEEN AN ADDITIVE FEED AND A SNAPSHOT ONE ──────────────────
//
// THE MEASURED DAMAGE IS ONE BUCKET OF MISALIGNMENT, NOT LOOK-AHEAD. The point sample
// does not move with the convention — asserted above, with the  numbers — and the additive
// field does: the convention decides WHICH interval it aggregates. Two feeds of different
// natures under the same declared convention, without proof, sit one bucket apart, and the
// symptom is the absence of a symptom: the values stay plausible, merely displaced.

// takerDeclaration is the declaration of the measured ADDITIVE feed. The endpoint is NOT
// ingested by this collector (which is why it is not in the production table), and that is why
// the falsifier exists as a machine: a future additive declaration must bring the proof, and the
// proof is this fixture plus this test.
func takerDeclaration() labelDeclaration {
	return labelDeclaration{
		venue:     venueBinance,
		endpoint:  "/futures/data/takerlongshortRatio",
		nature:    feedNatureAdditive,
		label:     derivatives.LabelIntervalStart,
		evidence:  "sum of the 288 5m sub-buckets of the interval starting at the label equals the 1d bar (deviation 0.111%, )",
		falsifier: additiveSubBucketFalsifierName,
	}
}

// lsrDeclaration is the declaration of the POINT-SAMPLE feed, taken from the production table —
// the one in force, with the basis the measurement supports and the caveat named.
func lsrDeclaration(t *testing.T) labelDeclaration {
	t.Helper()
	for _, d := range declarationsFor(venueBinance) {
		if d.feed == feedLongShortRatio {
			return d
		}
	}
	t.Fatal("the long/short feed is not declared — without it there is no pair to compare")
	return labelDeclaration{}
}

// knownAdditiveFalsifier resolves the declared NAME to the MACHINE backing it. This function is
// what separates "declared it proved" from "proved": a name outside this registry has no
// falsifier behind it, and any convention agreement it tried to justify would be prose.
func knownAdditiveFalsifier(name string) bool {
	return name == additiveSubBucketFalsifierName
}

// sharedConventionOffset returns where each feed places its interval, under the conventions each
// DECLARES, and refuses the pair that does not hold up.
//
// A SHARED CONVENTION WITHOUT PROOF IS REFUSED, not assumed: when both declare the same
// convention, what makes that agreement a DECLARATION — and not two guesses that happen to match
// — is the falsifier on the additive side, because it is the only side where the two readings
// predict different numbers. Without it there is no way to separate "we declare the same" from
// "we copied the neighbor's declaration".
func sharedConventionOffset(additive, snapshot labelDeclaration, labelMs, widthMs int64) (additiveStart, snapshotStart int64, err error) {
	if err := admitDeclaration(additive); err != nil {
		return 0, 0, fmt.Errorf("additive feed declaration (%s): %w", additive.endpoint, err)
	}
	if err := admitDeclaration(snapshot); err != nil {
		return 0, 0, fmt.Errorf("snapshot feed declaration (%s): %w", snapshot.endpoint, err)
	}
	if additive.nature != feedNatureAdditive {
		return 0, 0, fmt.Errorf("%s was passed as additive and declares nature %s", additive.endpoint, additive.nature)
	}
	if snapshot.nature != feedNaturePointSample {
		return 0, 0, fmt.Errorf("%s was passed as snapshot and declares nature %s", snapshot.endpoint, snapshot.nature)
	}
	if additive.label == snapshot.label && !knownAdditiveFalsifier(additive.falsifier) {
		return 0, 0, fmt.Errorf("both feeds declare %s and the additive one (%s) brings no known falsifier (%q): shared convention without proof",
			additive.label, additive.endpoint, additive.falsifier)
	}

	var ok bool
	additiveStart, ok = declaredIntervalStartMs(feedNatureAdditive, additive.label, labelMs, widthMs)
	if !ok {
		return 0, 0, fmt.Errorf("the additive feed produced no window under %s", additive.label)
	}
	snapshotStart, ok = declaredIntervalStartMs(feedNaturePointSample, snapshot.label, labelMs, widthMs)
	if !ok {
		return 0, 0, fmt.Errorf("the snapshot feed produced no window under %s", snapshot.label)
	}
	return additiveStart, snapshotStart, nil
}

// THE CONVENTION DECLARED IN COMMON ONLY HOLDS WITH THE ADDITIVE SIDE'S PROOF — and the proof is
// applied here, not cited: the measured fixture must CONFIRM the additive declaration. A
// falsifier name with no machine behind it is prose, and that is what this case refuses.
func TestTheTwoFeedsShareTheBucketOnlyWithProof(t *testing.T) {
	additive := takerDeclaration()
	snapshot := lsrDeclaration(t)

	addStart, snapStart, err := sharedConventionOffset(additive, snapshot, fixtureDayLabelMs, fixtureDayWidthMs)
	if err != nil {
		t.Fatalf("the declared pair was refused: %v", err)
	}
	if addStart != snapStart {
		t.Fatalf("both declare start and landed in different windows: additive at %d, snapshot at %d", addStart, snapStart)
	}
	if addStart != fixtureDayLabelMs {
		t.Fatalf("the agreed window is %d and both declarations say it starts at the label (%d)", addStart, fixtureDayLabelMs)
	}

	// THE PROOF, APPLIED: the same additive field, against the same fixture, confirms the
	// convention the declaration claims. Without this, the case above would assert only that two
	// equal numbers are equal.
	if _, confirmed := additiveResidual(additive.label,
		additiveBar{labelMs: fixtureDayLabelMs, value: fixtureBarBuyVol},
		subBucketAggregate{
			windowStartMs: fixtureDayLabelMs,
			windowEndMs:   fixtureDayLabelMs + fixtureDayWidthMs,
			count:         fixtureSubBucketCount,
			sum:           fixtureSubBucketSum,
		},
		fixtureDayWidthMs); !confirmed {
		t.Fatal("the fixture did not confirm the additive feed's declaration: the shared convention would be left without proof")
	}

	// AND WITHOUT THE PROOF THE PAIR IS REFUSED — the required half, not agreement by omission.
	// Two ways of having no proof, because there are two: NOT NAMING a falsifier, and naming one
	// that no machine backs. The second is the one that would pass as a declaration in a text
	// review.
	for _, semProva := range []labelDeclaration{
		func() labelDeclaration { d := additive; d.falsifier = ""; return d }(),
		func() labelDeclaration { d := additive; d.falsifier = "sum of sub-buckets"; return d }(),
	} {
		_, _, err := sharedConventionOffset(semProva, snapshot, fixtureDayLabelMs, fixtureDayWidthMs)
		if err == nil {
			t.Fatalf("both declared the same convention with falsifier %q and the pair was accepted", semProva.falsifier)
		}
		if !strings.Contains(err.Error(), semProva.endpoint) {
			t.Fatalf("the refusal did not name the additive endpoint: %v", err)
		}
	}

	// AND THE KNOWN NAME RESOLVES TO THE MACHINE — with it, the same pair passes.
	comProva := additive
	comProva.falsifier = additiveSubBucketFalsifierName
	if !knownAdditiveFalsifier(comProva.falsifier) {
		t.Fatal("the falsifier the declaration names does not resolve to the machine that exists")
	}
	if _, _, err := sharedConventionOffset(comProva, snapshot, fixtureDayLabelMs, fixtureDayWidthMs); err != nil {
		t.Fatalf("the pair with the named proof was refused: %v", err)
	}
}

// AND WHEN THE TWO DISAGREE, THE DISTANCE IS EXACTLY ONE BUCKET — the measured damage, not
// look-ahead. The additive feed declared as END aggregates the interval PREVIOUS to the label,
// while the snapshot publishes in the label's bucket: the two stop describing the same window,
// which is why a shared convention is what must be proved.
func TestASignDisagreementMovesTheAdditiveFeedByOneBucket(t *testing.T) {
	additive := takerDeclaration()
	additive.label = derivatives.LabelIntervalEnd
	snapshot := lsrDeclaration(t)

	addStart, snapStart, err := sharedConventionOffset(additive, snapshot, fixtureDayLabelMs, fixtureDayWidthMs)
	if err != nil {
		t.Fatalf("different declarations were refused: %v", err)
	}
	if offset := (snapStart - addStart) / fixtureDayWidthMs; offset != 1 {
		t.Fatalf("the misalignment is %d bucket(s), and the measured one is 1: additive at %d, snapshot at %d",
			offset, addStart, snapStart)
	}
	if addStart != fixtureDayLabelMs-fixtureDayWidthMs {
		t.Fatalf("the additive feed declared as end had to aggregate the window ending at the label and starting at %d, and it started at %d",
			fixtureDayLabelMs-fixtureDayWidthMs, addStart)
	}
	if snapStart != fixtureDayLabelMs {
		t.Fatalf("the snapshot had to stay in the label's bucket (%d) and went to %d — for a point sample the two conventions are the same reading",
			fixtureDayLabelMs, snapStart)
	}
}

// THE ROLES ARE NAMED BY TYPE, not by argument order: a swapped pair would give an offset with
// the inverted sign, which is a plausible and wrong number — the same class of defect as a
// one-bucket misalignment.
func TestTheOffsetRefusesSwappedRoles(t *testing.T) {
	additive := takerDeclaration()
	snapshot := lsrDeclaration(t)

	if _, _, err := sharedConventionOffset(snapshot, additive, fixtureDayLabelMs, fixtureDayWidthMs); err == nil {
		t.Fatal("the swapped pair was accepted as additive-and-snapshot")
	}
	lslOnly := additive
	lslOnly.nature = feedNaturePointSample
	if _, _, err := sharedConventionOffset(lslOnly, snapshot, fixtureDayLabelMs, fixtureDayWidthMs); err == nil {
		t.Fatal("a point-sample feed was accepted in the additive role")
	}
}
