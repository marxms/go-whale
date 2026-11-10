package derivatives

import (
	"math"
	"math/rand"
	"slices"
	"testing"
)

func TestLogIncrement(t *testing.T) {
	tests := []struct {
		name       string
		curr, prev float64
		want       float64
		wantOK     bool
	}{
		{"doubling is +ln2", 200, 100, math.Ln2, true},
		{"halving is -ln2", 100, 200, -math.Ln2, true},
		{"unchanged is zero", 100, 100, 0, true},
		{"zero current is not takeable", 0, 100, 0, false},
		{"zero previous is not takeable", 100, 0, 0, false},
		{"negative is not takeable", -1, 100, 0, false},
		{"NaN is not takeable", math.NaN(), 100, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := LogIncrement(tt.curr, tt.prev)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && math.Abs(got-tt.want) > 1e-12 {
				t.Errorf("increment = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestLogIncrement_MirroredMovesAreSymmetric locks the property that decides the
// logarithm for the long/short axis: doubling and halving are the same distance from
// unchanged, in opposite directions. Under a relative change they are +100% and -50%,
// which would rank a build-up above an equally-sized unwind.
func TestLogIncrement_MirroredMovesAreSymmetric(t *testing.T) {
	up, _ := LogIncrement(200, 100)
	down, _ := LogIncrement(100, 200)
	if math.Abs(up+down) > 1e-12 {
		t.Fatalf("mirrored moves are not symmetric: up=%v down=%v", up, down)
	}
}

// TestLogIncrement_AdditiveAcrossTimeframes is the property that makes the timeframe
// menu a zoom of one object rather than four unrelated objects: a coarse increment is
// EXACTLY the sum of the fine increments it contains.
//
// This is what decides the logarithm over the percentage change. Under percentage the
// same aggregation does not close, and the error grows with the coarseness — the 1h
// column would stop being the sum of its twelve 5m constituents.
func TestLogIncrement_AdditiveAcrossTimeframes(t *testing.T) {
	// Twelve 5m observations spanning one hour, deliberately irregular.
	series := []float64{100, 103, 101, 108, 112, 109, 121, 118, 130, 127, 141, 136, 150}

	var sumOfParts float64
	for i := 1; i < len(series); i++ {
		inc, ok := LogIncrement(series[i], series[i-1])
		if !ok {
			t.Fatalf("increment %d not takeable", i)
		}
		sumOfParts += inc
	}

	whole, ok := LogIncrement(series[len(series)-1], series[0])
	if !ok {
		t.Fatal("coarse increment not takeable")
	}

	if math.Abs(sumOfParts-whole) > 1e-12 {
		t.Fatalf("aggregation does not close: parts=%v whole=%v delta=%v",
			sumOfParts, whole, sumOfParts-whole)
	}

	// And the contrast: percentage change does NOT close, which is why it loses.
	pctParts := 1.0
	for i := 1; i < len(series); i++ {
		pctParts *= series[i] / series[i-1]
	}
	pctWhole := series[len(series)-1] / series[0]
	// The product form closes; the ADDITIVE percentage form is what breaks.
	var pctSum float64
	for i := 1; i < len(series); i++ {
		pctSum += (series[i] - series[i-1]) / series[i-1]
	}
	pctDirect := (series[len(series)-1] - series[0]) / series[0]
	if math.Abs(pctSum-pctDirect) < 1e-6 {
		t.Errorf("percentage unexpectedly additive here; the test premise is wrong "+
			"(sum=%v direct=%v product=%v whole=%v)", pctSum, pctDirect, pctParts, pctWhole)
	}
}

// TestMedian_DoesNotMutateInput guards a safety property with real consequences: the
// slice handed in IS the trailing baseline window, and sorting it in place would
// destroy the chronological order that span and continuity checks depend on.
func TestMedian_DoesNotMutateInput(t *testing.T) {
	input := []float64{5, 1, 4, 2, 3}
	original := slices.Clone(input)

	if _, ok := Median(input); !ok {
		t.Fatal("median not computable")
	}
	if !slices.Equal(input, original) {
		t.Fatalf("Median mutated its input: got %v, want %v", input, original)
	}
}

func TestMedianAndMAD(t *testing.T) {
	tests := []struct {
		name       string
		values     []float64
		wantMedian float64
		wantMAD    float64
	}{
		{"odd count", []float64{1, 3, 5}, 3, 2},
		{"even count", []float64{1, 2, 3, 4}, 2.5, 1},
		{"all identical gives zero scale", []float64{7, 7, 7, 7}, 7, 0},
		{"single point gives zero scale", []float64{42}, 42, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			median, ok := Median(tt.values)
			if !ok {
				t.Fatal("median not computable")
			}
			if math.Abs(median-tt.wantMedian) > 1e-12 {
				t.Errorf("median = %v, want %v", median, tt.wantMedian)
			}
			mad, ok := MAD(tt.values, median)
			if !ok {
				t.Fatal("MAD not computable")
			}
			if math.Abs(mad-tt.wantMAD) > 1e-12 {
				t.Errorf("MAD = %v, want %v", mad, tt.wantMAD)
			}
		})
	}
}

func TestBaselineState(t *testing.T) {
	const nMin = 30

	tests := []struct {
		name   string
		values []float64
		want   BaselineState
	}{
		{"too few observations", repeatVarying(10), BaselineImmature},
		{"enough and dispersed", repeatVarying(60), BaselineMature},
		{"enough but all identical", constant(60, 3.0), BaselineDegenerate},
		{"majority identical still degenerates", majorityTied(60, 40), BaselineDegenerate},

		// The three points flush against the boundary. `N < nMin` and `N <= nMin` agree
		// everywhere except at N == nMin, and that single point decides whether the
		// panel lights up or stays indeterminate on the very first eligible bar — the
		// declared minimum is a minimum that COUNTS, not a value still short of it.
		{"one short of the declared minimum is immature", repeatVarying(nMin - 1), BaselineImmature},
		{"exactly the declared minimum is mature", repeatVarying(nMin), BaselineMature},
		{"one past the declared minimum is mature", repeatVarying(nMin + 1), BaselineMature},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Compute(tt.values).State(nMin)
			if got != tt.want {
				t.Errorf("state = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestZ_AbsoluteValueFixesTheUnit is the sensor against a defect that ALREADY SHIPPED
// once: a previous revision of the derivatives thresholds carried the z unit wrong by
// exactly the MAD consistency factor, 1.4826×, and nothing failed.
//
// Every other test here compares a score against another score, or against a
// magnitude built from b.MAD — all of which stay true if the factor is deleted or
// changed. This one fixes the ARITHMETIC RESULT as a literal, so it survives a rename
// of the constant and dies on any change to its value.
//
// The consequence of the factor being wrong is not cosmetic: the same reading is
// z = 3.3724537973829762 with the factor and z = 5 without it, and the 1h cut sits at
// |z| >= 4.16 — one does not fire and the other does.
//
// The third case carries a NON-ZERO median so the assertion pins the whole formula
// (x − median) / (1.4826 × MAD) rather than the divisor alone: with median 0 a test
// cannot tell subtraction of the median from no subtraction at all.
func TestZ_AbsoluteValueFixesTheUnit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// values is chosen so that the median and the MAD are exact small integers,
		// leaving the scale factor as the only unknown in the expected number.
		values     []float64
		wantMedian float64
		wantMAD    float64
		x          float64
		wantZ      float64
	}{
		{
			// median 0, MAD 1 ⇒ z = 5 / 1.4826.
			name:       "baseline median 0 MAD 1 scores x=5 at 5/1.4826",
			values:     []float64{-2, -1, 0, 1, 2},
			wantMedian: 0,
			wantMAD:    1,
			x:          5,
			wantZ:      3.3724537973829762,
		},
		{
			// The mirror: same magnitude, opposite sign. A folded magnitude would put
			// a collapse and a build-up at the same rank.
			name:       "the mirrored reading is the same magnitude negated",
			values:     []float64{-2, -1, 0, 1, 2},
			wantMedian: 0,
			wantMAD:    1,
			x:          -5,
			wantZ:      -3.3724537973829762,
		},
		{
			// median 10, MAD 2 ⇒ z = (16 − 10) / (1.4826 × 2) = 6 / 2.9652.
			name:       "non-zero median: the whole formula, not just the divisor",
			values:     []float64{6, 8, 10, 12, 14},
			wantMedian: 10,
			wantMAD:    2,
			x:          16,
			wantZ:      2.023472278429786,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := Compute(tt.values)
			if b.Median != tt.wantMedian {
				t.Fatalf("baseline median = %v, want %v — the fixture no longer states what the expected z assumes",
					b.Median, tt.wantMedian)
			}
			if b.MAD != tt.wantMAD {
				t.Fatalf("baseline MAD = %v, want %v — the fixture no longer states what the expected z assumes",
					b.MAD, tt.wantMAD)
			}

			z, state, ok := b.Z(tt.x, len(tt.values))
			if !ok {
				t.Fatalf("no score emitted (state %q) for a mature baseline", state)
			}
			if state != BaselineMature {
				t.Fatalf("state = %q, want %q", state, BaselineMature)
			}
			if math.Abs(z-tt.wantZ) > 1e-9 {
				t.Fatalf("z = %.16g, want %.16g (delta %.3g) — the score is in the wrong UNIT; "+
					"z = (x − median) / (1.4826 × MAD), and dropping the 1.4826 gives %.16g",
					z, tt.wantZ, z-tt.wantZ, (tt.x-tt.wantMedian)/tt.wantMAD)
			}
		})
	}
}

// TestZ_DegenerateEmitsNothing is the decision the study closed: a zero scale is a
// DEGENERATE baseline and follows the same terminal path as an immature one — no
// score, with a reason. A floor would manufacture a score out of nothing, and the
// convention 0/0 := 0 would report "perfectly typical" for a dispersion nobody knows.
func TestZ_DegenerateEmitsNothing(t *testing.T) {
	b := Compute(constant(60, 3.0))
	z, state, ok := b.Z(99.0, 30)
	if ok {
		t.Fatalf("degenerate baseline emitted a score: z=%v", z)
	}
	if state != BaselineDegenerate {
		t.Errorf("state = %q, want %q", state, BaselineDegenerate)
	}
}

// TestZ_ImmatureIsDistinctFromDegenerate keeps the two reasons separable on the wire.
// Collapsing them would tell the user "no reading" without saying whether waiting
// fixes it.
func TestZ_ImmatureIsDistinctFromDegenerate(t *testing.T) {
	_, state, ok := Compute(repeatVarying(5)).Z(1.0, 30)
	if ok {
		t.Fatal("immature baseline emitted a score")
	}
	if state != BaselineImmature {
		t.Errorf("state = %q, want %q", state, BaselineImmature)
	}
}

// TestZ_SignIsPreserved locks the ordering property: expansion and contraction land at
// opposite ends. Folding them into a magnitude puts a collapse and a build-up at the
// same rank, which is what the removed Math.abs was doing.
func TestZ_SignIsPreserved(t *testing.T) {
	b := Compute(repeatVarying(60))

	up, _, ok := b.Z(b.Median+10*b.MAD, 30)
	if !ok {
		t.Fatal("expansion produced no score")
	}
	down, _, ok := b.Z(b.Median-10*b.MAD, 30)
	if !ok {
		t.Fatal("contraction produced no score")
	}
	if up <= 0 || down >= 0 {
		t.Fatalf("sign lost: up=%v down=%v", up, down)
	}
	if math.Abs(up+down) > 1e-9 {
		t.Errorf("symmetric deviations gave asymmetric scores: up=%v down=%v", up, down)
	}
}

// TestI7_SyntheticRampDoesNotInflateFiringRate is integrity check I7, a merge gate.
//
// A newly listed symbol's open interest ramps monotonically from near zero. On such a
// series the median and the MAD measure the TREND rather than the noise, and a
// difference-based score inflates systematically — the panel would fill its top rows
// with symbols that are merely young.
//
// The measured contrast that motivated this gate: on a 500x ramp the simple
// difference fires on 1.47% of bars against 0.12% for the stationary control, while
// the logarithmic increment fires on 0.12% — indistinguishable from the control.
//
// The criterion is the one in the spec: on a ramping series the firing rate must not
// exceed the stationary control's.
func TestI7_SyntheticRampDoesNotInflateFiringRate(t *testing.T) {
	const (
		seed      = 20260808 // declared seed — a campaign here was once invalidated for lacking one
		bars      = 2000
		window    = 288 // the declared baseline length
		nMin      = 30
		threshold = 3.0
		growth    = 500.0
	)

	stationary := stationarySeries(seed, bars)
	ramp := rampSeries(seed, bars, growth)

	stationaryRate := firingRate(t, stationary, window, nMin, threshold)
	rampRate := firingRate(t, ramp, window, nMin, threshold)

	t.Logf("firing rate — stationary %.4f%%, ramp %.4f%%", stationaryRate*100, rampRate*100)

	if rampRate > stationaryRate {
		t.Fatalf("ramp inflates the firing rate: ramp %.4f%% > stationary %.4f%%",
			rampRate*100, stationaryRate*100)
	}
}

// firingRate scores every point of series against a strictly trailing window of its
// own history and returns the fraction whose |z| exceeds threshold.
func firingRate(t *testing.T, series []float64, window, nMin int, threshold float64) float64 {
	t.Helper()

	increments := make([]float64, 0, len(series))
	for i := 1; i < len(series); i++ {
		inc, ok := LogIncrement(series[i], series[i-1])
		if !ok {
			t.Fatalf("increment %d not takeable on a positive series", i)
		}
		increments = append(increments, inc)
	}

	var scored, fired int
	for i := window; i < len(increments); i++ {
		// Strictly trailing: the point being scored is NOT in its own baseline.
		b := Compute(increments[i-window : i])
		z, _, ok := b.Z(increments[i], nMin)
		if !ok {
			continue
		}
		scored++
		if math.Abs(z) > threshold {
			fired++
		}
	}
	if scored == 0 {
		t.Fatal("no point could be scored")
	}
	return float64(fired) / float64(scored)
}

// stationarySeries is the control: multiplicative noise with no drift.
func stationarySeries(seed int64, n int) []float64 {
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // G404: deterministic test sampling, not cryptographic randomness.
	out := make([]float64, n)
	level := 1000.0
	for i := range out {
		level *= math.Exp(rng.NormFloat64() * 0.01)
		out[i] = level
	}
	return out
}

// rampSeries is the pathology: the same noise on top of monotonic growth of `growth`
// times across the whole series — a newly listed symbol filling up.
func rampSeries(seed int64, n int, growth float64) []float64 {
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // G404: deterministic test sampling, not cryptographic randomness.
	out := make([]float64, n)
	drift := math.Log(growth) / float64(n)
	level := 1000.0
	for i := range out {
		level *= math.Exp(drift + rng.NormFloat64()*0.01)
		out[i] = level
	}
	return out
}

func constant(n int, v float64) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = v
	}
	return out
}

func repeatVarying(n int) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = float64(i%7) - 3.0
	}
	return out
}

// majorityTied builds a sample where `tied` of `n` observations share one value, so
// the median lands on the tie and the MAD collapses even though n is large. This is
// the case a floor on sample size does NOT catch.
func majorityTied(n, tied int) []float64 {
	out := make([]float64, 0, n)
	for i := 0; i < tied; i++ {
		out = append(out, 5.0)
	}
	for i := tied; i < n; i++ {
		out = append(out, 5.0+float64(i))
	}
	return out
}
