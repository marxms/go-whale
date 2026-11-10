// Package derivatives implements the derivatives domain and the robust measurement
// machinery the panel ranks on: a logarithmic increment, and a robust z-score of that
// increment against the symbol's own trailing history.
//
// # WHY A ROBUST SCORE AND NOT A FIXED THRESHOLD
//
// A fixed rule like |ΔOI| > 1% fires on 0.00% of bars for some symbols and 65.41%
// for others — it is chronically on for a few names and chronically off for the rest,
// so it carries no information in either. Measured on the long/short ratio, a large
// share of the dispersion sits BETWEEN symbols rather than within them (ICC 0.74-0.80
// once the within-symbol autocorrelation is corrected for), which is why the ruler has
// to be built from each symbol's own history rather than shared across the fleet.
//
// That component is NOT established as permanent: the measured ICC falls monotonically
// as the window widens and had not settled at ~40 days, so the ruler is recomputed on a
// trailing window rather than fixed once per symbol.
//
// # WHY THE MEDIAN AND THE MAD, AND NOT SOMETHING BETTER
//
// The MAD is the least efficient member of the standard robust family (37% Gaussian
// efficiency against 58% for Sn and 82% for Qn). Swapping it was investigated and
// REFUTED by reproduction: all three implode to zero at the same tie threshold — 53%
// of ties at n=30, 52% at n=50, 51% at n=100 — so the swap costs more and fixes
// nothing in the failure mode that actually occurs here. The efficiency figures are
// derived under normality and do not transfer to a distribution with point mass, so
// they cannot rank the substitutes either.
//
// # WHY THE LOGARITHM
//
// The simple difference produces the SAME table (Spearman 0.993-0.999; paired block
// bootstrap p = 0.514, a verified tie). The logarithm is adopted for two properties
// the difference lacks: it is scale-invariant, and it is ADDITIVE across the
// timeframe grid. Aggregating twelve 5m increments against the direct 1h increment
// deviates by 4.6e-16 under the logarithm and by up to 2.5e-4 under percentage — so
// only under the logarithm is the coarse column exactly the sum of its constituents,
// which is what makes the timeframe menu a zoom of one object instead of four
// unrelated objects.
package derivatives

import (
	"math"
	"slices"
)

// MADScale makes the MAD a consistent estimator of the standard deviation under
// normality, so a score stays readable as "how many deviations".
const MADScale = 1.4826

// BaselineState names why a baseline can or cannot produce a score. The states are
// distinct on purpose: "not enough history yet" and "this symbol does not move" are
// different facts about the world and the surface must not collapse them.
type BaselineState string

const (
	// BaselineMature — enough non-degenerate history to emit a score.
	BaselineMature BaselineState = "mature"
	// BaselineImmature — fewer observations than the declared minimum.
	BaselineImmature BaselineState = "immature"
	// BaselineDegenerate — the robust scale is zero because more than half the
	// observations equal the median. No score is emitted; a floor would manufacture
	// one out of nothing, which is the same defect as substituting a missing input
	// with a neutral value.
	BaselineDegenerate BaselineState = "degenerate"
)

// Baseline is the persisted ruler for one (symbol, timeframe).
//
// SpanMs and N are both carried because they answer different questions: N says
// whether the sample is large enough, SpanMs says how far back it actually reaches.
// A sample can be large and discontinuous, and that is exactly the case a resurrected
// series produces — full N over a window with holes, whose MAD is biased low because
// the extremes are lost before the central values are.
type Baseline struct {
	Median    float64 `json:"median"`
	MAD       float64 `json:"mad"`
	N         int     `json:"n"`
	SpanMs    int64   `json:"spanMs,omitempty"`
	UpdatedAt int64   `json:"updatedAt,omitempty"`
}

// LogIncrement returns ln(curr/prev), the increment the panel ranks on.
//
// ok is false when either value is non-positive, which the logarithm cannot take.
// Open interest of zero is a real state — a symbol with no open contracts — so this
// is a normal branch, not an error path.
func LogIncrement(curr, prev float64) (increment float64, ok bool) {
	if curr <= 0 || prev <= 0 || math.IsNaN(curr) || math.IsNaN(prev) {
		return 0, false
	}
	return math.Log(curr) - math.Log(prev), true
}

// Median returns the median of values without mutating the caller's slice.
//
// The clone is not defensive politeness: the caller's slice is the trailing baseline
// window, and sorting it in place would silently destroy the chronological order that
// the span and continuity checks depend on.
func Median(values []float64) (median float64, ok bool) {
	if len(values) == 0 {
		return 0, false
	}
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	return medianOfSorted(sorted), true
}

func medianOfSorted(sorted []float64) float64 {
	n := len(sorted)
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2.0
}

// MAD returns the median absolute deviation of values around median, unscaled.
func MAD(values []float64, median float64) (mad float64, ok bool) {
	if len(values) == 0 {
		return 0, false
	}
	deviations := make([]float64, len(values))
	for i, v := range values {
		deviations[i] = math.Abs(v - median)
	}
	slices.Sort(deviations)
	return medianOfSorted(deviations), true
}

// Compute builds a baseline from a trailing sample.
//
// The caller is responsible for passing a STRICTLY TRAILING window that excludes the
// point being scored. Including it lets the observation shift its own ruler, which
// shrinks every score toward zero exactly when the observation is extreme — the
// scores would be smallest when the event is largest.
func Compute(values []float64) Baseline {
	b := Baseline{N: len(values)}
	if len(values) == 0 {
		return b
	}
	median, _ := Median(values)
	mad, _ := MAD(values, median)
	b.Median = median
	b.MAD = mad
	return b
}

// State reports whether the baseline can produce a score, and why not when it cannot.
func (b Baseline) State(nMin int) BaselineState {
	if b.N < nMin {
		return BaselineImmature
	}
	if b.MAD <= 0 {
		return BaselineDegenerate
	}
	return BaselineMature
}

// Z returns the robust z-score of x against the baseline, WITH SIGN.
//
// The sign is load-bearing: expansion and contraction are opposite readings, and
// folding them into a magnitude puts a collapse and a build-up at the same place in
// the ordering. ok is false whenever the baseline cannot support a score, and the
// caller must then report the state rather than substituting a value.
func (b Baseline) Z(x float64, nMin int) (z float64, state BaselineState, ok bool) {
	state = b.State(nMin)
	if state != BaselineMature {
		return 0, state, false
	}
	scale := MADScale * b.MAD
	if scale <= 0 {
		// Unreachable while State reports mature, but a division guarded only by an
		// invariant elsewhere is a division waiting for that invariant to move.
		return 0, BaselineDegenerate, false
	}
	return (x - b.Median) / scale, BaselineMature, true
}
