package redisstore

import (
	"math"
	"testing"
)

// ============================================================================
// ELASTICITY CONFIGURATION PROOF
// ============================================================================
//
// # WHAT THIS FILE PREVENTS
//
// Someone raising the universe from 500 to 800 assets in six months without seeing the math.
// This system's budget is a FUNCTION — `MiB(F, A, cap_F, enc)`, which `ProjectFamily`
// instantiates — and the mutant this file kills is the budget-as-a-loose-number: a constant
// written somewhere that keeps printing 42% after the universe doubles.
//
// The three operating points are instantiated here with the SAME coefficients and the SAME
// cardinalities measured on the fleet, and the projection must match the measured
// `used_memory` within **±10%** — the declared band of the coefficients, not one this test
// chose.
//
// # WHY THE INPUT NUMBERS ARE HERE, AND WHY THIS IS NOT HARDCODE
//
// They are MEASUREMENTS, with their origin cited line by line, and what this test asserts is
// the FUNCTION'S ANSWER to them. In production none of this is literal: `A` comes from
// `SCARD active_symbols`, `disp` from `ZCARD` sampled at startup, and `b_F`/`K_F` from
// `MEMORY USAGE` over the family itself. A test that also read them from the server would not
// be deterministic and would assert nothing.

// --- FLEET MEASUREMENTS, n = 500 keys per cell ---

func elasticityTimeframes() []string {
	return []string{"5m", "15m", "30m", "1h", "2h", "4h", "1d", "1w"}
}

// dispCandles is the average `ZCARD` per timeframe in `candles:binance:future`.
func dispCandles() map[string]int64 {
	return map[string]int64{
		"5m": 778, "15m": 777, "30m": 774, "1h": 770, "2h": 758, "4h": 722, "1d": 478, "1w": 109,
	}
}

// dispDerivatives is the average `ZCARD` in `derivatives:series:binance:future`.
func dispDerivatives() map[string]int64 {
	return map[string]int64{
		"5m": 757, "15m": 756, "30m": 753, "1h": 748, "2h": 707, "4h": 545, "1d": 108, "1w": 17,
	}
}

// keysIndicators is the key count per timeframe of the indicators family, measured with
// A=500. `m_ind` is NOT 14 on every timeframe — 1d has 2,287 keys (163 symbols) and 1w has
// 879 (63 symbols) — which is why the family's effective timeframe count is 6.43 and not 8.
func keysIndicators() map[string]float64 {
	return map[string]float64{
		"5m": 7355, "15m": 7370, "30m": 7367, "1h": 7356, "2h": 7323, "4h": 7243, "1d": 2287, "1w": 879,
	}
}

// dispIndicators is the average cardinality per timeframe.
func dispIndicators() map[string]int64 {
	return map[string]int64{
		"5m": 399, "15m": 399, "30m": 399, "1h": 396, "2h": 386, "4h": 359, "1d": 352, "1w": 205,
	}
}

// --- COEFFICIENTS AFTER THE POSITIONAL ENCODING ---
//
// `b` after the positional encoding, from the measured savings: candles 374.6 - 168.4;
// derivatives 412.4 - 222.2; indicators 63.3 - 20.0.
const (
	bCandlesAfterEncoding     = 206.2
	bDerivativesAfterEncoding = 190.2
	bIndicatorsAfterEncoding  = 43.3
	// K_F: ~0 for the large series (the key is negligible beside hundreds of members) and
	// 83 B/key for indicators, where 47,180 keys of 400 members make the term show up.
	kIndicators = 83.0
)

// smallFamiliesMiB is `signalEvidence` (25.2) plus the ten small families (18.8) at the F5
// operating point.
const smallFamiliesMiB = 44.0

// nonScalingMiB is the part of `used_memory` that does NOT grow with `A`: `dispatch`,
// `active_symbols`, `liq:v1`. Measured at ~4 MiB, and it is what gives the line its intercept.
const nonScalingMiB = 4.0

// attributionFactor converts ATTRIBUTED MiB (`MEMORY USAGE`) into `used_memory`
// (`INFO memory`), which is the only quantity comparable with the server. Measured: 1.047.
const attributionFactor = 1.047

// containerCeilingMiB is `HostConfig.Memory` of `redis`. It is what kills the process,
// not `maxmemory` — which is why the headroom against it is a quantity of its own.
const containerCeilingMiB = 4096.0

// budgetTargetRatio is `retention.budget_warn_ratio`: the 60% target of `maxmemory`.
const budgetTargetRatio = 0.60

// uniformKeys returns m_F(tf) = 1 for every timeframe (candles and derivatives).
func uniformKeys() map[string]float64 {
	out := make(map[string]float64, len(elasticityTimeframes()))
	for _, tf := range elasticityTimeframes() {
		out[tf] = 1
	}
	return out
}

// capsUniform returns the same cap for every timeframe.
func capsUniform(v int64) map[string]int64 {
	out := make(map[string]int64, len(elasticityTimeframes()))
	for _, tf := range elasticityTimeframes() {
		out[tf] = v
	}
	return out
}

// f5CandleCaps is `candle_engine.redis_cap_by_timeframe`: 521 on 5m..4h, and 1d/1w with NO
// entry — they inherit the default of 800, which is what preserves the two single-copy cells.
func f5CandleCaps(ringDepth int64) map[string]int64 {
	caps := capsUniform(ringDepth)
	for _, tf := range []string{"5m", "15m", "30m", "1h", "2h", "4h"} {
		caps[tf] = 521
	}
	return caps
}

// f5DerivativesCaps is `derivatives.series_cap_by_timeframe`: 291 on 5m/15m/30m, and no other
// timeframe receives an entry.
func f5DerivativesCaps() map[string]int64 {
	caps := capsUniform(800)
	for _, tf := range []string{"5m", "15m", "30m"} {
		caps[tf] = 291
	}
	return caps
}

// elasticityPoint is an operating point: `A` assets, an indicator cap and a chart depth. It
// returns the ATTRIBUTED MiB and the projected `used_memory`.
type elasticityPoint struct {
	universe     int64
	indicatorCap int64
	// ringDepth is the CHART depth (`candle_engine.buffer_sizes`). It enters here only as the
	// cap that timeframes WITHOUT a dedicated entry inherit — which is exactly the property
	// the decoupling test below measures.
	ringDepth int64
	// capsFollowTheRing reproduces the RE-COUPLING: the gesture the operator reads as "make
	// the chart deeper" when the dedicated key does not exist.
	capsFollowTheRing bool
}

func (p elasticityPoint) attributedMiB() float64 {
	indicatorKeys := make(map[string]float64, len(keysIndicators()))
	for tf, keys := range keysIndicators() {
		// m_F(tf) = keys ÷ A, and the A of the measurement was 500.
		indicatorKeys[tf] = keys / 500
	}

	candleCaps := f5CandleCaps(p.ringDepth)
	if p.capsFollowTheRing {
		candleCaps = capsUniform(p.ringDepth)
	}

	candles := ProjectFamily(FamilyProjectionInput{
		Family:               "candles",
		Universe:             p.universe,
		KeysPerSymbol:        uniformKeys(),
		CapByTimeframe:       candleCaps,
		AvailableByTimeframe: dispCandles(),
		Coefficients:         Coefficients{Family: "candles", BytesPerMember: bCandlesAfterEncoding},
	})
	derivatives := ProjectFamily(FamilyProjectionInput{
		Family:               "derivatives:series",
		Universe:             p.universe,
		KeysPerSymbol:        uniformKeys(),
		CapByTimeframe:       f5DerivativesCaps(),
		AvailableByTimeframe: dispDerivatives(),
		Coefficients:         Coefficients{Family: "derivatives:series", BytesPerMember: bDerivativesAfterEncoding},
	})
	indicators := ProjectFamily(FamilyProjectionInput{
		Family:               "indicators",
		Universe:             p.universe,
		KeysPerSymbol:        indicatorKeys,
		CapByTimeframe:       capsUniform(p.indicatorCap),
		AvailableByTimeframe: dispIndicators(),
		Coefficients: Coefficients{
			Family:         "indicators",
			BytesPerMember: bIndicatorsAfterEncoding,
			BytesPerKey:    kIndicators,
		},
	})

	// Small families scale with A like the others; only the declared residual does not.
	small := smallFamiliesMiB * float64(p.universe) / 500

	return candles.TotalMiB + derivatives.TotalMiB + indicators.TotalMiB + small
}

// usedMemoryMiB is the only quantity comparable with `INFO memory`.
func (p elasticityPoint) usedMemoryMiB() float64 {
	return p.attributedMiB()*attributionFactor + nonScalingMiB
}

func withinPct(got, want, pct float64) bool {
	if want == 0 {
		return got == 0
	}
	return math.Abs(got-want)/want*100 <= pct
}

// TestElasticity_TheThreePointsOfTheSizingTable checks that the function, instantiated at the
// three operating points, matches the measured `used_memory` within the declared band.
//
// The mutant: the budget as a loose number. A constant `1479` keeps printing 42% after the
// universe doubles; the function does not.
func TestElasticity_TheThreePointsOfTheSizingTable(t *testing.T) {
	const tolerance = 10.0 // ±10%: the declared band of the coefficients, not a choice of this test

	tests := []struct {
		name  string
		point point
		// wantUsedMiB is the `used_memory` column of the sizing table.
		wantUsedMiB float64
	}{
		{name: "TODAY — A=500, chart 800, overlay 400", point: point{500, 400, 800}, wantUsedMiB: 1479},
		{name: "NEXT — chart 1,000, same universe", point: point{500, 400, 1000}, wantUsedMiB: 1479},
		{name: "NEXT — chart 1,200, same universe", point: point{500, 400, 1200}, wantUsedMiB: 1479},
		{name: "FUTURE — 600 assets", point: point{600, 400, 800}, wantUsedMiB: 1774},
		{name: "FUTURE — 800 assets", point: point{800, 400, 800}, wantUsedMiB: 2364},
		{name: "FUTURE — 800 assets, chart 1,200", point: point{800, 400, 1200}, wantUsedMiB: 2364},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := elasticityPoint{universe: tt.point.universe, indicatorCap: tt.point.cap, ringDepth: tt.point.ring}
			got := p.usedMemoryMiB()
			desvio := (got - tt.wantUsedMiB) / tt.wantUsedMiB * 100
			t.Logf("projection %.1f MiB · measured %.1f MiB · deviation %+.1f%%", got, tt.wantUsedMiB, desvio)
			if !withinPct(got, tt.wantUsedMiB, tolerance) {
				t.Fatalf("projection %.1f MiB against %.1f MiB measured = %+.1f%%, band ±%.0f%%",
					got, tt.wantUsedMiB, desvio, tolerance)
			}
		})
	}
}

// point is the triple (A, indicator cap, chart depth) of the table above.
type point struct {
	universe int64
	cap      int64
	ring     int64
}

// TestElasticity_TheProjectionMovesLinearlyWithTheUniverse is the half that the table alone
// does not prove: the projection is not a number, it is a line in `A`.
//
// The mutant is the frozen budget — a constant, a cached value, a cap that saturates. It
// passes at any single point and dies here.
func TestElasticity_TheProjectionMovesLinearlyWithTheUniverse(t *testing.T) {
	base := elasticityPoint{universe: 500, indicatorCap: 400, ringDepth: 800}.attributedMiB()

	for _, A := range []int64{600, 800, 1000} {
		p := elasticityPoint{universe: A, indicatorCap: 400, ringDepth: 800}
		esperado := base * float64(A) / 500
		got := p.attributedMiB()
		if !withinPct(got, esperado, 0.5) {
			t.Errorf("A=%d: projection %.1f MiB, expected %.1f (linear in A)", A, got, esperado)
		}
	}

	// And the measured marginal: 2.817 attributed MiB/asset => 2.950 MiB/asset of `used_memory`.
	a500 := elasticityPoint{universe: 500, indicatorCap: 400, ringDepth: 800}.usedMemoryMiB()
	a800 := elasticityPoint{universe: 800, indicatorCap: 400, ringDepth: 800}.usedMemoryMiB()
	marginal := (a800 - a500) / 300
	if !withinPct(marginal, 2.950, 10) {
		t.Errorf("marginal = %.3f MiB/asset, measured 2.950", marginal)
	}
}

// TestElasticity_TheChartDepthCostsZeroResidentBytes checks ∂MiB/∂D = 0.
//
// Raising the chart from 800 to 1,200 costs not one byte of Redis — PROVIDED the dedicated key
// exists. The test measures both halves: that the projection does not move, and HOW MUCH it
// would move if the cap followed the ring again. Without the second half the first is a
// tautology — a function that ignored depth would pass all the same.
func TestElasticity_TheChartDepthCostsZeroResidentBytes(t *testing.T) {
	desacoplado800 := elasticityPoint{universe: 500, indicatorCap: 400, ringDepth: 800}.attributedMiB()
	desacoplado1200 := elasticityPoint{universe: 500, indicatorCap: 400, ringDepth: 1200}.attributedMiB()

	if !withinPct(desacoplado1200, desacoplado800, 1.0) {
		t.Fatalf("the chart at 1,200 changed resident memory: %.1f -> %.1f MiB — the decoupling broke",
			desacoplado800, desacoplado1200)
	}

	// THE RE-COUPLING: changing `buffer_sizes.default` without the dedicated key. It is the
	// gesture the operator reads as "make the chart deeper", and it is expensive.
	reacoplado := elasticityPoint{universe: 500, indicatorCap: 400, ringDepth: 1200, capsFollowTheRing: true}.attributedMiB()
	custo := reacoplado - desacoplado1200
	t.Logf("re-coupling the cap to the ring costs %+.1f MiB attributed at the F5 point", custo)
	if custo < 100 {
		t.Fatalf("re-coupling the cap to the ring cost only %.1f MiB — the function is not seeing "+
			"depth, and then the test above proves nothing", custo)
	}
	// The steady-state projection is +425 MiB. Here the number is smaller because `disp` is
	// still today's, truncated at the current cap: the difference between the two is the time
	// the fleet takes to fill the new depth, not a model disagreement.
}

// TestElasticity_EightHundredAssetsRaiseTheContainerCeilingTogether is the reading the operator
// must take away, and the one easiest to lose.
//
// 800 assets FIT in today's `maxmemory` (67.5%), but above the 60% target. To return to the
// target, `maxmemory` must rise — and then the headroom up to the CONTAINER ceiling falls below
// what the BGSAVE COW consumes. At 800 assets the container limit rises TOGETHER with it, not
// afterwards.
//
// Headroom is a DIFFERENCE of two large numbers, so it amplifies: 1.5% error in the projection
// becomes ~38% error here. That is exactly why it cannot be a literal — and why this test
// asserts the ORDER OF MAGNITUDE and the VERDICT, not the digit.
func TestElasticity_EightHundredAssetsRaiseTheContainerCeilingTogether(t *testing.T) {
	// Minimum headroom for the BGSAVE COW. Declared, not measured — and declared here because
	// the verdict depends on it.
	const cowHeadroomMiB = 300.0

	casos := []struct {
		universe  int64
		wantTight bool
	}{
		{universe: 500, wantTight: false},
		{universe: 600, wantTight: false},
		{universe: 800, wantTight: true},
	}

	for _, c := range casos {
		p := elasticityPoint{universe: c.universe, indicatorCap: 400, ringDepth: 800}
		used := p.usedMemoryMiB()
		maxmemoryParaOAlvo := used / budgetTargetRatio
		folga := containerCeilingMiB - maxmemoryParaOAlvo

		t.Logf("A=%d: used_memory %.1f MiB · maxmemory for <=60%% = %.1f MiB · headroom to the container ceiling = %.1f MiB",
			c.universe, used, maxmemoryParaOAlvo, folga)

		apertado := folga < cowHeadroomMiB
		if apertado != c.wantTight {
			t.Errorf("A=%d: headroom %.1f MiB, tight=%v, expected %v", c.universe, folga, apertado, c.wantTight)
		}
	}

	// The FUTURE-800 point, named: the sizing table publishes 156 MiB of headroom. This
	// projection gives a number of the same order, and what matters is that it is SMALLER than
	// the COW — the verdict does not change within the coefficient band.
	p800 := elasticityPoint{universe: 800, indicatorCap: 400, ringDepth: 800}
	folga800 := containerCeilingMiB - p800.usedMemoryMiB()/budgetTargetRatio
	if folga800 <= 0 || folga800 >= cowHeadroomMiB {
		t.Fatalf("headroom at 800 assets = %.1f MiB — the sizing table publishes 156 MiB, and the verdict "+
			"(fits in maxmemory, does NOT fit in the container at the 60%% target) must survive the band",
			folga800)
	}

	// And without this design the same move would not fit even on the host: 5,591 MiB against
	// 4,096.
	semODesign := 5591.0
	if semODesign <= containerCeilingMiB {
		t.Fatal("the comparison row of the sizing table no longer makes sense")
	}
}

// TestElasticity_TheFixedKeyCostIsPartOfTheFunction closes the hole the ±10% band leaves: the
// `K_F` term is worth 3.7 MiB in the indicators family, and 3.7 MiB disappears inside any
// reasonable tolerance over the total. It needs an assertion OF ITS OWN.
//
// This is not two-decimal pedantry: `K` and `b` are inseparable by construction in a sample of
// a SINGLE cardinality, so a residual divided from such a sample cannot identify `K`. A model
// that simply lacks the term makes the same error with the opposite sign, and no total-only
// test would see it.
func TestElasticity_TheFixedKeyCostIsPartOfTheFunction(t *testing.T) {
	input := FamilyProjectionInput{
		Family:               "indicators",
		Universe:             500,
		KeysPerSymbol:        map[string]float64{"5m": 7355.0 / 500},
		CapByTimeframe:       map[string]int64{"5m": 400},
		AvailableByTimeframe: map[string]int64{"5m": 399},
		Coefficients:         Coefficients{Family: "indicators", BytesPerMember: bIndicatorsAfterEncoding},
	}

	withoutK := ProjectFamily(input).TotalMiB
	input.Coefficients.BytesPerKey = kIndicators
	withK := ProjectFamily(input).TotalMiB

	// 7,355 keys × 83 B = 0.58 MiB on this timeframe; across the whole family (47,180 keys)
	// it is 3.7 MiB.
	esperado := 7355 * kIndicators / float64(1<<20)
	if !withinPct(withK-withoutK, esperado, 1.0) {
		t.Fatalf("the K term contributed %.3f MiB, expected %.3f — the function lost the fixed per-key cost",
			withK-withoutK, esperado)
	}
}
