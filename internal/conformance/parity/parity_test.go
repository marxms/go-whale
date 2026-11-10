package parity

import (
	"strings"
	"testing"
)

// bar builds one bucket. Prices are held constant across the fixtures so a flow assertion
// never fails for a price reason and vice versa.
func bar(ts int64, volume, takerBuy float64) Candle {
	return Candle{
		Timestamp: ts, Open: 100, High: 101, Low: 99, Close: 100.5,
		Volume: volume, TakerBuyVolume: takerBuy,
		// Fixtures declare LIVE provenance: the gate now admits only buckets we assembled from
		// the tape, so a fixture without a source would be excluded and every test would compare
		// nothing while appearing to pass.
		Source: "ws_delta",
		// Fixtures carry a sell leg that closes the identity, so a test that means to move ONE
		// quantity moves exactly one. A fixture left at zero on both sides would compare 0 to 0
		// and let the sell leg pass vacuously.
		TakerSellVolume: volume - takerBuy,
	}
}

const step = int64(300_000) // 5m

func series(vols ...float64) []Candle {
	out := make([]Candle, len(vols))
	for i, v := range vols {
		out[i] = bar(1_700_000_000_000+int64(i)*step, v, v*0.6)
	}
	return out
}

func TestCompare_IdenticalCandlesPass(t *testing.T) {
	t.Parallel()

	c := series(100, 110, 120, 130)
	rep := Compare("BTC/USDT", "5m", c, c)

	if !rep.Pass() {
		t.Fatalf("identical inputs must pass: %v", rep.Failures())
	}
	if rep.Compared != 4 {
		t.Errorf("compared = %d, want 4", rep.Compared)
	}
}

// TestCompare_SummationNoiseIsNotADivergence guards the floor against being so tight that
// float summation order alone puts bars into the pairing analysis. Our volume is accumulated
// per trade while Binance computes theirs independently, so the last bits legitimately differ.
func TestCompare_SummationNoiseIsNotADivergence(t *testing.T) {
	t.Parallel()

	theirs := series(1234.5678, 2345.6789, 3456.789, 4567.891)
	ours := make([]Candle, len(theirs))
	for i, c := range theirs {
		c.Volume *= 1 + 1e-12
		c.TakerBuyVolume *= 1 - 1e-12
		ours[i] = c
	}

	rep := Compare("BTC/USDT", "5m", ours, theirs)
	if !rep.Pass() {
		t.Fatalf("1e-12 relative noise must not fail G1: %v", rep.Failures())
	}
	for _, f := range rep.Flow {
		if len(f.Divergent) != 0 {
			t.Errorf("%s: float noise must not even count as divergent, got %d", f.Field, len(f.Divergent))
		}
	}
}

// TestCompare_AggTradeStraddlePasses is the whole reason the criterion changed.
//
// A group of fills timestamped as ONE aggTrade crosses the boundary between bucket 1 and
// bucket 2. Binance splits it; we cannot, so bucket 1 keeps the lot. Bucket 1 is over by
// exactly what bucket 2 is under. The window balances, and there is no defect anywhere.
//
// Under the old per-bar rule this was two mismatches and a permanent red gate.
//
// The straddle is ONE-SIDED, and the fixture says so: an aggTrade is an aggregation of fills
// against a single resting side, so a buy one displaces volume and the BUY leg and leaves the
// sell leg alone. That asymmetry is the assertion — the untouched leg must report zero divergent
// bars, which is what proves the gate reports movement it observed instead of smearing one
// field's divergence across the others.
func TestCompare_AggTradeStraddlePasses(t *testing.T) {
	t.Parallel()

	theirs := series(1000, 1000, 1000, 1000, 1000)
	ours := series(1000, 1000, 1000, 1000, 1000)
	const straddle = 7.5 // 0,75% of a bucket — orders of magnitude above BarNoiseFloor
	ours[1].Volume += straddle
	ours[2].Volume -= straddle
	ours[1].TakerBuyVolume += straddle
	ours[2].TakerBuyVolume -= straddle

	rep := Compare("BTC/USDT", "5m", ours, theirs)
	if !rep.Pass() {
		t.Fatalf("an adjacent, opposite-signed, canceling pair is the aggTrade straddle and MUST "+
			"pass — this is the criterion that reproved a healthy system forever: %v", rep.Failures())
	}
	moved := map[string]bool{"volume": true, "takerBuyVolume": true}
	for _, f := range rep.Flow {
		want := 0
		if moved[f.Field] {
			want = 2
		}
		if len(f.Divergent) != want {
			t.Errorf("%s: expected %d divergent bars, got %d", f.Field, want, len(f.Divergent))
		}
		if len(f.Unpaired) != 0 {
			t.Errorf("%s: the straddle must pair, got %d unpaired", f.Field, len(f.Unpaired))
		}
	}
}

// TestCompare_IsolatedBarFails is the other side: a bar that is simply wrong has no partner,
// so it is exactly what this gate is for.
func TestCompare_IsolatedBarFails(t *testing.T) {
	t.Parallel()

	theirs := series(1000, 1000, 1000, 1000, 1000)
	ours := series(1000, 1000, 1000, 1000, 1000)
	ours[2].Volume -= 7.5 // lost, not moved

	rep := Compare("BTC/USDT", "5m", ours, theirs)
	if rep.Pass() {
		t.Fatal("a lonely divergent bar must fail G1 — it is not a straddle, the volume went nowhere")
	}
	var vol FlowReport
	for _, f := range rep.Flow {
		if f.Field == "volume" {
			vol = f
		}
	}
	if len(vol.Unpaired) != 1 || vol.Unpaired[0].Timestamp != ours[2].Timestamp {
		t.Fatalf("expected exactly the middle bar reported unpaired, got %+v", vol.Unpaired)
	}
}

// TestCompare_OppositeSignsThatDoNotCancelStillFail closes the obvious hole in "opposite sign
// adjacent bar excuses it": a genuinely broken bar sitting next to an unrelated speck would
// otherwise be waved through by the speck.
func TestCompare_OppositeSignsThatDoNotCancelStillFail(t *testing.T) {
	t.Parallel()

	theirs := series(1000, 1000, 1000, 1000, 1000)
	ours := series(1000, 1000, 1000, 1000, 1000)
	ours[2].Volume -= 50  // broken
	ours[3].Volume += 0.5 // unrelated noise, opposite sign, nowhere near canceling

	rep := Compare("BTC/USDT", "5m", ours, theirs)
	if rep.Pass() {
		t.Fatal("an adjacent opposite sign that does NOT cancel is not a straddle and must not excuse the bar")
	}
}

// TestCompare_SystematicDeficitFailsTheWindowSum pins the criterion against the defect this
// change set was opened to remove: a per-bar loss small enough that no single bar looks
// dramatic, but which never comes back.
func TestCompare_SystematicDeficitFailsTheWindowSum(t *testing.T) {
	t.Parallel()

	theirs := series(1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000)
	ours := make([]Candle, len(theirs))
	for i, c := range theirs {
		c.Volume *= 1 - 9e-4 // the measured 0,09% deficit
		ours[i] = c
	}

	rep := Compare("ETH/USDT", "15m", ours, theirs)
	if rep.Pass() {
		t.Fatal("a 0,09% systematic volume deficit must fail G1 — a tolerance that accepts it makes the gate decorative")
	}
	for _, f := range rep.Flow {
		if f.Field != "volume" {
			continue
		}
		if f.WindowRelError <= WindowTolerance {
			t.Fatalf("window relative error = %.3g, must exceed %.3g", f.WindowRelError, WindowTolerance)
		}
	}
}

// TestCompare_HealthyStraddleResidueStaysUnderWindowTolerance keeps the two measured
// populations apart in code, not just in a comment: the worst healthy window observed after
// the deploy was 2,7e-4 and it must pass.
func TestCompare_HealthyStraddleResidueStaysUnderWindowTolerance(t *testing.T) {
	t.Parallel()

	theirs := series(1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000)
	ours := series(1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000)
	// One boundary group left over at the far edge of the window: 2,7e-4 of the total.
	ours[len(ours)-1].Volume += 2.7e-4 * 8000

	rep := Compare("BTC/USDT", "5m", ours, theirs)
	if !rep.Pass() {
		t.Fatalf("the worst MEASURED healthy window must pass, otherwise the gate reproves production: %v", rep.Failures())
	}
}

// TestCompare_EdgeBucketIsExemptFromPairing: a straddle at the first or last boundary has its
// partner OUTSIDE the sample and can never pair. Failing on it would flake the gate on 2
// buckets out of 50 for a reason that has nothing to do with ingestion.
func TestCompare_EdgeBucketIsExemptFromPairing(t *testing.T) {
	t.Parallel()

	theirs := series(1000, 1000, 1000, 1000, 1000)
	ours := series(1000, 1000, 1000, 1000, 1000)
	ours[0].Volume += 1.0 // 0,1% — divergent, no bucket before it in the sample

	rep := Compare("BTC/USDT", "5m", ours, theirs)
	if !rep.Pass() {
		t.Fatalf("an edge bucket whose partner lies outside the window must not fail: %v", rep.Failures())
	}
	var vol FlowReport
	for _, f := range rep.Flow {
		if f.Field == "volume" {
			vol = f
		}
	}
	if vol.EdgeExempt != 1 {
		t.Errorf("the exemption must be REPORTED, not silent: edgeExempt = %d, want 1", vol.EdgeExempt)
	}
}

// TestCompare_EdgeExemptionDoesNotHideASystematicDefect proves the exemption is bounded: an
// edge bar still counts toward the window sum, so a defect that happens to sit at the edge is
// caught by the other half of the criterion.
func TestCompare_EdgeExemptionDoesNotHideASystematicDefect(t *testing.T) {
	t.Parallel()

	theirs := series(1000, 1000, 1000, 1000, 1000)
	ours := series(1000, 1000, 1000, 1000, 1000)
	ours[0].Volume -= 30 // 0,6% of the window: exempt from pairing, NOT from the sum

	if rep := Compare("BTC/USDT", "5m", ours, theirs); rep.Pass() {
		t.Fatal("the edge exemption must not become a hole: the window sum still has to catch it")
	}
}

// TestCompare_TakerSplitIsJudgedIndependently: a bucket whose total volume is right but whose
// aggressor split is wrong is a real defect, and the flow analysis runs per field so it
// cannot be masked by a correct volume.
func TestCompare_TakerSplitIsJudgedIndependently(t *testing.T) {
	t.Parallel()

	theirs := series(1000, 1000, 1000, 1000, 1000)
	ours := series(1000, 1000, 1000, 1000, 1000)
	ours[2].TakerBuyVolume += 40 // volume untouched, classification wrong

	rep := Compare("BTC/USDT", "5m", ours, theirs)
	if rep.Pass() {
		t.Fatal("a wrong taker split with a correct volume must still fail G1")
	}
}

// TestCompare_DeficitConfinedToTheSellLegFails pins the hole this gate had: while additiveFields
// listed only {volume, takerBuyVolume}, a bucket could lose flow entirely out of the sell leg and
// mirror perfectly, because the two compared quantities were both untouched.
//
// That is not a constructed edge case. It is the measured shape of the XVG divergence, and it is
// reachable only because our three flow numbers are accumulated independently — the aggregator
// classifies each side from the aggTrade maker flag and parks undeterminable trades in a third
// bucket, so volume is NOT a function of buy and sell on our side.
//
// Deleting the takerSellVolume entry from additiveFields turns this test RED. Nothing else does.
func TestCompare_DeficitConfinedToTheSellLegFails(t *testing.T) {
	t.Parallel()

	theirs := series(1000, 1000, 1000, 1000, 1000)
	ours := series(1000, 1000, 1000, 1000, 1000)
	ours[2].TakerSellVolume -= 40 // volume and the buy leg both left exactly as they were

	if ours[2].Volume != theirs[2].Volume || ours[2].TakerBuyVolume != theirs[2].TakerBuyVolume {
		t.Fatal("fixture must move ONLY the sell leg, or it proves nothing about that leg")
	}

	rep := Compare("XVG/USDT", "5m", ours, theirs)
	if rep.Pass() {
		t.Fatal("flow lost entirely from the sell leg must fail G1, not mirror perfectly")
	}
}

// TestParseKlines_DerivesTheSellLegFromTheirIdentity: Binance publishes only the buy leg, and on
// THEIR side the remainder is exact — every trade has a taker, so there is no unclassified bucket.
// Deriving it there is what makes the independent comparison against our own accumulation possible.
func TestParseKlines_DerivesTheSellLegFromTheirIdentity(t *testing.T) {
	t.Parallel()

	got, err := parseKlines([][]any{{
		float64(1_700_000_000_000), "100.1", "105.5", "99.2", "104.4", "1234.5",
		float64(1_700_000_299_999), "0", float64(0), "700.25", "0", "0",
	}})
	if err != nil {
		t.Fatalf("parseKlines: %v", err)
	}
	if want := 1234.5 - 700.25; got[0].TakerSellVolume != want {
		t.Errorf("takerSellVolume = %v, want %v", got[0].TakerSellVolume, want)
	}
}

// TestCompare_PriceIsPerBar: prices obey no conservation law, so there is nothing to sum and
// nothing to pair. A wrong candle price is a per-bar failure, full stop.
func TestCompare_PriceIsPerBar(t *testing.T) {
	t.Parallel()

	theirs := series(1000, 1000, 1000)
	ours := series(1000, 1000, 1000)
	ours[1].Close = 150 // ~50% off

	rep := Compare("BTC/USDT", "5m", ours, theirs)
	if rep.Pass() {
		t.Fatal("a wrong close must fail G1")
	}
	if len(rep.PriceMismatches) != 1 || rep.PriceMismatches[0].Field != "close" {
		t.Fatalf("expected exactly one close mismatch, got %+v", rep.PriceMismatches)
	}
}

// TestCompare_PriceTickNoiseIsTolerated: if the boundary group carried the bar's extreme, our
// high is the next print instead. A sub-basis-point displacement is the straddle, not a bug.
func TestCompare_PriceTickNoiseIsTolerated(t *testing.T) {
	t.Parallel()

	theirs := series(1000, 1000, 1000)
	ours := series(1000, 1000, 1000)
	ours[1].High *= 1 - 5e-5 // half a basis point

	if rep := Compare("BTC/USDT", "5m", ours, theirs); !rep.Pass() {
		t.Fatalf("half a basis point on an extreme is the straddle moving the print: %v", rep.Failures())
	}
}

// TestCompare_MissingBucketIsCoverageNotValue pins that an empty store cannot score a
// perfect parity. Counting a missing bucket as "nothing to compare" would let a store that
// returns nothing at all pass with zero mismatches.
func TestCompare_MissingBucketIsCoverageNotValue(t *testing.T) {
	t.Parallel()

	theirs := series(10, 20)

	rep := Compare("BTC/USDT", "5m", nil, theirs)
	if rep.Pass() {
		t.Fatal("a store holding none of the exchange buckets must not pass G1")
	}
	if rep.SkippedNotInOurs != 2 {
		t.Errorf("skippedNotInOurs = %d, want 2", rep.SkippedNotInOurs)
	}
	if len(rep.PriceMismatches) != 0 {
		t.Errorf("a missing bucket is a coverage deficit, not a value mismatch: %+v", rep.PriceMismatches)
	}
}

// TestCompare_ExtraBucketOnOurSideIsIgnored: our store legitimately holds history outside
// the sampled exchange window, and that is not a parity failure.
func TestCompare_ExtraBucketOnOurSideIsIgnored(t *testing.T) {
	t.Parallel()

	all := series(10, 20, 30)
	if rep := Compare("BTC/USDT", "5m", all, all[1:]); !rep.Pass() {
		t.Fatalf("history outside the sampled window must not fail G1: %v", rep.Failures())
	}
}

// TestCompare_UnorderedResponseStillPairsAdjacently: "adjacent" must mean adjacent in TIME,
// not adjacent in whatever order the endpoint happened to return. Compare sorts; if it ever
// stops, a straddle stops pairing and the gate goes red on a healthy system.
func TestCompare_UnorderedResponseStillPairsAdjacently(t *testing.T) {
	t.Parallel()

	theirs := series(1000, 1000, 1000, 1000, 1000)
	ours := series(1000, 1000, 1000, 1000, 1000)
	ours[1].Volume += 7.5
	ours[2].Volume -= 7.5
	// Serve both sides newest-first, which is what /api/timeseries actually does.
	reverse := func(cs []Candle) []Candle {
		out := make([]Candle, len(cs))
		for i := range cs {
			out[len(cs)-1-i] = cs[i]
		}
		return out
	}

	if rep := Compare("BTC/USDT", "5m", reverse(ours), reverse(theirs)); !rep.Pass() {
		t.Fatalf("pairing must be time-ordered, not response-ordered: %v", rep.Failures())
	}
}

func TestWithinTolerance_ZeroExpectedRequiresExactZero(t *testing.T) {
	t.Parallel()

	if _, ok := withinTolerance(0, 0, PriceTolerance); !ok {
		t.Error("0 vs 0 must pass")
	}
	if _, ok := withinTolerance(1e-12, 0, PriceTolerance); ok {
		t.Error("a non-zero value against an expected zero must fail — relative error is undefined there, and a tiny value where none exists is still a real defect")
	}
}

func TestBucketStep_InfersFromSample(t *testing.T) {
	t.Parallel()

	if got := bucketStep([]int64{0, 300_000, 600_000, 900_000}); got != 300_000 {
		t.Errorf("bucketStep = %d, want 300000", got)
	}
	if got := bucketStep([]int64{1_700_000_000_000}); got != 0 {
		t.Errorf("a single bucket cannot imply a step, got %d", got)
	}
}

// TestParseKlines_MapsBinanceLayout pins the array offsets. Reading takerBuyBase from the
// wrong index would produce a parity report that is confidently wrong.
func TestParseKlines_MapsBinanceLayout(t *testing.T) {
	t.Parallel()

	raw := [][]any{{
		float64(1700000000000), "100.1", "105.5", "99.2", "104.4", "1234.5",
		float64(1700000299999), "128000.0", float64(4200), "700.25", "72000.0", "0",
	}}

	got, err := parseKlines(raw)
	if err != nil {
		t.Fatalf("parseKlines: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 candle, got %d", len(got))
	}
	c := got[0]
	for _, tc := range []struct {
		name      string
		got, want float64
	}{
		{name: "open", got: c.Open, want: 100.1},
		{name: "high", got: c.High, want: 105.5},
		{name: "low", got: c.Low, want: 99.2},
		{name: "close", got: c.Close, want: 104.4},
		{name: "volume", got: c.Volume, want: 1234.5},
		{name: "takerBuyVolume", got: c.TakerBuyVolume, want: 700.25},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %v, want %v", tc.name, tc.got, tc.want)
		}
	}
	if c.Timestamp != 1700000000000 {
		t.Errorf("timestamp = %d, want 1700000000000", c.Timestamp)
	}
}

func TestParseKlines_RejectsShortRow(t *testing.T) {
	t.Parallel()

	if _, err := parseKlines([][]any{{float64(1), "1", "1"}}); err == nil {
		t.Fatal("a truncated kline row must error, not silently produce zeros")
	}
}

// TestSummary_DistinguishesAQuietWindowFromAWorkingPairing keeps Summary REACHABLE from the
// untagged build and asserts the one thing it exists to say.
//
// Summary is logged on PASS as well as on failure, and the reason is that "divergent=0" and
// "divergent=N unpaired=0" are different facts: the first is a window where nothing moved and
// the pairing rule was never exercised, the second is a window where bars DID diverge and every
// one of them found its opposite neighbor. Only the second is evidence that the criterion is
// alive. A summary that could not tell those apart would let a silently broken pairing read as
// a healthy run for as long as the market stayed quiet.
//
// It is also the only caller of Summary outside the integration-tagged harness. Without it the
// reachability ratchet in scripts/quality-gate.sh reports Report.Summary as unreachable from
// main() — correctly, since `deadcode -test` does not compile //go:build integration files —
// and the gate fails on a NEW entry that is not actually dead.
func TestSummary_DistinguishesAQuietWindowFromAWorkingPairing(t *testing.T) {
	t.Parallel()

	quiet := series(100, 110, 120, 130)
	quietSummary := Compare("BTC/USDT", "5m", quiet, quiet).Summary()
	if !strings.Contains(quietSummary, "divergent=0") || !strings.Contains(quietSummary, "unpaired=0") {
		t.Fatalf("a quiet window must report divergent=0 unpaired=0, got %q", quietSummary)
	}

	// The straddle: 5 units moved from bucket 1 to bucket 2, total conserved.
	theirs := series(100, 110, 120, 130)
	ours := series(100, 105, 125, 130)
	rep := Compare("BTC/USDT", "5m", ours, theirs)
	if !rep.Pass() {
		t.Fatalf("a conserved straddle must pass: %v", rep.Failures())
	}
	straddleSummary := rep.Summary()
	if strings.Contains(straddleSummary, "divergent=0") {
		t.Fatalf("the straddle DID move volume between bars; a summary claiming divergent=0 "+
			"would make a broken pairing indistinguishable from a quiet window: %q", straddleSummary)
	}
	if !strings.Contains(straddleSummary, "unpaired=0") {
		t.Fatalf("every divergent bar in a conserved straddle must find its pair, got %q", straddleSummary)
	}
}

// TestCompare_AnUntradedBucketIsNotAHole separates two absences that were being counted as one.
//
// The exchange publishes a kline for every interval whether or not anything traded; an untraded
// one carries the previous close and a volume of zero. We build candles FROM TRADES, so we create
// no bucket at all — the absence is the correct representation of "nothing happened".
//
// Counting that as missing coverage fails the gate on illiquid symbols for a reason no code change
// can fix: measured against Binance, CRWD/USDT had 3 zero-volume klines in a 40-bucket window and
// HIMS/USDT had 9, and both were reported as coverage defects while every value they carried
// matched to float noise.
//
// A bucket the exchange says HAD volume and we do not hold is still a hole, and still fails.
func TestCompare_AnUntradedBucketIsNotAHole(t *testing.T) {
	t.Parallel()

	theirs := series(1000, 1000, 1000)
	theirs = append(theirs, bar(theirs[2].Timestamp+step, 0, 0)) // untraded on the exchange
	ours := series(1000, 1000, 1000)                             // we never created that bucket

	rep := Compare("CRWD/USDT", "5m", ours, theirs)
	if rep.SkippedEmptyOnExchange != 1 {
		t.Errorf("emptyOnExchange = %d, want 1", rep.SkippedEmptyOnExchange)
	}
	if rep.SkippedNotInOurs != 0 {
		t.Errorf("missing = %d, want 0 — an untraded interval is not lost data", rep.SkippedNotInOurs)
	}
	if !rep.Pass() {
		t.Fatalf("a window whose only absence is an UNTRADED exchange bucket must PASS: %v", rep.Failures())
	}
}

// TestCompare_AnAbsentTRADEDBucketStillFails is the other half, and it is what keeps the fix above
// from being a mute. Coverage still has to fail when the exchange says volume existed.
func TestCompare_AnAbsentTRADEDBucketStillFails(t *testing.T) {
	t.Parallel()

	theirs := series(1000, 1000, 1000, 1000)
	ours := series(1000, 1000, 1000) // the 4th bucket TRADED and we do not hold it

	rep := Compare("BTC/USDT", "5m", ours, theirs)
	if rep.SkippedNotInOurs != 1 {
		t.Errorf("missing = %d, want 1", rep.SkippedNotInOurs)
	}
	if rep.Pass() {
		t.Fatal("a bucket the exchange filled with volume and we do not hold is a real hole")
	}
}

// TestCompare_ASeededBucketIsNotEvidence is the third and largest ruler correction, and the one that
// invalidated every number the activity had produced.
//
// A bucket we seeded FROM a Binance kline IS a Binance kline. Comparing it against a Binance kline
// compares Binance to itself and agrees by construction, so including it does not add noise — it
// sets the verdict. Measured on one build, one window, one gate: 4,5% failing when 17% of the
// sample was live, 61% when 64% was. A gate whose answer depends on process uptime is not
// measuring the system.
func TestCompare_ASeededBucketIsNotEvidence(t *testing.T) {
	t.Parallel()

	theirs := series(1000, 1000, 1000, 1000)
	ours := series(1000, 1000, 1000, 1000)
	// The middle bucket is seeded — and WRONG. It must not be able to pass by provenance.
	ours[1].Source = "rest_total"
	ours[1].Volume = 500

	rep := Compare("BTC/USDT", "5m", ours, theirs)
	if rep.SkippedNotSelfAssembled != 1 {
		t.Errorf("seeded = %d, want 1", rep.SkippedNotSelfAssembled)
	}
	if rep.Compared != 3 {
		t.Errorf("compared = %d, want 3 — the seeded bucket is not evidence either way", rep.Compared)
	}
}

// TestCompare_AnUnlabelledBucketIsNotAdmitted: provenance is the whole point. A bucket that cannot
// say where it came from cannot support the claim "this compares OUR arithmetic to the exchange's",
// and admitting it is how a seeded bucket slips back in.
func TestCompare_AnUnlabelledBucketIsNotAdmitted(t *testing.T) {
	t.Parallel()

	theirs := series(1000, 1000, 1000)
	ours := series(1000, 1000, 1000)
	ours[0].Source = ""

	rep := Compare("BTC/USDT", "5m", ours, theirs)
	if rep.SkippedNotSelfAssembled != 1 {
		t.Fatalf("unlabelled = %d, want 1 — unknown origin is not admitted", rep.SkippedNotSelfAssembled)
	}
}

// TestCompare_AVenueTransportedBucketIsGraded: G1 grades WHAT WE SERVE. A bucket the venue pushed
// and we transported is ours to answer for — every step between the socket and the API can break
// it — so excluding it made the gate unsatisfiable rather than strict.
func TestCompare_AVenueTransportedBucketIsGraded(t *testing.T) {
	t.Parallel()

	theirs := series(1000, 1000, 1000)
	ours := series(1000, 1000, 1000)
	for i := range ours {
		ours[i].Source = "ws_kline"
	}
	ours[1].Volume = 500 // transported wrongly: the gate must SEE it

	rep := Compare("BTC/USDT", "5m", ours, theirs)
	if rep.Compared != 3 {
		t.Fatalf("compared = %d, want 3 — excluding transported buckets makes the gate "+
			"unsatisfiable, not strict", rep.Compared)
	}
	if rep.Pass() {
		t.Fatal("a wrongly transported bucket must fail G1")
	}
}

// TestCompare_InconclusiveIsNotAPassAndNotAFailure pins the distinction that made the gate
// unreadable: 374 of 480 pairs reported as failures with nothing compared, burying the gradeable
// ones — which were nearly clean.
//
// Held-but-seeded is INCONCLUSIVE: those buckets are the venue's own kline, so grading them
// against a kline is circular. NOT HELD is a real coverage hole and still fails. Collapsing the
// two in either direction is a defect this package has already shipped once in each.
func TestCompare_InconclusiveIsNotAPassAndNotAFailure(t *testing.T) {
	t.Parallel()

	theirs := series(1000, 1000)

	seeded := series(1000, 1000)
	for i := range seeded {
		seeded[i].Source = "rest_total"
	}
	rep := Compare("BTC/USDT", "5m", seeded, theirs)
	if !rep.Inconclusive() || !rep.Pass() {
		t.Errorf("all-seeded must be INCONCLUSIVE, not a failure: inconclusive=%v pass=%v",
			rep.Inconclusive(), rep.Pass())
	}

	hole := Compare("BTC/USDT", "5m", nil, theirs)
	if hole.Inconclusive() {
		t.Error("holding NOTHING is a coverage hole, never merely inconclusive")
	}
	if hole.Pass() {
		t.Error("a store holding none of the exchange buckets must fail")
	}
}
