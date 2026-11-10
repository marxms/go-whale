package parity

import (
	"testing"
	"time"
)

const bootStep = int64(5 * 60 * 1000)

// bars builds a matched pair of series where every bucket agrees except the ones named in
// `off`, which are shifted by the given absolute amount on OURS.
func bars(base int64, n int, off map[int]float64) (ours, theirs []Candle) {
	for i := range n {
		ts := base + int64(i)*bootStep
		v := 1000.0
		o := v
		if d, ok := off[i]; ok {
			o = v + d
		}
		// Source is LIVE: the gate admits only buckets we assembled from the tape, so an
		// unlabelled fixture would be excluded and the sample would silently be empty.
		ours = append(ours, Candle{Timestamp: ts, Open: 1, High: 1, Low: 1, Close: 1, Volume: o, TakerBuyVolume: o / 2, Source: "ws_delta"})
		theirs = append(theirs, Candle{Timestamp: ts, Open: 1, High: 1, Low: 1, Close: 1, Volume: v, TakerBuyVolume: v / 2})
	}
	return ours, theirs
}

// TestBootExempt_ExcusesOnlyTheBucketTheProcessBootedIn is the positive case, and it is the
// whole reason the exemption exists.
//
// The bucket open when the process starts cannot be reconstructed exactly. The exchange's
// REST snapshot is a total up to the instant it was OBSERVED; the live stream covers from the
// instant it began DELIVERING. The seed waits for delivery before snapshotting so the two
// windows overlap, but the trades between the server-side observation and its local
// application belong to neither source. Measured in production: p50 400 ms, p99 800 ms — on a
// 300 s bucket that is 0,27%, on exactly ONE bucket per symbol per boot, with every later
// bucket matching to 0,0000%.
func TestBootExempt_ExcusesOnlyTheBucketTheProcessBootedIn(t *testing.T) {
	t.Parallel()

	base := int64(1_700_000_000_000) / bootStep * bootStep
	// Bucket 3 is short by 0,3% — the shape of the snapshot-lag residual.
	ours, theirs := bars(base, 10, map[int]float64{3: -3})
	bootAt := time.UnixMilli(base + 3*bootStep + 90_000) // mid-bucket-3

	rep := CompareAt("ETH/USDT", "5m", ours, theirs, bootAt)

	if !rep.Pass() {
		t.Fatalf("expected PASS, got failures: %v", rep.Failures())
	}
	var exempt int
	for _, f := range rep.Flow {
		exempt += f.BootExempt
	}
	if exempt == 0 {
		t.Error("the bar was excused by some OTHER rule — BootExempt is zero, so the counter " +
			"does not reflect what this exemption actually rescued, and the report would hide " +
			"which rule did the excusing")
	}
}

// TestBootExempt_DoesNotExcuseAnyOtherBucket is the guard that keeps it from being a mute.
// The exemption names ONE bucket. A bar one bucket away is an ordinary failure.
func TestBootExempt_DoesNotExcuseAnyOtherBucket(t *testing.T) {
	t.Parallel()

	base := int64(1_700_000_000_000) / bootStep * bootStep
	ours, theirs := bars(base, 10, map[int]float64{5: -3})
	bootAt := time.UnixMilli(base + 3*bootStep + 90_000) // boot in bucket 3, defect in bucket 5

	if rep := CompareAt("ETH/USDT", "5m", ours, theirs, bootAt); rep.Pass() {
		t.Error("a divergent bar OUTSIDE the boot bucket passed — the exemption is excusing " +
			"bars it does not name, which makes it a blanket tolerance and defeats the gate")
	}
}

// TestBootExempt_IsCapped is the ceiling. The exemption covers a known, measured residual of
// well under 1%. A boot bucket wrong by an order of magnitude more is not that residual — it
// is an ingestion defect wearing its clothes, and it must fail.
func TestBootExempt_IsCapped(t *testing.T) {
	t.Parallel()

	base := int64(1_700_000_000_000) / bootStep * bootStep

	// THE SAMPLE IS SIZED SO THE CAP IS THE ONLY THING THAT CAN FAIL IT.
	//
	// Ten bars and a 30% defect would pass and prove nothing: 30% on one of ten bars is 3e-2 of the
	// window, sixty times WindowTolerance, so the rejection comes from the window sum and the cap is
	// never consulted. Setting BootBucketCap to 1e9 would leave the whole suite green — the shape of
	// a decorative assertion.
	//
	// With 400 bars a 10% defect on one bar is 2,5e-4 of the window — UNDER WindowTolerance
	// (5e-4) — so the window sum cannot reject it. If this test fails, only the cap can have
	// failed it.
	const n = 400
	ours, theirs := bars(base, n, map[int]float64{3: -100})
	bootAt := time.UnixMilli(base + 3*bootStep + 90_000)

	// Guard the guard: if the fixture ever drifts so the window sum could reject on its own,
	// this test would go back to proving nothing without anyone noticing.
	rep0 := CompareAt("ETH/USDT", "5m", ours, theirs, bootAt)
	for _, f := range rep0.Flow {
		if f.WindowRelError > WindowTolerance {
			t.Fatalf("%s: fixture windowRelErr=%.3g exceeds WindowTolerance=%.3g — the window "+
				"sum could reject this sample on its own, so the test would pass without the "+
				"cap ever being consulted", f.Field, f.WindowRelError, WindowTolerance)
		}
	}

	if rep := rep0; rep.Pass() {
		t.Errorf("a boot bucket 10%% short passed — the exemption has no effective ceiling. "+
			"BootBucketCap is %v and exists precisely so a real failure cannot hide behind a "+
			"documented limit", BootBucketCap)
	}
}

// TestBootExempt_WindowSumStillFails proves the second, independent backstop. Even below the
// per-bar cap, a boot bucket wrong by enough to matter drags the window sum past tolerance —
// so the exemption cannot silence a deficit that is material in aggregate.
func TestBootExempt_WindowSumStillFails(t *testing.T) {
	t.Parallel()

	base := int64(1_700_000_000_000) / bootStep * bootStep
	// 4% on one bar: under BootBucketCap (5%), but 4%/10 bars = 0,4% of the window,
	// which is above WindowTolerance (5e-4).
	ours, theirs := bars(base, 10, map[int]float64{3: -40})
	bootAt := time.UnixMilli(base + 3*bootStep + 90_000)

	rep := CompareAt("ETH/USDT", "5m", ours, theirs, bootAt)
	if rep.Pass() {
		t.Error("the window sum did not fail — the per-bar exemption became the ONLY rule. " +
			"The delta must keep counting toward the sum, or a deficit that is small per bar " +
			"and large in aggregate passes unseen")
	}
}

// TestBootExempt_DisabledWithoutABootInstant pins fail-closed. A harness that cannot
// establish when the process started gets the STRICT rule, never a silent exemption.
func TestBootExempt_DisabledWithoutABootInstant(t *testing.T) {
	t.Parallel()

	base := int64(1_700_000_000_000) / bootStep * bootStep
	ours, theirs := bars(base, 10, map[int]float64{3: -3})

	if rep := Compare("ETH/USDT", "5m", ours, theirs); rep.Pass() {
		t.Error("the bar was excused with no boot instant supplied — an unknown boot time must " +
			"disable the exemption, not widen it. Failing closed is what stops a harness that " +
			"lost track of the process from quietly passing everything")
	}
	if rep := CompareAt("ETH/USDT", "5m", ours, theirs, time.Time{}); rep.Pass() {
		t.Error("a zero boot instant behaved as an exemption rather than as its absence")
	}
}

// TestBootExempt_HealthyBootBucketIsNotCounted keeps the counter honest. BootExempt must
// report bars this rule actually rescued — if it incremented for every boot bucket, healthy or
// not, the number would say nothing and an operator reading it would learn nothing.
func TestBootExempt_HealthyBootBucketIsNotCounted(t *testing.T) {
	t.Parallel()

	base := int64(1_700_000_000_000) / bootStep * bootStep
	ours, theirs := bars(base, 10, nil) // nothing diverges anywhere
	bootAt := time.UnixMilli(base + 3*bootStep + 90_000)

	rep := CompareAt("ETH/USDT", "5m", ours, theirs, bootAt)
	for _, f := range rep.Flow {
		if f.BootExempt != 0 {
			t.Errorf("%s: BootExempt = %d on a window where nothing diverged — the counter must "+
				"measure rescues, not occurrences of the boot bucket", f.Field, f.BootExempt)
		}
	}
}

// TestInteriorImbalance_StillFailsTheWindowSum records a design decision that survived an
// attempt to change it.
//
// XRP/USDT 5m once returned 5,81e-4 against a 5e-4 tolerance with priceMismatches=0,
// unpaired=0 and exactly one EDGE bar, and the obvious reading was that the sum was measuring
// where the window had been cut. Removing the edge bar's imbalance from the sum made that pair
// pass — and immediately failed TestCompare_EdgeExemptionDoesNotHideASystematicDefect, which
// exists precisely because the edge exemption is scoped to the PER-BAR rule and keeps the
// window sum as its backstop.
//
// The exemption and its backstop are one mechanism. An edge bar is excused from being judged
// alone, never from being counted — otherwise a defect that lands on a boundary is invisible
// to both rules at once. The correction was reverted; this test states why it may not return.
func TestInteriorImbalance_StillFailsTheWindowSum(t *testing.T) {
	t.Parallel()

	base := int64(1_700_000_000_000) / bootStep * bootStep
	ours, theirs := bars(base, 20, map[int]float64{9: -30})

	if rep := Compare("XRP/USDT", "5m", ours, theirs); rep.Pass() {
		t.Error("an INTERIOR lone divergence passed — the edge correction has leaked inward " +
			"and the window sum no longer policies the middle of the sample")
	}
}

// TestSystematicDrift_StillFailsDespiteTheEdgeCorrection is the property that makes the
// correction safe: a real drift touches every bar, not only the two at the edges, so removing
// the edges cannot hide it.
func TestSystematicDrift_StillFailsDespiteTheEdgeCorrection(t *testing.T) {
	t.Parallel()

	base := int64(1_700_000_000_000) / bootStep * bootStep
	off := map[int]float64{}
	for i := range 20 {
		off[i] = -1 // 0,1% short on EVERY bar
	}
	ours, theirs := bars(base, 20, off)

	if rep := Compare("XRP/USDT", "5m", ours, theirs); rep.Pass() {
		t.Error("a systematic 0,1% deficit across every bar passed — the edge correction must " +
			"remove a boundary artifact, never a drift")
	}
}

// TestBootExempt_CoversTheRampBucketToo pins the scope at exactly two buckets, and both bounds
// are measured rather than chosen.
//
// The original scope was one bucket, on the assumption that a symbol starts delivering when
// the process starts. It does not: the fleet attaches in a single batch and then RAMPS. At the
// 05:34 boot only 328 of 500 symbols were delivering within ten seconds, and for the slow ones
// the shortfall landed in the bucket AFTER the one containing process start — a bucket the
// exemption did not cover, which is why G1 kept failing on bar boot+1 with deficits of 1,7e-4
// and 1,8e-3 while every other bar matched exactly.
func TestBootExempt_CoversTheRampBucketToo(t *testing.T) {
	t.Parallel()

	base := int64(1_700_000_000_000) / bootStep * bootStep
	// The deficit is in the bucket AFTER the one holding bootAt — the ramp bucket.
	ours, theirs := bars(base, 10, map[int]float64{4: -3})
	bootAt := time.UnixMilli(base + 3*bootStep + 90_000)

	if rep := CompareAt("XRP/USDT", "5m", ours, theirs, bootAt); !rep.Pass() {
		t.Errorf("the ramp bucket failed: %v — a symbol that starts speaking 40 s into a boot "+
			"lands its shortfall one bucket later than the process instant, and keying the "+
			"exemption on that instant alone leaves exactly this bar uncovered", rep.Failures())
	}
}

// TestBootExempt_StopsAtTwoBuckets is the bound. A symbol still silent after two bucket widths
// is not ramping — it is not delivering, which is a different failure and must not be excused
// by a window that keeps growing to fit whatever fails.
func TestBootExempt_StopsAtTwoBuckets(t *testing.T) {
	t.Parallel()

	base := int64(1_700_000_000_000) / bootStep * bootStep
	ours, theirs := bars(base, 10, map[int]float64{5: -3}) // two buckets past bootAt's bucket
	bootAt := time.UnixMilli(base + 3*bootStep + 90_000)

	if rep := CompareAt("XRP/USDT", "5m", ours, theirs, bootAt); rep.Pass() {
		t.Error("a bar two buckets past the boot bucket was excused — the exemption has started " +
			"absorbing whatever fails instead of naming a bounded, measured condition")
	}
}
