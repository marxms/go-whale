package derivatives

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The tests' clock. Every window below is measured backwards from it.
func derivNow() time.Time {
	return time.Date(2026, 8, 23, 0, 0, 0, 0, time.UTC)
}

const derivDay = 24 * time.Hour

// fakeHotReader is the ZSET: it returns DESCENDING, like the real ZREVRANGE, and declares a
// horizon.
type fakeHotReader struct {
	snapshots []*DerivativesSnapshot
	horizonMs int64
	lastLimit int
	err       error
}

func (h *fakeHotReader) GetSnapshotSeries(_ context.Context, _, _, _, _ string, limit int) ([]*DerivativesSnapshot, error) {
	if h.err != nil {
		return nil, h.err
	}
	h.lastLimit = limit

	desc := make([]*DerivativesSnapshot, 0, len(h.snapshots))
	for i := len(h.snapshots) - 1; i >= 0; i-- {
		desc = append(desc, h.snapshots[i])
	}
	if limit > 0 && len(desc) > limit {
		desc = desc[:limit]
	}
	return desc, nil
}

func (h *fakeHotReader) SeriesHorizonMs(string) int64 { return h.horizonMs }

// fakeColdReader is the durable table: it returns ASCENDING and counts queries.
type fakeColdReader struct {
	snapshots []*DerivativesSnapshot
	calls     int
	lastFrom  time.Time
	lastTo    time.Time
	err       error
}

func (c *fakeColdReader) QueryDerivativesRange(_ context.Context, _ DerivativesScope, from, to time.Time) ([]*DerivativesSnapshot, error) {
	c.calls++
	c.lastFrom, c.lastTo = from, to
	if c.err != nil {
		return nil, c.err
	}
	out := make([]*DerivativesSnapshot, 0, len(c.snapshots))
	for _, s := range c.snapshots {
		if s.Timestamp >= from.UnixMilli() && s.Timestamp <= to.UnixMilli() {
			out = append(out, s)
		}
	}
	return out, nil
}

// dailySeries builds `n` daily buckets ending at `end` (inclusive). Open interest carries the
// TIER MARKER and nothing else: the bucket is already identified by timestamp, and what the
// join must prove is which tier each bucket came from.
func dailySeries(end time.Time, n int, marker float64) []*DerivativesSnapshot {
	out := make([]*DerivativesSnapshot, 0, n)
	for i := n - 1; i >= 0; i-- {
		ts := end.Add(-time.Duration(i) * derivDay)
		v := marker
		out = append(out, &DerivativesSnapshot{
			Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT",
			Timestamp: ts.UnixMilli(), OpenInterestBase: &v,
		})
	}
	return out
}

const (
	hotMarker  = 1.0
	coldMarker = 2.0
)

// overlappingTiers builds both tiers AS THEY ARE IN REAL LIFE: the write feeds both, so the
// bucket sitting exactly at the ZSET floor exists IN BOTH.
//
// The fixture must include that overlap. Without it, raw concatenation and an inverted
// winner rule both pass — a join test without overlap does not test the join.
func overlappingTiers(coldDays int) (*fakeHotReader, *fakeColdReader) {
	const hotDays = 30
	hot := &fakeHotReader{
		// hotDays+1 buckets: from derivNow-30d (the floor, inclusive) to derivNow.
		snapshots: dailySeries(derivNow(), hotDays+1, hotMarker),
		horizonMs: hotDays * derivDay.Milliseconds(),
	}
	cold := &fakeColdReader{snapshots: dailySeries(derivNow(), coldDays, coldMarker)}
	return hot, cold
}

// boundaryBucketMs is the bucket both tiers have.
func boundaryBucketMs() int64 { return derivNow().Add(-30 * derivDay).UnixMilli() }

func newWindowReader(hot *fakeHotReader, cold ColdReader) *DerivativesWindowReader {
	r := NewDerivativesWindowReader(hot, cold)
	r.now = derivNow
	return r
}

func derivScope() DerivativesScope {
	return DerivativesScope{
		Source: "coinalyze", Exchange: "binance", MarketType: "future",
		Symbol: "BTC/USDT", Timeframe: "1d",
	}
}

// --- tests -----------------------------------------------------------------

// TestDerivativesWindow_WindowInsideTheCapNeverTouchesTheDurableTier is the first criterion.
//
// It is not an optimization: it is the difference between a recent read that costs one ZRANGE
// and one that opens disk partitions to re-find what is already in memory.
func TestDerivativesWindow_WindowInsideTheCapNeverTouchesTheDurableTier(t *testing.T) {
	hot := &fakeHotReader{snapshots: dailySeries(derivNow(), 30, hotMarker), horizonMs: 30 * derivDay.Milliseconds()}
	cold := &fakeColdReader{snapshots: dailySeries(derivNow(), 200, coldMarker)}
	reader := newWindowReader(hot, cold)

	got, err := reader.Series(context.Background(), derivScope(), derivNow().Add(-5*derivDay), derivNow())
	if err != nil {
		t.Fatalf("Series: %v", err)
	}
	if cold.calls != 0 {
		t.Fatalf("%d query(ies) to the durable tier for a window inside the cap, wanted 0", cold.calls)
	}
	if len(got) != 6 {
		t.Fatalf("%d bucket(s), wanted 6 (5 days inclusive at both ends)", len(got))
	}
	for _, s := range got {
		if *s.OpenInterestBase != hotMarker {
			t.Fatalf("bucket %d did not come from the ZSET (marker %v)", s.Timestamp, *s.OpenInterestBase)
		}
	}
}

// TestDerivativesWindow_WindowBeyondTheCapIsCompletedByTheDurableTier is the second criterion
// and the independent proof: one year of daily data over a 30-day cap.
func TestDerivativesWindow_WindowBeyondTheCapIsCompletedByTheDurableTier(t *testing.T) {
	hot, cold := overlappingTiers(366)
	reader := newWindowReader(hot, cold)

	from := derivNow().Add(-365 * derivDay)
	got, err := reader.Series(context.Background(), derivScope(), from, derivNow())
	if err != nil {
		t.Fatalf("Series: %v", err)
	}
	if cold.calls != 1 {
		t.Fatalf("%d query(ies) to the durable tier, wanted 1", cold.calls)
	}
	if len(got) != 366 {
		t.Fatalf("%d bucket(s) for 365 days, wanted 366 — the window was not covered in full", len(got))
	}
	if got[0].Timestamp != from.UnixMilli() {
		t.Fatalf("first bucket %d, wanted %d (the deep end of the window)", got[0].Timestamp, from.UnixMilli())
	}
	if got[len(got)-1].Timestamp != derivNow().UnixMilli() {
		t.Fatalf("last bucket %d, wanted %d", got[len(got)-1].Timestamp, derivNow().UnixMilli())
	}
}

// TestDerivativesWindow_NoBucketIsServedTwice: the deep part comes from the durable tier and
// the recent one from the ZSET, and where the two meet the bucket appears ONCE.
func TestDerivativesWindow_NoBucketIsServedTwice(t *testing.T) {
	// The durable tier covers the WHOLE window, including the boundary bucket the ZSET also
	// has — that is how it is in real life, because the write feeds both tiers.
	hot, cold := overlappingTiers(366)
	reader := newWindowReader(hot, cold)

	got, err := reader.Series(context.Background(), derivScope(), derivNow().Add(-365*derivDay), derivNow())
	if err != nil {
		t.Fatalf("Series: %v", err)
	}
	if len(got) != 366 {
		t.Fatalf("%d point(s) in the join, wanted 366 — the boundary bucket was served twice", len(got))
	}

	seen := make(map[int64]int, len(got))
	for _, s := range got {
		seen[s.Timestamp]++
	}
	for ts, n := range seen {
		if n != 1 {
			t.Fatalf("bucket %d appeared %d times in the join", ts, n)
		}
	}
	if len(seen) != len(got) {
		t.Fatalf("%d distinct buckets for %d points", len(seen), len(got))
	}
}

// TestDerivativesWindow_TheHotTierWinsOnAnOverlappingBucket: where the two speak of the same
// bucket, the ZSET wins — it received the live cycle's field-by-field merge, and the durable
// row is a snapshot of what existed when it was written.
func TestDerivativesWindow_TheHotTierWinsOnAnOverlappingBucket(t *testing.T) {
	hot, cold := overlappingTiers(366)
	reader := newWindowReader(hot, cold)

	got, err := reader.Series(context.Background(), derivScope(), derivNow().Add(-365*derivDay), derivNow())
	if err != nil {
		t.Fatalf("Series: %v", err)
	}

	// THE BOUNDARY BUCKET decides: it exists in both tiers, and it is the only point where the
	// "hot wins" rule has an observable consequence.
	var boundary *DerivativesSnapshot
	for _, s := range got {
		if s.Timestamp == boundaryBucketMs() {
			boundary = s
		}
	}
	if boundary == nil {
		t.Fatalf("boundary bucket %d is not in the join", boundaryBucketMs())
	}
	if *boundary.OpenInterestBase != hotMarker {
		t.Fatalf("the boundary bucket came from the durable tier (marker %v) — the ZSET is authoritative there",
			*boundary.OpenInterestBase)
	}

	last := got[len(got)-1]
	if *last.OpenInterestBase != hotMarker {
		t.Fatalf("the most recent bucket came from the durable tier (marker %v) — the ZSET is authoritative there",
			*last.OpenInterestBase)
	}
	// And the deep end, which only the durable tier has, came from it.
	if *got[0].OpenInterestBase != coldMarker {
		t.Fatalf("the oldest bucket (marker %v) did not come from the durable tier", *got[0].OpenInterestBase)
	}
}

// TestDerivativesWindow_TheJoinIsAscending is the third criterion.
//
// The ZSET returns DESCENDING and the table returns ascending; raw concatenation would give a
// series that rises, falls and rises again, and every time-series consumer here assumes
// increasing time.
func TestDerivativesWindow_TheJoinIsAscending(t *testing.T) {
	hot, cold := overlappingTiers(366)
	reader := newWindowReader(hot, cold)

	got, err := reader.Series(context.Background(), derivScope(), derivNow().Add(-365*derivDay), derivNow())
	if err != nil {
		t.Fatalf("Series: %v", err)
	}
	for i := 1; i < len(got); i++ {
		if got[i].Timestamp <= got[i-1].Timestamp {
			t.Fatalf("point %d (%d) is not later than the previous one (%d)", i, got[i].Timestamp, got[i-1].Timestamp)
		}
	}
}

// TestDerivativesWindow_TheDurableIsAskedForTheWholeWindowOnceItIsNeeded.
//
// Coverage is a PER-FIELD property, not an interval one: the ZSET had 156 buckets in which
// `openInterestBase` was null while the durable table had those same 156 WITH the value. Stopping
// the durable query at the hot floor meant it was never asked for them.
//
// The saving is preserved where it is real: a window that FITS in the hot tier does not query
// the durable one at all (case below). The deep query already had to happen — its upper bound
// widening costs no extra query.
func TestDerivativesWindow_TheDurableIsAskedForTheWholeWindowOnceItIsNeeded(t *testing.T) {
	hot, cold := overlappingTiers(366)
	reader := newWindowReader(hot, cold)

	from := derivNow().Add(-100 * derivDay)
	if _, err := reader.Series(context.Background(), derivScope(), from, derivNow()); err != nil {
		t.Fatalf("Series: %v", err)
	}

	if !cold.lastTo.Equal(derivNow()) {
		t.Fatalf("the durable tier was queried through %s, wanted %s (the top of the window)", cold.lastTo, derivNow())
	}
	if !cold.lastFrom.Equal(from) {
		t.Fatalf("the durable tier was queried from %s, wanted %s", cold.lastFrom, from)
	}
	if cold.calls != 1 {
		t.Errorf("the durable tier was queried %d times; widening the bound must not add a query", cold.calls)
	}
}

// TestDerivativesWindow_TheDurableWindowIsCappedAtRetention: querying below the retention
// ceiling only costs partitions opened to prove there is nothing there.
//
// THE CEILING IS THE DERIVATIVES ONE, not the candle one. A candle-sized cap (400 days) cuts
// history that EXISTS: Coinalyze serves 2000 daily buckets back to 2021-03-03 — 5.5 years. The
// deep read would stop at 400 days and leave the rest of the table unreachable.
func TestDerivativesWindow_TheDurableWindowIsCappedAtRetention(t *testing.T) {
	hot := &fakeHotReader{}
	cold := &fakeColdReader{}
	reader := newWindowReader(hot, cold)

	if _, err := reader.Series(context.Background(), derivScope(), derivNow().Add(-5000*derivDay), derivNow()); err != nil {
		t.Fatalf("Series: %v", err)
	}

	// LITERAL, NOT THE CONSTANT. Comparing `cold.lastFrom` with `cold.lastTo -
	// derivativesColdMaxLookback` checks the constant against itself: any value passes,
	// including the one that cut history off.
	//
	// The number below comes from the PROVIDER: 2000 daily buckets back to 2021-03-03. A
	// ceiling that does not reach that leaves the table unreachable.
	alcance := cold.lastTo.Sub(cold.lastFrom)
	if minimo := 2000 * derivDay; alcance < minimo {
		t.Errorf("the durable read reaches %.0f days; the provider serves %.0f and the rest is unreachable",
			alcance.Hours()/24, minimo.Hours()/24)
	}

	// AND IT IS STILL A CEILING: asking for 5000 days does not open a partition for 5000.
	if alcance > 3000*derivDay {
		t.Errorf("the durable read reaches %.0f days — querying below retention only costs partitions opened",
			alcance.Hours()/24)
	}
}

// TestDerivativesWindow_DurableFailureIsNotSilentlyTruncated: a one-year window served as three
// weeks is the silent truncation this path exists to abolish. The read FAILS instead of
// inventing coverage.
func TestDerivativesWindow_DurableFailureIsNotSilentlyTruncated(t *testing.T) {
	hot := &fakeHotReader{snapshots: dailySeries(derivNow(), 30, hotMarker), horizonMs: 30 * derivDay.Milliseconds()}
	cold := &fakeColdReader{err: errors.New("questdb down")}
	reader := newWindowReader(hot, cold)

	got, err := reader.Series(context.Background(), derivScope(), derivNow().Add(-365*derivDay), derivNow())
	if err == nil {
		t.Fatalf("the read returned %d point(s) of a 365-day window without saying the rest was missing", len(got))
	}
	if got != nil {
		t.Fatalf("%d point(s) returned together with the error — a partial window cannot pass as complete", len(got))
	}
}

// TestDerivativesWindow_NoDurableTierRefusesADeepWindow: a disabled durable tier is legitimate
// configuration, and a window deeper than the cap is REFUSED, not truncated.
func TestDerivativesWindow_NoDurableTierRefusesADeepWindow(t *testing.T) {
	hot := &fakeHotReader{snapshots: dailySeries(derivNow(), 30, hotMarker), horizonMs: 30 * derivDay.Milliseconds()}
	reader := newWindowReader(hot, nil)

	if _, err := reader.Series(context.Background(), derivScope(), derivNow().Add(-365*derivDay), derivNow()); !errors.Is(err, ErrDerivativesWindowUnservable) {
		t.Fatalf("err=%v, wanted ErrDerivativesWindowUnservable", err)
	}
	// And the shallow window keeps being served.
	got, err := reader.Series(context.Background(), derivScope(), derivNow().Add(-5*derivDay), derivNow())
	if err != nil {
		t.Fatalf("window inside the cap refused without a durable tier: %v", err)
	}
	if len(got) != 6 {
		t.Fatalf("%d bucket(s), wanted 6", len(got))
	}
}

// TestDerivativesWindow_TheHotLimitComesFromTheWindow: the ZSET limit derives from the window.
// A fixed number truncates the old end of the slice without saying anything.
func TestDerivativesWindow_TheHotLimitComesFromTheWindow(t *testing.T) {
	hot := &fakeHotReader{snapshots: dailySeries(derivNow(), 30, hotMarker), horizonMs: 30 * derivDay.Milliseconds()}
	reader := newWindowReader(hot, &fakeColdReader{})

	if _, err := reader.Series(context.Background(), derivScope(), derivNow().Add(-9*derivDay), derivNow()); err != nil {
		t.Fatalf("Series: %v", err)
	}
	if hot.lastLimit != 10 {
		t.Fatalf("limit requested from the ZSET = %d, wanted 10 (9 days, inclusive at both ends)", hot.lastLimit)
	}
}

// TestDerivativesWindow_UnknownTimeframeIsRefused: the project's tolerant parser returns 1h for
// invalid input, and a window measured in 1h buckets for a timeframe that does not exist would
// produce a plausible and wrong result.
func TestDerivativesWindow_UnknownTimeframeIsRefused(t *testing.T) {
	reader := newWindowReader(&fakeHotReader{horizonMs: derivDay.Milliseconds()}, &fakeColdReader{})

	scope := derivScope()
	scope.Timeframe = "3z"
	if _, err := reader.Series(context.Background(), scope, derivNow().Add(-derivDay), derivNow()); err == nil {
		t.Fatal("a nonexistent timeframe was accepted")
	}
}

// TestDerivativesWindow_EmptyWindowIsRefused: a `to` that is not later than `from` is not a
// small window, it is none.
func TestDerivativesWindow_EmptyWindowIsRefused(t *testing.T) {
	reader := newWindowReader(&fakeHotReader{horizonMs: derivDay.Milliseconds()}, &fakeColdReader{})

	if _, err := reader.Series(context.Background(), derivScope(), derivNow(), derivNow()); err == nil {
		t.Fatal("an empty window was accepted")
	}
}

// TestDerivativesWindow_HotFailurePropagates: a failure of the authoritative tier does not
// become silence.
func TestDerivativesWindow_HotFailurePropagates(t *testing.T) {
	hot := &fakeHotReader{horizonMs: 30 * derivDay.Milliseconds(), err: errors.New("redis down")}
	reader := newWindowReader(hot, &fakeColdReader{})

	if _, err := reader.Series(context.Background(), derivScope(), derivNow().Add(-derivDay), derivNow()); err == nil {
		t.Fatal("the ZSET failure was swallowed")
	}
}

// TestDerivativesRangeQuery_TheIntervalIsALiteralAndTheScopeIsBound pins the measured
// asymmetry: QuestDB only prunes a partition when it resolves the designated-timestamp predicate
// at PLANNING time, and it does not resolve `timestamp >= $n`.
func TestDerivativesWindow_HotDoesNotEraseAFieldItLacks(t *testing.T) {
	bucket := derivNow().Add(-2 * derivDay).Truncate(derivDay)
	oi, fr, ls := 1234.5, 0.0001, 2.5

	cold := &fakeColdReader{snapshots: []*DerivativesSnapshot{{
		Timestamp: bucket.UnixMilli(), Source: SourceCoinalyze,
		OpenInterestBase: &oi, FundingRate: &fr, LongShortRatio: &ls,
	}}}
	// The hot tier has the SAME bucket, with funding and WITHOUT open interest — which is
	// exactly the shape of the measured 156 days.
	frHot := 0.0002
	hot := &fakeHotReader{snapshots: []*DerivativesSnapshot{{
		Timestamp: bucket.UnixMilli(), Source: "venue", FundingRate: &frHot,
	}}}

	out, err := newWindowReader(hot, cold).Series(
		context.Background(), derivScope(), derivNow().Add(-400*derivDay), derivNow())
	if err != nil {
		t.Fatalf("Series: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("the same bucket came out %d times, expected 1", len(out))
	}

	got := out[0]
	if got.OpenInterestBase == nil {
		t.Error("the hot tier erased the open interest only the durable tier had — that is the flat line on the chart")
	} else if *got.OpenInterestBase != oi {
		t.Errorf("open interest %v, expected %v", *got.OpenInterestBase, oi)
	}
	// AND THE HOT TIER STILL WINS on the field it HAS: it received the live cycle's
	// field-by-field merge, and the durable row is a snapshot of what existed when it was
	// written.
	if got.FundingRate == nil || *got.FundingRate != frHot {
		t.Errorf("funding %v, expected the hot tier's (%v)", got.FundingRate, frHot)
	}
	if got.Source != "venue" {
		t.Errorf("source %q; the hot tier contributed the funding, so the point belongs to it", got.Source)
	}
}

// THE OTHER END: when the hot tier brings not ONE number, the point belongs to the durable tier —
// and the source too. Labeling it with a source that supplied none of the point's numbers would
// be decorative provenance; provenance must mean something.
func TestDerivativesWindow_AnEmptyHotBucketKeepsTheDurableProvenance(t *testing.T) {
	bucket := derivNow().Add(-2 * derivDay).Truncate(derivDay)
	oi := 999.0

	cold := &fakeColdReader{snapshots: []*DerivativesSnapshot{{
		Timestamp: bucket.UnixMilli(), Source: SourceCoinalyze, OpenInterestBase: &oi,
	}}}
	hot := &fakeHotReader{snapshots: []*DerivativesSnapshot{{
		Timestamp: bucket.UnixMilli(), Source: "venue",
	}}}

	out, err := newWindowReader(hot, cold).Series(
		context.Background(), derivScope(), derivNow().Add(-400*derivDay), derivNow())
	if err != nil {
		t.Fatalf("Series: %v", err)
	}
	if len(out) != 1 || out[0].OpenInterestBase == nil || *out[0].OpenInterestBase != oi {
		t.Fatalf("the durable value did not survive: %+v", out)
	}
	if out[0].Source != SourceCoinalyze {
		t.Errorf("source %q; the hot tier brought no number at all, so the point belongs to the durable tier", out[0].Source)
	}
}

// THE NULL FIELD INSIDE THE HOT RANGE — the second act of the flat line.
//
// Overlaying field by field was not enough: the durable query stopped at the hot floor, and the
// bucket with a null `openInterestBase` sat ABOVE that floor. The durable tier was never asked
// for it, so there was nothing to overlay. Only joining the two fixes closes it.
func TestDerivativesWindow_ADeepReadFillsNullFieldsInsideTheHotRange(t *testing.T) {
	dentro := derivNow().Add(-10 * derivDay).Truncate(derivDay)
	oi := 4242.0
	fr := 0.0003

	// The durable tier has the bucket WITH open interest.
	cold := &fakeColdReader{snapshots: []*DerivativesSnapshot{{
		Timestamp: dentro.UnixMilli(), Source: SourceCoinalyze, OpenInterestBase: &oi,
	}}}
	// The hot tier has 30 days, and the same bucket WITHOUT open interest — the exact shape of
	// the measured 156 days.
	hotPoints := make([]*DerivativesSnapshot, 0, 30)
	for i := 29; i >= 0; i-- {
		ts := derivNow().Add(-time.Duration(i) * derivDay).Truncate(derivDay)
		p := &DerivativesSnapshot{Timestamp: ts.UnixMilli(), Source: "venue", FundingRate: &fr}
		hotPoints = append(hotPoints, p)
	}
	hot := &fakeHotReader{snapshots: hotPoints}

	// The requested window is DEEP, so the durable tier is queried.
	out, err := newWindowReader(hot, cold).Series(
		context.Background(), derivScope(), derivNow().Add(-100*derivDay), derivNow())
	if err != nil {
		t.Fatalf("Series: %v", err)
	}

	var achou bool
	for _, p := range out {
		if p.Timestamp != dentro.UnixMilli() {
			continue
		}
		achou = true
		if p.OpenInterestBase == nil {
			t.Error("the bucket inside the hot range was left without open interest — that is the 156-day flat line")
		} else if *p.OpenInterestBase != oi {
			t.Errorf("open interest %v, expected %v", *p.OpenInterestBase, oi)
		}
	}
	if !achou {
		t.Fatal("the bucket did not appear in the output")
	}
}

// AND THE SAVING STAYS REAL where it is real: a window that fits in the hot tier opens no
// partition.
func TestDerivativesWindow_AShallowReadStillSkipsTheDurableTier(t *testing.T) {
	hot, cold := overlappingTiers(366)
	reader := newWindowReader(hot, cold)

	if _, err := reader.Series(context.Background(), derivScope(), derivNow().Add(-5*derivDay), derivNow()); err != nil {
		t.Fatalf("Series: %v", err)
	}
	if cold.calls != 0 {
		t.Errorf("the durable tier was queried %d time(s) for a 5-day window the ZSET fully covers", cold.calls)
	}
}

// THE NaN GUARD — the durable read's second line of defense.
//
// QuestDB represents a null double as NaN INTERNALLY and exposes it that way on some surfaces.
// A NaN that slipped through would enter a field consumers treat as a number, and every
// comparison with NaN is false: the value is neither greater nor smaller than anything, passes
// every filter and triggers no error. It is the worst kind of bad data — the kind that does not
// complain.
func mergedBucket(t *testing.T, cold, hot *DerivativesSnapshot) *DerivativesSnapshot {
	t.Helper()
	ts := time.UnixMilli(cold.Timestamp)
	out := mergeDerivativesTiers(
		[]*DerivativesSnapshot{cold},
		[]*DerivativesSnapshot{hot},
		ts.Add(-time.Hour), ts.Add(time.Hour))
	if len(out) != 1 {
		t.Fatalf("%d merged point(s), wanted 1", len(out))
	}
	return out[0]
}

// TestOverlayDerivatives_TheFundingGroupMovesAsAUnit ⭐ kills the mutant that serves
// `fundingRate8h` from the cold tier with `nStep` coming from the hot one.
//
// The six funding fields are NOT six independent observations: they are ONE observed rate plus
// the interval resolution that accompanied it, tied by the identity
// `FundingRate8h == FundingRate × 8/FundingIntervalHours`. Merging them field by field — each
// `if X == nil` its own decision — leaves the hot tier's step describing the cold tier's number.
// Measured on the hot tier, the field-by-field version produced 59 of 611 points violating the
// identity, one of them with the SIGN FLIPPED.
func TestOverlayDerivatives_TheFundingGroupMovesAsAUnit(t *testing.T) {
	ts := derivNow().Add(-60 * derivDay).UnixMilli()
	hours, rate, rate8h := 4.0, -8.547e-05, -1.7094e-04

	// The hot tier has the bucket and NO funding; its step is that of a contract the ladder did
	// not resolve at that instant.
	hot := &DerivativesSnapshot{
		Timestamp: ts, Source: SourceVenue,
		OpenInterestBase: fpw(1234.5),
		NStep:            NUnavailable,
	}
	cold := &DerivativesSnapshot{
		Timestamp: ts, Source: "coinalyze",
		FundingRate: &rate, FundingRate8h: &rate8h,
		FundingIntervalHours: &hours, NStep: NFromMeasuredSpacing,
	}

	got := mergedBucket(t, cold, hot)

	if got.FundingRate == nil || *got.FundingRate != rate {
		t.Fatalf("FundingRate=%v, wanted %v", got.FundingRate, rate)
	}
	if got.NStep != NFromMeasuredSpacing {
		t.Fatalf("NStep=%q, wanted %q — the step must come from the SAME reading as the rate. "+
			"%q is the hot tier's, which brought no rate at all",
			got.NStep, NFromMeasuredSpacing, NUnavailable)
	}
	if got.FundingIntervalHours == nil || *got.FundingIntervalHours != hours {
		t.Fatalf("FundingIntervalHours=%v, wanted %v", got.FundingIntervalHours, hours)
	}
	if got.FundingRate8h == nil || *got.FundingRate8h != rate8h {
		t.Fatalf("FundingRate8h=%v, wanted %v", got.FundingRate8h, rate8h)
	}
}

// TestOverlayDerivatives_AnEquivalentWithoutEvidenceIsNulled is the second half.
//
// When the SURVIVING step carries no evidence, the eight-hour equivalent that came with it
// cannot be served: it would describe a cadence nobody observed in that bucket. It is the same
// coherence the hot tier already applies to its own merge.
func TestOverlayDerivatives_AnEquivalentWithoutEvidenceIsNulled(t *testing.T) {
	ts := derivNow().Add(-60 * derivDay).UnixMilli()
	hours, rate, rate8h := 4.0, -8.547e-05, -1.7094e-04

	// The bucket PRECEDES the window in which the interval was observed: the step declares
	// inheritance, and an 8h left beside it is a normalization by a cadence that may not have
	// been in force there.
	cold := &DerivativesSnapshot{
		Timestamp: ts, Source: "coinalyze",
		FundingRate: &rate, FundingRate8h: &rate8h,
		FundingIntervalHours: &hours, NStep: NInheritedOutsideWindow,
	}
	hot := &DerivativesSnapshot{
		Timestamp: ts, Source: SourceVenue, OpenInterestBase: fpw(1234.5),
	}

	got := mergedBucket(t, cold, hot)

	if got.NStep != NInheritedOutsideWindow {
		t.Fatalf("NStep=%q, wanted %q", got.NStep, NInheritedOutsideWindow)
	}
	if got.FundingRate8h != nil {
		t.Fatalf("FundingRate8h=%v under step %q — the inherited step carries no evidence "+
			"for this bucket, and the equivalent claims it does", *got.FundingRate8h, got.NStep)
	}
	// THE INTERVAL SURVIVES: it remains the best thing known about the contract, and the step
	// says it was not observed here.
	if got.FundingIntervalHours == nil || *got.FundingIntervalHours != hours {
		t.Fatalf("FundingIntervalHours=%v, wanted %v — nulling the interval would erase the difference "+
			"between inheritance and unavailability", got.FundingIntervalHours, hours)
	}
	// And the RAW value survives: it is observation, not derived.
	if got.FundingRate == nil || *got.FundingRate != rate {
		t.Fatalf("FundingRate=%v, wanted %v", got.FundingRate, rate)
	}
}

// TestOverlayDerivatives_TheHotReadingKeepsItsOwnGroup — the group moves as a unit in BOTH
// directions. When the hot tier HAS the rate, its group stays whole and the cold one does not
// enter half-way.
func TestOverlayDerivatives_TheHotReadingKeepsItsOwnGroup(t *testing.T) {
	ts := derivNow().Add(-60 * derivDay).UnixMilli()
	hotHours, hotRate, hotRate8h := 8.0, 1e-04, 1e-04
	coldHours, coldRate, coldRate8h := 1.0, 5e-05, 4e-04

	hot := &DerivativesSnapshot{
		Timestamp: ts, Source: SourceVenue,
		FundingRate: &hotRate, FundingRate8h: &hotRate8h,
		FundingIntervalHours: &hotHours, NStep: NFromMeasuredSpacing,
	}
	cold := &DerivativesSnapshot{
		Timestamp: ts, Source: "coinalyze",
		FundingRate: &coldRate, FundingRate8h: &coldRate8h,
		FundingIntervalHours: &coldHours, NStep: NFromMetadataSeed,
	}

	got := mergedBucket(t, cold, hot)

	if got.FundingIntervalHours == nil || *got.FundingIntervalHours != hotHours {
		t.Fatalf("FundingIntervalHours=%v, wanted %v — the cold N describes another reading",
			got.FundingIntervalHours, hotHours)
	}
	if got.FundingRate8h == nil || *got.FundingRate8h != hotRate8h {
		t.Fatalf("FundingRate8h=%v, wanted %v", got.FundingRate8h, hotRate8h)
	}
	if got.NStep != NFromMeasuredSpacing {
		t.Fatalf("NStep=%q, wanted %q", got.NStep, NFromMeasuredSpacing)
	}
}

// TestOverlayDerivatives_ASeamDeclaresItsProvenanceByField ⭐ kills the mutant that lets
// `merged := *hot` inherit the hot `Source` while filling `OpenInterestBase` from the cold tier.
//
// MEASURED ON THE LAN: 156 buckets in which the ZSET has the bucket and no `openInterestBase`,
// while the durable tier has both. The delivered point claimed the hot source over a number the
// cold source measured — and the baseline ruler, which filters by `Source`, had no way to
// notice.
func TestOverlayDerivatives_ASeamDeclaresItsProvenanceByField(t *testing.T) {
	ts := derivNow().Add(-60 * derivDay).UnixMilli()
	rate := 1e-04

	// The measured case: the hot tier has the bucket with funding and WITHOUT open interest.
	hot := &DerivativesSnapshot{
		Timestamp: ts, Source: SourceVenue,
		FundingRate: &rate, NStep: NUnavailable,
	}
	cold := &DerivativesSnapshot{
		Timestamp: ts, Source: "coinalyze",
		OpenInterestBase: fpw(106117.954),
	}

	got := mergedBucket(t, cold, hot)

	if got.OpenInterestBase == nil {
		t.Fatal("the durable open interest was discarded — that is the flat-line defect")
	}
	if !got.HasMixedProvenance() {
		t.Fatalf("the stitched point does not declare itself mixed: Source=%q, FieldSources=%v — "+
			"the baseline ruler filters by Source and would measure the distance between two providers "+
			"as market variance", got.Source, got.FieldSources)
	}
	if got.FieldSources["openInterestBase"] != "coinalyze" {
		t.Fatalf("openInterestBase declared as %q, wanted \"coinalyze\" — the durable tier measured it",
			got.FieldSources["openInterestBase"])
	}
	if got.FieldSources["fundingRate"] != SourceVenue {
		t.Fatalf("fundingRate declared as %q, wanted %q",
			got.FieldSources["fundingRate"], SourceVenue)
	}
}

// TestOverlayDerivatives_ASingleProvenancePointDeclaresNothingByField — the per-field
// declaration is for the SEAM. A point whose fields all came from the same source already
// declares itself through `Source`, and repeating the same string per field on each of a
// series' 2,000 points would be wire weight without information.
func TestOverlayDerivatives_ASingleProvenancePointDeclaresNothingByField(t *testing.T) {
	ts := derivNow().Add(-60 * derivDay).UnixMilli()

	// The hot tier brings EVERYTHING the point has: nothing is stitched.
	hot := &DerivativesSnapshot{
		Timestamp: ts, Source: SourceVenue,
		OpenInterestBase: fpw(1), LongShortRatio: fpw(0.95),
	}
	cold := &DerivativesSnapshot{Timestamp: ts, Source: "coinalyze", OpenInterestBase: fpw(2)}

	got := mergedBucket(t, cold, hot)

	if got.HasMixedProvenance() {
		t.Fatalf("single-provenance point declared as mixed: %v", got.FieldSources)
	}
	if len(got.FieldSources) != 0 {
		t.Fatalf("FieldSources=%v on a point that is not a seam", got.FieldSources)
	}
}

// fpw is the float pointer for the window cases.
func fpw(v float64) *float64 { return &v }

func TestMergeDerivativesTiers_AColdOnlyBucketPaysTheSameCoherenceRule(t *testing.T) {
	hours, rate, rate8h := 4.0, -8.547e-05, -1.7094e-04

	tests := []struct {
		name string
		step NDerivationStep
		// want8h says whether the equivalent survives serving.
		want8h bool
		// wantInterval says whether the interval survives.
		wantInterval bool
	}{
		{name: "inherited does not authorize the equivalent", step: NInheritedOutsideWindow, want8h: false, wantInterval: true},
		{name: "unavailable drops the equivalent AND the interval", step: NUnavailable, want8h: false, wantInterval: false},
		{name: "the empty step of the old row normalizes nothing", step: NDerivationStep(""), want8h: false, wantInterval: true},
		{name: "measured serves the equivalent", step: NFromMeasuredSpacing, want8h: true, wantInterval: true},
		{name: "seed serves the equivalent", step: NFromMetadataSeed, want8h: true, wantInterval: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// DEEP: the cold-only bucket is BEYOND what the ZSET covers.
			fundo := derivNow().Add(-600 * derivDay).UnixMilli()
			recente := derivNow().Add(-1 * derivDay).UnixMilli()

			cold := &DerivativesSnapshot{
				Timestamp: fundo, Source: "coinalyze",
				FundingRate: &rate, FundingRate8h: &rate8h,
				FundingIntervalHours: &hours, NStep: tt.step,
			}
			// The hot tier speaks of ANOTHER bucket: the deep one has no hot counterpart at all.
			hot := &DerivativesSnapshot{
				Timestamp: recente, Source: SourceVenue, OpenInterestBase: fpw(1234.5),
			}

			out := mergeDerivativesTiers(
				[]*DerivativesSnapshot{cold},
				[]*DerivativesSnapshot{hot},
				time.UnixMilli(fundo).Add(-time.Hour), time.UnixMilli(recente).Add(time.Hour))
			if len(out) != 2 {
				t.Fatalf("%d point(s) served, wanted 2", len(out))
			}
			got := out[0]
			if got.Timestamp != fundo {
				t.Fatalf("the first point is %d, wanted the deep bucket %d", got.Timestamp, fundo)
			}

			if tt.want8h && (got.FundingRate8h == nil || *got.FundingRate8h != rate8h) {
				t.Fatalf("FundingRate8h=%v under n_step=%q, wanted %v — the step carries evidence "+
					"and the equivalent was erased", got.FundingRate8h, tt.step, rate8h)
			}
			if !tt.want8h && got.FundingRate8h != nil {
				t.Fatalf("FundingRate8h=%v served under n_step=%q on a bucket that exists only in "+
					"the cold tier — the coherence rule did not reach the deep part", *got.FundingRate8h, tt.step)
			}
			if tt.wantInterval && (got.FundingIntervalHours == nil || *got.FundingIntervalHours != hours) {
				t.Fatalf("FundingIntervalHours=%v under n_step=%q, wanted %v", got.FundingIntervalHours, tt.step, hours)
			}
			if !tt.wantInterval && got.FundingIntervalHours != nil {
				t.Fatalf("FundingIntervalHours=%v under n_step=%q — unavailable is absence of an "+
					"interval, not an inherited interval", *got.FundingIntervalHours, tt.step)
			}
			// THE RAW RATE ALWAYS SURVIVES: it is observation, not derived.
			if got.FundingRate == nil || *got.FundingRate != rate {
				t.Fatalf("FundingRate=%v, wanted %v", got.FundingRate, rate)
			}

			// AND THE INPUT IS NOT MUTATED. The cold slice belongs to the caller; fixing coherence
			// by writing into it would leak the fix outside the read.
			if cold.FundingRate8h == nil || *cold.FundingRate8h != rate8h {
				t.Fatalf("the input cold snapshot was mutated: FundingRate8h=%v", cold.FundingRate8h)
			}
			if cold.NStep != tt.step {
				t.Fatalf("the input cold snapshot was mutated: NStep=%q", cold.NStep)
			}
		})
	}
}

// ============================================================================
// THE ROUTE RETURNS EVERY DURABLE COLUMN IT WRITES
// ============================================================================

// TestDerivativesReadColumns_EveryWrittenColumnIsAsked ⭐ is STRICTLY stronger than the
// coverage check above.
//
// The other one compares the read with the coverage DECLARATION. This one compares the read with
// what the write actually EMITS — and the difference is exactly the interval in which the defect
// lives: a column added to the table and to the write but not yet declared is already unreachable,
// and nothing fails, nothing warns, and the field reaches the client null.
//
// Both sides are BEHAVIOR: the emitted columns come from a full snapshot pushed through the real
// durable write path, and the requested ones come from the list the query builds.
func TestOverlayDerivatives_TheCompanionsTravelWithTheirPrincipal(t *testing.T) {
	base := &DerivativesSnapshot{
		Source:           "coinalyze",
		Timestamp:        1_756_000_000_000,
		OpenInterestBase: fp(106117.954),
		OpenInterest:     fp(5_432_100_000),
		LongShortRatio:   fp(0.9554),
		LSLongPct:        fp(48.86),
		LSShortPct:       fp(51.14),
	}
	// The hot tier brought NEITHER of the two independent quantities.
	hot := &DerivativesSnapshot{Source: "binance", Timestamp: 1_756_000_000_000}

	got := overlayDerivatives(base, hot)

	if got.OpenInterest == nil || *got.OpenInterest != 5_432_100_000 {
		t.Errorf("the notional did not come with the base: %v", got.OpenInterest)
	}
	if got.LSLongPct == nil || got.LSShortPct == nil {
		t.Fatalf("the shares did not come with the ratio: long=%v short=%v", got.LSLongPct, got.LSShortPct)
	}
	if *got.LSLongPct != 48.86 || *got.LSShortPct != 51.14 {
		t.Errorf("shares = %v/%v, wanted 48.86/51.14", *got.LSLongPct, *got.LSShortPct)
	}
}

// AND THEY DO NOT CROSS WHEN THE HOT PRINCIPAL WINS. This is the discriminating half: without
// it, "always copy from the cold tier" would pass the case above and stitch the cold share onto
// the hot ratio — two readings from different instants summing to 100 by chance.
func TestOverlayDerivatives_TheCompanionsDoNotCrossWhenTheHotPrincipalWins(t *testing.T) {
	base := &DerivativesSnapshot{
		Source:           "coinalyze",
		Timestamp:        1_756_000_000_000,
		OpenInterestBase: fp(1),
		OpenInterest:     fp(999),
		LongShortRatio:   fp(2),
		LSLongPct:        fp(10),
		LSShortPct:       fp(90),
	}
	hot := &DerivativesSnapshot{
		Source:           "binance",
		Timestamp:        1_756_000_000_000,
		OpenInterestBase: fp(50),
		LongShortRatio:   fp(60),
	}

	got := overlayDerivatives(base, hot)

	if got.OpenInterest != nil {
		t.Errorf("the cold notional (%v) was stitched onto a hot open interest", *got.OpenInterest)
	}
	if got.LSLongPct != nil || got.LSShortPct != nil {
		t.Errorf("the cold shares were stitched onto a hot ratio: %v/%v", got.LSLongPct, got.LSShortPct)
	}
}
