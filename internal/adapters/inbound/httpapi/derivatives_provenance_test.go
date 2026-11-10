package httpapi

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/marxms/go-whale/internal/derivatives"
)

// provSnapshot builds a point with a declared source and open-interest value.
func provSnapshot(ts int64, source string, oi float64) *derivatives.DerivativesSnapshot {
	v := oi
	return &derivatives.DerivativesSnapshot{
		Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT",
		Timestamp: ts, Source: source, OpenInterestBase: &v,
	}
}

// --- every point declares its source ----------------------------------------

// TestDeclareProvenance_EveryPointCarriesASource: a point without a source in the output is a
// point the consumer has to guess, and their guess would be "the usual one" — exactly the
// assumption that stops holding the day the second source enters.
func TestDeclareProvenance_EveryPointCarriesASource(t *testing.T) {
	in := []*derivatives.DerivativesSnapshot{
		provSnapshot(3_000, "", 1),
		provSnapshot(2_000, "", 2),
		provSnapshot(1_000, "", 3),
	}

	got := declareProvenance(in, derivatives.SourceVenue)

	if len(got) != 3 {
		t.Fatalf("%d point(s), want 3", len(got))
	}
	for i, snapshot := range got {
		if snapshot.Source != derivatives.SourceVenue {
			t.Fatalf("point %d has source %q, want %q", i, snapshot.Source, derivatives.SourceVenue)
		}
	}
}

// TestDeclareProvenance_ADeclaredSourceIsNotOverwritten: the fallback fills in what is
// missing; it does not rewrite what the point already declared. Overwriting would turn a
// point from the second source into a point from the first — cross-source contamination
// happening at read time.
func TestDeclareProvenance_ADeclaredSourceIsNotOverwritten(t *testing.T) {
	in := []*derivatives.DerivativesSnapshot{
		provSnapshot(2_000, derivatives.SourceCoinalyze, 1),
		provSnapshot(1_000, "", 2),
	}

	got := declareProvenance(in, derivatives.SourceVenue)

	if got[0].Source != derivatives.SourceCoinalyze {
		t.Fatalf("declared source became %q", got[0].Source)
	}
	if got[1].Source != derivatives.SourceVenue {
		t.Fatalf("absent source became %q, want %q", got[1].Source, derivatives.SourceVenue)
	}
}

// TestDeclareProvenance_DoesNotMutateTheCallerRecords: the declaration returns copies.
// Marking the object that was read is how one request's envelope ends up writing into data
// another part of the response is still reading.
func TestDeclareProvenance_DoesNotMutateTheCallerRecords(t *testing.T) {
	original := provSnapshot(1_000, "", 1)

	got := declareProvenance([]*derivatives.DerivativesSnapshot{original}, derivatives.SourceVenue)

	if original.Source != "" {
		t.Fatalf("the caller's record was marked with %q", original.Source)
	}
	if got[0].Source != derivatives.SourceVenue {
		t.Fatalf("the copy was not marked: %q", got[0].Source)
	}
	if got[0] == original {
		t.Fatal("the output points at the same record as the input")
	}
}

// TestDeclareProvenance_NilPointsAreDropped: a nil pointer is not a point, and it cannot
// become a point with no number and a declared source.
func TestDeclareProvenance_NilPointsAreDropped(t *testing.T) {
	got := declareProvenance([]*derivatives.DerivativesSnapshot{nil, provSnapshot(1_000, "", 1), nil}, derivatives.SourceVenue)
	if len(got) != 1 {
		t.Fatalf("%d point(s), want 1", len(got))
	}
}

// --- the ruler sees one provenance ------------------------------------------

// TestSingleProvenanceSeries_TheRulerNeverSeesTwoSources
//
// The ruler is a median and a robust scale over the increments. Two providers measuring the
// same market do not return the same number — they sample at different instants and round
// differently — so a ruler over the mixture measures the distance BETWEEN them and presents
// it as market variance.
func TestSingleProvenanceSeries_TheRulerNeverSeesTwoSources(t *testing.T) {
	// Stitched series: the recent part comes from the venue, the deep part comes from the
	// aggregator (which is what crossing the cap produces).
	series := []*derivatives.DerivativesSnapshot{
		provSnapshot(5_000, derivatives.SourceVenue, 100),
		provSnapshot(4_000, derivatives.SourceVenue, 101),
		provSnapshot(3_000, derivatives.SourceCoinalyze, 900),
		provSnapshot(2_000, derivatives.SourceCoinalyze, 901),
		provSnapshot(1_000, derivatives.SourceCoinalyze, 902),
	}

	got := singleProvenanceSeries(series)

	if len(got) != 2 {
		t.Fatalf("%d point(s) in the ruler, want 2 — only those of the scored point's provenance", len(got))
	}
	for _, snapshot := range got {
		if snapshot.Source != derivatives.SourceVenue {
			t.Fatalf("point from %q entered the scored point's ruler (%q)", snapshot.Source, derivatives.SourceVenue)
		}
	}
}

// TestSingleProvenanceSeries_TheNewestPointDecides: it is the newest point that is being
// scored, so it is ITS provenance that defines the ruler.
func TestSingleProvenanceSeries_TheNewestPointDecides(t *testing.T) {
	series := []*derivatives.DerivativesSnapshot{
		provSnapshot(3_000, derivatives.SourceCoinalyze, 900),
		provSnapshot(2_000, derivatives.SourceVenue, 100),
		provSnapshot(1_000, derivatives.SourceCoinalyze, 901),
	}

	got := singleProvenanceSeries(series)

	if len(got) != 2 {
		t.Fatalf("%d point(s), want 2", len(got))
	}
	for _, snapshot := range got {
		if snapshot.Source != derivatives.SourceCoinalyze {
			t.Fatalf("the ruler followed %q instead of the newest point's provenance", snapshot.Source)
		}
	}
}

// TestSingleProvenanceSeries_ASingleProvenanceSeriesIsUntouched: a series from a single
// source passes through whole. A filter that shrank the ruler of every existing symbol would
// be a silent baseline-maturity regression.
func TestSingleProvenanceSeries_ASingleProvenanceSeriesIsUntouched(t *testing.T) {
	series := []*derivatives.DerivativesSnapshot{
		provSnapshot(3_000, derivatives.SourceVenue, 100),
		provSnapshot(2_000, derivatives.SourceVenue, 101),
		provSnapshot(1_000, derivatives.SourceVenue, 102),
	}
	if got := singleProvenanceSeries(series); len(got) != 3 {
		t.Fatalf("%d point(s), want 3", len(got))
	}

	// And so does the historical series, with no source declared on any point.
	legacy := []*derivatives.DerivativesSnapshot{
		provSnapshot(3_000, "", 100),
		provSnapshot(2_000, "", 101),
	}
	if got := singleProvenanceSeries(legacy); len(got) != 2 {
		t.Fatalf("%d point(s) in the historical series, want 2", len(got))
	}
}

// TestNormalizedFromSeries_ForeignProvenanceDoesNotReachTheReading is the same rule measured
// in the RESULT and not in the filter: two adjacent points of different provenances do not
// form an increment.
//
// The series below has the scored point (venue) and, in the immediately preceding bucket, a
// point from the aggregator. If the ruler did not filter, the increment would go from 100 to
// 900 — an 800% variation that is a provider swap, not market movement.
func TestNormalizedFromSeries_ForeignProvenanceDoesNotReachTheReading(t *testing.T) {
	const tfMs = int64(60_000)
	nowMs := int64(10 * 60_000)

	baseline := &derivatives.DerivativesBaseline{
		NMin:         2,
		WindowN:      8,
		OpenInterest: &derivatives.Baseline{N: 8, Median: 0, MAD: 0.01},
	}

	series := []*derivatives.DerivativesSnapshot{
		provSnapshot(5*tfMs, derivatives.SourceVenue, 100),
		provSnapshot(4*tfMs, derivatives.SourceCoinalyze, 900),
		provSnapshot(3*tfMs, derivatives.SourceCoinalyze, 901),
	}

	reading := normalizedFromSeries(baseline, series, tfMs, nowMs, priceIncrement{missing: "no candle"}, 0)

	if reading.OpenInterest.Value != nil {
		t.Fatalf("zOpenInterest=%v — the increment crossed a provider swap",
			*reading.OpenInterest.Value)
	}
	if reading.OpenInterest.Reason == "" {
		t.Fatal("the absence came without a reason")
	}
}

// --- the ruler refuses the mixed-field point --------------------------------

// seamSnapshot builds the STITCHED point the deep-window read produces: the label belongs to
// the warm tier and the open interest was measured by the durable one.
func seamSnapshot(ts int64, oi float64) *derivatives.DerivativesSnapshot {
	snap := provSnapshot(ts, derivatives.SourceVenue, oi)
	snap.FieldSources = map[string]string{
		"openInterestBase": derivatives.SourceCoinalyze,
		"fundingRate":      derivatives.SourceVenue,
	}
	return snap
}

// TestSingleProvenanceSeries_AMixedPointNeverReachesTheRuler.
//
// Comparing only `Source` is not enough: the stitched point has a single `Source` — the warm
// one's, which was the basis of the copy. MEASURED on the LAN: 156 buckets in which the
// open-interest number came from the durable tier under the warm label. To a label-only filter
// those points are indistinguishable from native ones, and the increment between a native and
// a stitched point is the distance between two providers presented as market variance.
func TestSingleProvenanceSeries_AMixedPointNeverReachesTheRuler(t *testing.T) {
	series := []*derivatives.DerivativesSnapshot{
		provSnapshot(5_000, derivatives.SourceVenue, 100),
		seamSnapshot(4_000, 900),
		provSnapshot(3_000, derivatives.SourceVenue, 101),
	}

	got := singleProvenanceSeries(series)

	if len(got) != 2 {
		t.Fatalf("%d point(s) in the ruler, want 2 — the stitched point has the right label and a "+
			"number from another provider", len(got))
	}
	for _, snapshot := range got {
		if snapshot.HasMixedProvenance() {
			t.Fatalf("a mixed-field point entered the ruler: Source=%q, FieldSources=%v",
				snapshot.Source, snapshot.FieldSources)
		}
	}
}

// TestSingleProvenanceSeries_TheNewestMixedPointDoesNotDecideTheProvenance — the newest point
// decides the ruler, BUT a stitched point decides nothing: its `Source` is true about part of
// the fields and false about the rest.
func TestSingleProvenanceSeries_TheNewestMixedPointDoesNotDecideTheProvenance(t *testing.T) {
	series := []*derivatives.DerivativesSnapshot{
		seamSnapshot(5_000, 900),
		provSnapshot(4_000, derivatives.SourceCoinalyze, 901),
		provSnapshot(3_000, derivatives.SourceCoinalyze, 902),
		provSnapshot(2_000, derivatives.SourceVenue, 100),
	}

	got := singleProvenanceSeries(series)

	if len(got) != 2 {
		t.Fatalf("%d point(s), want 2 — the provenance must be decided by the newest INTACT "+
			"point", len(got))
	}
	for _, snapshot := range got {
		if snapshot.Source != derivatives.SourceCoinalyze {
			t.Fatalf("the ruler followed %q", snapshot.Source)
		}
	}
}

// TestSingleProvenanceSeries_AWindowMadeEntirelyOfSeamsYieldsNoRuler is the edge case: with
// no intact point, there is no provenance to elect.
//
// Returning the raw series here would put into the ruler exactly the points the filter exists
// to keep out — a `!found` fallback that returns the input does that whenever the whole series
// is mixed.
func TestSingleProvenanceSeries_AWindowMadeEntirelyOfSeamsYieldsNoRuler(t *testing.T) {
	series := []*derivatives.DerivativesSnapshot{
		seamSnapshot(5_000, 900),
		seamSnapshot(4_000, 901),
	}

	if got := singleProvenanceSeries(series); len(got) != 0 {
		t.Fatalf("%d point(s) in the ruler for an entirely stitched window", len(got))
	}
}

// TestNormalizedFromSeries_ASeamDoesNotBecomeAnIncrement is the same rule measured in the
// RESULT: a stitched point adjacent to the scored point does not form an increment.
func TestNormalizedFromSeries_ASeamDoesNotBecomeAnIncrement(t *testing.T) {
	const tfMs = int64(60_000)
	nowMs := int64(10 * 60_000)

	baseline := &derivatives.DerivativesBaseline{
		NMin:         2,
		WindowN:      8,
		OpenInterest: &derivatives.Baseline{N: 8, Median: 0, MAD: 0.01},
	}

	series := []*derivatives.DerivativesSnapshot{
		provSnapshot(5*tfMs, derivatives.SourceVenue, 100),
		seamSnapshot(4*tfMs, 900),
	}

	reading := normalizedFromSeries(baseline, series, tfMs, nowMs, priceIncrement{missing: "no candle"}, 0)

	if reading.OpenInterest.Value != nil {
		t.Fatalf("zOpenInterest=%v — the increment crossed a provider seam",
			*reading.OpenInterest.Value)
	}
	if reading.OpenInterest.Reason == "" {
		t.Fatal("the absence came without a reason")
	}
}

// --- the declaration covers the new field -----------------------------------

// TestMeasurementDeclaration_DeclaresTheProvenanceField is the third criterion of the
// provenance requirement.
//
// A new field on the wire without a declaration is a number (or a label) the consumer has to
// interpret on its own — the same class of defect the self-describing envelope exists to
// remove.
func TestMeasurementDeclaration_DeclaresTheProvenanceField(t *testing.T) {
	declaration := newMeasurementDeclaration("1h")

	var found *FieldDeclaration
	for i := range declaration.Fields {
		if declaration.Fields[i].Field == "source" {
			found = &declaration.Fields[i]
		}
	}
	if found == nil {
		t.Fatal("the `source` field has no measurement declaration")
	}
	if found.Role != RoleProvenance {
		t.Fatalf("`source` role = %q, want %q", found.Role, RoleProvenance)
	}
	if found.TimestampLabel != "" {
		t.Fatalf("`source` declares timestamp label %q — it labels no instant", found.TimestampLabel)
	}
	if !strings.Contains(found.Note, "provider") {
		t.Fatalf("the `source` note does not distinguish provider from venue: %q", found.Note)
	}
}

// TestBatchEntry_CarriesTheSourceOnTheWire: the batch point is FLAT — it copies fields from
// the snapshot instead of embedding it — so a field that is not COPIED by the handler does not
// reach the consumer that reads the whole fleet at once.
//
// Asserting only on the struct by reflection is not enough: deleting `Source: snap.Source`
// from the handler passes untouched. A field declared and not filled in is exactly the class
// of defect this envelope exists to prevent, so the assertion is on the SERVED JSON.
func TestBatchEntry_CarriesTheSourceOnTheWire(t *testing.T) {
	series := []*derivatives.DerivativesSnapshot{
		{Timestamp: 2000, OpenInterestBase: floatPtr(1234), LongShortRatio: floatPtr(1.8)},
	}
	h := newMarketDataHandlerWith(&mockDerivativesStore{series: series}, "binance", "future", nil)
	w := batchRequest(t, h, "/api/batch/derivatives-series?timeframe=1h&limit=5&symbols=BTC/USDT")

	if w.Code != 200 {
		t.Fatalf("status %d, want 200", w.Code)
	}

	var resp declaredBatchResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	points := resp.Data["BTC/USDT"]
	if len(points) != 1 {
		t.Fatalf("%d point(s) in the batch, want 1", len(points))
	}

	raw, present := points[0]["source"]
	if !present {
		t.Fatal("the batch point was served without `source` — the fleet consumer does not know whose number it is")
	}
	var source string
	if err := json.Unmarshal(raw, &source); err != nil {
		t.Fatalf("`source` is not a string: %v", err)
	}
	if source != derivatives.SourceVenue {
		t.Fatalf("source=%q, want %q", source, derivatives.SourceVenue)
	}
}

// TestSeriesRoute_EveryServedPointCarriesTheSource is the same proof on the single-symbol
// series route, which is the one the AC names ("when a consumer reads a SERIES").
func TestSeriesRoute_EveryServedPointCarriesTheSource(t *testing.T) {
	series := []*derivatives.DerivativesSnapshot{
		{Timestamp: 3000, OpenInterestBase: floatPtr(1234)},
		{Timestamp: 2000, OpenInterestBase: floatPtr(1230)},
	}
	h := newMarketDataHandlerWith(&mockDerivativesStore{series: series}, "binance", "future", nil)

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/api/derivatives/BTC%2FUSDT/series?timeframe=1h", nil)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, attachSymbolParam("BTC/USDT")))
	w := httptest.NewRecorder()
	h.GetDerivativesSeries(w, req)

	if w.Code != 200 {
		t.Fatalf("status %d, want 200", w.Code)
	}

	var resp struct {
		Data []struct {
			Snapshot map[string]json.RawMessage `json:"snapshot"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Data) != 2 {
		t.Fatalf("%d point(s), want 2", len(resp.Data))
	}
	for i, point := range resp.Data {
		raw, present := point.Snapshot["source"]
		if !present {
			t.Fatalf("point %d served without `source`", i)
		}
		var source string
		if err := json.Unmarshal(raw, &source); err != nil {
			t.Fatalf("point %d: `source` is not a string: %v", i, err)
		}
		if source != derivatives.SourceVenue {
			t.Fatalf("point %d: source=%q, want %q", i, source, derivatives.SourceVenue)
		}
	}
}

// TestSnapshotWireField_IsNamedSource pins the field name on the wire. A consumer reads the
// key, not the Go field.
func TestSnapshotWireField_IsNamedSource(t *testing.T) {
	snapshotType := reflect.TypeOf(derivatives.DerivativesSnapshot{})

	field, ok := snapshotType.FieldByName("Source")
	if !ok {
		t.Fatal("DerivativesSnapshot does not carry Source")
	}
	if name := strings.Split(field.Tag.Get("json"), ",")[0]; name != "source" {
		t.Fatalf("wire name = %q, want \"source\"", name)
	}
}
