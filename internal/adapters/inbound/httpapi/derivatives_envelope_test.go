package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/platform/config"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-chi/chi/v5"
	"github.com/redis/go-redis/v9"
)

func floatPtr(v float64) *float64 { return &v }

func boolPtr(v bool) *bool { return &v }

// identityFields are the addressing fields of a snapshot. They carry no
// measurement, so they need no unit/window declaration.
func identityFields() map[string]struct{} {
	return map[string]struct{}{
		"exchange":   {},
		"marketType": {},
		"symbol":     {},
		"timestamp":  {},
		"observedAt": {},
	}
}

// TestFieldDeclarations_CoverEveryServedField is the drift guard: the moment
// somebody adds a field to the persisted snapshot without saying what it is
// measured in, this fails. An undeclared number on the wire is exactly the defect the drift
// guard exists to catch.
func TestFieldDeclarations_CoverEveryServedField(t *testing.T) {
	declared := make(map[string]FieldDeclaration, len(derivativesFields()))
	for _, f := range derivativesFields() {
		if _, dup := declared[f.Field]; dup {
			t.Fatalf("field %q declared twice", f.Field)
		}
		declared[f.Field] = f
	}

	snapshotType := reflect.TypeOf(derivatives.DerivativesSnapshot{})
	for i := 0; i < snapshotType.NumField(); i++ {
		tag := snapshotType.Field(i).Tag.Get("json")
		if tag == "" || tag == "-" {
			continue
		}
		name := strings.Split(tag, ",")[0]
		if _, skip := identityFields()[name]; skip {
			continue
		}
		if _, ok := declared[name]; !ok {
			t.Errorf("wire field %q has no measurement declaration", name)
		}
	}

	for name := range declared {
		found := false
		for i := 0; i < snapshotType.NumField(); i++ {
			tag := snapshotType.Field(i).Tag.Get("json")
			if strings.Split(tag, ",")[0] == name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("declaration for %q describes a field nothing serves", name)
		}
	}
}

func TestFieldDeclarations_UnitAndRole(t *testing.T) {
	tests := []struct {
		field      string
		wantUnit   string
		wantRole   FieldRole
		wantWindow string
	}{
		{"openInterestBase", "contracts", RoleDecision, windowPoint},
		{"openInterest", "quote currency (notional)", RoleDisplay, windowPoint},
		// The window string is the POINT, not an incidental detail: the delta must not declare
		// its own window as "undeclared", and a percentage nobody can size is what the
		// dashboard contract forbids rendering. It names the field that carries the measured
		// span.
		{"oiChangePct", "percent", RoleDeprecated, "measured per reading, published as oiChangePctWindowMs"},
		{"oiChangePctWindowMs", "milliseconds", RoleDisplay, "self (it IS the window of oiChangePct)"},
		{"fundingRate", "fraction per settlement interval", RoleDisplay, "one settlement interval"},
		{"fundingRate8h", "fraction per 8h", RoleDecision, "8h equivalent"},
		// THE LADDER HAS FOUR RUNGS. This line claimed three, and asserting the stale
		// string froze the mismatch instead of denouncing it: `inherited` is emitted by the
		// code, and a consumer validating against the declaration would refuse it. The
		// expected value is a literal ON PURPOSE — the declaration is derived from the
		// ladder, and a test that also derived it would pass with the wrong ladder.
		{"nStep", "enum(measured|seed|inherited|unavailable)", RoleProvenance, windowPoint},
		// DISPLAY, not decision. This row pinned `RoleDecision` and was pinning a
		// CONTRADICTION: `derivatives-confluence.types.ts` has declared the level
		// DISPLAY ONLY, and the code agrees with the type — the only
		// read of the raw level in the whole classifier is inside a reasoning STRING
		// (`L/S 1.20 (z +2.50)`); every gate and the score read `longShortRatioZ`.
		// A consumer following the envelope's role table would have decided on the
		// level, which is precisely what 4.1 removed and replaced.
		{"longShortRatio", "ratio of accounts (long / short)", RoleDisplay, windowPoint},
	}

	declared := make(map[string]FieldDeclaration, len(derivativesFields()))
	for _, f := range derivativesFields() {
		declared[f.Field] = f
	}

	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			got, ok := declared[tt.field]
			if !ok {
				t.Fatalf("field %q is not declared", tt.field)
			}
			if got.Unit != tt.wantUnit {
				t.Errorf("unit = %q, want %q", got.Unit, tt.wantUnit)
			}
			if got.Role != tt.wantRole {
				t.Errorf("role = %q, want %q", got.Role, tt.wantRole)
			}
			if got.Window != tt.wantWindow {
				t.Errorf("window = %q, want %q", got.Window, tt.wantWindow)
			}
		})
	}
}

// TestFieldDeclarations_NotionalIsNeverDecision pins the one rule the whole change
// turns on: the notional column may be shown and never decided upon.
func TestFieldDeclarations_NotionalIsNeverDecision(t *testing.T) {
	for _, f := range derivativesFields() {
		if f.Field == "openInterest" && f.Role == RoleDecision {
			t.Fatalf("notional open interest declared as a decision input")
		}
	}
}

// TestServedLabel_IsTheStartOfTheBucketHoldingTheObservation puts an observation
// through the real write path and measures which end of the bar the label the server
// hands back marks.
//
// The predecessor of this test, TestFieldDeclarations_EndLabelledFeeds, asserted only
// that a constant string appeared on four fields. That is green whichever end the
// integer actually marks, which is how the declaration and the producer came to
// disagree for as long as they did — the producer floors, the constant said
// "periodEnd". So the assertions here are arithmetic on the served integer and name no
// declared string at all: changing timestampLabelBucketStart cannot move this test.
//
// The observation is placed 13m20s INSIDE its hour, so bucket start, observation and
// bucket end are three distinct instants and a producer stamping any one of them is
// distinguishable from a producer stamping another.
// Both producers are covered because they are two different lines: the live poll fans
// out through upsertSeriesBatch and the backfill goes through upsertSeries, each
// stamping the member's timestamp on its own. The backfill is the one that carries the
// VENUE's stamp into the store, so it is the path where a per-feed convention would
// have shown up if the label followed one.
func TestServedLabel_IsTheStartOfTheBucketHoldingTheObservation(t *testing.T) {
	const tfMs int64 = 3_600_000
	const observedAtMs int64 = 1_700_000_000_000 // 1699999200000 + 800000

	writePaths := []struct {
		name string
		save func(context.Context, *redisstore.DerivativesRedisStore, *derivatives.DerivativesSnapshot) error
	}{
		{
			name: "live poll (SaveSnapshot fans out through upsertSeriesBatch)",
			save: func(ctx context.Context, s *redisstore.DerivativesRedisStore, snap *derivatives.DerivativesSnapshot) error {
				return s.SaveSnapshot(ctx, snap)
			},
		},
		{
			name: "backfill (SaveSeriesOnly goes through upsertSeries)",
			save: func(ctx context.Context, s *redisstore.DerivativesRedisStore, snap *derivatives.DerivativesSnapshot) error {
				return s.SaveSeriesOnly(ctx, snap, "1h")
			},
		},
	}

	for _, path := range writePaths {
		t.Run(path.name, func(t *testing.T) {
			ctx := context.Background()
			m := miniredis.RunT(t)
			client := redis.NewClient(&redis.Options{Addr: m.Addr()})
			t.Cleanup(func() { _ = client.Close() })
			store := redisstore.NewDerivativesRedisStore(client,
				config.DerivativesConfig{MaxSeriesSize: 200},
				candle.NewTimeframeRegistry([]string{"1h"}))

			snap := &derivatives.DerivativesSnapshot{
				Exchange:         "binance",
				MarketType:       "future",
				Symbol:           "BTCUSDT",
				Timestamp:        observedAtMs,
				OpenInterestBase: floatPtr(84_231.5),
				LongShortRatio:   floatPtr(1.61),
			}
			if err := path.save(ctx, store, snap); err != nil {
				t.Fatalf("write: %v", err)
			}

			served, err := store.GetSnapshotSeries(ctx, "binance", "future", "BTCUSDT", "1h", 10)
			if err != nil {
				t.Fatalf("GetSnapshotSeries: %v", err)
			}
			if len(served) != 1 {
				t.Fatalf("the series holds %d members, want 1", len(served))
			}
			label := served[0].Timestamp

			// Three facts, one per clause of "the start of the timeframe bucket the
			// record was filed under". Together they pin the integer to one instant.
			if label%tfMs != 0 {
				t.Errorf("label %d is not on the %d-ms grid — it is not the boundary of any bucket", label, tfMs)
			}
			if label > observedAtMs {
				t.Errorf("label %d is AFTER the observation at %d (by %d ms) — it marks the end of a "+
					"period, not the start of the bucket that contains it", label, observedAtMs, label-observedAtMs)
			}
			if observedAtMs >= label+tfMs {
				t.Errorf("the observation at %d falls outside [%d, %d) — the label is not the bucket holding it",
					observedAtMs, label, label+tfMs)
			}

			// The same integer is what the envelope publishes as the scored bar and what
			// redisstore.BucketClosed runs its labelTs+tfMs <= nowMs on, so the two halves of
			// the binary are measuring the same end of the same bar.
			reading := normalizedFromSeries(nil, served, tfMs, observedAtMs+2*tfMs,
				priceIncrement{missing: "no candle in this test"}, 0)
			if reading.Timestamp != label {
				t.Errorf("the reading is stamped %d while the stored member is stamped %d — the surface "+
					"republishes the label under different arithmetic", reading.Timestamp, label)
			}
		})
	}
}

// TestMeasurementDeclaration_ClaimsNoBucketWhereThereIsNoBucket covers the route that
// serves the declaration without one: GET /api/derivatives/{symbol} passes no
// timeframe, and the record it serves is the `latest` key, whose timestamp is the poll
// clock and never passed through bucketAlignMs.
func TestMeasurementDeclaration_ClaimsNoBucketWhereThereIsNoBucket(t *testing.T) {
	unbucketed := newMeasurementDeclaration("")
	for _, f := range append(append([]FieldDeclaration(nil), unbucketed.Fields...), unbucketed.Normalized...) {
		if f.TimestampLabel != "" {
			t.Errorf("%s declares timestampLabel %q on a response that carries no timeframe",
				f.Field, f.TimestampLabel)
		}
	}

	// And the bucketed route still says what its label is, or the line above would be
	// passing because the declaration went away everywhere.
	bucketed := newMeasurementDeclaration("1h")
	for _, name := range []string{"openInterestBase", "openInterest", "longShortRatio", "fundingRate"} {
		var found bool
		for _, f := range bucketed.Fields {
			if f.Field == name {
				found = true
				if f.TimestampLabel == "" {
					t.Errorf("%s declares no timestampLabel on a bucketed response", name)
				}
			}
		}
		if !found {
			t.Errorf("field %q is not declared at all", name)
		}
	}
}

func TestNewMeasurementDeclaration_CallerCannotMutateTheTable(t *testing.T) {
	first := newMeasurementDeclaration("1h")
	if len(first.Fields) == 0 {
		t.Fatal("declaration served no fields")
	}
	first.Fields[0].Unit = "corrupted"
	first.Limitations[0] = "corrupted"

	second := newMeasurementDeclaration("1h")
	if second.Fields[0].Unit == "corrupted" {
		t.Error("field table is shared with the caller and was corrupted")
	}
	if second.Limitations[0] == "corrupted" {
		t.Error("limitations table is shared with the caller and was corrupted")
	}
	if second.Version != derivativesContractVersion {
		t.Errorf("version = %q, want %q", second.Version, derivativesContractVersion)
	}
	if second.Timeframe != "1h" {
		t.Errorf("timeframe = %q, want 1h", second.Timeframe)
	}
}

func TestNewMeasurementDeclaration_CarriesTheNoEdgeStatement(t *testing.T) {
	decl := newMeasurementDeclaration("5m")
	found := false
	for _, limitation := range decl.Limitations {
		if strings.Contains(limitation, "no predictive edge") {
			found = true
		}
	}
	if !found {
		t.Fatal("the declaration does not carry the no-predictive-edge statement")
	}
}

func TestDeriveFunding(t *testing.T) {
	tests := []struct {
		name          string
		snapshots     []*derivatives.DerivativesSnapshot
		wantStep      derivatives.NDerivationStep
		wantInterval  *float64
		wantAsOf      int64
		wantReasonSet bool
	}{
		{
			name:          "no records at all",
			snapshots:     nil,
			wantStep:      derivatives.NUnavailable,
			wantReasonSet: true,
		},
		{
			name: "newest record declares a measured interval",
			snapshots: []*derivatives.DerivativesSnapshot{
				{Timestamp: 300, NStep: derivatives.NFromMeasuredSpacing, FundingIntervalHours: floatPtr(4)},
				{Timestamp: 200, NStep: derivatives.NFromMetadataSeed, FundingIntervalHours: floatPtr(8)},
			},
			wantStep:     derivatives.NFromMeasuredSpacing,
			wantInterval: floatPtr(4),
			wantAsOf:     300,
		},
		{
			name: "backfilled newest bucket falls through to the declared one below",
			snapshots: []*derivatives.DerivativesSnapshot{
				{Timestamp: 300},
				{Timestamp: 200, NStep: derivatives.NFromMetadataSeed, FundingIntervalHours: floatPtr(8)},
			},
			wantStep:     derivatives.NFromMetadataSeed,
			wantInterval: floatPtr(8),
			wantAsOf:     200,
		},
		{
			name: "producer declared unavailable — no interval travels",
			snapshots: []*derivatives.DerivativesSnapshot{
				{Timestamp: 300, NStep: derivatives.NUnavailable, FundingIntervalHours: floatPtr(8)},
			},
			wantStep:      derivatives.NUnavailable,
			wantAsOf:      300,
			wantReasonSet: true,
		},
		{
			name: "nothing in the window declares a rung",
			snapshots: []*derivatives.DerivativesSnapshot{
				{Timestamp: 300},
				{Timestamp: 200},
			},
			wantStep:      derivatives.NUnavailable,
			wantReasonSet: true,
		},
		{
			name:          "nil entries are not mistaken for declarations",
			snapshots:     []*derivatives.DerivativesSnapshot{nil, nil},
			wantStep:      derivatives.NUnavailable,
			wantReasonSet: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := deriveFunding(tt.snapshots)
			if got.NStep != tt.wantStep {
				t.Errorf("nStep = %q, want %q", got.NStep, tt.wantStep)
			}
			if got.AsOf != tt.wantAsOf {
				t.Errorf("asOf = %d, want %d", got.AsOf, tt.wantAsOf)
			}
			if (got.Reason != "") != tt.wantReasonSet {
				t.Errorf("reason = %q, wantReasonSet = %v", got.Reason, tt.wantReasonSet)
			}
			switch {
			case tt.wantInterval == nil && got.IntervalHours != nil:
				t.Errorf("intervalHours = %v, want absent", *got.IntervalHours)
			case tt.wantInterval != nil && got.IntervalHours == nil:
				t.Errorf("intervalHours absent, want %v", *tt.wantInterval)
			case tt.wantInterval != nil && *got.IntervalHours != *tt.wantInterval:
				t.Errorf("intervalHours = %v, want %v", *got.IntervalHours, *tt.wantInterval)
			}
		})
	}
}

func TestHasDerivedInterval(t *testing.T) {
	tests := []struct {
		step derivatives.NDerivationStep
		want bool
	}{
		{derivatives.NFromMeasuredSpacing, true},
		{derivatives.NFromMetadataSeed, true},
		{derivatives.NUnavailable, false},
		{"", false},
	}
	for _, tt := range tests {
		if got := hasDerivedInterval(tt.step); got != tt.want {
			t.Errorf("hasDerivedInterval(%q) = %v, want %v", tt.step, got, tt.want)
		}
	}
}

// ── endpoint envelopes ────────────────────────────────────────────────────

type declaredSeriesResponse struct {
	Status      string                   `json:"status"`
	Timeframe   string                   `json:"timeframe"`
	Count       int                      `json:"count"`
	Data        []derivativesSeriesPoint `json:"data"`
	Funding     FundingDerivation        `json:"funding"`
	Measurement MeasurementDeclaration   `json:"measurement"`
}

func serieRequest(t *testing.T, h *MarketDataHandler, url string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), "GET", url, nil)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, attachSymbolParam("BTC/USDT")))
	w := httptest.NewRecorder()
	h.GetDerivativesSeries(w, req)
	return w
}

func TestGetDerivativesSeries_EnvelopeDeclaresItself(t *testing.T) {
	series := []*derivatives.DerivativesSnapshot{
		{
			Symbol:               "BTC/USDT",
			Timestamp:            2000,
			OpenInterestBase:     floatPtr(1234),
			OpenInterest:         floatPtr(1234 * 60000),
			FundingRate:          floatPtr(0.0001),
			FundingRate8h:        floatPtr(0.0002),
			FundingIntervalHours: floatPtr(4),
			NStep:                derivatives.NFromMeasuredSpacing,
			InterestRate:         floatPtr(0.0001),
			FundingAtAnchor:      boolPtr(false),
		},
		{
			Symbol:           "BTC/USDT",
			Timestamp:        1000,
			OpenInterestBase: floatPtr(1200),
			OpenInterest:     floatPtr(1200 * 59000),
		},
	}

	h := newMarketDataHandlerWith(&mockDerivativesStore{series: series}, "binance", "future", nil)
	w := serieRequest(t, h, "/api/derivatives/BTC%2FUSDT/series?timeframe=1h&limit=10")

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp declaredSeriesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if resp.Measurement.Version != derivativesContractVersion {
		t.Errorf("measurement.version = %q, want %q", resp.Measurement.Version, derivativesContractVersion)
	}
	if resp.Measurement.Timeframe != "1h" {
		t.Errorf("measurement.timeframe = %q, want 1h", resp.Measurement.Timeframe)
	}
	if len(resp.Measurement.Fields) != len(derivativesFields()) {
		t.Errorf("measurement.fields = %d entries, want %d", len(resp.Measurement.Fields), len(derivativesFields()))
	}
	if len(resp.Measurement.Limitations) == 0 {
		t.Error("measurement.limitations is empty — the limits do not reach the surface")
	}
	if resp.Funding.NStep != derivatives.NFromMeasuredSpacing {
		t.Errorf("funding.nStep = %q, want %q", resp.Funding.NStep, derivatives.NFromMeasuredSpacing)
	}
	if resp.Funding.IntervalHours == nil || *resp.Funding.IntervalHours != 4 {
		t.Errorf("funding.intervalHours = %v, want 4", resp.Funding.IntervalHours)
	}

	if resp.Count != 2 || len(resp.Data) != 2 {
		t.Fatalf("count = %d, data = %d, want 2 and 2", resp.Count, len(resp.Data))
	}
	// The decision quantity must be reachable on the wire, not only the notional.
	if resp.Data[0].Snapshot.OpenInterestBase == nil {
		t.Error("openInterestBase absent from the series point")
	}
	if resp.Data[0].Snapshot.NStep != derivatives.NFromMeasuredSpacing {
		t.Error("per-point nStep did not survive the projection")
	}
}

// THE ENVELOPE DECLARES WHICH AGGREGATION THE DAILY FUNDING BUCKET CARRIES.
//
// The dictionary stated unit, window, timestamp label and role per field, and did not state
// the AGGREGATION. MEASURED against the live venue: the 1d bucket carried
// 9.422e-05, which is the 16:00 print — the LAST of the day. The day's three prints sum to
// 2.9077e-04, roughly 3x that. Whoever read the daily bucket as the day's funding COST was
// wrong by a factor equal to the number of settlements in the day, and nothing in the
// response contradicted it.
//
// The case reads through the same path as a consumer — the serialized HTTP response — and
// pins the measured facts, not the package variable: comparing against
// `fundingBucketAggregation` would be the constant compared with itself.
func TestMeasurementDeclaration_DeclaresHowTheDailyFundingBucketAggregates(t *testing.T) {
	series := []*derivatives.DerivativesSnapshot{
		{
			Symbol:               "BTC/USDT",
			Timestamp:            1_787_184_000_000, // the measured bucket
			FundingRate:          floatPtr(0.00009422),
			FundingRate8h:        floatPtr(0.00009422),
			FundingIntervalHours: floatPtr(8),
			NStep:                derivatives.NFromMeasuredSpacing,
		},
	}

	h := newMarketDataHandlerWith(&mockDerivativesStore{series: series}, "binance", "future", nil)
	w := serieRequest(t, h, "/api/derivatives/BTC%2FUSDT/series?timeframe=1d&limit=10")
	if w.Code != 200 {
		t.Fatalf("esperava 200, veio %d", w.Code)
	}

	var resp declaredSeriesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	declared := make(map[string]FieldDeclaration, len(resp.Measurement.Fields))
	for _, f := range resp.Measurement.Fields {
		declared[f.Field] = f
	}

	// THE MEASURED FACTS, AS LITERALS OF THIS FILE.
	medidos := []string{
		"last settlement in the bucket", // what the bucket carries
		"NOT a sum",                     // and what it is NOT
		"NOT an average",
		"9.422e-05",  // the value of the measured 1d bucket
		"2.9077e-04", // the sum of the same day's three prints
	}
	for _, field := range []string{"fundingRate", "fundingRate8h"} {
		decl, ok := declared[field]
		if !ok {
			t.Fatalf("%s is not declared in the served envelope", field)
		}
		if decl.Aggregation == "" {
			t.Fatalf("%s reaches the consumer without declaring the aggregation — summing the daily bucket is off by ~3x", field)
		}
		for _, fato := range medidos {
			if !strings.Contains(decl.Aggregation, fato) {
				t.Errorf("the aggregation of %s does not say %q: %q", field, fato, decl.Aggregation)
			}
		}
	}

	// AND A LEVEL DOES NOT CARRY THE DECLARATION. An aggregation repeated on every field is
	// uniform noise: nobody sums open interest, and it is the asymmetry between stock and
	// flow that makes the funding declaration mean anything.
	if decl := declared["openInterestBase"]; decl.Aggregation != "" {
		t.Errorf("openInterestBase declared an aggregation (%q); it exists for what is summed by mistake", decl.Aggregation)
	}
}

// TestGetDerivativesSeries_NoForwardFill is the removal of the read-path forward fill
// held in place: a bucket without a settlement has no funding rate, and the read path must
// not invent one from the neighbor below it.
func TestGetDerivativesSeries_NoForwardFill(t *testing.T) {
	series := []*derivatives.DerivativesSnapshot{
		{Timestamp: 3000, OpenInterestBase: floatPtr(120)},
		{Timestamp: 2000, OpenInterestBase: floatPtr(110)},
		{Timestamp: 1000, OpenInterestBase: floatPtr(100), FundingRate: floatPtr(0.0001), NextFundingRate: floatPtr(0.0002)},
	}

	h := newMarketDataHandlerWith(&mockDerivativesStore{series: series}, "binance", "future", nil)
	w := serieRequest(t, h, "/api/derivatives/BTC%2FUSDT/series?timeframe=1h&limit=10")

	var resp declaredSeriesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Data) != 3 {
		t.Fatalf("data = %d points, want 3", len(resp.Data))
	}

	for i, point := range resp.Data[:2] {
		if point.Snapshot.FundingRate != nil {
			t.Errorf("point %d: fundingRate = %v, want absent — the read path forward-filled", i, *point.Snapshot.FundingRate)
		}
		if point.Snapshot.NextFundingRate != nil {
			t.Errorf("point %d: nextFundingRate = %v, want absent", i, *point.Snapshot.NextFundingRate)
		}
		// Absence must not be laundered into a derived number either.
		if point.Snapshot.OIChangePct != nil {
			t.Errorf("point %d: oiChangePct = %v, want absent — the read path recomputed it", i, *point.Snapshot.OIChangePct)
		}
	}

	// The settlement bucket keeps its own reading untouched.
	if got := resp.Data[2].Snapshot.FundingRate; got == nil || *got != 0.0001 {
		t.Errorf("settlement bucket fundingRate = %v, want 0.0001", got)
	}
}

func TestGetDerivativesSeries_EmptySeriesStillDeclares(t *testing.T) {
	h := newMarketDataHandlerWith(&mockDerivativesStore{}, "binance", "future", nil)
	w := serieRequest(t, h, "/api/derivatives/BTC%2FUSDT/series?timeframe=4h")

	var resp declaredSeriesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Count != 0 {
		t.Errorf("count = %d, want 0", resp.Count)
	}
	if resp.Data == nil {
		t.Error("data serialized as null; the empty set must serialize as []")
	}
	if resp.Funding.NStep != derivatives.NUnavailable || resp.Funding.Reason == "" {
		t.Errorf("funding = %+v, want unavailable with a named reason", resp.Funding)
	}
	if len(resp.Measurement.Fields) == 0 {
		t.Error("an empty series still has to declare what it would have carried")
	}
}

// ── batch envelope ────────────────────────────────────────────────────────

type declaredBatchResponse struct {
	Timeframe   string                                  `json:"timeframe"`
	Data        map[string][]map[string]json.RawMessage `json:"data"`
	Funding     map[string]FundingDerivation            `json:"funding"`
	Normalized  map[string]NormalizedReading            `json:"normalized"`
	Measurement MeasurementDeclaration                  `json:"measurement"`
}

func batchRequest(t *testing.T, h *MarketDataHandler, url string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), "GET", url, nil)
	w := httptest.NewRecorder()
	h.GetBatchDerivativesSeries(w, req)
	return w
}

func TestGetBatchDerivativesSeries_EntryCarriesDecisionFields(t *testing.T) {
	series := []*derivatives.DerivativesSnapshot{
		{
			Timestamp:            2000,
			OpenInterestBase:     floatPtr(1234),
			OpenInterest:         floatPtr(74040000),
			FundingRate:          floatPtr(0.00005),
			FundingRate8h:        floatPtr(0.0001),
			FundingIntervalHours: floatPtr(4),
			NStep:                derivatives.NFromMeasuredSpacing,
			FundingAtAnchor:      boolPtr(true),
			LongShortRatio:       floatPtr(1.8),
		},
	}
	h := newMarketDataHandlerWith(&mockDerivativesStore{series: series}, "binance", "future", nil)
	w := batchRequest(t, h, "/api/batch/derivatives-series?timeframe=1h&limit=5&symbols=BTC/USDT")

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp declaredBatchResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if resp.Measurement.Version != derivativesContractVersion {
		t.Errorf("measurement.version = %q", resp.Measurement.Version)
	}
	if got := resp.Funding["BTC/USDT"].NStep; got != derivatives.NFromMeasuredSpacing {
		t.Errorf("per-symbol funding.nStep = %q, want %q", got, derivatives.NFromMeasuredSpacing)
	}

	points := resp.Data["BTC/USDT"]
	if len(points) != 1 {
		t.Fatalf("data = %d points, want 1", len(points))
	}
	// The embedded existing point and the added decision fields must land flat, in
	// the same object — a consumer mid-migration reads both from one place.
	for _, key := range []string{"timestamp", "openInterest", "openInterestBase", "fundingRate", "fundingRate8h", "nStep", "fundingAtAnchor"} {
		if _, ok := points[0][key]; !ok {
			t.Errorf("batch point is missing %q", key)
		}
	}
}

// TestGetBatchDerivativesSeries_NoIntervalNoNormalizedValue holds the ladder's
// terminal rung: without a derived N there is no comparable rate, so none is served.
func TestGetBatchDerivativesSeries_NoIntervalNoNormalizedValue(t *testing.T) {
	tests := []struct {
		name           string
		step           derivatives.NDerivationStep
		wantNormalized bool
	}{
		{"measured spacing emits", derivatives.NFromMeasuredSpacing, true},
		{"metadata seed emits", derivatives.NFromMetadataSeed, true},
		{"unavailable does not emit", derivatives.NUnavailable, false},
		{"undeclared does not emit", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			series := []*derivatives.DerivativesSnapshot{
				{
					Timestamp:     2000,
					FundingRate:   floatPtr(0.00005),
					FundingRate8h: floatPtr(0.0001),
					NStep:         tt.step,
				},
			}
			h := newMarketDataHandlerWith(&mockDerivativesStore{series: series}, "binance", "future", nil)
			w := batchRequest(t, h, "/api/batch/derivatives-series?timeframe=1h&symbols=BTC/USDT")

			var resp declaredBatchResponse
			if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			points := resp.Data["BTC/USDT"]
			if len(points) != 1 {
				t.Fatalf("data = %d points, want 1", len(points))
			}
			_, present := points[0]["fundingRate8h"]
			if present != tt.wantNormalized {
				t.Errorf("fundingRate8h present = %v, want %v", present, tt.wantNormalized)
			}
		})
	}
}

// ── normalized reading ────────────────────────────────────────────────────

// baselineMockStore is a mockDerivativesStore that ALSO serves baselines, so the
// optional-capability assertion in the handler is exercised in both directions.
type baselineMockStore struct {
	mockDerivativesStore
	baseline    *derivatives.DerivativesBaseline
	baselineErr error
}

func (m *baselineMockStore) GetBaseline(_ context.Context, _, _, _, _ string) (*derivatives.DerivativesBaseline, error) {
	return m.baseline, m.baselineErr
}

// oiPair builds a two-bar series, newest first, at the given spacing.
func oiPair(newestTs, spacingMs int64, newest, previous float64) []*derivatives.DerivativesSnapshot {
	return []*derivatives.DerivativesSnapshot{
		{Timestamp: newestTs, OpenInterestBase: floatPtr(newest)},
		{Timestamp: newestTs - spacingMs, OpenInterestBase: floatPtr(previous)},
	}
}

// lsrPair builds a two-bar long/short series, newest first, at the given spacing.
func lsrPair(newestTs, spacingMs int64, newest, previous float64) []*derivatives.DerivativesSnapshot {
	return []*derivatives.DerivativesSnapshot{
		{Timestamp: newestTs, LongShortRatio: floatPtr(newest)},
		{Timestamp: newestTs - spacingMs, LongShortRatio: floatPtr(previous)},
	}
}

// afterBar is the clock reading at which the bucket labeled barTs has CLOSED: one
// bar width past its own label, which is the earliest instant the reading is taken
// on it. Tests that are not about bucket closure use it so their fixture's newest
// bucket is the scored one.
func afterBar(barTs, tfMs int64) int64 { return barTs + tfMs }

// noPriceIncrement is the fixture for "the candle series had nothing for this bar",
// which is the state every test that is not about the price axis is in.
func noPriceIncrement() priceIncrement {
	return priceIncrement{missing: "no candle is stored for this bar or the one before it"}
}

// TestNormalizedFromSeries pins the whole terminal gate: what is scored, what is
// refused, and — critically — that a refusal is a SEAL carrying its reason, never a
// missing row and never a fabricated zero.
func TestNormalizedFromSeries(t *testing.T) {
	const tfMs int64 = 3600 * 1000
	const ts int64 = 1_700_000_000_000

	mature := &derivatives.DerivativesBaseline{
		WindowN: 288, NMin: 30,
		// median 0, scale 1.4826 × 0.01 => a +0.02 increment scores ≈ +1.349
		OpenInterest: &derivatives.Baseline{Median: 0, MAD: 0.01, N: 288},
	}
	degenerate := &derivatives.DerivativesBaseline{
		WindowN: 288, NMin: 30,
		OpenInterest: &derivatives.Baseline{Median: 0, MAD: 0, N: 288},
	}
	immature := &derivatives.DerivativesBaseline{
		WindowN: 288, NMin: 30,
		OpenInterest: &derivatives.Baseline{Median: 0, MAD: 0.01, N: 12},
	}

	tests := []struct {
		name        string
		baseline    *derivatives.DerivativesBaseline
		snapshots   []*derivatives.DerivativesSnapshot
		wantState   derivatives.BaselineState
		wantZ       bool
		wantValue   bool
		wantZApprox float64
	}{
		{
			name:      "mature baseline over adjacent bars emits a signed score",
			baseline:  mature,
			snapshots: oiPair(ts, tfMs, 102.0, 100.0),
			wantState: derivatives.BaselineMature,
			wantZ:     true, wantValue: true,
			wantZApprox: math.Log(1.02) / (derivatives.MADScale * 0.01),
		},
		{
			name:      "a contraction scores NEGATIVE — the sign is load-bearing",
			baseline:  mature,
			snapshots: oiPair(ts, tfMs, 100.0, 102.0),
			wantState: derivatives.BaselineMature,
			wantZ:     true, wantValue: true,
			wantZApprox: math.Log(100.0/102.0) / (derivatives.MADScale * 0.01),
		},
		{
			name:      "MAD of zero is degenerate, NOT immature, and emits nothing",
			baseline:  degenerate,
			snapshots: oiPair(ts, tfMs, 102.0, 100.0),
			wantState: derivatives.BaselineDegenerate,
			wantZ:     false, wantValue: true,
		},
		{
			name:      "a short sample is immature and emits nothing",
			baseline:  immature,
			snapshots: oiPair(ts, tfMs, 102.0, 100.0),
			wantState: derivatives.BaselineImmature,
			wantZ:     false, wantValue: true,
		},
		{
			name:      "a hole between the two newest bars yields no increment",
			baseline:  mature,
			snapshots: oiPair(ts, 3*tfMs, 102.0, 100.0),
			wantState: derivatives.BaselineMature,
			wantZ:     false, wantValue: false,
		},
		{
			name:      "one bar cannot make an increment",
			baseline:  mature,
			snapshots: []*derivatives.DerivativesSnapshot{{Timestamp: ts, OpenInterestBase: floatPtr(100)}},
			wantState: derivatives.BaselineMature,
			wantZ:     false, wantValue: false,
		},
		{
			name:      "no baseline at all is still a row, sealed with a reason",
			baseline:  nil,
			snapshots: oiPair(ts, tfMs, 102.0, 100.0),
			wantState: derivatives.BaselineImmature,
			wantZ:     false, wantValue: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizedFromSeries(tc.baseline, tc.snapshots, tfMs, afterBar(ts, tfMs), noPriceIncrement(), 0)
			axis := got.OpenInterest

			if axis.State != tc.wantState {
				t.Fatalf("state = %q, want %q", axis.State, tc.wantState)
			}
			if (axis.Z != nil) != tc.wantZ {
				t.Fatalf("z presence = %v, want %v (z=%v)", axis.Z != nil, tc.wantZ, axis.Z)
			}
			if (axis.Value != nil) != tc.wantValue {
				t.Fatalf("value presence = %v, want %v", axis.Value != nil, tc.wantValue)
			}
			if !tc.wantZ && axis.Reason == "" && tc.wantValue == false {
				t.Fatal("nothing was computed and nothing said why — absence must name its cause")
			}
			if tc.wantZ && math.Abs(*axis.Z-tc.wantZApprox) > 1e-9 {
				t.Fatalf("z = %v, want %v", *axis.Z, tc.wantZApprox)
			}
		})
	}
}

// TestNormalizedFromSeries_LongShortIsScoredAsLevel asserts the long/short axis is
// scored on the ONE-BAR INCREMENT of ln(LSR), not on its level.
//
// The two forms are told apart by construction, not by wording: the fixture below is a
// pair whose LEVEL is far from zero (ratio 2.0, level ln 2 ≈ 0.693) and whose INCREMENT
// is exactly zero (2.0 → 2.0). A level estimator scores +4.67 here; the increment scores 0.
// A single implementation can satisfy one of those, never both, so reverting to the level
// kills this test.
//
// It also pins the reason the change was made: as a level the series carries ρ₁ ≈ 0.99
// and the extreme cut never fired.
func TestNormalizedFromSeries_LongShortIsScoredAsAnIncrement(t *testing.T) {
	const tfMs int64 = 3600 * 1000
	const ts int64 = 1_700_000_000_000
	baseline := &derivatives.DerivativesBaseline{
		WindowN: 288, NMin: 30,
		LongShortRatio: &derivatives.Baseline{Median: 0, MAD: 0.1, N: 288},
	}

	// Flat ratio at a level far from 1: the increment is zero and so is the score.
	flat := normalizedFromSeries(baseline, lsrPair(ts, tfMs, 2.0, 2.0), tfMs, afterBar(ts, tfMs), noPriceIncrement(), 0)
	if flat.LongShortRatio.Z == nil {
		t.Fatalf("no score for a flat pair: %+v", flat.LongShortRatio)
	}
	if math.Abs(*flat.LongShortRatio.Z) > 1e-9 {
		t.Fatalf("a ratio that did not move scored %v — that is the LEVEL, not the increment",
			*flat.LongShortRatio.Z)
	}

	// A move scores the log of the ratio of ratios.
	moved := normalizedFromSeries(baseline, lsrPair(ts, tfMs, 2.2, 2.0), tfMs, afterBar(ts, tfMs), noPriceIncrement(), 0)
	want := math.Log(2.2/2.0) / (derivatives.MADScale * 0.1)
	if moved.LongShortRatio.Z == nil || math.Abs(*moved.LongShortRatio.Z-want) > 1e-9 {
		t.Fatalf("z = %v, want %v", moved.LongShortRatio.Z, want)
	}

	// Mirrored moves must give mirrored scores — that is what the logarithm buys.
	down := normalizedFromSeries(baseline, lsrPair(ts, tfMs, 2.0, 2.2), tfMs, afterBar(ts, tfMs), noPriceIncrement(), 0)
	if down.LongShortRatio.Z == nil || math.Abs(*down.LongShortRatio.Z+*moved.LongShortRatio.Z) > 1e-9 {
		t.Fatalf("up scored %v and down scored %v — not mirrored", *moved.LongShortRatio.Z, down.LongShortRatio.Z)
	}

	// One bar cannot make an increment, and the refusal names its cause instead of
	// falling back to the level form.
	single := []*derivatives.DerivativesSnapshot{{Timestamp: ts, LongShortRatio: floatPtr(2.0)}}
	lone := normalizedFromSeries(baseline, single, tfMs, afterBar(ts, tfMs), noPriceIncrement(), 0)
	if lone.LongShortRatio.Z != nil {
		t.Fatalf("a single bar produced a score (%v) — the level estimator is still in there",
			*lone.LongShortRatio.Z)
	}
	if lone.LongShortRatio.Reason == "" {
		t.Fatal("nothing was scored and nothing said why")
	}

	// A hole is never bridged, on this axis as on the others.
	holed := normalizedFromSeries(baseline, lsrPair(ts, 3*tfMs, 2.2, 2.0), tfMs, afterBar(ts, tfMs), noPriceIncrement(), 0)
	if holed.LongShortRatio.Z != nil {
		t.Fatalf("an increment was taken across a hole: z = %v", *holed.LongShortRatio.Z)
	}
}

// TestNormalizedFromSeries_PriceAxisIsServed pins the third axis: it carries the value
// it was handed, it scores against the price ruler and NOT against another axis's, and
// its absence is a sealed row with a reason rather than a zero.
func TestNormalizedFromSeries_PriceAxisIsServed(t *testing.T) {
	const tfMs int64 = 3600 * 1000
	const ts int64 = 1_700_000_000_000

	// The three rulers carry DIFFERENT scales on purpose: a price scored against the
	// open-interest ruler would come out at a different number, so a crossed wire
	// fails here instead of looking plausible.
	baseline := &derivatives.DerivativesBaseline{
		WindowN: 288, NMin: 30,
		OpenInterest:   &derivatives.Baseline{Median: 0, MAD: 0.01, N: 288},
		LongShortRatio: &derivatives.Baseline{Median: 0, MAD: 0.1, N: 288},
		Price:          &derivatives.Baseline{Median: 0, MAD: 0.004, N: 288},
	}

	r := math.Log(1.03)
	got := normalizedFromSeries(baseline, oiPair(ts, tfMs, 102.0, 100.0), tfMs, afterBar(ts, tfMs), priceIncrement{value: &r}, 0)
	if got.Price.Z == nil {
		t.Fatalf("the price axis produced no score: %+v", got.Price)
	}
	want := r / (derivatives.MADScale * 0.004)
	if math.Abs(*got.Price.Z-want) > 1e-9 {
		t.Fatalf("price z = %v, want %v — it is not being scored against the price ruler",
			*got.Price.Z, want)
	}
	if got.Price.Value == nil || math.Abs(*got.Price.Value-r) > 1e-12 {
		t.Fatalf("price value = %v, want %v", got.Price.Value, r)
	}

	// A negative return scores negative: the sign is load-bearing here too.
	down := math.Log(0.97)
	fell := normalizedFromSeries(baseline, oiPair(ts, tfMs, 102.0, 100.0), tfMs, afterBar(ts, tfMs), priceIncrement{value: &down}, 0)
	if fell.Price.Z == nil || *fell.Price.Z >= 0 {
		t.Fatalf("a fall scored %v, want a negative score", fell.Price.Z)
	}

	// Absent input: sealed, named, and NOT zero.
	absent := normalizedFromSeries(baseline, oiPair(ts, tfMs, 102.0, 100.0), tfMs, afterBar(ts, tfMs), noPriceIncrement(), 0)
	if absent.Price.Z != nil || absent.Price.Value != nil {
		t.Fatalf("an absent close produced a reading: %+v", absent.Price)
	}
	if absent.Price.Reason == "" {
		t.Fatal("the price axis is absent and does not say why")
	}
	// The state is still reported — the row is sealed, never dropped.
	if absent.Price.State != derivatives.BaselineMature {
		t.Fatalf("state = %q, want the ruler's own state to survive the missing input", absent.Price.State)
	}
}

// retractedNoteClaims are statements the measurement RETRACTED, checked against the
// text this service serves to every consumer.
//
// `Note` is not an internal comment: it ships in the `measurement` object of every
// derivatives response, so a stale claim there is a claim the product makes. Both of
// these were on the wire until the re-measurement:
//
// - "0.874" — the ICC, which corrected to a range of 0.74-0.80.
// - "permanent" — the assertion that the symbol-level component does not decay. It
// is NOT established: the measured ICC falls monotonically as the window widens
// and had not settled at ~40 days.
//
// The check is on the SUBSTRING and is case-insensitive, so "Permanent" or a sentence
// rebuilt around the same number fails too.
func retractedNoteClaims() []string {
	return []string{"0.874", "permanent"}
}

// TestServedNotesDoNotCarryRetractedClaims fails if either retracted statement returns
// to the declaration this service serves.
func TestServedNotesDoNotCarryRetractedClaims(t *testing.T) {
	declaration := newMeasurementDeclaration("1h")

	all := append(append([]FieldDeclaration(nil), declaration.Fields...), declaration.Normalized...)
	if len(all) == 0 {
		t.Fatal("no declarations were checked — the guard would pass vacuously")
	}

	for _, field := range all {
		note := strings.ToLower(field.Note)
		for _, claim := range retractedNoteClaims() {
			if strings.Contains(note, claim) {
				t.Errorf("field %q serves the retracted claim %q in its note: %s", field.Field, claim, field.Note)
			}
		}
	}

	// Belt and braces: the same check against the SERIALIZED envelope, so a claim that
	// migrates from Note into Limitations or into a new text field is caught too.
	encoded, err := json.Marshal(declaration)
	if err != nil {
		t.Fatalf("marshal declaration: %v", err)
	}
	lowered := strings.ToLower(string(encoded))
	for _, claim := range retractedNoteClaims() {
		if strings.Contains(lowered, claim) {
			t.Errorf("the served measurement declaration carries the retracted claim %q", claim)
		}
	}
}

// TestLongShortNoteDeclaresTheIncrement asserts the note for the long/short score
// describes what the code now computes.
//
// It is a separate test from the one above because "does not lie about the ICC" and
// "says which quantity is scored" are different failures, and the second is specific to
// the estimator: a note left saying "as a LEVEL" would be a false statement about the code's
// behavior.
func TestLongShortNoteDeclaresTheIncrement(t *testing.T) {
	var note, window string
	for _, f := range newMeasurementDeclaration("1h").Normalized {
		if f.Field == "zLongShortRatio" {
			note, window = f.Note, f.Window
		}
	}
	if note == "" {
		t.Fatal("zLongShortRatio is not declared at all")
	}
	if strings.Contains(strings.ToLower(note), "as a level") {
		t.Errorf("the note still calls the long/short score a level: %s", note)
	}
	if !strings.Contains(note, "ln LSR(t) − ln LSR(t−1)") {
		t.Errorf("the note does not name the quantity that is scored: %s", note)
	}
	if window == windowPoint {
		t.Errorf("window = %q, but an increment spans a bar and is not a point sample", window)
	}
}

// TestNormalizedFieldsCoverEveryAxis is the drift guard for the derived columns: an
// axis added to NormalizedReading without a unit/window declaration fails here.
func TestNormalizedFieldsCoverEveryAxis(t *testing.T) {
	declared := make(map[string]struct{}, len(normalizedFields()))
	for _, f := range normalizedFields() {
		declared[f.Field] = struct{}{}
		if f.Unit == "" || f.Window == "" || f.Role == "" {
			t.Errorf("normalized field %q is not fully declared: %+v", f.Field, f)
		}
	}

	readingType := reflect.TypeOf(NormalizedReading{})
	for i := 0; i < readingType.NumField(); i++ {
		field := readingType.Field(i)
		if field.Type != reflect.TypeOf(NormalizedAxis{}) {
			continue
		}
		name := strings.Split(field.Tag.Get("json"), ",")[0]
		if _, ok := declared[name]; !ok {
			t.Errorf("normalized axis %q has no measurement declaration", name)
		}
	}
}

// TestAxisPublishesTheSpanItActuallyReaches is the live half of the requirement "the
// baseline window is defined in time, not in bucket count".
//
// The strikethrough half of that requirement was revoked: the series ARE capped by
// count, deliberately, and funding — the cadence the original rationale was about — has
// no baseline at all. What survived is narrower and true: open interest and long/short
// live on the timeframe grid, so count and time are the same quantity for them ONLY
// while the grid is unbroken, and the span is the number that says whether it was.
//
// `Baseline.SpanMs` has been computed and persisted since robuststat existed
// (`derivatives_baseline.go:359`) and reached no consumer — the reach was measured every
// cycle and thrown away at the store boundary. This test exists because that is a silent
// failure mode: N stays at 288 and looks healthy while the span behind it doubles.
func TestAxisPublishesTheSpanItActuallyReaches(t *testing.T) {
	const dia = int64(24 * 60 * 60 * 1000)

	t.Run("the span reaches the envelope", func(t *testing.T) {
		valor := 0.02
		axis := axisFromBaseline(
			&derivatives.Baseline{Median: 0, MAD: 0.01, N: 288, SpanMs: dia},
			30, &valor, "unused",
		)
		if axis.SpanMs != dia {
			t.Errorf("spanMs = %d, want %d — the baseline measured its own reach and the "+
				"envelope dropped it", axis.SpanMs, dia)
		}
	})

	t.Run("the same N can carry different spans, which is the whole point", func(t *testing.T) {
		valor := 0.02
		denso := axisFromBaseline(&derivatives.Baseline{Median: 0, MAD: 0.01, N: 288, SpanMs: dia}, 30, &valor, "x")
		esparso := axisFromBaseline(&derivatives.Baseline{Median: 0, MAD: 0.01, N: 288, SpanMs: 4 * dia}, 30, &valor, "x")

		if denso.N != esparso.N {
			t.Fatalf("the fixture is wrong: both baselines must carry the same N")
		}
		if denso.SpanMs == esparso.SpanMs {
			t.Errorf("two baselines with N=288 reported the same span (%d) — if the field "+
				"cannot distinguish a day of unbroken 5m bars from four days with three "+
				"quarters of them missing, publishing it bought nothing", denso.SpanMs)
		}
	})

	t.Run("no baseline reports no reach", func(t *testing.T) {
		axis := axisFromBaseline(nil, 30, nil, "no baseline")
		if axis.SpanMs != 0 {
			t.Errorf("spanMs = %d for a nil baseline, want 0 — a ruler that was never "+
				"built has no reach to claim", axis.SpanMs)
		}
	})

	t.Run("the span is omitted from the wire when it is zero, never sent as a false zero", func(t *testing.T) {
		raw, err := json.Marshal(axisFromBaseline(nil, 30, nil, "no baseline"))
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if strings.Contains(string(raw), "spanMs") {
			t.Errorf("payload = %s — a zero span on a nil baseline must not be serialized; "+
				"a consumer reading spanMs:0 would take it for a measurement", raw)
		}
	})
}

// TestGetDerivativesSeries_ServesNormalizedReading walks the handler end to end and
// asserts the reading reaches the wire — the sensor is connected, not merely built.
func TestGetDerivativesSeries_ServesNormalizedReading(t *testing.T) {
	const tfMs int64 = 30 * 60 * 1000
	const ts int64 = 1_700_000_100_000

	store := &baselineMockStore{
		mockDerivativesStore: mockDerivativesStore{series: oiPair(ts, tfMs, 102.0, 100.0)},
		baseline: &derivatives.DerivativesBaseline{
			WindowN:      288,
			NMin:         30,
			OpenInterest: &derivatives.Baseline{Median: 0, MAD: 0.01, N: 288},
		},
	}
	handler := newMarketDataHandlerWith(store, "binance", "future", candle.NewTimeframeRegistry(nil))

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/api/derivatives/BTCUSDT/series?timeframe=30m", nil)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, attachSymbolParam("BTCUSDT")))
	rec := httptest.NewRecorder()
	handler.GetDerivativesSeries(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	var got struct {
		Normalized struct {
			OpenInterest struct {
				Z     *float64 `json:"z"`
				State string   `json:"state"`
			} `json:"zOpenInterest"`
		} `json:"normalized"`
		Measurement struct {
			Normalized []FieldDeclaration `json:"normalizedFields"`
		} `json:"measurement"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Normalized.OpenInterest.Z == nil {
		t.Fatalf("no z on the wire: %s", rec.Body.String())
	}
	if got.Normalized.OpenInterest.State != string(derivatives.BaselineMature) {
		t.Fatalf("state = %q on the wire", got.Normalized.OpenInterest.State)
	}
	if len(got.Measurement.Normalized) != len(normalizedFields()) {
		t.Fatalf("declaration of the derived columns did not reach the wire: %+v", got.Measurement.Normalized)
	}
}

// TestGetDerivativesSeries_ServesThePriceAxis walks the price axis end to end.
//
// Two claims, and the second is the one that has no other guard: the score reaches the
// wire under `zPrice`, and the bar it was computed for is the bar the OTHER axes were
// computed for. A price axis silently scored on the newest CANDLE instead of the newest
// derivatives bucket would look perfectly healthy in the response — same shape, same
// state, plausible number — and would be comparing two different instants.
func TestGetDerivativesSeries_ServesThePriceAxis(t *testing.T) {
	const tfMs int64 = 30 * 60 * 1000
	const ts int64 = 1_700_000_100_000

	r := math.Log(1.012)
	store := &baselineMockStore{
		mockDerivativesStore: mockDerivativesStore{
			series:              oiPair(ts, tfMs, 102.0, 100.0),
			priceIncrementValue: &r,
		},
		baseline: &derivatives.DerivativesBaseline{
			WindowN:      288,
			NMin:         30,
			OpenInterest: &derivatives.Baseline{Median: 0, MAD: 0.01, N: 288},
			Price:        &derivatives.Baseline{Median: 0, MAD: 0.004, N: 288},
		},
	}
	handler := newMarketDataHandlerWith(store, "binance", "future", candle.NewTimeframeRegistry(nil))

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/api/derivatives/BTCUSDT/series?timeframe=30m", nil)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, attachSymbolParam("BTCUSDT")))
	rec := httptest.NewRecorder()
	handler.GetDerivativesSeries(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	var got struct {
		Normalized struct {
			Timestamp int64 `json:"timestamp"`
			Price     struct {
				Value  *float64 `json:"value"`
				Z      *float64 `json:"z"`
				State  string   `json:"state"`
				Reason string   `json:"reason"`
			} `json:"zPrice"`
		} `json:"normalized"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.Normalized.Price.Z == nil {
		t.Fatalf("no price z on the wire (reason %q): %s", got.Normalized.Price.Reason, rec.Body.String())
	}
	want := r / (derivatives.MADScale * 0.004)
	if math.Abs(*got.Normalized.Price.Z-want) > 1e-9 {
		t.Fatalf("price z = %v, want %v", *got.Normalized.Price.Z, want)
	}
	if got.Normalized.Price.State != string(derivatives.BaselineMature) {
		t.Fatalf("price state = %q on the wire", got.Normalized.Price.State)
	}

	// The bar the store was asked about IS the bar the reading is stamped with.
	if store.priceIncrementBar != ts {
		t.Fatalf("the price axis was scored on bar %d while the reading is stamped %d — different instants",
			store.priceIncrementBar, ts)
	}
	if got.Normalized.Timestamp != ts {
		t.Fatalf("reading timestamp = %d, want %d", got.Normalized.Timestamp, ts)
	}
}

// TestGetDerivativesSeries_UnreadableCandlesSealThePriceAxis asserts a candle store
// that will not answer produces a NAMED absence and never a zero — and that it does not
// take the other two axes down with it.
func TestGetDerivativesSeries_UnreadableCandlesSealThePriceAxis(t *testing.T) {
	const tfMs int64 = 30 * 60 * 1000
	const ts int64 = 1_700_000_100_000

	store := &baselineMockStore{
		mockDerivativesStore: mockDerivativesStore{
			series:            oiPair(ts, tfMs, 102.0, 100.0),
			priceIncrementErr: errors.New("redis: connection refused"),
		},
		baseline: &derivatives.DerivativesBaseline{
			WindowN:      288,
			NMin:         30,
			OpenInterest: &derivatives.Baseline{Median: 0, MAD: 0.01, N: 288},
			Price:        &derivatives.Baseline{Median: 0, MAD: 0.004, N: 288},
		},
	}
	handler := newMarketDataHandlerWith(store, "binance", "future", candle.NewTimeframeRegistry(nil))

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/api/derivatives/BTCUSDT/series?timeframe=30m", nil)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, attachSymbolParam("BTCUSDT")))
	rec := httptest.NewRecorder()
	handler.GetDerivativesSeries(rec, req)

	var got struct {
		Normalized struct {
			OpenInterest struct {
				Z *float64 `json:"z"`
			} `json:"zOpenInterest"`
			Price struct {
				Value  *float64 `json:"value"`
				Z      *float64 `json:"z"`
				Reason string   `json:"reason"`
			} `json:"zPrice"`
		} `json:"normalized"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Normalized.Price.Z != nil || got.Normalized.Price.Value != nil {
		t.Fatalf("an unreadable candle store produced a price reading: %+v", got.Normalized.Price)
	}
	if got.Normalized.Price.Reason == "" {
		t.Fatal("the price axis is absent and the wire does not say why")
	}
	if got.Normalized.OpenInterest.Z == nil {
		t.Fatal("a candle failure blanked the open-interest axis, which reads a different series")
	}
}

// TestNormalizedReading_UnreadableBaselineIsNotReportedAsNeverComputed pins the
// distinction the batch route now makes 500 times a cycle: a store that would not
// answer is a different fact from a ruler nobody has built, and only one of the two
// improves by waiting.
func TestNormalizedReading_UnreadableBaselineIsNotReportedAsNeverComputed(t *testing.T) {
	store := &baselineMockStore{
		mockDerivativesStore: mockDerivativesStore{series: oiPair(1_700_000_100_000, 30*60*1000, 102.0, 100.0)},
		baselineErr:          errors.New("redis: connection refused"),
	}
	h := newMarketDataHandlerWith(store, "binance", "future", candle.NewTimeframeRegistry(nil))
	w := batchRequest(t, h, "/api/batch/derivatives-series?timeframe=30m&symbols=BTC/USDT")

	var resp declaredBatchResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	reading, ok := resp.Normalized["BTC/USDT"]
	if !ok {
		t.Fatalf("the pair was dropped on a read failure: %s", w.Body.String())
	}
	for name, axis := range map[string]NormalizedAxis{
		"zOpenInterest":   reading.OpenInterest,
		"zLongShortRatio": reading.LongShortRatio,
	} {
		if axis.Z != nil {
			t.Errorf("%s: scored against a ruler that could not be read", name)
		}
		if axis.Reason != baselineUnreadable {
			t.Errorf("%s: reason = %q, want %q", name, axis.Reason, baselineUnreadable)
		}
	}
}

// TestGetBatchDerivativesSeries_ServesNormalizedReadingPerSymbol is the reachability
// test for the batch axis: it walks the handler the FLEET reads and asserts the score
// arrives keyed by symbol.
//
// It exists because the axis was computed, declared in `normalizedFields`, and served
// only on the single-symbol route — which the fleet consumer cannot call 500 times a
// cycle. A number that reaches only the route nobody reads is not served.
func TestGetBatchDerivativesSeries_ServesNormalizedReadingPerSymbol(t *testing.T) {
	const tfMs int64 = 30 * 60 * 1000
	const ts int64 = 1_700_000_100_000

	store := &baselineMockStore{
		mockDerivativesStore: mockDerivativesStore{series: oiPair(ts, tfMs, 102.0, 100.0)},
		baseline: &derivatives.DerivativesBaseline{
			WindowN:      288,
			NMin:         30,
			OpenInterest: &derivatives.Baseline{Median: 0, MAD: 0.01, N: 288},
		},
	}
	h := newMarketDataHandlerWith(store, "binance", "future", candle.NewTimeframeRegistry(nil))
	w := batchRequest(t, h, "/api/batch/derivatives-series?timeframe=30m&symbols=BTC/USDT,ETH/USDT")

	var resp declaredBatchResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	for _, symbol := range []string{"BTC/USDT", "ETH/USDT"} {
		reading, ok := resp.Normalized[symbol]
		if !ok {
			t.Fatalf("%s carries no normalized reading: %s", symbol, w.Body.String())
		}
		if reading.OpenInterest.Z == nil {
			t.Fatalf("%s: no z on the wire: %+v", symbol, reading.OpenInterest)
		}
		// The sign is load-bearing: 102 over 100 is an EXPANSION.
		if *reading.OpenInterest.Z <= 0 {
			t.Errorf("%s: z = %v, want positive for a rising open interest", symbol, *reading.OpenInterest.Z)
		}
		if reading.OpenInterest.State != derivatives.BaselineMature {
			t.Errorf("%s: state = %q, want mature", symbol, reading.OpenInterest.State)
		}
		// nMin travels so a consumer can see how far a sample is from usable. n on its
		// own cannot answer that, and `state` is the only field that may be trusted to.
		if reading.OpenInterest.NMin != 30 {
			t.Errorf("%s: nMin = %d, want 30", symbol, reading.OpenInterest.NMin)
		}
		if reading.Timestamp != ts {
			t.Errorf("%s: timestamp = %d, want %d — a stale reading must look stale", symbol, reading.Timestamp, ts)
		}
	}
}

// TestGetBatchDerivativesSeries_UnscorableSymbolIsSealedNotOmitted pins the LEI on this
// route: a pair with no ruler keeps its row, carrying the state and the reason that
// names what was missing. Dropping it would make "we cannot score this yet"
// indistinguishable from "this symbol is unremarkable".
func TestGetBatchDerivativesSeries_UnscorableSymbolIsSealedNotOmitted(t *testing.T) {
	// mockDerivativesStore returns (nil, nil) from GetBaseline — the state every pair
	// is in before the first recompute.
	store := &mockDerivativesStore{series: oiPair(1_700_000_100_000, 30*60*1000, 102.0, 100.0)}
	h := newMarketDataHandlerWith(store, "binance", "future", candle.NewTimeframeRegistry(nil))
	w := batchRequest(t, h, "/api/batch/derivatives-series?timeframe=30m&symbols=BTC/USDT")

	var resp declaredBatchResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	reading, ok := resp.Normalized["BTC/USDT"]
	if !ok {
		t.Fatalf("the unscorable pair was omitted instead of sealed: %s", w.Body.String())
	}
	if reading.OpenInterest.Z != nil {
		t.Errorf("z = %v, want absent — there is no ruler to score against", *reading.OpenInterest.Z)
	}
	if reading.OpenInterest.State != derivatives.BaselineImmature {
		t.Errorf("state = %q, want immature", reading.OpenInterest.State)
	}
	if reading.OpenInterest.Reason == "" {
		t.Error("the absence does not name what was missing")
	}
}

// TestGetDerivativesSeries_NoBaselineYetStillServes asserts that a pair whose ruler
// has never been computed still gets a 200 with a SEALED reading — not a 500, and not
// a missing object. This is the state every (symbol, timeframe) is in before the first
// recompute, and on this fleet it is the permanent state of 2h/4h/1d/1w.
func TestGetDerivativesSeries_NoBaselineYetStillServes(t *testing.T) {
	const tfMs int64 = 30 * 60 * 1000
	store := &mockDerivativesStore{series: oiPair(1_700_000_100_000, tfMs, 102.0, 100.0)}
	handler := newMarketDataHandlerWith(store, "binance", "future", candle.NewTimeframeRegistry(nil))

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/api/derivatives/BTCUSDT/series?timeframe=30m", nil)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, attachSymbolParam("BTCUSDT")))
	rec := httptest.NewRecorder()
	handler.GetDerivativesSeries(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"zOpenInterest"`) {
		t.Fatalf("the sealed reading was omitted instead of declared: %s", rec.Body.String())
	}
}

// ── the scored bucket is the last CLOSED one ──────────────────────────────

// oiSeriesDesc builds a newest-first open-interest series, one bar apart, from
// newestTs downwards. values[0] is the newest bucket.
func oiSeriesDesc(newestTs, tfMs int64, values ...float64) []*derivatives.DerivativesSnapshot {
	series := make([]*derivatives.DerivativesSnapshot, 0, len(values))
	for i, v := range values {
		series = append(series, &derivatives.DerivativesSnapshot{
			Timestamp:        newestTs - int64(i)*tfMs,
			OpenInterestBase: floatPtr(v),
		})
	}
	return series
}

// TestNormalizedFromSeries_ScoresTheLastClosedBucket pins WHICH pair is scored.
//
// The store writes the value observed at `now` under the label bucketAlignMs(now), so
// the newest bucket is rewritten until its bar turns over: its label claims one bar of
// separation from its predecessor while the real span between the two observations
// grows across the bar. Scoring the last CLOSED bucket is what removes that.
//
// The fixture makes the two candidate pairs impossible to confuse: the open bucket
// carries 1000 against 110, the closed pair carries 110 against 100. Whichever pair is
// scored is legible in the value alone.
func TestNormalizedFromSeries_ScoresTheLastClosedBucket(t *testing.T) {
	const tfMs int64 = 30 * 60 * 1000
	const newestTs int64 = 1_700_000_100_000

	// newestTs is the label of the newest bucket; it closes at newestTs+tfMs.
	series := oiSeriesDesc(newestTs, tfMs, 1000, 110, 100)

	baseline := &derivatives.DerivativesBaseline{
		WindowN: 288, NMin: 30,
		OpenInterest: &derivatives.Baseline{Median: 0, MAD: 0.01, N: 288},
	}

	tests := []struct {
		name      string
		nowMs     int64
		wantBarTs int64
		wantValue float64
	}{
		{
			name:      "mid-bar the newest bucket is still open, so the pair behind it is scored",
			nowMs:     newestTs + tfMs/2,
			wantBarTs: newestTs - tfMs,
			wantValue: math.Log(110.0 / 100.0),
		},
		{
			name:      "one millisecond before the turn it is still open",
			nowMs:     newestTs + tfMs - 1,
			wantBarTs: newestTs - tfMs,
			wantValue: math.Log(110.0 / 100.0),
		},
		{
			name:      "at the turn the bucket has closed and becomes the scored one",
			nowMs:     newestTs + tfMs,
			wantBarTs: newestTs,
			wantValue: math.Log(1000.0 / 110.0),
		},
		{
			// The case that rules out the clockless variant. "Always skip the first
			// member" would score (110, 100) here and hand back a reading two bars old
			// precisely when the feed has stalled and the reading is already stale.
			name:      "when the poller has stalled the newest stored bucket is closed and IS scored",
			nowMs:     newestTs + 4*tfMs,
			wantBarTs: newestTs,
			wantValue: math.Log(1000.0 / 110.0),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizedFromSeries(baseline, series, tfMs, tc.nowMs, noPriceIncrement(), 0)

			if got.Timestamp != tc.wantBarTs {
				t.Fatalf("scored bar = %d, want %d (offset %+d bars from the newest label)",
					got.Timestamp, tc.wantBarTs, (got.Timestamp-newestTs)/tfMs)
			}
			if got.OpenInterest.Value == nil {
				t.Fatalf("no increment was produced: %+v", got.OpenInterest)
			}
			if math.Abs(*got.OpenInterest.Value-tc.wantValue) > 1e-12 {
				t.Fatalf("increment = %v, want %v — the wrong pair was scored",
					*got.OpenInterest.Value, tc.wantValue)
			}
			wantZ := tc.wantValue / (derivatives.MADScale * 0.01)
			if got.OpenInterest.Z == nil || math.Abs(*got.OpenInterest.Z-wantZ) > 1e-9 {
				t.Fatalf("z = %v, want %v", got.OpenInterest.Z, wantZ)
			}
		})
	}
}

// TestNormalizedFromSeries_ReadingIsConstantWithinTheBar is the test the whole change
// exists for.
//
// MEASURED DEFECT: with the newest bucket scored, the reading ramps across the bar and
// collapses at the turn. Intra-bar sweep of one 30m bar, 200 symbols per reading:
// p50 0.19 → 0.29 → 0.43 → 0.52 → 0.55, then 0.19 again on the new bar. Fleet pair at
// the two ends of one bar (n=500): p95 2.77 → 4.81 on 30m, 1.58 → 2.70 on 1h,
// 0.66 → 1.11 on 4h — 1.7x on all three at once.
//
// The mechanism is reproduced here rather than described: the open bucket is REWRITTEN
// as the bar progresses, exactly as the poller rewrites it, and the clock advances with
// it. Scoring the open bucket makes the reading follow those rewrites; scoring the last
// closed one makes it a step function that moves once, at the turn.
func TestNormalizedFromSeries_ReadingIsConstantWithinTheBar(t *testing.T) {
	const tfMs int64 = 30 * 60 * 1000
	const openTs int64 = 1_700_000_100_000

	baseline := &derivatives.DerivativesBaseline{
		WindowN: 288, NMin: 30,
		OpenInterest: &derivatives.Baseline{Median: 0, MAD: 0.01, N: 288},
	}

	// The closed pair, fixed for the whole bar: it has taken its final write.
	closedZ := math.Log(110.0/100.0) / (derivatives.MADScale * 0.01)

	withinBar := []struct {
		name      string
		nowOffset int64
		// openValue is what the poller has written into the open bucket by then. It
		// drifts across the bar; the reading must not.
		openValue float64
	}{
		{"just after the bar opened", tfMs / 10, 111},
		{"a third of the way in", tfMs / 3, 140},
		{"halfway", tfMs / 2, 180},
		{"one millisecond before the turn", tfMs - 1, 260},
	}

	for _, tc := range withinBar {
		t.Run(tc.name, func(t *testing.T) {
			series := oiSeriesDesc(openTs, tfMs, tc.openValue, 110, 100)
			got := normalizedFromSeries(baseline, series, tfMs, openTs+tc.nowOffset, noPriceIncrement(), 0)

			if got.Timestamp != openTs-tfMs {
				t.Fatalf("scored bar = %d, want %d — the open bucket was scored",
					got.Timestamp, openTs-tfMs)
			}
			if got.OpenInterest.Z == nil {
				t.Fatalf("no score: %+v", got.OpenInterest)
			}
			if math.Abs(*got.OpenInterest.Z-closedZ) > 1e-9 {
				t.Fatalf("z = %v at %d ms into the bar, want the constant %v — the reading is "+
					"tracking the open bucket, which is the sawtooth",
					*got.OpenInterest.Z, tc.nowOffset, closedZ)
			}
		})
	}

	// And it DOES move at the turn: the bar that was open closes, a new one opens
	// behind it, and the scored pair advances by exactly one bar.
	t.Run("the reading changes once, at the turn of the bar", func(t *testing.T) {
		series := oiSeriesDesc(openTs+tfMs, tfMs, 300, 260, 110, 100)
		got := normalizedFromSeries(baseline, series, tfMs, openTs+tfMs+tfMs/2, noPriceIncrement(), 0)

		if got.Timestamp != openTs {
			t.Fatalf("scored bar = %d, want %d — the reading did not advance with the bar",
				got.Timestamp, openTs)
		}
		wantZ := math.Log(260.0/110.0) / (derivatives.MADScale * 0.01)
		if got.OpenInterest.Z == nil || math.Abs(*got.OpenInterest.Z-wantZ) > 1e-9 {
			t.Fatalf("z = %v after the turn, want %v", got.OpenInterest.Z, wantZ)
		}
		if math.Abs(wantZ-closedZ) < 1e-6 {
			t.Fatal("the fixture cannot tell the two bars apart — it proves nothing")
		}
	})
}

// TestGetDerivativesSeries_BothAxesScoreTheSameBar closes the loop at the handler: the
// price axis is resolved on the bar scoredPair chose, not on the newest bucket in the
// series.
//
// Without this, the two axes drift apart the moment the scored bucket stops being the
// newest one: open interest would be measured on the last closed bar and price on the
// bar still being written, and the response would look perfectly healthy while
// comparing two different instants.
func TestGetDerivativesSeries_BothAxesScoreTheSameBar(t *testing.T) {
	const tfMs int64 = 30 * 60 * 1000
	const openTs int64 = 1_700_000_100_000
	const scoredTs = openTs - tfMs

	r := math.Log(1.012)
	store := &baselineMockStore{
		mockDerivativesStore: mockDerivativesStore{
			series:              oiSeriesDesc(openTs, tfMs, 1000, 110, 100),
			priceIncrementValue: &r,
		},
		baseline: &derivatives.DerivativesBaseline{
			WindowN: 288, NMin: 30,
			OpenInterest: &derivatives.Baseline{Median: 0, MAD: 0.01, N: 288},
			Price:        &derivatives.Baseline{Median: 0, MAD: 0.004, N: 288},
		},
	}
	handler := newMarketDataHandlerWith(store, "binance", "future", candle.NewTimeframeRegistry(nil))
	// Mid-bar: the newest bucket is still open, so the scored bar is the one behind it.
	handler.nowMs = func() int64 { return openTs + tfMs/2 }

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/api/derivatives/BTCUSDT/series?timeframe=30m", nil)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, attachSymbolParam("BTCUSDT")))
	rec := httptest.NewRecorder()
	handler.GetDerivativesSeries(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	var got struct {
		Normalized struct {
			Timestamp    int64 `json:"timestamp"`
			OpenInterest struct {
				Value *float64 `json:"value"`
			} `json:"zOpenInterest"`
			Price struct {
				Z *float64 `json:"z"`
			} `json:"zPrice"`
		} `json:"normalized"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if store.priceIncrementBar != scoredTs {
		t.Fatalf("the price axis was resolved on bar %d, want %d — it followed the newest bucket "+
			"instead of the scored one", store.priceIncrementBar, scoredTs)
	}
	if got.Normalized.Timestamp != scoredTs {
		t.Fatalf("reading timestamp = %d, want %d", got.Normalized.Timestamp, scoredTs)
	}
	if got.Normalized.OpenInterest.Value == nil ||
		math.Abs(*got.Normalized.OpenInterest.Value-math.Log(110.0/100.0)) > 1e-12 {
		t.Fatalf("open-interest increment = %v, want %v — the two axes are on different bars",
			got.Normalized.OpenInterest.Value, math.Log(110.0/100.0))
	}
	if got.Normalized.Price.Z == nil {
		t.Fatal("no price z on the wire")
	}
}

// TestFieldDeclarations_TheRungVocabularyIsTheWholeLadder ⭐ is the ratchet that keeps the
// rung declaration from aging again.
//
// The case above pins the EXACT string — which is the contract a consumer validates. This
// one pins the property: every rung the domain declares appears in the declaration, and the
// declaration names no rung that does not exist. A fifth rung added to the ladder and
// forgotten here fails both cases, for different reasons.
func TestFieldDeclarations_TheRungVocabularyIsTheWholeLadder(t *testing.T) {
	var declared string
	for _, f := range derivativesFields() {
		if f.Field == "nStep" {
			declared = f.Unit
		}
	}
	if declared == "" {
		t.Fatal("nStep is not declared")
	}

	vocabulario := strings.TrimSuffix(strings.TrimPrefix(declared, "enum("), ")")
	if vocabulario == declared {
		t.Fatalf("the nStep declaration is not an enum: %q", declared)
	}
	nomes := strings.Split(vocabulario, "|")

	escada := derivatives.NDerivationSteps()
	if len(nomes) != len(escada) {
		t.Fatalf("the declaration names %d rung(s) and the ladder has %d: %q", len(nomes), len(escada), declared)
	}
	declarados := make(map[string]bool, len(nomes))
	for _, nome := range nomes {
		declarados[nome] = true
	}
	for _, rung := range escada {
		if !declarados[string(rung)] {
			t.Errorf("the rung %q is emitted and the declaration does not name it — a consumer that "+
				"validates against %q rejects it", rung, declared)
		}
	}
}

// AN AXIS WITHOUT A SCORE MUST SAY WHY — measured IN THE FIELD, not in theory.
//
// The `if !ok` branch of `axisFromBaseline` was the ONLY one that returned the axis without
// a `Reason`, and the field documents itself as "names what was missing when nothing could
// be computed" — that is, the code violated its own contract on that branch.
//
// THE MEASURED EFFECT, on the LAN stack: the ranking served rows with
// `oiZScore: null`, `baselineState: "mature"` and NO reason — while the Go producer, at the
// same instant, HAD the sentence ("only one closed bar is available; an increment needs
// two"). The sentence existed and did not arrive: here because this branch does not write
// it, and downstream because `screener-row-builder.ts` does not project `oiBaselineReason`.
// The operator received an axis indistinguishable, from outside, from a healthy axis with
// no value — the exact shape one cannot act on.
//
// THE TWO HALVES ARE DIFFERENT FACTS, and only one of them improves with time. It is the
// same distinction `NormalizedAxis.State` already declares to justify `degenerate` existing
// apart from `immature`; naming them together would let "this symbol does not move" arrive
// disguised as "give it time".
func TestAxisFromBaseline_AnAxisWithoutAScoreAlwaysNamesAReason(t *testing.T) {
	valor := 1.0

	casos := []struct {
		nome       string
		baseline   derivatives.Baseline
		querEstado derivatives.BaselineState
		querTrecho string
	}{
		{
			// FIELD case: the `1w` axis measured with n=8 against nMin=30.
			nome:       "immature — the ruler has few observations",
			baseline:   derivatives.Baseline{Median: 0, MAD: 0.5, N: 8, SpanMs: 1000},
			querEstado: derivatives.BaselineImmature,
			querTrecho: "8 of the 30",
		},
		{
			// The symbol that does not move: MAD zero. It does not improve with time.
			nome:       "degenerate — the ruler has no spread to divide by",
			baseline:   derivatives.Baseline{Median: 0, MAD: 0, N: 288, SpanMs: 1000},
			querEstado: derivatives.BaselineDegenerate,
			querTrecho: "no spread",
		},
	}

	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			axis := axisFromBaseline(&c.baseline, 30, &valor, "")

			if axis.Z != nil {
				t.Fatalf("the axis came with z=%v, and the ruler must not score in this case", *axis.Z)
			}
			if axis.State != c.querEstado {
				t.Fatalf("state = %q, want %q", axis.State, c.querEstado)
			}
			if axis.Reason == "" {
				t.Fatalf("MUTE AXIS: state=%q n=%d nMin=%d and empty reason — from outside this is "+
					"indistinguishable from a healthy axis with no value, and the operator has "+
					"nothing to act on", axis.State, axis.N, axis.NMin)
			}
			if !strings.Contains(axis.Reason, c.querTrecho) {
				t.Errorf("reason = %q, expected to contain %q — the text must name the FACT, and "+
					"the two facts are different", axis.Reason, c.querTrecho)
			}
		})
	}
}

// TestNormalizedFromSeries_TheReasonNamesTheProvenanceFilter ⭐ is.
//
// The sentence "only one closed bar is available; an increment needs two" is TRUE when the
// store holds a single closed bar and FALSE when it was the provenance filter
// that took the other one: there it describes the state BEFORE the filter as if the payload
// did not carry both points. The counter-example is the HTTP body carrying `data[]` with two
// closed bars AND the single-bar sentence.
//
// The sibling of the same family is the OPEN bar at the top: it defines the ruler's
// provenance, the two closed ones fall away, and the reading publishes "the only stored bar
// is still open" over a series of three bars.
func TestNormalizedFromSeries_TheReasonNamesTheProvenanceFilter(t *testing.T) {
	const tfMs int64 = 60 * 60 * 1000
	const ts int64 = 1_700_000_000_000

	baseline := &derivatives.DerivativesBaseline{
		WindowN: 288, NMin: 30,
		OpenInterest:   &derivatives.Baseline{Median: 0, MAD: 0.01, N: 288},
		LongShortRatio: &derivatives.Baseline{Median: 0, MAD: 0.01, N: 288},
	}

	// filteredSnapshot builds a point with both decision quantities, so the two axes go
	// through the same branch.
	filteredSnapshot := func(at int64, source string, oi, lsr float64) *derivatives.DerivativesSnapshot {
		snap := provSnapshot(at, source, oi)
		snap.LongShortRatio = floatPtr(lsr)
		return snap
	}

	t.Run("the discarded predecessor does not become 'a single bar'", func(t *testing.T) {
		raw := []*derivatives.DerivativesSnapshot{
			filteredSnapshot(ts, derivatives.SourceVenue, 110, 1.2),
			filteredSnapshot(ts-tfMs, derivatives.SourceCoinalyze, 100, 1.1),
		}

		// THE INCREMENT IS COMPUTABLE over the raw series — that is what makes the single-bar
		// sentence lie — and the proof uses the SAME calculation as the axis.
		if _, ok := derivatives.LogIncrement(110, 100); !ok {
			t.Fatal("fixture without a computable increment: it does not prove what it claims")
		}
		if _, ok := derivatives.LogIncrement(1.2, 1.1); !ok {
			t.Fatal("fixture without a computable LSR increment")
		}
		if got := singleProvenanceSeries(raw); len(got) != 1 {
			t.Fatalf("%d point(s) in the ruler, want 1: the filter is not the cause of the absence", len(got))
		}

		reading := normalizedFromSeries(baseline, raw, tfMs, ts+tfMs, noPriceIncrement(), 0)

		if reading.Timestamp != ts {
			t.Fatalf("scored bar = %d, want %d", reading.Timestamp, ts)
		}
		for _, eixo := range []struct {
			nome string
			axis NormalizedAxis
		}{
			{"zOpenInterest", reading.OpenInterest},
			{"zLongShortRatio", reading.LongShortRatio},
		} {
			if eixo.axis.Value != nil {
				t.Fatalf("%s: the increment crossed the provider swap: %v", eixo.nome, *eixo.axis.Value)
			}
			if eixo.axis.Reason != filterDiscardedThePredecessor {
				t.Fatalf("%s: reason = %q, expected to name the provenance discard (%q) — the "+
					"published sentence describes the state before the filter, and the payload contradicts it",
					eixo.nome, eixo.axis.Reason, filterDiscardedThePredecessor)
			}
			if strings.Contains(eixo.axis.Reason, "only one closed bar is available") {
				t.Fatalf("%s: the single-bar sentence survived with two closed bars in the raw series", eixo.nome)
			}
		}
	})

	t.Run("the open bar at the top does not turn the closed ones into 'still open'", func(t *testing.T) {
		raw := []*derivatives.DerivativesSnapshot{
			filteredSnapshot(ts+tfMs, derivatives.SourceVenue, 120, 1.3),
			filteredSnapshot(ts, derivatives.SourceCoinalyze, 110, 1.2),
			filteredSnapshot(ts-tfMs, derivatives.SourceCoinalyze, 100, 1.1),
		}

		reading := normalizedFromSeries(baseline, raw, tfMs, ts+tfMs+tfMs/2, noPriceIncrement(), 0)

		if reading.Timestamp != 0 {
			t.Fatalf("scored bar = %d, wanted none", reading.Timestamp)
		}
		for _, eixo := range []struct {
			nome string
			axis NormalizedAxis
		}{
			{"zOpenInterest", reading.OpenInterest},
			{"zLongShortRatio", reading.LongShortRatio},
		} {
			if eixo.axis.Reason != filterDiscardedEveryClosedBar {
				t.Fatalf("%s: reason = %q, expected to name the provenance discard (%q)",
					eixo.nome, eixo.axis.Reason, filterDiscardedEveryClosedBar)
			}
			if strings.Contains(eixo.axis.Reason, "still open") {
				t.Fatalf("%s: the single-open-bar sentence survived with two closed bars "+
					"discarded in the raw series", eixo.nome)
			}
		}
	})

	t.Run("control: the store with a single closed bar keeps the single-bar sentence", func(t *testing.T) {
		raw := []*derivatives.DerivativesSnapshot{
			filteredSnapshot(ts, derivatives.SourceVenue, 110, 1.2),
		}

		reading := normalizedFromSeries(baseline, raw, tfMs, ts+tfMs, noPriceIncrement(), 0)

		const antiga = "only one closed bar is available; an increment needs two"
		if reading.OpenInterest.Reason != antiga || reading.LongShortRatio.Reason != antiga {
			t.Fatalf("reason = %q / %q, expected the single-bar sentence (%q) — here the filter "+
				"discarded nothing, and naming it would be the same cause swap in reverse",
				reading.OpenInterest.Reason, reading.LongShortRatio.Reason, antiga)
		}
	})
}

// TestGetDerivativesSeries_PriceIsResolvedOnTheFilteredBar.
//
// The field's contract is "All three axes below are scored on THIS bar", and it was
// violated from the inside: `priceIncrement` was resolved with `scoredBarTimestamp` over the
// RAW series while OI and LSR were scored over the FILTERED one. The two disagree
// whenever the filter discards the newest point — and the stitched point is exactly that: it
// has ONE `Source` and more than one provenance, so the ruler discards it, and the raw
// series' bar decision fell on it.
//
// MEASURED: `priceIncrementBar` one bar ahead of the axes' timestamp. A price `z` from one
// instant compared to an open-interest `z` from another is a composite no isolated reading
// explains.
func TestGetDerivativesSeries_PriceIsResolvedOnTheFilteredBar(t *testing.T) {
	const tfMs int64 = 60 * 60 * 1000
	const seamTs int64 = 1_700_000_000_000
	const scoredTs = seamTs - tfMs
	const nowMs = seamTs + tfMs // the seam bucket has closed

	r := math.Log(1.012)
	store := &baselineMockStore{
		mockDerivativesStore: mockDerivativesStore{
			series: []*derivatives.DerivativesSnapshot{
				seamSnapshot(seamTs, 900),
				provSnapshot(scoredTs, derivatives.SourceCoinalyze, 110),
				provSnapshot(scoredTs-tfMs, derivatives.SourceCoinalyze, 100),
			},
			priceIncrementValue: &r,
		},
		baseline: &derivatives.DerivativesBaseline{
			WindowN: 288, NMin: 30,
			OpenInterest: &derivatives.Baseline{Median: 0, MAD: 0.01, N: 288},
			Price:        &derivatives.Baseline{Median: 0, MAD: 0.004, N: 288},
		},
	}
	handler := newMarketDataHandlerWith(store, "binance", "future", candle.NewTimeframeRegistry(nil))
	handler.nowMs = func() int64 { return nowMs }

	// THE FIXTURE DISCRIMINATES: the bar the RAW series would choose is the stitched one,
	// one bar ahead of the one the reading scores. Without this the case would pass with any
	// implementation.
	if raw := scoredBarTimestamp(store.series, tfMs, nowMs); raw != seamTs {
		t.Fatalf("raw-series bar = %d, want %d — the fixture does not separate the two decisions",
			raw, seamTs)
	}

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/api/derivatives/BTCUSDT/series?timeframe=1h", nil)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, attachSymbolParam("BTCUSDT")))
	rec := httptest.NewRecorder()
	handler.GetDerivativesSeries(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var got struct {
		Normalized struct {
			Timestamp    int64 `json:"timestamp"`
			OpenInterest struct {
				Value *float64 `json:"value"`
				Z     *float64 `json:"z"`
			} `json:"zOpenInterest"`
			Price struct {
				Z *float64 `json:"z"`
			} `json:"zPrice"`
		} `json:"normalized"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.Normalized.Timestamp != scoredTs {
		t.Fatalf("axis timestamp = %d, want %d", got.Normalized.Timestamp, scoredTs)
	}
	if store.priceIncrementBar != got.Normalized.Timestamp {
		t.Fatalf("the price was resolved on bar %d and the axes stamp %d — the three axes "+
			"belong to different instants", store.priceIncrementBar, got.Normalized.Timestamp)
	}
	if got.Normalized.OpenInterest.Value == nil ||
		math.Abs(*got.Normalized.OpenInterest.Value-math.Log(110.0/100.0)) > 1e-12 {
		t.Fatalf("OI increment = %v, want %v", got.Normalized.OpenInterest.Value, math.Log(110.0/100.0))
	}
	if got.Normalized.OpenInterest.Z == nil || got.Normalized.Price.Z == nil {
		t.Fatalf("axis without z: %s", rec.Body.String())
	}
}
