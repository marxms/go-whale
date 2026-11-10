package httpapi

import (
	"encoding/json"
	"testing"

	"github.com/marxms/go-whale/internal/derivatives"
)

func TestBatchCoverageCountsOnlyEmittedMeasurements(t *testing.T) {
	series := []*derivatives.DerivativesSnapshot{
		{
			Timestamp: 900000, OpenInterestBase: covF64(0), FundingRate: covF64(0),
			FundingRate8h: covF64(0), NStep: derivatives.NFromMeasuredSpacing, LongShortRatio: covF64(0),
		},
		{Timestamp: 600000, OpenInterestBase: covF64(11), FundingRate8h: covF64(0.1)},
		{Timestamp: 300000, LongShortRatio: covF64(1.1)},
		nil,
	}
	h := newMarketDataHandlerWith(&mockDerivativesStore{series: series}, "binance", "future", nil)
	w := batchRequest(t, h, "/api/batch/derivatives-series?timeframe=5m&limit=5&symbols=EWZ/USDT")
	if w.Code != 200 {
		t.Fatalf("HTTP %d: %s", w.Code, w.Body.String())
	}
	var response BatchDerivativesSeriesEnvelope
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	coverage := response.Coverage["EWZ/USDT"]
	if coverage.Requested != 5 || coverage.Served != 3 || !coverage.Partial || coverage.Reason != "the available series ends before the requested range" {
		t.Fatalf("wrong bucket coverage: %+v", coverage)
	}
	for field, want := range map[string]int{
		"openInterestBase": 2, "fundingRate": 1, "fundingRate8h": 1, "longShortRatio": 2,
	} {
		if got := coverage.Fields[field]; got != want {
			t.Errorf("coverage.fields.%s=%d, want %d", field, got, want)
		}
	}
	if response.Data["EWZ/USDT"][1].FundingRate8h != nil {
		t.Fatal("funding without interval provenance must not be emitted or counted")
	}
}

func TestBatchCoverageDeclaresFullyAbsentFields(t *testing.T) {
	h := newMarketDataHandlerWith(&mockDerivativesStore{series: []*derivatives.DerivativesSnapshot{
		{Timestamp: 300000, OpenInterestBase: covF64(10)},
	}}, "binance", "future", nil)
	w := batchRequest(t, h, "/api/batch/derivatives-series?timeframe=5m&limit=1&symbols=EWZ/USDT")
	var response BatchDerivativesSeriesEnvelope
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	coverage := response.Coverage["EWZ/USDT"]
	if coverage.Partial || coverage.Served != 1 || coverage.Requested != 1 {
		t.Fatalf("wrong bucket coverage: %+v", coverage)
	}
	for _, field := range []string{"fundingRate", "fundingRate8h", "longShortRatio"} {
		if got, exists := coverage.Fields[field]; !exists || got != 0 {
			t.Errorf("absent %s must be declared zero: got=%d exists=%t", field, got, exists)
		}
	}
	if coverage.Fields["openInterestBase"] != 1 {
		t.Fatalf("measured OI coverage=%d, want 1", coverage.Fields["openInterestBase"])
	}
}

func covF64(v float64) *float64 { return &v }

// COVERAGE BY BUCKET COUNT IS NOT COVERAGE BY FIELD, and a consumer can read the first as if
// it were the second.
//
// MEASURED on the LAN: `/api/derivatives/BTC%2FUSDT/series?timeframe=2h&limit=800`
// returns 800 points — `requested: 800, served: 800, partial: false` — and **232 of them have
// no `openInterestBase`**. The envelope did not lie about what it promised (buckets), but
// nothing in it allowed discovering that 29% of the field the panel plots was absent. The
// chart then drew a straight line over the hole, and nothing in the payload contradicted the
// drawing.
//
// The declaration is ADDITIVE PER FIELD, which is the same discipline
// `BatchDerivativesSeriesEntry` already applies just below in the file: nothing that was on
// the wire is reinterpreted.
func TestSeriesCoverageDeclaresFieldFillNotOnlyBucketCount(t *testing.T) {
	pontos := []DerivativesSeriesPoint{
		{Snapshot: &derivatives.DerivativesSnapshot{OpenInterestBase: covF64(1), FundingRate: covF64(0.0001), LongShortRatio: covF64(1.1)}},
		{Snapshot: &derivatives.DerivativesSnapshot{OpenInterestBase: covF64(2), LongShortRatio: covF64(1.2)}},
		{Snapshot: &derivatives.DerivativesSnapshot{LongShortRatio: covF64(1.3)}},
		{Snapshot: nil},
	}

	cov := newSeriesCoverage(4, len(pontos), pontos)

	if cov.Partial {
		t.Fatalf("four requested and four served is not partial: %+v", cov)
	}
	// THIS IS EXACTLY WHERE AN ENVELOPE WITHOUT PER-FIELD FILL GOES SILENT: fully served
	// buckets, a field with a hole.
	if cov.Fields == nil {
		t.Fatal("coverage does not declare per-field fill — the consumer has no way to " +
			"know the plotted field is holed in an envelope that says partial=false")
	}
	for campo, quer := range map[string]int{
		"openInterestBase": 2,
		"fundingRate":      1,
		"longShortRatio":   3,
		"fundingRate8h":    0,
	} {
		if got := cov.Fields[campo]; got != quer {
			t.Errorf("%s filled in %d point(s), expected %d", campo, got, quer)
		}
	}
}

// AND A COMPLETELY ABSENT FIELD MUST APPEAR AS ZERO, not vanish from the map: a field that
// vanishes reads as "I did not ask", and the durable tier's `funding_rate_8h` is precisely
// the absolute-zero case the spec requires to be declared.
func TestSeriesCoverageKeepsAFullyAbsentFieldAsZero(t *testing.T) {
	pontos := []DerivativesSeriesPoint{
		{Snapshot: &derivatives.DerivativesSnapshot{OpenInterestBase: covF64(1)}},
	}
	cov := newSeriesCoverage(1, 1, pontos)

	if _, ok := cov.Fields["fundingRate8h"]; !ok {
		t.Fatal("a fully absent field vanished from the map — declared absence becomes silent absence")
	}
	if cov.Fields["fundingRate8h"] != 0 {
		t.Fatalf("fundingRate8h = %d, expected 0", cov.Fields["fundingRate8h"])
	}
}
