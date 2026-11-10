package questdb

import (
	"testing"

	"github.com/marxms/go-whale/internal/derivatives"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// THE REFUSED ROW COMES OUT AT ERROR.
//
// A row without source, denomination or convention is REFUSED instead of written with the
// field blank, because blank it is indistinguishable, on read, from a measured row. The refusal
// is SILENT for the caller (the batch continues) — so the log is the only evidence, and at the
// process default level anything below `Error` does not come out.
func TestRefusedRow_LogsAtErrorLevel(t *testing.T) {
	for _, tc := range []struct {
		name     string
		mutate   func(*derivatives.DerivativesColdPoint)
		fragment string
	}{
		// The `fragment` values are substrings of the domain's error text.
		{"no source", func(p *derivatives.DerivativesColdPoint) { p.Source = "" }, "without a source"},
		{"no denomination", func(p *derivatives.DerivativesColdPoint) { p.DenominatedIn = "" }, "without a denomination"},
		{"no convention", func(p *derivatives.DerivativesColdPoint) { p.LabelConvention = "" }, "without a label convention"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log, sink := logger.NewCapture(logger.LevelDebug)

			point := samplePoints(1)[0]
			tc.mutate(&point)

			writable, refused := admissibleDerivativesRows([]derivatives.DerivativesColdPoint{point}, log)
			if len(writable) != 0 || refused != 1 {
				t.Fatalf("writable=%d refused=%d, expected 0 and 1", len(writable), refused)
			}

			level, found := sink.LevelOf(tc.fragment)
			if !found {
				t.Fatalf("the row was refused silently; captured: %v", sink.Records())
			}
			if level != logger.LevelError {
				t.Errorf("the refusal came out at %s, expected %s — the refusal is silent for the caller, the log is the only evidence",
					level, logger.LevelError)
			}
		})
	}
}

// NEGATIVE CONTROL AND THE DISTINCTION THAT MATTERS: an EMPTY row is also not written, and does
// NOT log. An empty row is a cycle in which the provider returned nothing for that bucket —
// routine. A row without a declaration is an assembly defect. Treating the two the same way
// would drown the real alarm in noise.
func TestEmptyRow_IsRefusedWithoutLogging(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	ponto := samplePoints(1)[0]
	ponto.OpenInterestBase, ponto.FundingRate, ponto.FundingRate8h, ponto.LongShortRatio = nil, nil, nil, nil

	gravaveis, recusadas := admissibleDerivativesRows([]derivatives.DerivativesColdPoint{ponto}, log)
	if len(gravaveis) != 0 || recusadas != 1 {
		t.Fatalf("writable=%d refused=%d, expected 0 and 1", len(gravaveis), recusadas)
	}
	for _, r := range sink.Records() {
		if r.Level == logger.LevelError {
			t.Errorf("empty row logged at ERROR: %q", r.Text())
		}
	}
}

// AND THE COMPLETE ROW PASSES — without it, "refuse everything" would satisfy the cases above.
func TestDeclaredRow_IsAdmitted(t *testing.T) {
	gravaveis, recusadas := admissibleDerivativesRows(samplePoints(3))
	if len(gravaveis) != 3 || recusadas != 0 {
		t.Fatalf("writable=%d refused=%d, expected 3 and 0", len(gravaveis), recusadas)
	}
}
