package engine

import (
	"context"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/derivatives"
)

// ─────────────────────────────────────────────────────────────────────────────
// THE DIMENSIONS AS A CONTINUOUS SIGNAL, ASSERTED AT EMISSION
//
// Asserting the `DurableIntegrityReport` — the CALCULATION — is not enough: deleting the gauge
// publications would leave the whole suite green.
//
// A report nobody publishes is not a continuous signal: it is a struct that dies at the end of the
// function. The number is what accumulates across cycles and becomes an alert; text tells the
// episode once.
//
// A GAUGE, NOT A COUNTER, ALLOWS READING THE ABSOLUTE. Each `Set` overwrites, and each case reads
// immediately after its own call — there is no delta to accumulate and no test order to
// respect.
// ─────────────────────────────────────────────────────────────────────────────

// TestDurableIntegrity_TheDeclaredDimensionsAreEmittedAsGauges ⭐ pins the dimensions at the
// emission boundary.
//
// THE MUTANT IT KILLS: deleting the gauge publications while keeping the calculation. It also kills the
// mutant that deletes only the per-column fill publication — the dimension that was the only
// one with no metric at all, and the one the user story names as an example.
//
// THE INCOHERENT ROW ENTERS IN THE SAME MEASURED SHAPE, as a third defect: it does not change
// any of the gauges of the other four dimensions, and that is why it needs its own.
func TestDurableIntegrity_TheDeclaredDimensionsAreEmittedAsGauges(t *testing.T) {
	// The shape measured on the LAN, with the two defects the dimensions exist to find: a
	// hole of 20 buckets in the middle of a 2,000-bucket window, and `funding_rate_8h` declared
	// covered with ZERO rows while every other column is full.
	rows := todasAsColunasCheias("1d", 60000)
	rows["1d"]["funding_rate_8h"] = 0
	comBuraco := diariosContinuos(2000, 30)
	comBuraco.DistinctBuckets = 1980
	spans := &fakeSpanReader{spans: map[string]derivatives.DerivativesTimeframeSpan{"1d": comBuraco}}
	// And the third defect, the one only the fifth dimension sees: rows with an eight-hour
	// equivalent under a step that does not authorize it.
	coherence := &fakeCoherenceReader{incoerentes: map[string]int64{"1d": 137}}

	// Grace period expired: two daily buckets are 48h and the process already has 72.
	probe := probeCompleto(&fakeFillCounter{rows: rows}, spans, coherence, 72*time.Hour)
	rec := newEngineTestMetrics()
	probe.SetMetrics(rec.port())
	probe.ReportIntegrity(context.Background())

	for _, tc := range []struct {
		dimension string
		got       float64
		want      float64
		reason    string
	}{
		{
			dimension: string(DimensionRowCount),
			got:       rec.durableRows["1d"],
			want:      60000,
			reason:    "2,000 buckets x 30 symbols",
		},
		{
			dimension: string(DimensionTimeContinuity),
			got:       rec.durableMiss["1d"],
			want:      20,
			reason:    "2,000 expected, 1,980 observed — and the per-column count does not see it",
		},
		{
			dimension: string(DimensionColumnFill) + " (the EMPTY column)",
			got:       rec.durableColumn[key("1d", "funding_rate_8h")],
			want:      0,
			reason:    "declared covered with not a single row, the defect that opened the feature",
		},
		{
			dimension: string(DimensionColumnFill) + " (a FULL column)",
			got:       rec.durableColumn[key("1d", "funding_rate")],
			want:      1,
			reason:    "the finding is per column, and a gauge that zeroed the whole table would distinguish nothing",
		},
		{
			dimension: string(DimensionTimeframeCoverage),
			got:       rec.durableTF["1d"],
			want:      1,
			reason:    "the timeframe has a row, even if one of its columns does not",
		},
		{
			dimension: string(DimensionFundingCoherence),
			got:       rec.durableIncoh["1d"],
			want:      137,
			reason:    "the group contradicts itself in those rows, and they ADD to the column fill — the monitor rewarded the defect",
		},
		{
			dimension: "measured dimensions",
			got:       rec.durableDims,
			want:      float64(len(declaredIntegrityDimensions())),
			reason:    "the sensor of the sensor: every required dimension was measured in this pass",
		},
	} {
		if tc.got != tc.want {
			t.Errorf("%s published as %v, want %v (%s) — a report nobody publishes "+
				"is not a continuous signal", tc.dimension, tc.got, tc.want, tc.reason)
		}
	}
}

// TestDurableIntegrity_WithoutASpanReaderTheEmissionShrinksWithTheMechanism is the control that
// prevents the assertions above from being satisfied by fixed numbers.
//
// Without a span reader three dimensions are left WITHOUT a mechanism: the sensor-of-the-sensor gauge drops
// to ONE, and the per-column fill — the only one that can still be measured — continues
// to be published. The number shrinks together with the mechanism.
func TestDurableIntegrity_WithoutASpanReaderTheEmissionShrinksWithTheMechanism(t *testing.T) {
	rows := todasAsColunasCheias("4h", 1000)
	rows["4h"]["funding_rate_8h"] = 0

	probe := NewDurableFillProbe(DurableFillProbeOptions{
		Counter:    &fakeFillCounter{rows: rows},
		Timeframes: []string{"4h"},
		CycleFloor: time.Hour,
		Elapsed:    func() time.Duration { return 72 * time.Hour },
	})
	rec := newEngineTestMetrics()
	probe.SetMetrics(rec.port())
	probe.ReportIntegrity(context.Background())

	if got := rec.durableDims; got != 1 {
		t.Errorf("measured dimensions published as %v, want 1 — without a span reader only "+
			"per-column fill has a mechanism, and a fixed number would be the "+
			"monitor that promises what it does not measure", got)
	}
	if got := rec.durableColumn[key("4h", "funding_rate_8h")]; got != 0 {
		t.Errorf("the empty 4h column came out as %v, want 0 — the dimension that HAS a mechanism "+
			"continues to be published", got)
	}
	if got := rec.durableColumn[key("4h", "funding_rate")]; got != 1 {
		t.Errorf("the full 4h column came out as %v, want 1", got)
	}
}
