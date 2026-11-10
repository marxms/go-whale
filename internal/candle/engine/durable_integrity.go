package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// ─────────────────────────────────────────────────────────────────────────────
// DURABLE TIER INTEGRITY IN FIVE DIMENSIONS
//
// # WHY MORE THAN ONE, AND NOT ONE WITH SEVERAL NAMES
//
// A per-column fill checker answers a single question: does that column have a non-null row?
// Three states it does NOT distinguish:
//
//	table with 200,854 rows and a 20h hole in the middle → all columns filled;
//	newly covered timeframe nobody has seeded yet .......→ no rows, and the checker
//	                                                        only talks about the columns it lists;
//	collection stopped three days ago ...................→ columns filled, series dead.
//
// # AND THE FIFTH, WHICH THE OTHER FOUR DO NOT MERELY LET THROUGH — THEY REWARD
//
// Count, continuity, per-column fill and per-timeframe coverage all ask
// about ONE column or about the INSTANT. None asks whether two columns contradict each other, and the
// funding group is precisely a set of columns tied by identity: a
// `funding_rate_8h` under an `n_step` that declares it has no evidence for that bucket is
// a number normalized by a cadence nobody observed there.
//
// WORSE THAN INVISIBLE: such a row INCREASES the fill of `funding_rate_8h`, which was the
// column whose zero opened this feature. The monitor rewarded the defect. That is how two
// routes — the preservation against erosion and the joining of the two tiers — produced and served the
// incoherent row without any of the four dimensions blinking.
//
// # THE LABEL RULE, AND HOW IT IS ENFORCED INSTEAD OF PROMISED
//
// The label names ONLY what the mechanism measured. It is not written by hand: it is DERIVED
// from the findings the pass actually produced (`MeasuredDimensions`). A dimension that stopped
// being computed disappears from the label on its own — and the ratchet compares the label with
// the declaration in both directions, so "three of four with the label of four" fails.
//
// A monitor whose name promises more than its mechanism measures is worse than no monitor;
// derivation is the answer to that.
// ─────────────────────────────────────────────────────────────────────────────

// DurableIntegrityDimension names ONE of the integrity quantities the contract requires.
type DurableIntegrityDimension string

const (
	// DimensionRowCount — how many rows the table carries per timeframe.
	DimensionRowCount DurableIntegrityDimension = "contagem_de_linhas"
	// DimensionTimeContinuity — how many buckets are missing between the oldest and the newest.
	DimensionTimeContinuity DurableIntegrityDimension = "continuidade_temporal"
	// DimensionColumnFill — which declared columns actually have a non-null row.
	DimensionColumnFill DurableIntegrityDimension = "preenchimento_por_coluna"
	// DimensionTimeframeCoverage — which timeframes of the declared coverage have a row.
	DimensionTimeframeCoverage DurableIntegrityDimension = "cobertura_por_timeframe"
	// DimensionFundingCoherence — how many rows carry an eight-hour equivalent
	// under a step that does not authorize normalization.
	DimensionFundingCoherence DurableIntegrityDimension = "coerencia_do_grupo_de_funding"
)

// declaredIntegrityDimensions is what the requirement REQUIRES to be measured.
//
// It is NOT the label. The label comes from `MeasuredDimensions`, and the difference between the two is
// exactly the finding the ratchet looks for.
func declaredIntegrityDimensions() []DurableIntegrityDimension {
	return []DurableIntegrityDimension{
		DimensionRowCount,
		DimensionTimeContinuity,
		DimensionColumnFill,
		DimensionTimeframeCoverage,
		DimensionFundingCoherence,
	}
}

// derivativesSpanReader reads a timeframe's snapshot in the durable table.
//
// The four quantities come from ONE query because they describe the same window, and two
// queries seconds apart would describe different windows — the newest of one would not
// be the newest of the other.
type derivativesSpanReader interface {
	DerivativesTimeframeSpan(ctx context.Context, timeframe string) (derivatives.DerivativesTimeframeSpan, error)
}

// derivativesCoherenceReader counts the timeframe's rows whose funding group is incoherent.
//
// A PORT OF ITS OWN, and not one more column in the fill counter: the question is not
// "does this column have a row", it is "do these two columns contradict each other". A per-column counter
// cannot ask it, and that is exactly why the incoherence crossed the other four
// dimensions unseen.
type derivativesCoherenceReader interface {
	CountDerivativesIncoherentFunding(ctx context.Context, timeframe string) (int64, error)
}

// DurableContinuityFinding is the continuity verdict for ONE timeframe.
type DurableContinuityFinding struct {
	Timeframe string
	// ExpectedBuckets is how many instants would fit between the oldest and the newest.
	ExpectedBuckets int64
	// ObservedBuckets is how many exist.
	ObservedBuckets int64
	// MissingBuckets is the difference. It is the finding.
	MissingBuckets int64
}

// DurableCoverageFinding is the coverage verdict for ONE timeframe.
type DurableCoverageFinding struct {
	Timeframe string
	Rows      int64
	// Covered is `Rows > 0`. Derived, not a boolean somebody fills in.
	Covered bool
	// WithinGrace says the timeframe has not had time to receive any row yet.
	WithinGrace bool
}

// DurableCoherenceFinding is the coherence verdict for ONE timeframe.
type DurableCoherenceFinding struct {
	Timeframe string
	// IncoherentRows are the rows that carry `funding_rate_8h` under a step that does not
	// authorize normalization. Zero is the healthy verdict, and it is MEASURED: a timeframe whose
	// query failed produces no finding at all, instead of producing zero.
	IncoherentRows int64
}

// DurableIntegrityReport is the result of ONE pass.
type DurableIntegrityReport struct {
	Fill       []DurableFillFinding
	Rows       map[string]int64
	Continuity []DurableContinuityFinding
	Coverage   []DurableCoverageFinding
	Coherence  []DurableCoherenceFinding
}

// MeasuredDimensions returns, in declared order, the dimensions THIS pass actually
// produced a finding for.
//
// IT IS DERIVED FROM THE FINDINGS, and that is what makes the label unable to lie: a dimension that
// stopped being computed disappears from here without anyone having to remember to remove it from the text.
func (r DurableIntegrityReport) MeasuredDimensions() []DurableIntegrityDimension {
	var out []DurableIntegrityDimension
	for _, dim := range declaredIntegrityDimensions() {
		switch dim {
		case DimensionRowCount:
			if len(r.Rows) > 0 {
				out = append(out, dim)
			}
		case DimensionTimeContinuity:
			if len(r.Continuity) > 0 {
				out = append(out, dim)
			}
		case DimensionColumnFill:
			if len(r.Fill) > 0 {
				out = append(out, dim)
			}
		case DimensionTimeframeCoverage:
			if len(r.Coverage) > 0 {
				out = append(out, dim)
			}
		case DimensionFundingCoherence:
			if len(r.Coherence) > 0 {
				out = append(out, dim)
			}
		}
	}
	return out
}

// UnmeasuredDimensions is the complement, and it is also published: a required dimension that
// this pass did not measure is a finding, not a silence.
func (r DurableIntegrityReport) UnmeasuredDimensions() []DurableIntegrityDimension {
	medidas := map[DurableIntegrityDimension]bool{}
	for _, dim := range r.MeasuredDimensions() {
		medidas[dim] = true
	}
	var out []DurableIntegrityDimension
	for _, dim := range declaredIntegrityDimensions() {
		if !medidas[dim] {
			out = append(out, dim)
		}
	}
	return out
}

// Integrity runs the pass over the declared dimensions.
//
// PER-COLUMN FILL REUSES `Check` instead of recalculating: it already has per-timeframe grace
// and readability-before-count order, and a second implementation here would be the usual
// divergence.
func (p *DurableFillProbe) Integrity(ctx context.Context) DurableIntegrityReport {
	if p == nil {
		return DurableIntegrityReport{}
	}
	report := DurableIntegrityReport{Fill: p.Check(ctx)}
	p.measureCoherence(ctx, &report)
	if p.spans == nil {
		// NOT KNOWING IS NOT ZERO. Without a span reader the three remaining dimensions are left
		// WITHOUT a finding, and that is how they disappear from the label instead of becoming a false zero.
		return report
	}

	report.Rows = map[string]int64{}
	for _, timeframe := range p.timeframes {
		span, err := p.spans.DerivativesTimeframeSpan(ctx, timeframe)
		if err != nil {
			logger.OrDiscard(p.log).Warn("durable integrity: timeframe window could not be read", "timeframe", timeframe, "error", err)
			continue
		}
		report.Rows[timeframe] = span.Rows
		report.Coverage = append(report.Coverage, DurableCoverageFinding{
			Timeframe:   timeframe,
			Rows:        span.Rows,
			Covered:     span.Rows > 0,
			WithinGrace: span.Rows == 0 && p.elapsed() < durableFillGrace(timeframe, p.cycleFloor),
		})
		if esperado, ok := expectedSpanBuckets(timeframe, span); ok {
			report.Continuity = append(report.Continuity, DurableContinuityFinding{
				Timeframe:       timeframe,
				ExpectedBuckets: esperado,
				ObservedBuckets: span.DistinctBuckets,
				MissingBuckets:  esperado - span.DistinctBuckets,
			})
		}
	}
	if len(report.Rows) == 0 {
		// An empty map would make `MeasuredDimensions` count the dimension as measured without a
		// single number behind it.
		report.Rows = nil
	}
	return report
}

// measureCoherence asks, per timeframe, how many rows contradict their own step.
//
// A PASS OF ITS OWN, AND BEFORE THE SPAN READER: coherence depends on no span, and
// tying it to the same `if p.spans == nil` would make the dimension disappear for an absence that is not
// its own. NOT KNOWING REMAINS NOT ZERO: a timeframe whose query failed produces no
// finding, and that is how the dimension shrinks out of the label instead of declaring the table coherent.
func (p *DurableFillProbe) measureCoherence(ctx context.Context, report *DurableIntegrityReport) {
	if p.coherence == nil {
		return
	}
	for _, timeframe := range p.timeframes {
		incoerentes, err := p.coherence.CountDerivativesIncoherentFunding(ctx, timeframe)
		if err != nil {
			logger.OrDiscard(p.log).Warn("durable integrity: funding group coherence could not be read",
				"timeframe", timeframe, "error", err)
			continue
		}
		report.Coherence = append(report.Coherence, DurableCoherenceFinding{
			Timeframe:      timeframe,
			IncoherentRows: incoerentes,
		})
	}
}

// expectedSpanBuckets is how many instants would fit between the oldest and the newest.
//
// `false` when there is no way to ask: empty table, timeframe with unknown duration, or
// a window of a single bucket — none of them admits a continuity verdict, and inventing
// zero there would be declaring whole a series nobody measured.
func expectedSpanBuckets(timeframe string, span derivatives.DerivativesTimeframeSpan) (int64, bool) {
	if span.Rows == 0 || span.Newest.Before(span.Oldest) {
		return 0, false
	}
	bucket := int64(candle.ParseTimeframeToSeconds(timeframe))
	if bucket <= 0 {
		return 0, false
	}
	return span.Newest.Unix()/bucket - span.Oldest.Unix()/bucket + 1, true
}

// ReportIntegrity runs the pass and publishes the verdict in every declared dimension.
//
// IT WARNS, NEVER TAKES DOWN — the same doctrine as `Report`, for the same reason: zero rows admits
// two causes with opposite consequences, and this feature's first boot has zero by
// definition.
func (p *DurableFillProbe) ReportIntegrity(ctx context.Context) {
	if p == nil {
		return
	}
	report := p.Integrity(ctx)

	for timeframe, rows := range report.Rows {
		p.metrics.setDurableRows(timeframe, float64(rows))
	}
	for _, c := range report.Continuity {
		p.metrics.setDurableMissingBuckets(c.Timeframe, float64(c.MissingBuckets))
	}
	// THE THIRD DIMENSION BECOMES A NUMBER HERE, and not just a log line.
	//
	// Log text tells the episode once and does not accumulate across cycles: it is not possible to
	// ask a log file whether this is getting worse, nor to alert on it.
	for _, f := range report.Fill {
		coberta := 0.0
		if f.State == FillFilled {
			coberta = 1
		}
		p.metrics.setDurableColumnCovered(f.Timeframe, f.Column, coberta)
	}
	for _, c := range report.Coverage {
		covered := 0.0
		if c.Covered {
			covered = 1
		}
		p.metrics.setDurableTimeframeCovered(c.Timeframe, covered)
	}
	// THE FIFTH DIMENSION BECOMES A NUMBER HERE, and it is the only one that gets WORSE when ignored: an
	// incoherent row adds to the fill of `funding_rate_8h`, so the other four
	// read it as health.
	for _, c := range report.Coherence {
		p.metrics.setDurableIncoherentFunding(c.Timeframe, float64(c.IncoherentRows))
	}
	medidas := report.MeasuredDimensions()
	p.metrics.setDurableDimensionsMeasured(float64(len(medidas)))

	// THE LABEL IS DERIVED. It lists what was measured, and never what was required.
	logger.OrDiscard(p.log).Info("durable integrity: dimensions measured in this pass",
		"dimensions", dimensionList(medidas))
	if faltando := report.UnmeasuredDimensions(); len(faltando) > 0 {
		logger.OrDiscard(p.log).Warn("durable integrity: required dimensions were not measured in this pass",
			"dimensions", dimensionList(faltando))
	}

	for _, c := range report.Continuity {
		if c.MissingBuckets > 0 {
			logger.OrDiscard(p.log).Warn("durable integrity: buckets without any row",
				"timeframe", c.Timeframe, "missing_buckets", c.MissingBuckets, "expected_buckets", c.ExpectedBuckets)
		}
	}
	for _, c := range report.Coherence {
		if c.IncoherentRows > 0 {
			logger.OrDiscard(p.log).Warn("durable integrity: rows carry funding_rate_8h under a step that does not authorize normalization",
				"timeframe", c.Timeframe, "incoherent_rows", c.IncoherentRows)
		}
	}
	var descobertos []string
	for _, c := range report.Coverage {
		if c.Covered || c.WithinGrace {
			continue
		}
		descobertos = append(descobertos, c.Timeframe)
	}
	if len(descobertos) > 0 {
		sort.Strings(descobertos)
		logger.OrDiscard(p.log).Warn("durable integrity: declared-coverage timeframes have no rows; capping their hot window would destroy the only copy",
			"timeframes", strings.Join(descobertos, ", "))
	}
}

func dimensionList(dims []DurableIntegrityDimension) string {
	if len(dims) == 0 {
		return "NENHUMA"
	}
	out := make([]string, 0, len(dims))
	for _, d := range dims {
		out = append(out, string(d))
	}
	return fmt.Sprintf("%d/%d — %s", len(dims), len(declaredIntegrityDimensions()), strings.Join(out, ", "))
}
