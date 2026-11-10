package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// ─────────────────────────────────────────────────────────────────────────────
// DURABLE FILL CHECKER
//
// Two earlier checks of the coverage declaration could both pass with `funding_rate_8h` at ZERO
// ROWS:
//
//	1. against the struct — every snapshot field appears in the declaration;
//	2. against the WRITE — the set of columns the write emits equals the declared set.
//
// The second checks that `if p.FundingRate8h != nil` EXISTS. It does not check that the
// pointer was ever non-nil. A column can be declared, have a schema mechanism, have a write
// branch, be read by the query — and have not a single row.
//
// This checker adds the third dimension (rows actually written) and the fourth (the read reaches
// the column). Together they mean coverage is written AND readable. Without them the chain closes
// against us — column written, declaration says covered, cap released, and the only copy that
// reaches the client is destroyed.
//
// # WHY IT WARNS AND DOES NOT FAIL
//
// The FATAL verdict is reserved for a gate whose condition is the comparison of TWO READ NUMBERS.
// Zero rows is not that: it admits two causes with opposite consequences — a write regression, and
// "it has not warmed up yet". The FIRST BOOT has zero rows BY DEFINITION, because the columns have
// just been born. A fatal gate here would take down the deployment that installs the feature
// itself, which is why the grace period exists and why the worst verdict remains a warning.
// ─────────────────────────────────────────────────────────────────────────────

// fillSeverity is the weight of a finding, and it is what decides the log line level.
//
// It exists as a VALUE, not as a decision embedded in the logger call, because the promise
// "warns without blocking boot" is only checkable if there is something to compare.
// `fillWarn` is this checker's CEILING, and the ratchet walks every state requiring that none
// exceeds it: a new step, whatever its name, fails there.
type fillSeverity int

const (
	// fillOK is routine: either the column is filled, or it is still within the grace period.
	fillOK fillSeverity = iota
	// fillWarn is the worst verdict this checker emits.
	fillWarn
)

// DurableFillState names what the checker found for ONE (timeframe, field).
type DurableFillState string

const (
	// FillFilled — the column carries rows.
	FillFilled DurableFillState = "preenchido"
	// FillWithinGrace — zero rows, but the process has not had time to fill it yet.
	FillWithinGrace DurableFillState = "within grace"
	// FillEmpty — zero rows after the grace period. It is the finding the feature exists to
	// keep from going unnoticed.
	FillEmpty DurableFillState = "vazio"
	// FillUnreadable — the column is written and the durable read does not request it, so it never
	// reaches any consumer. It does not count as coverage, however many rows it has.
	FillUnreadable DurableFillState = "unreadable by the read path"
)

// severityOf is the table that decides the weight of each state.
//
// A TABLE AND NOT A SCATTERED `if`: the ratchet walks it to assert that NO state exceeds
// `fillWarn`. A new state without an entry here falls into `fillWarn`, which is the safe side —
// loud and not fatal.
func severityOf(state DurableFillState) fillSeverity {
	switch state {
	case FillFilled, FillWithinGrace:
		return fillOK
	default:
		return fillWarn
	}
}

// DurableFillFinding is the verdict per (timeframe, field).
type DurableFillFinding struct {
	Timeframe string
	Field     string
	Column    string
	// Rows is the count of NON-NULL rows for that column in that timeframe.
	Rows  int64
	State DurableFillState
}

// derivativesFillCounter counts, per column, how many rows of a timeframe carry it.
//
// It is a seam because the property under test is the DECISION about the count — what counts
// as empty, what counts as grace —, and it is invisible behind a connection pool.
type derivativesFillCounter interface {
	CountDerivativesColumnRows(ctx context.Context, timeframe string, columns []string) (map[string]int64, error)
}

// DurableFillProbeOptions assembles the checker.
type DurableFillProbeOptions struct {
	Counter    derivativesFillCounter
	Timeframes []string
	// CycleFloor is the grace FLOOR: the cadence at which collection runs. A column cannot
	// be charged before a cycle that would write it exists.
	CycleFloor time.Duration
	// Elapsed tells how long this collection has had to fill the columns. It is a
	// function because the checker runs more than once: at startup it is ~zero, and that is what
	// prevents the premature verdict of the first boot.
	Elapsed func() time.Duration
	// Spans reads each timeframe's window: rows, distinct buckets, oldest and newest.
	// It is through it that the other three integrity dimensions exist.
	//
	// NIL IS ALLOWED, and it is not silent degradation: without a span reader the three
	// dimensions have no finding, disappear from the derived label, and the warning line names them.
	Spans derivativesSpanReader
	// Coherence answers the GROUP COHERENCE dimension: how many rows carry an
	// eight-hour equivalent under a step that does not authorize it.
	//
	// NIL IS ALLOWED, by the same rule as `Spans`: without a reader the dimension has no finding,
	// disappears from the derived label and is named in the warning line — it never becomes a zero that
	// disguises itself as a coherent table.
	Coherence derivativesCoherenceReader
	Log       *slog.Logger
}

// DurableFillProbe checks the DECLARED coverage against what the table actually carries.
type DurableFillProbe struct {
	counter    derivativesFillCounter
	timeframes []string
	cycleFloor time.Duration
	elapsed    func() time.Duration
	spans      derivativesSpanReader
	coherence  derivativesCoherenceReader
	metrics    Metrics
	log        *slog.Logger
}

// SetMetrics injects the observability port. The composition root calls it before the first
// integrity pass.
func (p *DurableFillProbe) SetMetrics(m Metrics) {
	if p != nil {
		p.metrics = m
	}
}

// NewDurableFillProbe assembles the checker. It returns nil when there is no one to count from — without
// a durable tier there is no question to ask, and a checker that warned about a table
// that is not wired would be an alarm about the configuration itself.
func NewDurableFillProbe(opts DurableFillProbeOptions) *DurableFillProbe {
	if opts.Counter == nil || len(opts.Timeframes) == 0 {
		return nil
	}
	if opts.Elapsed == nil {
		opts.Elapsed = func() time.Duration { return 0 }
	}
	return &DurableFillProbe{
		counter:    opts.Counter,
		timeframes: append([]string(nil), opts.Timeframes...),
		cycleFloor: opts.CycleFloor,
		elapsed:    opts.Elapsed,
		spans:      opts.Spans,
		coherence:  opts.Coherence,
		log:        logger.OrDiscard(opts.Log),
	}
}

// durableFillGraceBuckets is how many timeframe buckets make up the grace period.
//
// TWO, and the pair is justified: the first is the bucket that must CLOSE for the collection to have
// something to write; the second is the following pass, because the cycle tail requests the latest buckets and
// it is that tail that rewrites what the previous pass left incomplete. One alone would charge the column
// at the instant the first bucket closes, which is before any rewrite.
const durableFillGraceBuckets = 2

// durableFillGrace is the DECLARED grace period for a timeframe.
//
// It is PER TIMEFRAME because the time to fill belongs to the timeframe: a daily bucket takes a
// day to close and an hourly one takes an hour, and charging both on the same window would flag
// the daily one every day for not being hourly. The floor is the collection cadence — no column
// can be charged before a cycle that would write it exists.
func durableFillGrace(timeframe string, cycleFloor time.Duration) time.Duration {
	bucket := time.Duration(candle.ParseTimeframeToSeconds(timeframe)) * time.Second
	grace := durableFillGraceBuckets * bucket
	if grace < cycleFloor {
		return cycleFloor
	}
	return grace
}

// declaredDurableFields returns, sorted by field, the (field, column) pairs that the
// coverage declaration asserts the durable tier carries.
//
// It READS THE DECLARATION instead of repeating it: a second list here would be the divergence the
// checker exists to catch, written inside the checker itself.
func declaredDurableFields() [][2]string {
	var out [][2]string
	for field, column := range redisstore.DerivativesDurableCoverage() {
		if column == "" {
			continue
		}
		out = append(out, [2]string{field, column})
	}
	sort.Slice(out, func(i, j int) bool { return out[i][0] < out[j][0] })
	return out
}

// Check checks each declared field in each timeframe and returns the findings, sorted.
func (p *DurableFillProbe) Check(ctx context.Context) []DurableFillFinding {
	if p == nil {
		return nil
	}

	campos := declaredDurableFields()
	readable := make(map[string]bool, len(derivatives.DurableReadColumns()))
	for _, coluna := range derivatives.DurableReadColumns() {
		readable[coluna] = true
	}

	colunas := make([]string, 0, len(campos))
	for _, par := range campos {
		colunas = append(colunas, par[1])
	}

	var out []DurableFillFinding
	for _, timeframe := range p.timeframes {
		contagem, err := p.counter.CountDerivativesColumnRows(ctx, timeframe, colunas)
		if err != nil {
			// NOT KNOWING IS NOT "IT IS EMPTY". A query that did not answer becomes a warning through
			// its own error, and not a per-field finding that would flag six healthy columns.
			logger.OrDiscard(p.log).Warn("durable coverage: could not count rows", "timeframe", timeframe, "error", err)
			continue
		}
		carencia := durableFillGrace(timeframe, p.cycleFloor)
		for _, par := range campos {
			field, column := par[0], par[1]
			out = append(out, DurableFillFinding{
				Timeframe: timeframe,
				Field:     field,
				Column:    column,
				Rows:      contagem[column],
				State:     fillStateOf(contagem[column], readable[column], p.elapsed(), carencia),
			})
		}
	}
	return out
}

// fillStateOf is the decision, isolated from the data feeding it.
//
// THE ORDER OF THE QUESTIONS IS THE DECISION. Readability comes FIRST because it depends on neither
// time nor count: a column the read does not request reaches no consumer at all,
// even if it has a million rows. Approving it for having rows would be declaring covered a column
// the chart client never sees — which is exactly the half the two
// previous checkers were missing.
func fillStateOf(rows int64, readable bool, elapsed, grace time.Duration) DurableFillState {
	if !readable {
		return FillUnreadable
	}
	if rows > 0 {
		return FillFilled
	}
	if elapsed < grace {
		return FillWithinGrace
	}
	return FillEmpty
}

// Report runs the checker and records the verdict.
//
// IT DOES NOT RETURN AN ERROR, and the absence of a return is half of the promise: there is no way
// for the caller to turn this verdict into a boot refusal. The other half is `Blocks`,
// which names the same promise as a checkable value.
func (p *DurableFillProbe) Report(ctx context.Context) {
	if p == nil {
		return
	}
	achados := p.Check(ctx)
	if len(achados) == 0 {
		// ZERO FINDINGS IS NOT CHECKED COVERAGE. `Check` returns one finding per declared (field,
		// timeframe), so the list only comes out empty when NO count answered. Without this line,
		// a checker with no answer and an approving checker would produce the same log. The count
		// that failed already names the timeframe; this line names the CONSEQUENCE — nothing was
		// checked — so the absence of a verdict does not pass for a verdict.
		logger.OrDiscard(p.log).Warn("durable coverage: nothing was checked; no count answered and the declaration could not be compared with the table",
			"timeframes", len(p.timeframes))
		return
	}

	var issues []string
	preenchidos, carencia := 0, 0
	for _, f := range achados {
		if severityOf(f.State) == fillOK {
			if f.State == FillFilled {
				preenchidos++
			} else {
				carencia++
			}
			continue
		}
		issues = append(issues, fmt.Sprintf("%s.%s=%s (%d linha[s])",
			f.Timeframe, f.Column, f.State, f.Rows))
	}

	if len(issues) == 0 {
		logger.OrDiscard(p.log).Info("durable coverage: fields filled per timeframe",
			"filled", preenchidos, "in_grace", carencia)
		return
	}
	// LOUD AND NAMING THE FIELD, and still a warning: see the top block. The operator
	// needs the name because the consequence is per field — capping the cell of a field
	// declared covered and empty destroys its only readable copy.
	logger.OrDiscard(p.log).Warn("durable coverage declared and not delivered; capping the hot window would destroy the only readable copy",
		"fields", strings.Join(issues, ", "))
}
