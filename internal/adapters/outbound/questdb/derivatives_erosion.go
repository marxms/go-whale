package questdb

import (
	"context"
	"log/slog"
	"time"

	"github.com/marxms/go-whale/internal/derivatives"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// derivativesColdDegradedTotal counts, PER ROW, what the erosion mechanism chose in a
// degraded cycle.
//
// COUNTABLE, not merely narrated: degradation is rare and silent by nature, and one log
// line tells the episode once and does not sum with the others.
//
// The label is WHAT WAS CHOSEN, and only that: `preserved` means the old data survived the
// rewrite, `hole` means the NEW data did not enter.
//
//	rate(gowhale_derivatives_cold_degraded_rows_total{outcome="hole"}[6h]) > 0
const (
	outcomePreserved = "preserved"
	outcomeHole      = "hole"
)

// recordErosionOutcome publishes the cycle's choice as an aggregable number.
func (q *Store) recordErosionOutcome(o erosionOutcome) {
	if o.preserved > 0 {
		q.metrics.addDerivativesColdDegraded(outcomePreserved, float64(o.preserved))
	}
	if o.holes > 0 {
		q.metrics.addDerivativesColdDegraded(outcomeHole, float64(o.holes))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// EROSION BY PARTIAL REWRITE
//
// Measured in this project: on a table with DEDUP, omitting a column in the row that
// rewrites a bucket ERASES the value that was there. ILP has no NULL, so "I did not send
// this column" and "this column is now nothing" are the same row on the wire.
//
// THE SOURCE OF TRUTH IS THE TABLE ITSELF, read through the same door the window read uses
// (`QueryDerivativesRange`), restricted to the batch's scope and to the range of buckets it
// rewrites. Process memory DOES NOT EXIST here: `newCoinalyzePointSet()` is born from zero
// on every cycle, and the deep seeding runs once per process.
// ─────────────────────────────────────────────────────────────────────────────

// derivativesPriorSource is the source of truth about what a durable row already carries.
type derivativesPriorSource interface {
	QueryDerivativesRange(ctx context.Context, scope derivatives.DerivativesScope, from, to time.Time) ([]*derivatives.DerivativesSnapshot, error)
}

// erosionOutcome is the mechanism's choice, counted PER ROW.
type erosionOutcome struct {
	// preserved are the rows in which at least one omitted column was recovered from the
	// previous row before the rewrite.
	preserved int
	// holes are the rows discarded to avoid erosion: the previous value stays, the new one
	// does not enter.
	holes int
}

func (o erosionOutcome) degraded() bool { return o.preserved > 0 || o.holes > 0 }

// completeRow reports whether the row carries ALL durable quantities.
//
// THE TWO UNIT COLUMNS COUNT HERE: the question is "was any column OMITTED", not "was any
// quantity measured". A row with the four older values and without `n_step` rewrites the
// bucket and erases the rung that was there.
func completeRow(p derivatives.DerivativesColdPoint) bool {
	return p.OpenInterestBase != nil && p.FundingRate != nil &&
		p.FundingRate8h != nil && p.LongShortRatio != nil &&
		p.FundingIntervalHours != nil && p.NStep != "" &&
		p.OpenInterest != nil && p.NextFundingRate != nil &&
		p.InterestRate != nil && p.FundingAtAnchor != nil &&
		p.LSLongPct != nil && p.LSShortPct != nil
}

func scopeOf(p derivatives.DerivativesColdPoint) derivatives.DerivativesScope {
	return derivatives.DerivativesScope{
		Source:     p.Source,
		Exchange:   p.Exchange,
		MarketType: p.MarketType,
		Symbol:     p.Symbol,
		Timeframe:  p.Timeframe,
	}
}

// preserveOmittedColumns returns the rows to write, with omitted columns recovered from the
// row already in the table, and the choice made per row.
func preserveOmittedColumns(
	ctx context.Context,
	priors derivativesPriorSource,
	points []derivatives.DerivativesColdPoint,
	logs ...*slog.Logger,
) ([]derivatives.DerivativesColdPoint, erosionOutcome) {
	log := logger.OrDiscard(logs...)

	var outcome erosionOutcome
	scopes := scopesAtRisk(points)
	if len(scopes) == 0 {
		return points, outcome
	}

	// One read per scope, over the span that scope rewrites.
	known := make(map[derivatives.DerivativesScope]map[int64]*derivatives.DerivativesSnapshot, len(scopes))
	unreadable := make(map[derivatives.DerivativesScope]bool, len(scopes))
	for scope, span := range scopes {
		rows, err := priors.QueryDerivativesRange(ctx, scope, span.from, span.to)
		if err != nil {
			// NOT KNOWING is not "there is none". The incomplete row of this scope is not
			// written.
			unreadable[scope] = true
			log.Error("questdb derivatives: could not read what the scope already carried; incomplete rows will not be written",
				"symbol", scope.Symbol, "timeframe", scope.Timeframe, "error", err)
			continue
		}
		byBucket := make(map[int64]*derivatives.DerivativesSnapshot, len(rows))
		for _, row := range rows {
			byBucket[row.Timestamp] = row
		}
		known[scope] = byBucket
	}

	out := make([]derivatives.DerivativesColdPoint, 0, len(points))
	for _, p := range points {
		if completeRow(p) {
			out = append(out, p)
			continue
		}
		scope := scopeOf(p)
		if unreadable[scope] {
			outcome.holes++
			continue
		}
		prior := known[scope][p.Timestamp.UnixMilli()]
		if prior == nil {
			// Bucket that does not exist yet: there is nothing to erode.
			out = append(out, p)
			continue
		}
		if filled := fillFromPrior(&p, prior); filled {
			outcome.preserved++
		}
		out = append(out, p)
	}
	return out, outcome
}

// bucketSpan is the range of buckets a scope rewrites in this batch.
type bucketSpan struct{ from, to time.Time }

// scopesAtRisk groups, by scope, the range of rows that omit some quantity.
func scopesAtRisk(points []derivatives.DerivativesColdPoint) map[derivatives.DerivativesScope]bucketSpan {
	spans := make(map[derivatives.DerivativesScope]bucketSpan)
	for _, p := range points {
		if completeRow(p) {
			continue
		}
		scope := scopeOf(p)
		span, seen := spans[scope]
		if !seen {
			spans[scope] = bucketSpan{from: p.Timestamp, to: p.Timestamp}
			continue
		}
		if p.Timestamp.Before(span.from) {
			span.from = p.Timestamp
		}
		if p.Timestamp.After(span.to) {
			span.to = p.Timestamp
		}
		spans[scope] = span
	}
	return spans
}

// fillFromPrior copies into the new row the quantities it omits and the old row carried.
func fillFromPrior(p *derivatives.DerivativesColdPoint, prior *derivatives.DerivativesSnapshot) bool {
	var filled bool
	if p.OpenInterestBase == nil && prior.OpenInterestBase != nil {
		p.OpenInterestBase = prior.OpenInterestBase
		filled = true
	}
	if p.FundingRate == nil && prior.FundingRate != nil {
		p.FundingRate = prior.FundingRate
		filled = true
	}
	if p.LongShortRatio == nil && prior.LongShortRatio != nil {
		p.LongShortRatio = prior.LongShortRatio
		filled = true
	}
	if p.OpenInterest == nil && prior.OpenInterest != nil {
		p.OpenInterest = prior.OpenInterest
		filled = true
	}
	// THE SHARES ARE A PAIR, and the pair is preserved together or not at all.
	if p.LSLongPct == nil && p.LSShortPct == nil && prior.LSLongPct != nil && prior.LSShortPct != nil {
		p.LSLongPct = prior.LSLongPct
		p.LSShortPct = prior.LSShortPct
		filled = true
	}
	// THE UNIT GROUP MOVES WITH THE NUMBER IT DESCRIBES.
	if p.FundingRate != nil {
		if p.FundingIntervalHours == nil && prior.FundingIntervalHours != nil {
			p.FundingIntervalHours = prior.FundingIntervalHours
			filled = true
		}
		if p.NStep == "" && prior.NStep != "" {
			p.NStep = prior.NStep
			filled = true
		}
		if p.InterestRate == nil && prior.InterestRate != nil {
			p.InterestRate = prior.InterestRate
			filled = true
		}
		if p.FundingAtAnchor == nil && prior.FundingAtAnchor != nil {
			p.FundingAtAnchor = prior.FundingAtAnchor
			filled = true
		}
		if p.NextFundingRate == nil && prior.NextFundingRate != nil {
			p.NextFundingRate = prior.NextFundingRate
			filled = true
		}
	}

	// AND THE EIGHT-HOUR EQUIVALENT PAYS ONE MORE RUNG THAN THE REST OF THE GROUP.
	if p.NStep.Normalizes() {
		if p.FundingRate != nil && p.FundingRate8h == nil && prior.FundingRate8h != nil {
			p.FundingRate8h = prior.FundingRate8h
			filled = true
		}
	} else {
		p.FundingRate8h = nil
	}
	return filled
}

// priorsOf returns this store's source of truth. Nil in production, where it is the store
// itself.
func (q *Store) priorsOf() derivativesPriorSource {
	if q.priors != nil {
		return q.priors
	}
	return q
}
