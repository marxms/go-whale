package questdb

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/marxms/go-whale/internal/derivatives"
)

// Compile-time check: Store satisfies derivatives.ColdReader.
var _ derivatives.ColdReader = (*Store)(nil)

// derivativesReadColumns is the list, IN ORDER, of the columns the durable read requests.
//
// THE ORDER IS CONTRACT. `scanTargets` repeats it, and one of the two edited without the
// other is a type error on every row read — or, worse, a value read from the wrong column
// when the types coincide. The list lives in the domain (`derivatives.DurableReadColumns`)
// because the "readable" half of coverage is its property, and the decision to cap a cell
// needs the same answer.
func derivativesReadColumns() []string {
	return derivatives.DurableReadColumns()
}

// derivativesRow is ONE row of the durable read, with one field per requested column.
//
// Every measurement field is a pointer because the column is NULL when the write omitted it,
// and absent is different from zero. `nStep` is `*string` because the driver performs the
// `Scan`; the translation to the rung happens in `snapshot`.
type derivativesRow struct {
	ts                   time.Time
	openInterestBase     *float64
	fundingRate          *float64
	fundingRate8h        *float64
	longShortRatio       *float64
	fundingIntervalHours *float64
	nStep                *string
	openInterest         *float64
	nextFundingRate      *float64
	interestRate         *float64
	// fundingAtAnchor is `*bool` and not `*float64`: the durable column is BOOLEAN, and
	// reading a predicate as a number would invite arithmetic over it.
	fundingAtAnchor *bool
	lsLongPct       *float64
	lsShortPct      *float64
}

// scanTargets returns the `Scan` targets, IN THE SAME ORDER as derivativesReadColumns.
func (r *derivativesRow) scanTargets() []any {
	return []any{
		&r.ts,
		&r.openInterestBase,
		&r.fundingRate,
		&r.fundingRate8h,
		&r.longShortRatio,
		&r.fundingIntervalHours,
		&r.nStep,
		&r.openInterest,
		&r.nextFundingRate,
		&r.interestRate,
		&r.fundingAtAnchor,
		&r.lsLongPct,
		&r.lsShortPct,
	}
}

// snapshot translates the read row into a series point.
func (r derivativesRow) snapshot(scope derivatives.DerivativesScope) *derivatives.DerivativesSnapshot {
	var step derivatives.NDerivationStep
	if r.nStep != nil {
		step = derivatives.NDerivationStep(*r.nStep)
	}
	return &derivatives.DerivativesSnapshot{
		Exchange:   scope.Exchange,
		MarketType: scope.MarketType,
		Symbol:     scope.Symbol,
		// Provenance is a COLUMN of the table and a term of the queried scope, so it crosses
		// the read.
		Source:           scope.Source,
		Timestamp:        r.ts.UnixMilli(),
		OpenInterestBase: presentFloat(r.openInterestBase),
		FundingRate:      presentFloat(r.fundingRate),
		FundingRate8h:    presentFloat(r.fundingRate8h),
		LongShortRatio:   presentFloat(r.longShortRatio),
		// THE UNIT GROUP CROSSES TOGETHER WITH THE NUMBER IT DESCRIBES.
		FundingIntervalHours: presentFloat(r.fundingIntervalHours),
		NStep:                step,
		// THE SIX ADDITIVE COLUMNS CROSS. Without this half the column exists, is written,
		// is requested by the query — and dies in `Scan`.
		OpenInterest:    presentFloat(r.openInterest),
		NextFundingRate: presentFloat(r.nextFundingRate),
		InterestRate:    presentFloat(r.interestRate),
		FundingAtAnchor: presentBool(r.fundingAtAnchor),
		LSLongPct:       presentFloat(r.lsLongPct),
		LSShortPct:      presentFloat(r.lsShortPct),
	}
}

// presentBool is the predicate's `presentFloat`: it returns a COPY, so the pointer handed out
// does not point inside the row reused by the driver.
func presentBool(v *bool) *bool {
	if v == nil {
		return nil
	}
	out := *v
	return &out
}

// derivativesRangeQuery builds the durable read.
//
// THE INTERVAL IS A LITERAL AND THE SYMBOL PREDICATES ARE PARAMETERS — the same asymmetry
// measured on the candles path: QuestDB prunes partitions only when it resolves the
// designated timestamp predicate while PLANNING.
func derivativesRangeQuery(from, to time.Time) string {
	return "SELECT " + strings.Join(derivativesReadColumns(), ", ") +
		" FROM " + derivativesTableName +
		" WHERE source = $1 AND exchange = $2 AND market_type = $3 AND symbol = $4 AND timeframe = $5" +
		" AND timestamp >= " + timestampLiteral(from) +
		" AND timestamp <= " + timestampLiteral(to) +
		" ORDER BY timestamp ASC"
}

// QueryDerivativesRange reads the durable tier for ONE scope and window, ascending.
func (q *Store) QueryDerivativesRange(ctx context.Context, scope derivatives.DerivativesScope, from, to time.Time) ([]*derivatives.DerivativesSnapshot, error) {
	rows, err := q.pgPool.Query(ctx, derivativesRangeQuery(from, to),
		scope.Source, scope.Exchange, scope.MarketType, scope.Symbol, scope.Timeframe)
	if err != nil {
		q.errCount.Add(1)
		return nil, fmt.Errorf("questdb derivatives query: %w", err)
	}
	defer rows.Close()

	var out []*derivatives.DerivativesSnapshot
	for rows.Next() {
		var row derivativesRow
		if err := rows.Scan(row.scanTargets()...); err != nil {
			return nil, fmt.Errorf("questdb derivatives scan: %w", err)
		}
		out = append(out, row.snapshot(scope))
	}
	if err := rows.Err(); err != nil {
		q.errCount.Add(1)
		return nil, fmt.Errorf("questdb derivatives rows: %w", err)
	}
	return out, nil
}

// presentFloat treats NON-FINITE as ABSENT, in addition to NULL.
//
// Measured against QuestDB 8.2.3: a DOUBLE column omitted on write comes back as a true NULL
// through the PG protocol. NaN remains as a second guard because QuestDB represents a null
// double as NaN INTERNALLY. AND INFINITY FOR THE SAME REASON: neither triggers an error, they
// cross every filter and reach the consumer as a number.
func presentFloat(v *float64) *float64 {
	if v == nil || math.IsNaN(*v) || math.IsInf(*v, 0) {
		return nil
	}
	return v
}
