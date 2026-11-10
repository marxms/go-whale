package questdb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/marxms/go-whale/internal/derivatives"
)

// derivativesFillCountExpression returns the expression that counts the rows of ONE column.
//
// THE EXPRESSION DOES NOT LOOK AT THE COLUMN TYPE, and that is what makes it valid for EVERY
// type the schema declares. The bare form `count(column)` is NOT: measured against the live
// database, `count(funding_at_anchor)` is REFUSED —
//
//	ERROR: there is no matching function `count` with the argument types: (BOOLEAN)
//
// Because the columns travel in a SINGLE query with ONE `QueryRow().Scan(...)`, one refused
// column zeroed the ENTIRE timeframe. Column names are INTERPOLATED because an identifier
// accepts no parameter, and they are therefore validated against the same pattern the rest
// of the schema uses.
func derivativesFillCountExpression(column string) (string, error) {
	if !validTableName.MatchString(column) {
		return "", fmt.Errorf("invalid column name: %q", column)
	}
	return "count(case when " + column + " is not null then 1 end)", nil
}

// derivativesFillCountQuery assembles the checker's query: ONE projection per column, in ONE
// query per timeframe.
func derivativesFillCountQuery(columns []string) (string, error) {
	projecao := make([]string, 0, len(columns))
	for _, column := range columns {
		expression, err := derivativesFillCountExpression(column)
		if err != nil {
			return "", err
		}
		projecao = append(projecao, expression)
	}
	return "SELECT " + strings.Join(projecao, ", ") +
		" FROM " + derivativesTableName + " WHERE timeframe = $1", nil
}

// CountDerivativesColumnRows counts, per column, how many rows of a timeframe carry it.
func (q *Store) CountDerivativesColumnRows(ctx context.Context, timeframe string, columns []string) (map[string]int64, error) {
	if len(columns) == 0 {
		return map[string]int64{}, nil
	}
	query, err := derivativesFillCountQuery(columns)
	if err != nil {
		return nil, err
	}

	destinos := make([]any, len(columns))
	contagens := make([]int64, len(columns))
	for i := range contagens {
		destinos[i] = &contagens[i]
	}
	if err = q.pgPool.QueryRow(ctx, query, timeframe).Scan(destinos...); err != nil {
		q.errCount.Add(1)
		return nil, fmt.Errorf("questdb derivatives fill count: %w", err)
	}

	out := make(map[string]int64, len(columns))
	for i, column := range columns {
		out[column] = contagens[i]
	}
	return out, nil
}

// CountDerivativesIncoherentFunding counts the rows of a timeframe whose funding group
// contradicts itself: a `funding_rate_8h` present under an `n_step` that does not authorize
// normalization.
//
// The rung list comes from `derivatives.NDerivationSteps()` filtered by `Normalizes()`; a
// literal here would be the usual divergence. The `IS NULL` is explicit because
// `n_step NOT IN (...)` over NULL is UNKNOWN in SQL.
func (q *Store) CountDerivativesIncoherentFunding(ctx context.Context, timeframe string) (int64, error) {
	rungs := normalizingRungLiterals()
	if len(rungs) == 0 {
		return 0, fmt.Errorf("no normalization rung declared")
	}

	query := "SELECT count() FROM " + derivativesTableName +
		" WHERE timeframe = $1 AND funding_rate_8h IS NOT NULL" +
		" AND (n_step IS NULL OR n_step NOT IN (" + strings.Join(rungs, ", ") + "))"

	var incoerentes int64
	if err := q.pgPool.QueryRow(ctx, query, timeframe).Scan(&incoerentes); err != nil {
		q.errCount.Add(1)
		return 0, fmt.Errorf("questdb derivatives coherence of %q: %w", timeframe, err)
	}
	return incoerentes, nil
}

// normalizingRungLiterals returns the rungs that authorize normalization, already quoted.
func normalizingRungLiterals() []string {
	var out []string
	for _, rung := range derivatives.NDerivationSteps() {
		if !rung.Normalizes() || !validTableName.MatchString(string(rung)) {
			continue
		}
		out = append(out, "'"+string(rung)+"'")
	}
	return out
}

// DerivativesTimeframeSpan reads the four quantities of ONE timeframe in a single query.
//
// `count_distinct(timestamp)` is what answers continuity: the table has several rows per
// bucket — one per symbol —, so `count()` alone would say "full" about a series with holes.
// Checked against QuestDB 8.2.3.
func (q *Store) DerivativesTimeframeSpan(ctx context.Context, timeframe string) (derivatives.DerivativesTimeframeSpan, error) {
	query := "SELECT count(), count_distinct(timestamp), min(timestamp), max(timestamp) FROM " +
		derivativesTableName + " WHERE timeframe = $1"

	var span derivatives.DerivativesTimeframeSpan
	var oldest, newest *time.Time
	if err := q.pgPool.QueryRow(ctx, query, timeframe).
		Scan(&span.Rows, &span.DistinctBuckets, &oldest, &newest); err != nil {
		q.errCount.Add(1)
		return derivatives.DerivativesTimeframeSpan{}, fmt.Errorf("questdb derivatives span of %q: %w", timeframe, err)
	}
	if oldest != nil {
		span.Oldest = oldest.UTC()
	}
	if newest != nil {
		span.Newest = newest.UTC()
	}
	return span, nil
}
