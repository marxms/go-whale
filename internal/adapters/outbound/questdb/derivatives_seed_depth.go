package questdb

import (
	"context"
	"fmt"
	"time"
)

// DURABLE DEPTH PER SYMBOL — the read that makes the seeded mark survive a deploy.
//
// ONE query per timeframe, not one per symbol: the resolved fleet measures 499 members, and
// asking one by one would be 1,497 database round trips per cycle to answer a question a
// `GROUP BY` answers at once.
//
// THE AGGREGATION IS `min(timestamp)` BECAUSE THE QUESTION IS DEPTH, not volume. Counting
// rows does not distinguish a seeding of 2000 buckets from 2000 tail cycles; the oldest
// point does.

// derivativesOldestBySymbolQuery is the depth read.
//
// THE ENTIRE SCOPE IS BOUND, no part of it becomes SQL text: the exchange, the market type
// and the timeframe come from configuration, and concatenating them would be injection
// through the front door in a query that runs every cycle.
const derivativesOldestBySymbolQuery = "SELECT symbol, min(timestamp) FROM " + derivativesTableName +
	" WHERE source = $1 AND exchange = $2 AND market_type = $3 AND timeframe = $4" +
	" GROUP BY symbol"

// OldestDerivativesBySymbol returns, per symbol, the oldest instant the durable table holds
// for that scope.
//
// A symbol with no rows simply does NOT APPEAR in the map — absence is absence, and filling
// it with `time.Time`'s zero would make the caller read "January 1st of year 1" as depth.
func (q *Store) OldestDerivativesBySymbol(
	ctx context.Context,
	source, exchange, marketType, timeframe string,
) (map[string]time.Time, error) {
	rows, err := q.pgPool.Query(ctx, derivativesOldestBySymbolQuery, source, exchange, marketType, timeframe)
	if err != nil {
		q.errCount.Add(1)
		return nil, fmt.Errorf("questdb durable depth of %q: %w", timeframe, err)
	}
	defer rows.Close()

	out := make(map[string]time.Time)
	for rows.Next() {
		var symbol string
		var oldest *time.Time
		if err := rows.Scan(&symbol, &oldest); err != nil {
			q.errCount.Add(1)
			return nil, fmt.Errorf("questdb durable depth of %q: scan: %w", timeframe, err)
		}
		if oldest == nil {
			continue
		}
		out[symbol] = oldest.UTC()
	}
	if err := rows.Err(); err != nil {
		q.errCount.Add(1)
		return nil, fmt.Errorf("questdb durable depth of %q: iteration: %w", timeframe, err)
	}
	return out, nil
}
