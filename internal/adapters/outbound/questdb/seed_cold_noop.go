package questdb

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// The input channel is bounded to 8192 entries. A bulk seed normally contributes
// two symbols x eight timeframes; limit the added cold reads to that same scale.
// Unprobed groups keep every entry and follow the original write path.
const maxColdNoopReadsPerBatch = 16

type coldNoopGroup struct {
	key     candle.SeriesKey
	indices []int
	last    map[int64]candle.Candle
	minMs   int64
	maxMs   int64
}

// filterNewColdBatch is called only for a newly collected sweep, before it is
// partitioned into DAYs. It marks, rather than removes, redundant entries so a
// later WAL revision can restore every unsent DAY without reconstructing data.
// The returned permit remains held through the first DAY. A predecessor, read
// error, or cancellation leaves the original entries intact.
func (q *Store) filterNewColdBatch(ctx context.Context, entries []candleEntry) ([]candleEntry, func(), int64, int) {
	if len(entries) == 0 {
		return entries, nil, 0, 0
	}
	groups := make([]*coldNoopGroup, 0)
	byKey := make(map[candle.SeriesKey]*coldNoopGroup)
	nonCandidate := make(map[candle.SeriesKey]map[int64]bool)
	for i, entry := range entries {
		key, keyErr := candle.SeriesKeyOf(entry.Exchange, entry.MarketType, entry.Symbol, entry.Timeframe)
		if keyErr != nil {
			// An identity that cannot be named cannot have an applied counterpart.
			// Leaving it unwritten here keeps the ordinary write path in charge.
			continue
		}
		// Live closes use Enqueue and are never candidates. Only the blocking
		// historical path can remove a physically identical cold write.
		if !entry.coldNoopCandidate {
			if nonCandidate[key] == nil {
				nonCandidate[key] = make(map[int64]bool)
			}
			nonCandidate[key][entry.Candle.Timestamp] = true
			continue
		}
		group := byKey[key]
		if group == nil {
			group = &coldNoopGroup{key: key, last: make(map[int64]candle.Candle), minMs: entry.Candle.Timestamp, maxMs: entry.Candle.Timestamp}
			byKey[key] = group
			groups = append(groups, group)
		}
		group.indices = append(group.indices, i)
		group.last[entry.Candle.Timestamp] = entry.Candle
		if entry.Candle.Timestamp < group.minMs {
			group.minMs = entry.Candle.Timestamp
		}
		if entry.Candle.Timestamp > group.maxMs {
			group.maxMs = entry.Candle.Timestamp
		}
	}

	eligible := make([]*coldNoopGroup, 0, maxColdNoopReadsPerBatch)
	for _, group := range groups {
		if len(group.indices) < 2 || sameUTCDay(group.minMs, group.maxMs) {
			continue // one-bucket recovery and live closes must not add a cold read
		}
		eligible = append(eligible, group)
		if len(eligible) == maxColdNoopReadsPerBatch {
			break
		}
	}
	if len(eligible) == 0 {
		return entries, nil, 0, 0
	}

	release, err := q.acquireCandleDayPermit(ctx)
	if err != nil {
		return entries, nil, 0, 0
	}
	fallback := func() ([]candleEntry, func(), int64, int) {
		release()
		return entries, nil, 0, 0
	}
	// The pipeline has no pending DAY, but the migration may own a retained
	// predecessor on this store. The ordinary coordinator resolves it; a no-op
	// cannot claim equality ahead of it or replay an uncertain ACK.
	if q.pendingPlan != nil {
		return fallback()
	}
	target, err := q.CaptureApplyTarget(ctx)
	if err != nil {
		return fallback()
	}
	if err := q.WaitApplied(ctx, target); err != nil {
		return fallback()
	}
	position, err := q.readColdWALPosition(ctx)
	if err != nil || position.writer != target || position.sequencer != target || ctx.Err() != nil {
		return fallback()
	}

	skip := make(map[int]struct{})
	for _, group := range eligible {
		limit := len(group.indices) + 1 // a full result at this limit is not proof
		columns, rows, err := q.readRawSeedRows(ctx, group.key, time.UnixMilli(group.minMs), time.UnixMilli(group.maxMs), limit)
		if err != nil || ctx.Err() != nil {
			return fallback() // discard every partial comparison
		}
		matches, certain := rawColdMatches(columns, rows, group.last, group.key, limit)
		if !certain {
			continue // unknown schema or truncated result: write this group
		}
		for _, i := range group.indices {
			if matches[entries[i].Candle.Timestamp] &&
				!nonCandidate[group.key][entries[i].Candle.Timestamp] {
				// Remove ALL revisions of this identity only if its final
				// revision equals the applied cold row. [C,B] vs B cannot
				// leave C behind to overwrite B.
				skip[i] = struct{}{}
			}
		}
	}
	if len(skip) == 0 {
		return fallback()
	}
	marked := append([]candleEntry(nil), entries...)
	for i := range skip {
		marked[i].coldNoopSkip = true
	}
	return marked, release, target, len(skip)
}

func sameUTCDay(a, b int64) bool {
	return time.UnixMilli(a).UTC().Truncate(24 * time.Hour).Equal(time.UnixMilli(b).UTC().Truncate(24 * time.Hour))
}

func (q *Store) readRawSeedRows(ctx context.Context, key candle.SeriesKey, from, to time.Time, limit int) ([]string, [][]any, error) {
	if q.rawSeedRead != nil {
		return q.rawSeedRead(ctx, key, from, to, limit)
	}
	if q.pgPool == nil {
		return nil, nil, fmt.Errorf("questdb raw seed read: no connection")
	}
	query := "SELECT * FROM " + q.tableName +
		" WHERE exchange = $1 AND market_type = $2 AND symbol = $3 AND timeframe = $4" +
		" AND timestamp >= " + timestampLiteral(from) +
		" AND timestamp <= " + timestampLiteral(to) +
		" ORDER BY timestamp DESC LIMIT $5"
	rows, err := q.pgPool.Query(ctx, query,
		key.Scope.Exchange.String(), key.Scope.MarketType.String(),
		key.Scope.Symbol.String(), key.Timeframe.String(), limit)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	fields := rows.FieldDescriptions()
	columns := make([]string, len(fields))
	for i := range fields {
		columns[i] = fields[i].Name
	}
	values := make([][]any, 0, min(limit, 1024))
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			return nil, nil, err
		}
		values = append(values, append([]any(nil), row...))
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return columns, values, nil
}

// rawColdMatches tests the physical effect of WriteBatches, not QueryRange's
// compatibility reconstruction. Required numeric columns must be PRESENT and
// bit-identical; any extra column omitted by ILP must already be NULL.
func rawColdMatches(columns []string, rows [][]any, final map[int64]candle.Candle, key candle.SeriesKey, limit int) (map[int64]bool, bool) {
	if len(rows) >= limit {
		return nil, false
	}
	indices := make(map[string]int, len(columns))
	for i, name := range columns {
		if _, duplicate := indices[name]; duplicate {
			return nil, false
		}
		indices[name] = i
	}
	required := []string{columnTimestamp, columnExchange, columnMarketType, columnSymbol, columnTimeframe, columnOpen, columnHigh, columnLow, columnClose, columnVolume, columnTakerBuyVolume, columnTakerSellVolume}
	for _, name := range required {
		if _, ok := indices[name]; !ok {
			return nil, false
		}
	}
	known := make(map[string]struct{}, len(required))
	for _, name := range required {
		known[name] = struct{}{}
	}
	matches := make(map[int64]bool)
	seen := make(map[int64]bool)
	for _, row := range rows {
		if len(row) != len(columns) {
			return nil, false
		}
		ts, ok := row[indices[columnTimestamp]].(time.Time)
		if !ok {
			return nil, false
		}
		ms := ts.UnixMilli()
		want, candidate := final[ms]
		if !candidate {
			continue
		}
		if seen[ms] {
			matches[ms] = false // duplicate physical identity is never a no-op
			continue
		}
		seen[ms] = true
		exchange, exOK := row[indices[columnExchange]].(string)
		marketType, marketOK := row[indices[columnMarketType]].(string)
		symbol, symbolOK := row[indices[columnSymbol]].(string)
		timeframe, tfOK := row[indices[columnTimeframe]].(string)
		match := ts.Equal(time.UnixMilli(ms)) && exOK && marketOK && symbolOK && tfOK &&
			exchange == key.Scope.Exchange.String() && marketType == key.Scope.MarketType.String() &&
			symbol == key.Scope.Symbol.String() && timeframe == key.Timeframe.String()
		for name, value := range map[string]float64{
			columnOpen: want.Open, columnHigh: want.High, columnLow: want.Low, columnClose: want.Close,
			columnVolume: want.Volume, columnTakerBuyVolume: want.TakerBuyVolume, columnTakerSellVolume: want.TakerSellVolume,
		} {
			got, ok := row[indices[name]].(float64)
			match = match && ok && math.Float64bits(got) == math.Float64bits(value)
		}
		for i, name := range columns {
			if _, required := known[name]; !required && row[i] != nil {
				match = false
			}
		}
		matches[ms] = match
	}
	return matches, true
}
