package derivatives

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// CoarseDerivativesHistory keeps the daily and weekly ruler on one provider.
// It reads existing durable daily observations; it never overlays venue hot data.
type CoarseDerivativesHistory struct {
	cold ColdReader
	now  func() time.Time
}

// NewCoarseDerivativesHistory builds the daily/weekly reader over the cold tier.
func NewCoarseDerivativesHistory(cold ColdReader) *CoarseDerivativesHistory {
	return &CoarseDerivativesHistory{cold: cold, now: time.Now}
}

// Series returns newest-first points. Closed weeks require the Sunday observation;
// the open week may show its latest daily observation without claiming to be closed.
func (r *CoarseDerivativesHistory) Series(ctx context.Context, exchange, marketType, symbol, timeframe string, limit int) ([]*DerivativesSnapshot, error) {
	if timeframe != "1d" && timeframe != "1w" {
		return nil, fmt.Errorf("coarse derivatives: unsupported timeframe %q", timeframe)
	}
	if r.cold == nil {
		return nil, fmt.Errorf("coarse derivatives: durable Coinalyze history unavailable")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	const day = 24 * time.Hour
	span := day
	if timeframe == "1w" {
		span *= 7
	}
	maxPoints := int(DurableMaxLookback / span)
	if limit <= 0 || limit > maxPoints {
		limit = maxPoints
	}
	now := r.now()
	// Daily labels are midnight UTC: a shared literal window also permits PgWire
	// query reuse across the fleet rather than one SQL text per observation clock.
	to := time.UnixMilli(bucketStartMs(now.UnixMilli(), "1d"))
	current := bucketStartMs(now.UnixMilli(), timeframe)
	from := time.UnixMilli(current).Add(-time.Duration(limit) * span)
	if floor := to.Add(-DurableMaxLookback); from.Before(floor) {
		from = floor
	}
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	scope := DerivativesScope{Source: SourceCoinalyze, Exchange: exchange, MarketType: marketType, Symbol: symbol, Timeframe: "1d"}
	rows, err := r.cold.QueryDerivativesRange(readCtx, scope, from, to)
	if err != nil {
		return nil, fmt.Errorf("coarse derivatives %s %s: %w", symbol, timeframe, err)
	}
	byBucket := make(map[int64]*DerivativesSnapshot, limit+1)
	for _, row := range rows {
		if row == nil || row.Timestamp < from.UnixMilli() || row.Timestamp > to.UnixMilli() {
			continue
		}
		if row.Source != scope.Source || row.Exchange != exchange || row.MarketType != marketType || row.Symbol != symbol {
			return nil, fmt.Errorf("coarse derivatives: incoherent scope for %s", symbol)
		}
		if row.Timestamp != bucketStartMs(row.Timestamp, "1d") {
			return nil, fmt.Errorf("coarse derivatives: unaligned daily label %d", row.Timestamp)
		}
		bucket := bucketStartMs(row.Timestamp, timeframe)
		if bucket < from.UnixMilli() {
			continue
		}
		if timeframe == "1w" && bucket < current && row.Timestamp != bucket+6*day.Milliseconds() {
			continue
		}
		if previous, exists := byBucket[bucket]; exists {
			if timeframe == "1d" || row.Timestamp == previous.Timestamp {
				return nil, fmt.Errorf("coarse derivatives: duplicate daily label %d", row.Timestamp)
			}
			if row.Timestamp < previous.Timestamp {
				continue
			}
		}
		// Keep the original daily label until selecting the latest open-week row.
		byBucket[bucket] = coherentDerivatives(row)
	}
	out := make([]*DerivativesSnapshot, 0, len(byBucket))
	for bucket, point := range byBucket {
		point.Timestamp = bucket
		out = append(out, point)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp > out[j].Timestamp })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
