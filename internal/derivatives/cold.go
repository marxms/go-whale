package derivatives

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// DerivativesColdPoint is ONE row of the durable derivatives table.
//
// The values are pointers because absent and zero are different things: zero funding is a
// reading, absent funding is a call that did not return.
type DerivativesColdPoint struct {
	// Timestamp is the ALREADY ALIGNED bucket — see DerivativesTieredWriter, which derives it
	// through BucketAlignMs so the durable tier and the ZSET name the same bucket.
	Timestamp  time.Time
	Source     string
	Exchange   string
	MarketType string
	Symbol     string
	Timeframe  string
	// DenominatedIn and LabelConvention are the point's UNIT and time anchor.
	DenominatedIn   string
	LabelConvention string

	OpenInterestBase *float64
	FundingRate      *float64
	FundingRate8h    *float64
	LongShortRatio   *float64

	// FundingIntervalHours is the `N` IN FORCE WHEN this row was written, and NStep is its
	// evidence. NStep's zero value is a nameless rung, which is why the write OMITS it instead
	// of storing it.
	FundingIntervalHours *float64
	NStep                NDerivationStep

	// THE SIX COMPANION QUANTITIES — values otherwise present only inside the ZSET member.
	OpenInterest    *float64
	NextFundingRate *float64
	InterestRate    *float64
	FundingAtAnchor *bool
	LSLongPct       *float64
	LSShortPct      *float64
}

// ErrDerivativesPointUndeclared refuses a point that does not declare what it measures.
var ErrDerivativesPointUndeclared = errors.New("derivatives cold: point without a source, denomination or label convention")

// Declared reports whether the point carries everything provenance and units require.
func (p DerivativesColdPoint) Declared() error {
	switch {
	case p.Source == "":
		return fmt.Errorf("%w: fonte vazia", ErrDerivativesPointUndeclared)
	case p.DenominatedIn == "":
		return fmt.Errorf("%w: %s without a denomination", ErrDerivativesPointUndeclared, p.Symbol)
	case p.LabelConvention == "":
		return fmt.Errorf("%w: %s without a label convention", ErrDerivativesPointUndeclared, p.Symbol)
	}
	return nil
}

// Empty reports whether the point carries no measurement at all.
//
// THE TWO UNIT COLUMNS DO NOT ENTER HERE: they describe funding, they do not measure anything on
// their own. A row carrying only them would be a unit without a number.
func (p DerivativesColdPoint) Empty() bool {
	return p.OpenInterestBase == nil && p.FundingRate == nil &&
		p.FundingRate8h == nil && p.LongShortRatio == nil
}

// DerivativesBatch is what ONE collection cycle produced for one (symbol, timeframe).
type DerivativesBatch struct {
	Timeframe     string
	Source        string
	DenominatedIn string
	Label         LabelConvention
	Snapshots     []*DerivativesSnapshot
}

// DerivativesTieredWriter is the collection → tiered-storage crossing.
//
// THE NAME STAYS even with a single tier, and the reason is the boundary, not the count: this type
// exists so the poller knows no tier at all.
type DerivativesTieredWriter struct {
	cold ColdWriter
}

// NewDerivativesTieredWriter assembles the writer.
func NewDerivativesTieredWriter(cold ColdWriter) *DerivativesTieredWriter {
	return &DerivativesTieredWriter{cold: cold}
}

// WriteBatch stores the batch in the durable tier, and returns any failure that prevents it.
func (w *DerivativesTieredWriter) WriteBatch(ctx context.Context, batch DerivativesBatch) error {
	if len(batch.Snapshots) == 0 {
		return nil
	}
	if !batch.Label.Valid() {
		return fmt.Errorf("derivatives: batch of %s refused — label convention %s", batch.Timeframe, batch.Label)
	}
	if w.cold == nil {
		return fmt.Errorf("derivatives: batch of %d point(s) in %s discarded — no durable tier is enabled",
			len(batch.Snapshots), batch.Timeframe)
	}

	points := make([]DerivativesColdPoint, 0, len(batch.Snapshots))
	for _, snapshot := range batch.Snapshots {
		if snapshot == nil {
			continue
		}
		points = append(points, coldPointOf(snapshot, batch))
	}

	if err := w.cold.WriteDerivativesBatch(ctx, points); err != nil {
		return fmt.Errorf("derivatives: durable tier %s: %w", batch.Timeframe, err)
	}
	return nil
}

// coldPointOf translates a snapshot into the durable row, aligning the bucket by the SAME rule
// the ZSET uses.
func coldPointOf(snapshot *DerivativesSnapshot, batch DerivativesBatch) DerivativesColdPoint {
	bucketMs := BucketAlignMs(snapshot.Timestamp, batch.Timeframe, batch.Label)
	return DerivativesColdPoint{
		Timestamp:        time.UnixMilli(bucketMs).UTC(),
		Source:           batch.Source,
		Exchange:         snapshot.Exchange,
		MarketType:       snapshot.MarketType,
		Symbol:           snapshot.Symbol,
		Timeframe:        batch.Timeframe,
		DenominatedIn:    batch.DenominatedIn,
		LabelConvention:  batch.Label.String(),
		OpenInterestBase: snapshot.OpenInterestBase,
		FundingRate:      snapshot.FundingRate,
		FundingRate8h:    snapshot.FundingRate8h,
		LongShortRatio:   snapshot.LongShortRatio,
		// COPIED, NEVER RECOMPUTED. `FundingRate8h` already is `rate × 8/N`, applied once on the
		// polling path; deriving it again here would give (8/N)².
		FundingIntervalHours: snapshot.FundingIntervalHours,
		NStep:                snapshot.NStep,
		OpenInterest:         snapshot.OpenInterest,
		NextFundingRate:      snapshot.NextFundingRate,
		InterestRate:         snapshot.InterestRate,
		FundingAtAnchor:      snapshot.FundingAtAnchor,
		LSLongPct:            snapshot.LSLongPct,
		LSShortPct:           snapshot.LSShortPct,
	}
}

// BucketAlignMs aligns an observation to the bucket label the durable table and the
// hot ZSET must agree on.
//
// The rule lives here, not in the Redis adapter, because both tiers consume it: if each layer
// computed the bucket on its own, the join would silently stop matching the day one of them
// changed — and the case that diverges first is precisely `1w`.
func BucketAlignMs(timestampMs int64, timeframe string, convention LabelConvention) int64 {
	if convention != LabelIntervalEnd {
		return bucketAlignMs(timestampMs, timeframe)
	}
	bucketMs, ok := timeframeMilliseconds(timeframe)
	if !ok || bucketMs <= 0 {
		return bucketAlignMs(timestampMs, timeframe)
	}
	// The end of `[S, E)` belongs to `S`: stepping back one period before flooring takes the
	// closing label to the interval it closed.
	return bucketAlignMs(timestampMs-bucketMs, timeframe)
}

func bucketAlignMs(timestampMs int64, timeframe string) int64 {
	bucketMs, ok := timeframeMilliseconds(timeframe)
	if !ok || bucketMs <= 0 {
		return 0
	}
	if timeframe == "1w" {
		// Weekly candles start on Monday 00:00 UTC. Unix epoch (1970-01-01) was
		// Thursday, so offset by 3 days.
		const mondayOffsetMs int64 = 3 * 86400 * 1000
		return (timestampMs+mondayOffsetMs)/bucketMs*bucketMs - mondayOffsetMs
	}
	return (timestampMs / bucketMs) * bucketMs
}
