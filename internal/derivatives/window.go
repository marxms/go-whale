package derivatives

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// DurableMaxLookback is the reach ceiling of the durable read.
//
// SEPARATE from the candle ceiling (`coldReadMaxLookback`, 400 days) because the retentions differ
// and the smaller number cut off history that EXISTS.
const DurableMaxLookback = 2100 * 24 * time.Hour

// DerivativesScope names ONE durable series. The source enters the scope because the durable table
// holds both provenances side by side — without that term the read would mix the two back together
// on output.
type DerivativesScope struct {
	Source     string
	Exchange   string
	MarketType string
	Symbol     string
	Timeframe  string
}

// DerivativesHotReader is the ZSET side of the join.
//
// THE PORT DOES NOT EXPOSE `SeriesHorizonMs`: the declared horizon is the ZSET's CAPACITY, not the
// coverage it actually delivered. The floor must be measured from what came back.
type DerivativesHotReader interface {
	GetSnapshotSeries(ctx context.Context, exchange, marketType, symbol, timeframe string, limit int) ([]*DerivativesSnapshot, error)
}

// DerivativesWindowReader serves a time window from the two tiers.
type DerivativesWindowReader struct {
	hot  DerivativesHotReader
	cold ColdReader
	now  func() time.Time
}

// NewDerivativesWindowReader assembles the reader. A nil `cold` is a disabled durable tier: the read
// becomes what the ZSET covers, and a deeper window is REFUSED instead of truncated.
func NewDerivativesWindowReader(hot DerivativesHotReader, cold ColdReader) *DerivativesWindowReader {
	return &DerivativesWindowReader{hot: hot, cold: cold, now: time.Now}
}

// ErrDerivativesWindowUnservable says the requested window cannot be served in full.
var ErrDerivativesWindowUnservable = fmt.Errorf("derivatives: window beyond the ZSET horizon and without a durable tier")

// Series returns every bucket in [from, to], ascending, without duplicates.
func (r *DerivativesWindowReader) Series(ctx context.Context, scope DerivativesScope, from, to time.Time) ([]*DerivativesSnapshot, error) {
	tfMs, ok := timeframeMilliseconds(scope.Timeframe)
	if !ok || tfMs <= 0 {
		// STRICT. The project's tolerant parser returns 1h for invalid input, and a window
		// measured in 1h buckets for a timeframe that does not exist would produce a plausible
		// and wrong result.
		return nil, fmt.Errorf("derivatives: timeframe %q not recognized", scope.Timeframe)
	}
	if !to.After(from) {
		return nil, fmt.Errorf("derivatives: empty window (%s..%s)", from.UTC(), to.UTC())
	}

	hot, err := r.readHot(ctx, scope, tfMs, from, to)
	if err != nil {
		return nil, err
	}

	// THE HOT FLOOR IS MEASURED FROM WHAT CAME BACK, not derived from the cap. See
	// DerivativesHotReader.
	hotFloor := r.hotFloor(hot, to)

	// THE WHOLE WINDOW FITS IN WHAT THE HOT TIER HAS: the durable tier is not queried.
	if !from.Before(hotFloor) {
		return hot, nil
	}

	// THE COLD QUERY GOES TO THE TOP OF THE WINDOW, not to the hot floor. The real gap is PER
	// FIELD, and stopping at the floor would leave the durable tier unasked for the buckets in
	// which the hot tier has the point but not the value.
	coldTo := to
	if r.cold == nil {
		return nil, fmt.Errorf("%w: %s desde %s", ErrDerivativesWindowUnservable, scope.Timeframe, from.UTC())
	}

	coldFrom := from
	if floor := coldTo.Add(-DurableMaxLookback); coldFrom.Before(floor) {
		// The durable tier's retention ceiling. Querying below it only costs partitions opened to
		// prove there is nothing there.
		coldFrom = floor
	}

	cold, err := r.cold.QueryDerivativesRange(ctx, scope, coldFrom, coldTo)
	if err != nil {
		return nil, fmt.Errorf("derivatives: durable read %s %s: %w", scope.Symbol, scope.Timeframe, err)
	}

	return mergeDerivativesTiers(cold, hot, from, to), nil
}

// hotFloor is the oldest instant the ZSET ACTUALLY delivered in this read.
func (r *DerivativesWindowReader) hotFloor(hot []*DerivativesSnapshot, to time.Time) time.Time {
	if len(hot) == 0 || hot[0] == nil {
		return to
	}
	return time.UnixMilli(hot[0].Timestamp).UTC()
}

// readHot reads the ZSET and slices the requested window.
func (r *DerivativesWindowReader) readHot(
	ctx context.Context,
	scope DerivativesScope,
	tfMs int64,
	from, to time.Time,
) ([]*DerivativesSnapshot, error) {
	// The limit derives from the WINDOW, not from a round number: requesting less truncates the
	// old end of the slice, and requesting "everything" brings the whole series on every read.
	buckets := (to.UnixMilli()-from.UnixMilli())/tfMs + 1
	if buckets < 1 {
		buckets = 1
	}

	snapshots, err := r.hot.GetSnapshotSeries(ctx, scope.Exchange, scope.MarketType, scope.Symbol, scope.Timeframe, int(buckets))
	if err != nil {
		return nil, fmt.Errorf("derivatives: zset read %s %s: %w", scope.Symbol, scope.Timeframe, err)
	}
	return sortedWindow(snapshots, from, to), nil
}

// mergeDerivativesTiers joins the two slices by bucket and returns them in ascending order.
//
// THE HOT TIER WINS PER FIELD, NOT PER POINT: overwriting the whole point replaced a value with a
// null in buckets where the ZSET has the point but not `openInterestBase`.
func mergeDerivativesTiers(cold, hot []*DerivativesSnapshot, from, to time.Time) []*DerivativesSnapshot {
	byBucket := make(map[int64]*DerivativesSnapshot, len(cold)+len(hot))
	for _, snapshot := range cold {
		if snapshot != nil {
			byBucket[snapshot.Timestamp] = coherentDerivatives(snapshot)
		}
	}
	for _, snapshot := range hot {
		if snapshot == nil {
			continue
		}
		byBucket[snapshot.Timestamp] = overlayDerivatives(byBucket[snapshot.Timestamp], snapshot)
	}

	merged := make([]*DerivativesSnapshot, 0, len(byBucket))
	for _, snapshot := range byBucket {
		merged = append(merged, snapshot)
	}
	return sortedWindow(merged, from, to)
}

// overlayDerivatives overlays `hot` on `base`, field by field — EXCEPT the funding group, which
// moves as a unit.
func overlayDerivatives(base, hot *DerivativesSnapshot) *DerivativesSnapshot {
	if base == nil {
		return hot
	}

	merged := *hot
	sources := make(map[string]string, 3)
	contributed := false

	// --- the two INDEPENDENT quantities, each with what travels with it ---
	//
	// THE COMPANION MOVES WITH THE PRINCIPAL, not on its own.
	for _, field := range []struct {
		name      string
		dst, src  **float64
		companion func()
	}{
		{
			name: "openInterestBase", dst: &merged.OpenInterestBase, src: &base.OpenInterestBase,
			companion: func() { merged.OpenInterest = base.OpenInterest },
		},
		{
			name: "longShortRatio", dst: &merged.LongShortRatio, src: &base.LongShortRatio,
			companion: func() {
				merged.LSLongPct = base.LSLongPct
				merged.LSShortPct = base.LSShortPct
			},
		},
	} {
		switch {
		case *field.dst != nil:
			sources[field.name] = hot.Source
			contributed = true
		case *field.src != nil:
			*field.dst = *field.src
			field.companion()
			sources[field.name] = base.Source
		}
	}

	// --- the funding group, ALL OR NOTHING ---
	switch {
	case merged.FundingRate != nil:
		sources["fundingRate"] = hot.Source
		contributed = true
	case base.FundingRate != nil:
		merged.FundingRate = base.FundingRate
		merged.FundingRate8h = base.FundingRate8h
		merged.FundingIntervalHours = base.FundingIntervalHours
		merged.NStep = base.NStep
		merged.InterestRate = base.InterestRate
		merged.FundingAtAnchor = base.FundingAtAnchor
		merged.NextFundingRate = base.NextFundingRate
		sources["fundingRate"] = base.Source
	}

	enforceFundingCoherence(&merged)

	if !contributed {
		// The hot tier brought not ONE number: the point is the durable one's, and the source too.
		merged.Source = base.Source
	}
	merged.FieldSources = costuraDeclarada(merged.Source, sources)
	return &merged
}

// enforceFundingCoherence applies the funding group's coherence rule to ONE point.
func enforceFundingCoherence(s *DerivativesSnapshot) {
	if !s.NStep.Normalizes() {
		s.FundingRate8h = nil
	}
	if s.NStep == NUnavailable {
		s.FundingIntervalHours = nil
		s.FundingAtAnchor = nil
	}
}

// coherentDerivatives returns a COPY of the durable point with a coherent funding group.
func coherentDerivatives(s *DerivativesSnapshot) *DerivativesSnapshot {
	out := *s
	enforceFundingCoherence(&out)
	return &out
}

// costuraDeclarada returns the per-field provenance WHEN it disagrees with the point's label, and
// nothing when the point has a single source.
func costuraDeclarada(source string, sources map[string]string) map[string]string {
	for _, fonte := range sources {
		if fonte != source {
			return sources
		}
	}
	return nil
}

// sortedWindow slices the window and sorts ASCENDING.
func sortedWindow(snapshots []*DerivativesSnapshot, from, to time.Time) []*DerivativesSnapshot {
	fromMs, toMs := from.UnixMilli(), to.UnixMilli()

	out := make([]*DerivativesSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if snapshot == nil || snapshot.Timestamp < fromMs || snapshot.Timestamp > toMs {
			continue
		}
		out = append(out, snapshot)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Timestamp < out[j].Timestamp })
	return out
}
