package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	logger "github.com/marxms/go-whale/internal/platform/logging"

	"github.com/redis/go-redis/v9"
)

// ============================================================================
// ROBUST BASELINE PER (SYMBOL, TIMEFRAME)
// ============================================================================
//
// The panel ranks on a robust z-score against each symbol's own history, because a fixed
// absolute rule fires at wildly different rates per symbol. This file owns the ruler: it
// recomputes {median, MAD, n, span, updatedAt} from the series ZSET and persists it.

const (
	// derivativesBaselineNMin is n_min: below this many observations the baseline is
	// IMMATURE and emits nothing.
	derivativesBaselineNMin = 30

	// derivativesBaselineMaxN is the declared baseline window, fixed at 288 points of the
	// timeframe itself. Measured: 288 beats 48 at p = 0.001; N moves the top-10, so it is
	// declared rather than discovered.
	derivativesBaselineMaxN = 288

	// derivativesRetentionDays is the measured retention of the positioning feeds: HTTP 400 at
	// 35 days, OK at 25. It truncates coarse timeframes below 288 points.
	derivativesRetentionDays = 30

	// derivativesBaselineRefreshInterval is the low cadence at which a baseline is recomputed.
	// One 5m bar moves 1/288 of the window, so 30 minutes drifts the ruler by about 2%.
	derivativesBaselineRefreshInterval = 30 * time.Minute

	// derivativesTrailingExcluded is how many newest stored points the derivatives rulers drop:
	// the possibly-open bucket and the bucket being scored. A ruler containing the observation
	// being scored lets it move its own ruler, hardest when it is extreme. It does not transfer
	// to the price ruler, which excludes by the identity of the scored bar and keeps this only
	// as a fallback. See priceAxis.
	derivativesTrailingExcluded = 2
)

// BucketClosed reports whether the bucket labeled labelTs has taken its final write by nowMs.
// The label is the bucket's start, so it closes once nowMs reaches labelTs+tfMs. This is the
// single definition of closure: the read path picks the scored bucket with it and the ruler
// excludes that bucket with it. With no usable bar width it answers true; both callers hold
// their own bar-width gates downstream.
func BucketClosed(labelTs, tfMs, nowMs int64) bool {
	if tfMs <= 0 {
		return true
	}
	return labelTs+tfMs <= nowMs
}

// scoredDerivativesBar returns the label of the bucket the surface scores — the last
// CLOSED one — out of a chronologically ASCENDING slice. Zero when no stored bucket has
// closed yet, which is also the answer for an empty series.
func scoredDerivativesBar(points []*derivatives.DerivativesSnapshot, tfMs, nowMs int64) int64 {
	for i := len(points) - 1; i >= 0; i-- {
		if points[i] == nil {
			continue
		}
		if BucketClosed(points[i].Timestamp, tfMs, nowMs) {
			return points[i].Timestamp
		}
	}
	return 0
}

// DerivativesBaseline is the persisted ruler for one (symbol, timeframe).
//
// Every axis is scored on the one-bar log increment x(t) = ln V(t) - ln V(t-1), over
// contracts, the long/short ratio and the close. The long/short axis is scored on the
// increment, not the level: as a level it carries strong autocorrelation, leaving an effective
// sample near 4 behind a nominal 288, and no threshold repair fixes a defect in the quantity.
//
// A nil axis means the baseline could not be built for it, not a zeroed ruler: a median and MAD
// of zero would read as a legitimate ruler for a symbol that never moves.
type DerivativesBaseline = derivatives.DerivativesBaseline

// derivativesBaselineWindowN returns 288 points of the timeframe, truncated by upstream
// retention: 5m and 1h reach 288, 4h reaches 180, 1d reaches 30. It returns 0 for an
// unparseable timeframe, and the caller must decline rather than borrow another window.
func derivativesBaselineWindowN(timeframe string) int {
	tfSec := candle.ParseTimeframeToSeconds(timeframe)
	if tfSec <= 0 {
		return 0
	}
	retentionPoints := int(int64(derivativesRetentionDays) * 86400 / tfSec)
	if retentionPoints < derivativesBaselineMaxN {
		return retentionPoints
	}
	return derivativesBaselineMaxN
}

// baselineFetchCount is how many stored points a recompute reads: the window plus the
// trailing-excluded points plus one, because N increments span N+1 observations. The price
// ruler excludes by the identity of the scored bar, so its sample can come up short of N; n
// reports that shortfall.
func baselineFetchCount(windowN int) int64 {
	return int64(windowN) + derivativesTrailingExcluded + 1
}

// RecomputeBaseline rebuilds the baseline for one (symbol, timeframe) from the series ZSET and
// persists it. It always writes, even for a short or degenerate sample: "we looked and there is
// not enough" must be distinguishable from "nobody computed this yet".
func (s *DerivativesRedisStore) RecomputeBaseline(ctx context.Context, exchange, marketType, symbol, timeframe string) (*DerivativesBaseline, error) {
	windowN := derivativesBaselineWindowN(timeframe)
	if s.HasCanonicalCoarse(timeframe) {
		windowN = derivativesBaselineMaxN
	}
	if windowN <= 0 {
		return nil, fmt.Errorf("derivatives baseline: unusable timeframe %q", timeframe)
	}

	ident := derivativesSeriesIdentity{Exchange: exchange, MarketType: marketType, Symbol: symbol}
	var points []*derivatives.DerivativesSnapshot
	if s.HasCanonicalCoarse(timeframe) {
		newest, err := s.coarseSeries(ctx, exchange, marketType, symbol, timeframe, int(baselineFetchCount(windowN)))
		if err != nil {
			return nil, err
		}
		for _, point := range newest {
			if point == nil || point.Source != derivatives.SourceCoinalyze || point.Exchange != exchange || point.MarketType != marketType || point.Symbol != symbol {
				return nil, fmt.Errorf("coarse derivatives: inconsistent canonical series for %s %s", symbol, timeframe)
			}
		}
		points = append(points, newest...)
		sort.Slice(points, func(i, j int) bool { return points[i].Timestamp < points[j].Timestamp })
	} else {
		seriesKey := derivSeriesZsetKey(exchange, marketType, symbol, timeframe)

		tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
		members, err := s.client.ZRange(tctx, seriesKey, -baselineFetchCount(windowN), -1).Result()
		cancel()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("derivatives baseline: zrange %s: %w", seriesKey, err)
		}

		points = make([]*derivatives.DerivativesSnapshot, 0, len(members))
		for _, m := range members {
			snap, decErr := decodeDerivativesSeriesMember(m, ident)
			if decErr != nil {
				if errors.Is(decErr, ErrDerivativesSeriesUnknownVersion) {
					// An unknown version means the whole key is in a format this binary does
					// not read, so the ruler cannot be built.
					return nil, fmt.Errorf("derivatives baseline: %s: %w", seriesKey, decErr)
				}
				// One corrupt member drops one observation; n reports the shortfall.
				continue
			}
			points = append(points, snap)
		}

	}

	nowMs := time.Now().UnixMilli()
	var baseline *DerivativesBaseline
	if s.HasCanonicalCoarse(timeframe) {
		tfMs := candle.ParseTimeframeToSeconds(timeframe) * 1000
		scored := scoredDerivativesBar(points, tfMs, nowMs)
		cut := 0
		for cut < len(points) && points[cut].Timestamp < scored {
			cut++
		}
		baseline = buildDerivativesBaselineSample(points[:cut], timeframe, windowN, nowMs)
		baseline.Source = derivatives.SourceCoinalyze
		for i := len(points) - 1; i >= 0 && len(baseline.SeriesTail) < 3; i-- {
			baseline.SeriesTail = append(baseline.SeriesTail, points[i])
		}
	} else {
		baseline = buildDerivativesBaseline(points, timeframe, windowN, nowMs)
	}
	// The price axis comes from the candle series of the same (symbol, timeframe), the only
	// place a close exists. It is fetched separately because availability is independent: a
	// derivatives hole must not blank the price ruler and vice versa. The scored bar label does
	// travel across, so the price ruler excludes the same bar; excluding this one also excludes
	// any newer one, so the exclusion only ever errs toward dropping more.
	tfMs := candle.ParseTimeframeToSeconds(timeframe) * 1000
	baseline.Price, baseline.PriceState = s.priceAxis(ctx, exchange, marketType, symbol, timeframe,
		windowN, nowMs, scoredDerivativesBar(points, tfMs, nowMs))
	baseline.Exchange = exchange
	baseline.MarketType = marketType
	baseline.Symbol = symbol
	baseline.Timeframe = timeframe

	data, err := json.Marshal(baseline)
	if err != nil {
		return nil, fmt.Errorf("derivatives baseline: marshal %s %s: %w", symbol, timeframe, err)
	}

	wctx, wcancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	err = s.client.Set(wctx, derivBaselineKey(exchange, marketType, symbol, timeframe), data, 0).Err()
	wcancel()
	if err != nil {
		return nil, fmt.Errorf("derivatives baseline: set %s %s: %w", symbol, timeframe, err)
	}

	return baseline, nil
}

// GetBaseline reads the persisted baseline. A missing key returns (nil, nil): the
// ruler has not been built yet, which is not an error.
func (s *DerivativesRedisStore) GetBaseline(ctx context.Context, exchange, marketType, symbol, timeframe string) (*DerivativesBaseline, error) {
	tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	data, err := s.client.Get(tctx, derivBaselineKey(exchange, marketType, symbol, timeframe)).Bytes()
	cancel()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("derivatives baseline: get %s %s: %w", symbol, timeframe, err)
	}

	var baseline DerivativesBaseline
	if err := json.Unmarshal(data, &baseline); err != nil {
		return nil, fmt.Errorf("derivatives baseline: unmarshal %s %s: %w", symbol, timeframe, err)
	}
	if s.HasCanonicalCoarse(timeframe) && baseline.Source != derivatives.SourceCoinalyze {
		return nil, nil
	}
	return &baseline, nil
}

// buildDerivativesBaseline computes both axes from a chronologically ASCENDING slice
// of stored points.
//
// # STRICTLY TRAILING
//
// The derivativesTrailingExcluded newest points are dropped before anything is
// computed — the possibly-open bucket and, behind it, the bucket being scored. A
// baseline that contains the observation being scored lets that observation move its
// own ruler, and it does so hardest exactly when the observation is extreme — the
// score would be smallest when the event is largest.
//
// # ADJACENCY
//
// An increment is taken only between two points exactly one bar apart. Bridging a
// hole would silently produce a WIDER window than the timeframe declares, which is
// the phantom-reading defect the measurement spec forbids; the pair is dropped
// instead, and n reports the loss.
func buildDerivativesBaseline(points []*derivatives.DerivativesSnapshot, timeframe string, windowN int, nowMs int64) *DerivativesBaseline {
	// Strictly trailing: exclude the possibly-open bucket and the scored one.
	if len(points) > derivativesTrailingExcluded {
		points = points[:len(points)-derivativesTrailingExcluded]
	} else {
		points = nil
	}

	return buildDerivativesBaselineSample(points, timeframe, windowN, nowMs)
}

func buildDerivativesBaselineSample(points []*derivatives.DerivativesSnapshot, timeframe string, windowN int, nowMs int64) *DerivativesBaseline {
	baseline := &DerivativesBaseline{
		Timeframe: timeframe,
		WindowN:   windowN,
		NMin:      derivativesBaselineNMin,
		UpdatedAt: nowMs,
	}

	tfMs := candle.ParseTimeframeToSeconds(timeframe) * 1000

	var oi, lsr baselineSample
	for i, p := range points {
		if i == 0 || p == nil {
			continue
		}
		prev := points[i-1]
		if prev == nil {
			continue
		}
		if prev.OpenInterestBase != nil && p.OpenInterestBase != nil {
			addIncrement(&oi, *p.OpenInterestBase, *prev.OpenInterestBase, p.Timestamp, prev.Timestamp, tfMs)
		}
		if prev.LongShortRatio != nil && p.LongShortRatio != nil {
			addIncrement(&lsr, *p.LongShortRatio, *prev.LongShortRatio, p.Timestamp, prev.Timestamp, tfMs)
		}
	}

	baseline.OpenInterest, baseline.OpenInterestState = oi.finish(windowN, nowMs)
	baseline.LongShortRatio, baseline.LongShortRatioState = lsr.finish(windowN, nowMs)
	return baseline
}

// addIncrement records the one-bar log increment between two observations, and is the
// single place both gates live so no axis can quietly acquire a different window than
// the others.
//
// ADJACENCY: the pair is dropped unless the two timestamps are exactly one bar apart.
// Bridging a hole would produce an increment over a WIDER span than the timeframe
// declares, which reads as a move on a bar where nothing moved.
//
// POSITIVITY is inherited from LogIncrement rather than restated here.
func addIncrement(sample *baselineSample, curr, prev float64, currTs, prevTs, tfMs int64) {
	if tfMs <= 0 || currTs-prevTs != tfMs {
		return
	}
	if increment, ok := derivatives.LogIncrement(curr, prev); ok {
		sample.add(increment, currTs)
	}
}

// priceAxis builds the ruler of r(t) = ln(close(t)/close(t-1)) from the candle series of the
// same pair. The notional on the derivatives snapshot is contracts x price, so a return derived
// from it would carry position flow. An absent candle series leaves the axis absent rather than
// failing the whole recompute; the other rulers are still good. scoredBarTs is the bar the
// reading is scored on, zero when none has closed.
func (s *DerivativesRedisStore) priceAxis(ctx context.Context, exchange, marketType, symbol, timeframe string,
	windowN int, nowMs, scoredBarTs int64,
) (*derivatives.Baseline, derivatives.BaselineState) {
	var sample baselineSample

	key := candlesKeyV2(exchange, marketType, symbol, timeframe)
	tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	members, err := s.client.ZRange(tctx, key, -baselineFetchCount(windowN), -1).Result()
	cancel()
	if err != nil && !errors.Is(err, redis.Nil) {
		logger.OrDiscard(s.log).Warn("derivatives: price baseline zrange failed",
			"symbol", symbol, "timeframe", timeframe, "key", key, "error", err)
		return sample.finish(windowN, nowMs)
	}

	closes := make([]candle.Candle, 0, len(members))
	for _, m := range members {
		candle, decErr := decodeCandleMember(m)
		if decErr != nil {
			if errors.Is(decErr, ErrCandleMemberUnknownVersion) {
				// The whole key is in a format this binary does not read, so the ruler cannot
				// be built.
				logger.OrDiscard(s.log).Error("derivatives: price baseline decode failed",
					"symbol", symbol, "timeframe", timeframe, "error", decErr)
				return sample.finish(windowN, nowMs)
			}
			// One corrupt member costs one observation; n reports the shortfall.
			continue
		}
		closes = append(closes, candle)
	}

	// Strictly trailing, by the identity of the scored bar rather than by position. The
	// observation being scored must not be in its own ruler, or the z comes out smallest
	// exactly when the move is largest. Every increment ending at or after the scored bar is
	// dropped, which also covers the open candle. A fixed count would be equivalent only while
	// both series sit at the same bar, with zero margin.
	switch {
	case scoredBarTs > 0:
		cut := len(closes)
		for cut > 0 && closes[cut-1].Timestamp >= scoredBarTs {
			cut--
		}
		closes = closes[:cut]
	case len(closes) > derivativesTrailingExcluded:
		// No derivatives bucket has closed, so the positional drop stands in: the newest
		// candle may be open, and the one behind it becomes the scored bar.
		closes = closes[:len(closes)-derivativesTrailingExcluded]
	default:
		closes = nil
	}

	tfMs := candle.ParseTimeframeToSeconds(timeframe) * 1000
	for i := 1; i < len(closes); i++ {
		addIncrement(&sample, closes[i].Close, closes[i-1].Close, closes[i].Timestamp, closes[i-1].Timestamp, tfMs)
	}

	return sample.finish(windowN, nowMs)
}

// baselineSample accumulates one axis's values and their timestamps, so the persisted span
// describes the sample actually used, not the range read.
type baselineSample struct {
	values    []float64
	timestamp []int64
}

func (s *baselineSample) add(value float64, timestampMs int64) {
	s.values = append(s.values, value)
	s.timestamp = append(s.timestamp, timestampMs)
}

// finish keeps the most recent windowN observations and returns the baseline plus its state.
// Nothing to compute returns a nil baseline, never a zeroed ruler.
func (s *baselineSample) finish(windowN int, nowMs int64) (*derivatives.Baseline, derivatives.BaselineState) {
	if len(s.values) == 0 {
		return nil, derivatives.Baseline{}.State(derivativesBaselineNMin)
	}
	if windowN > 0 && len(s.values) > windowN {
		s.values = s.values[len(s.values)-windowN:]
		s.timestamp = s.timestamp[len(s.timestamp)-windowN:]
	}

	baseline := derivatives.Compute(s.values)
	baseline.SpanMs = s.timestamp[len(s.timestamp)-1] - s.timestamp[0]
	baseline.UpdatedAt = nowMs
	return &baseline, baseline.State(derivativesBaselineNMin)
}

// PriceIncrement returns r(t) = ln(close(t)/close(t-1)) for the bar stamped barTs, so the price
// axis is scored on the same bar as the other two. A missing candle returns (nil, reason, nil);
// only a store failure is an error.
func (s *DerivativesRedisStore) PriceIncrement(ctx context.Context, exchange, marketType, symbol, timeframe string,
	barTs int64,
) (*float64, string, error) {
	tfMs := candle.ParseTimeframeToSeconds(timeframe) * 1000
	if tfMs <= 0 {
		return nil, "the timeframe carries no bar width, so no increment is defined", nil
	}
	if barTs <= 0 {
		return nil, "no bar to score", nil
	}

	key := candlesKeyV2(exchange, marketType, symbol, timeframe)
	tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	members, err := s.client.ZRangeByScore(tctx, key, &redis.ZRangeBy{
		Min: strconv.FormatInt(barTs-tfMs, 10),
		Max: strconv.FormatInt(barTs, 10),
	}).Result()
	cancel()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, "", fmt.Errorf("derivatives price increment: zrange %s: %w", key, err)
	}

	// The two bars are looked up by timestamp, not position: a missing member is a hole, and
	// an increment is never taken across a hole.
	byTs := make(map[int64]float64, 2)
	for _, m := range members {
		candle, decErr := decodeCandleMember(m)
		if decErr != nil {
			if errors.Is(decErr, ErrCandleMemberUnknownVersion) {
				return nil, "", fmt.Errorf("derivatives price increment: %s: %w", key, decErr)
			}
			continue
		}
		byTs[candle.Timestamp] = candle.Close
	}

	curr, hasCurr := byTs[barTs]
	prev, hasPrev := byTs[barTs-tfMs]
	switch {
	case !hasCurr && !hasPrev:
		return nil, "no candle is stored for this bar or the one before it", nil
	case !hasCurr:
		return nil, "no candle is stored for the bar the derivatives reading belongs to", nil
	case !hasPrev:
		return nil, "the preceding bar has no candle; an increment is never taken across a hole", nil
	}

	increment, ok := derivatives.LogIncrement(curr, prev)
	if !ok {
		return nil, "the close is not positive, so it has no logarithm", nil
	}
	return &increment, "", nil
}

// ============================================================================
// LOW-CADENCE REFRESH — DRIVEN BY THE WRITE PATH
// ============================================================================

// refreshDueBaseline recomputes at most one (symbol, timeframe) baseline per saved snapshot,
// so a restart does not rebuild the whole fleet inside the poller's goroutines. Two pollers
// racing on the same pair may both recompute; the recompute is idempotent, and holding a lock
// across a Redis round trip would serialize every save.
//
// It serves the oldest due timeframe rather than the first in registry order, which would
// starve the tail: a sweep of eight timeframes costs eight saves, longer than the refresh
// interval, so the prefix fell due again before the end was reached. A timeframe with no ruler
// is maximally overdue. With eight timeframes this refreshes each ruler about every 40 minutes
// rather than the declared 30.
func (s *DerivativesRedisStore) refreshDueBaseline(ctx context.Context, snapshot *derivatives.DerivativesSnapshot, timeframes []string) {
	if snapshot == nil {
		return
	}
	nowMs := time.Now().UnixMilli()

	var (
		chosenTf  string
		chosenKey string
		chosenAge int64 = -1
	)
	for _, tf := range timeframes {
		trackerKey := baselineTrackerKey(snapshot.Exchange, snapshot.MarketType, snapshot.Symbol, tf)
		age, due := s.baselineAge(ctx, snapshot, tf, trackerKey, nowMs)
		if !due || age <= chosenAge {
			continue
		}
		chosenTf, chosenKey, chosenAge = tf, trackerKey, age
	}
	if chosenAge < 0 {
		return
	}

	// Marked before the outcome is known: a failing recompute must retry on the same low
	// cadence, and a permanently unreadable timeframe must not pin itself at the top.
	s.markBaselineRefreshed(chosenKey, nowMs)

	if _, err := s.RecomputeBaseline(ctx, snapshot.Exchange, snapshot.MarketType, snapshot.Symbol, chosenTf); err != nil {
		logger.OrDiscard(s.log).Warn("derivatives: baseline recompute failed",
			"symbol", snapshot.Symbol, "timeframe", chosenTf, "error", err)
	}
}

func baselineTrackerKey(exchange, marketType, symbol, timeframe string) string {
	return exchange + ":" + marketType + ":" + symbol + ":" + timeframe
}

// baselineAge reports how long ago this (symbol, timeframe) was refreshed, and whether it is
// past the interval. It returns the age because the caller ranks candidates: a timeframe never
// computed must outrank one a minute stale. The in-process map answers the steady state; only
// after a restart does it fall through to the persisted UpdatedAt.
func (s *DerivativesRedisStore) baselineAge(ctx context.Context, snapshot *derivatives.DerivativesSnapshot, timeframe, trackerKey string, nowMs int64) (ageMs int64, due bool) {
	intervalMs := derivativesBaselineRefreshInterval.Milliseconds()

	s.baselineMu.Lock()
	lastAt, seen := s.baselineLastAt[trackerKey]
	s.baselineMu.Unlock()
	if seen {
		age := nowMs - lastAt
		return age, age >= intervalMs
	}

	persisted, err := s.GetBaseline(ctx, snapshot.Exchange, snapshot.MarketType, snapshot.Symbol, timeframe)
	if err != nil || persisted == nil {
		// Unreadable or absent: maximally overdue, so a timeframe with no ruler outranks one
		// with a stale ruler. The tracker is marked either way, so this cannot spin.
		return math.MaxInt64, true
	}
	s.markBaselineRefreshed(trackerKey, persisted.UpdatedAt)
	age := nowMs - persisted.UpdatedAt
	return age, age >= intervalMs
}

func (s *DerivativesRedisStore) markBaselineRefreshed(trackerKey string, atMs int64) {
	s.baselineMu.Lock()
	defer s.baselineMu.Unlock()
	if s.baselineLastAt == nil {
		s.baselineLastAt = make(map[string]int64)
	}
	s.baselineLastAt[trackerKey] = atMs
}
