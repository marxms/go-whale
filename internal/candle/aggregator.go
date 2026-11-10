// Package candle owns candle values, bucketing and exact timeframe aggregation.
package candle

import (
	"math"
	"strconv"
	"strings"

	"github.com/marxms/go-whale/internal/market"
)

// BucketedCandle represents an aggregated candle for a specific bucket.
type BucketedCandle struct {
	Timestamp int64
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64

	// The measured order-flow split. Populated whenever the trades carried side
	// information; both zero when no side could be determined for any trade in the bucket.
	//
	// Only what was MEASURED lives here. Per-bar delta, cumulative delta and aggression
	// ratio are arithmetic on this pair and are computed by whoever consumes it — keeping
	// them here put the same formula in two languages and they drifted.
	TakerBuyVolume  float64
	TakerSellVolume float64
	// SideUnknownCount is volume seen but not attributable to a side. It is deliberately NOT
	// folded into either side: doing so would invent a measurement.
	SideUnknownCount int64
	// LastTradeAt is the exchange timestamp of the newest trade folded into this bucket. It is
	// how far this measurement REACHES, and the open-bucket merge cannot decide anything about
	// an exchange total without it — see candles.Candle.LastTradeAt.
	LastTradeAt int64
}

// BuildOHLCVC aggregates trades into candles per timeframe bucket.
// This is equivalent to the reference implementation's build_ohlcvc(trades, timeframe).
// Returns a map of bucket_timestamp -> aggregated_candle.
//
// It is a thin wrapper over BuildOHLCVCInto that allocates the two working maps
// per call. Hot callers that flush on every trade batch should call
// BuildOHLCVCInto with pooled maps instead to avoid that per-call allocation.
func BuildOHLCVC(trades []Trade, timeframe string) map[int64]BucketedCandle {
	result := make(map[int64]BucketedCandle)
	if len(trades) == 0 {
		return result
	}
	BuildOHLCVCInto(trades, timeframe, make(map[int64][]Trade), result)
	return result
}

// BuildOHLCVCInto is the allocation-free core of BuildOHLCVC: it writes the
// aggregated candles into the caller-provided result map, using buckets as
// scratch space for grouping trades by timeframe bucket. Both maps MUST be
// empty on entry; the caller owns them and is responsible for clearing/reusing
// them (e.g. from a sync.Pool). result holds only value-type BucketedCandle
// entries, so a caller that reads them out before clearing retains no reference
// into either map.
func BuildOHLCVCInto(trades []Trade, timeframe string, buckets map[int64][]Trade, result map[int64]BucketedCandle) {
	if len(trades) == 0 {
		return
	}

	// OPTIMIZATION: Assume trades are already sorted by timestamp (from WebSocket)
	// Avoid unnecessary sorting overhead for real-time streams

	// Group trades by bucket
	for i := range trades {
		bucket := BucketTimestamp(trades[i].Timestamp, timeframe)
		buckets[bucket] = append(buckets[bucket], trades[i])
	}

	// Aggregate each bucket
	for bucket, bucketTrades := range buckets {
		if len(bucketTrades) == 0 {
			continue
		}

		// Compute high/low, volume, and correct open/close using earliest/latest timestamp
		high, low := math.Inf(-1), math.Inf(1)
		var volume float64
		var earliestID, latestID string
		var earliestTs int64
		var earliestPrice float64
		var latestTs int64
		var latestPrice float64

		var takerBuyVolume, takerSellVolume float64
		var sideUnknownCount int64

		for i := range bucketTrades {
			ts := bucketTrades[i].Timestamp
			price := bucketTrades[i].Price
			amount := bucketTrades[i].Amount
			if price > high {
				high = price
			}
			if price < low {
				low = price
			}
			volume += amount

			// OPEN AND CLOSE ARE ELECTED BY (timestamp, trade id) — NOT BY SLICE ORDER.
			//
			// An exchange emits many aggregate trades inside the same millisecond. Resolving a
			// tie by position in the slice made the open whichever trade the batch listed first
			// and the close whichever it listed last; measured against the aggTrade tape, 4.2%
			// of sampled bars carried a price the exchange never printed as that bar's open. A
			// wrong price is not a rounding error.
			//
			// The aggregate-trade id IS the exchange's own ordering within a millisecond, so
			// using it makes the election deterministic. A trade with no usable id never
			// displaces one that has an id (other venues send non-numeric ids), which keeps the
			// fallback stable instead of worse.
			id := bucketTrades[i].ID
			if earliestTs == 0 || ts < earliestTs || (ts == earliestTs && tradeIDPrecedes(id, earliestID)) {
				earliestTs = ts
				earliestPrice = price
				earliestID = id
			}
			if ts > latestTs || (ts == latestTs && tradeIDPrecedes(latestID, id)) {
				latestTs = ts
				latestPrice = price
				latestID = id
			}

			switch strings.ToLower(strings.TrimSpace(string(bucketTrades[i].Side))) {
			case "buy":
				takerBuyVolume += amount
			case "sell":
				takerSellVolume += amount
			default:
				sideUnknownCount++
			}
		}

		candle := BucketedCandle{
			Timestamp: bucket,
			Open:      earliestPrice, // Earliest trade in bucket by timestamp
			High:      high,
			Low:       low,
			Close:     latestPrice, // Latest trade in bucket by timestamp
			Volume:    volume,

			TakerBuyVolume:   takerBuyVolume,
			TakerSellVolume:  takerSellVolume,
			SideUnknownCount: sideUnknownCount,
			// latestTs is elected by (timestamp, trade id) above, so this is the true newest
			// trade of the bucket and not an artifact of slice order.
			LastTradeAt: latestTs,
		}

		result[bucket] = candle
	}
}

// BucketTimestamp rounds a timestamp down to the start of its timeframe bucket.
// This is the canonical bucket-boundary logic (including the 1w Monday-00:00-UTC
// alignment). Gap-recovery, the seeder, and gapfiller MUST delegate to it rather than
// compute (t/tfMs)*tfMs, which is Thursday-epoch-aligned and misclassifies the 1w
// bucket by 72h (leaking the open weekly candle into cold as a partial "closed" one).
func BucketTimestamp(ts int64, timeframe string) int64 {
	interval, err := market.ParseTimeframe(timeframe)
	if err != nil {
		return 0
	}
	return interval.BucketStart(ts)
}

// tradeIDPrecedes reports whether trade id `a` comes before `b` in the exchange's own ordering.
//
// Aggregate-trade ids are monotonically increasing per symbol, so within a single millisecond
// they are the only total order available — timestamps tie and slice position is an artifact of
// how the batch was assembled, not of what happened on the exchange.
//
// A trade whose id is missing or non-numeric NEVER displaces one that has a usable id: venues
// other than Binance send opaque ids, and for those this degrades to first-or-last-seen
// behavior rather than to something worse. When neither side has a usable id the answer is
// false, which keeps the incumbent and makes the election stable.
func tradeIDPrecedes(a, b string) bool {
	an, aok := strconv.ParseInt(a, 10, 64)
	bn, bok := strconv.ParseInt(b, 10, 64)
	switch {
	case aok == nil && bok == nil:
		return an < bn
	case aok == nil:
		// `a` is ordered and `b` is not: prefer the one the exchange can order.
		return true
	default:
		return false
	}
}
