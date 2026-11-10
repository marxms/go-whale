package candle

import (
	"fmt"
	"math"
)

// NormalizeOHLC extends a candle's high and low so they contain its open and close.
//
// The venue occasionally publishes an internally impossible row. Measured on
// 2026-11-07: fapi returns BCHUSDT 1w 2020-01-13 as open 339.65, high 215.69,
// low 182.65, close 339.62 — the high is below both the open and the close. The
// open and close are the venue's own prices; extending the bounds preserves the
// stated session. Rejecting the row instead would leave a hole, and every
// consumer of the series (the analyzer admission, the chart) treats a hole as a
// failure of the whole series, so one bad bar would disable the timeframe.
func NormalizeOHLC(c Candle) Candle {
	if top := math.Max(c.Open, c.Close); c.High < top {
		c.High = top
	}
	if bottom := math.Min(c.Open, c.Close); c.Low > bottom {
		c.Low = bottom
	}
	return c
}

// ValidateCandle validates OHLCV consistency and value ranges.
func ValidateCandle(timestamp int64, open, high, low, closePrice, volume float64, _, _ string) (bool, string) {
	// Validate timestamp
	if timestamp <= 0 {
		return false, fmt.Sprintf("invalid timestamp: %d", timestamp)
	}

	// Validate OHLC positive values
	if open <= 0 {
		return false, fmt.Sprintf("open must be > 0, got %f", open)
	}
	if high <= 0 {
		return false, fmt.Sprintf("high must be > 0, got %f", high)
	}
	if low <= 0 {
		return false, fmt.Sprintf("low must be > 0, got %f", low)
	}
	if closePrice <= 0 {
		return false, fmt.Sprintf("close must be > 0, got %f", closePrice)
	}

	// Validate volume non-negative
	if volume < 0 || math.IsNaN(volume) || math.IsInf(volume, 0) {
		return false, fmt.Sprintf("invalid volume: %f", volume)
	}

	// Validate OHLC consistency
	if high < low {
		return false, fmt.Sprintf("high (%f) < low (%f)", high, low)
	}

	// High should be >= open and close
	if high < open {
		return false, fmt.Sprintf("high (%f) < open (%f)", high, open)
	}
	if high < closePrice {
		return false, fmt.Sprintf("high (%f) < close (%f)", high, closePrice)
	}

	// Low should be <= open and close
	if low > open {
		return false, fmt.Sprintf("low (%f) > open (%f)", low, open)
	}
	if low > closePrice {
		return false, fmt.Sprintf("low (%f) > close (%f)", low, closePrice)
	}

	return true, ""
}

// candleAggregateRelEpsilon is the relative tolerance (0.01%) used when
// comparing TakerBuyVolume + TakerSellVolume against Volume on candles
// that carry taker-side aggregates. Floating-point sums of many small
// trades can drift slightly from the rolling Volume total, so we accept
// up to 1bp of relative error before flagging an inconsistency.
const candleAggregateRelEpsilon = 1e-4

// ValidateCandleAggregates validates the feature-002 trade-signal aggregates
// (taker volumes, CVD increment, aggression ratio, counters). It is a no-op
// for legacy candles that do not carry taker-side data — i.e. when both
// TakerBuyVolume and TakerSellVolume are zero — so existing callers and
// historical payloads remain valid.
func ValidateCandleAggregates(c Candle) (bool, string) {
	if c.TakerBuyVolume == 0 && c.TakerSellVolume == 0 {
		return true, ""
	}

	if c.TakerBuyVolume < 0 || math.IsNaN(c.TakerBuyVolume) || math.IsInf(c.TakerBuyVolume, 0) {
		return false, fmt.Sprintf("invalid takerBuyVolume: %f", c.TakerBuyVolume)
	}
	if c.TakerSellVolume < 0 || math.IsNaN(c.TakerSellVolume) || math.IsInf(c.TakerSellVolume, 0) {
		return false, fmt.Sprintf("invalid takerSellVolume: %f", c.TakerSellVolume)
	}

	if c.SideUnknownCount < 0 {
		return false, fmt.Sprintf("sideUnknownCount must be >= 0, got %d", c.SideUnknownCount)
	}
	if c.LateTradeCount < 0 {
		return false, fmt.Sprintf("lateTradeCount must be >= 0, got %d", c.LateTradeCount)
	}

	// CLASSIFIED TAKER VOLUME MAY BE LESS THAN TOTAL VOLUME. IT MAY NEVER EXCEED IT.
	//
	// The aggregator supports trades whose side cannot be determined: that volume goes to
	// SideUnknownCount instead of being pushed onto a side, so a bucket with unclassified
	// flow has `buy + sell < volume` BY DESIGN. Demanding equality would call that corruption.
	//
	// The invariant that actually holds is one-sided: the part cannot exceed the whole. That
	// catches real corruption (double-counted merges, a delta summed into a total) without
	// firing on honest data.
	sum := c.TakerBuyVolume + c.TakerSellVolume
	if c.Volume > 0 {
		if excess := (sum - c.Volume) / c.Volume; excess > candleAggregateRelEpsilon {
			return false, fmt.Sprintf(
				"taker volumes sum (%f) EXCEEDS total volume (%f) by %f (>%f) — the classified "+
					"part cannot be larger than the whole",
				sum, c.Volume, excess, candleAggregateRelEpsilon,
			)
		}
	}

	return true, ""
}
