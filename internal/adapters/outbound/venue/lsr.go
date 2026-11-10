package venue

import (
	"context"
	"math"
	"strconv"

	"github.com/marxms/go-whale/internal/derivatives"
)

// lsrShareSumTolerance accepts a pair whose two rounded shares sum to one
// within the venue's four-decimal rounding floor.
const lsrShareSumTolerance = 1e-3

// LongShortRatioHistory returns global long/short account ratio samples.
func (c *Client) LongShortRatioHistory(
	ctx context.Context,
	symbol, timeframe string,
	limit int64,
	sinceMs, endMs int64,
) ([]derivatives.LongShortRatioPoint, error) {
	return c.driver.LongShortRatioHistory(ctx, symbol, timeframe, limit, sinceMs, endMs)
}

func parseShareFraction(raw any) (float64, bool) {
	value, ok := asString(raw)
	if !ok {
		return 0, false
	}
	fraction, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(fraction) || math.IsInf(fraction, 0) {
		return 0, false
	}
	return fraction, true
}

func validateShareSum(longFraction, shortFraction float64, hasLong, hasShort bool) (bool, string) {
	if !hasLong || !hasShort {
		return false, "share_absent"
	}
	if math.IsNaN(longFraction) || math.IsInf(longFraction, 0) ||
		math.IsNaN(shortFraction) || math.IsInf(shortFraction, 0) {
		return false, "share_not_finite"
	}
	if math.Abs(longFraction+shortFraction-1) > lsrShareSumTolerance {
		return false, "sum_off"
	}
	return true, ""
}
