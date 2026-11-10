package venue

import (
	"context"

	"github.com/marxms/go-whale/internal/derivatives"
)

// FundingRate returns the current funding reading for one contract.
func (c *Client) FundingRate(ctx context.Context, symbol string) (derivatives.FundingRate, error) {
	return c.driver.FundingRate(ctx, symbol)
}

// FundingRateHistory returns settlements in venue order; absent fields stay absent so the
// caller can distinguish a missing rate from zero.
func (c *Client) FundingRateHistory(
	ctx context.Context,
	symbol string,
	limit int64,
) ([]derivatives.FundingRateHistoryPoint, error) {
	return c.driver.FundingRateHistory(ctx, symbol, limit)
}

func fundingRow(payload any) (map[string]any, bool) {
	if row, ok := asMap(payload); ok {
		return row, true
	}
	if rows, ok := asSlice(payload); ok && len(rows) > 0 {
		return asMap(rows[0])
	}
	return nil, false
}

func clampHistoryLimit(limit int64) int64 {
	if limit <= 0 {
		return 1
	}
	if limit > maxKlineLimit {
		return maxKlineLimit
	}
	return limit
}
