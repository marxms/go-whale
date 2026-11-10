package venue

import (
	"context"

	"github.com/marxms/go-whale/internal/derivatives"
)

const maxOpenInterestHistoryLimit = 500

// OpenInterest returns the current contract count for one contract.
func (c *Client) OpenInterest(ctx context.Context, symbol string) (derivatives.OpenInterest, error) {
	return c.driver.OpenInterest(ctx, symbol)
}

// OpenInterestHistory returns samples inside the explicit page window.
func (c *Client) OpenInterestHistory(
	ctx context.Context,
	symbol, timeframe string,
	sinceMs, untilMs, limit int64,
) ([]derivatives.OpenInterestHistoryPoint, error) {
	return c.driver.OpenInterestHistory(ctx, symbol, timeframe, sinceMs, untilMs, limit)
}

func clampOIHistoryLimit(limit int64) int64 {
	if limit <= 0 {
		return maxOpenInterestHistoryLimit
	}
	if limit > maxOpenInterestHistoryLimit {
		return maxOpenInterestHistoryLimit
	}
	return limit
}

func firstNonZero(row map[string]any, keys ...string) any {
	for _, key := range keys {
		if value, ok := asInt64(row[key]); ok && value > 0 {
			return value
		}
	}
	return nil
}
