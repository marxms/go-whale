package venue

import (
	"context"

	"github.com/marxms/go-whale/internal/candle"
)

const (
	defaultKlineLimit = 500
	maxKlineLimit     = 1000
)

// FetchOHLCV returns OHLCV-only candles for one internal symbol.
func (c *Client) FetchOHLCV(
	ctx context.Context,
	symbol, timeframe string,
	sinceMs, limit int64,
) ([]candle.Candle, error) {
	return c.driver.FetchOHLCV(ctx, symbol, timeframe, sinceMs, limit)
}

// FetchKlinesWithCVD returns CVD-complete candles when the venue serves the raw kline
// carrying taker-buy volume. ok=false means the venue does not support it (or the response
// was unusable) and the caller must fall back to FetchOHLCV.
func (c *Client) FetchKlinesWithCVD(
	ctx context.Context,
	symbol, timeframe string,
	limit int,
) ([]candle.Candle, bool, error) {
	return c.driver.FetchKlinesWithCVD(ctx, symbol, timeframe, limit)
}

func clampKlineLimit(limit int64) int64 {
	if limit <= 0 {
		return defaultKlineLimit
	}
	if limit > maxKlineLimit {
		return maxKlineLimit
	}
	return limit
}

func parseOHLCVRow(raw any) (candle.Candle, bool) {
	switch row := raw.(type) {
	case []any:
		return parseOHLCVArray(row)
	case map[string]any:
		return parseOHLCVObject(row)
	default:
		return candle.Candle{}, false
	}
}

func parseOHLCVArray(row []any) (candle.Candle, bool) {
	if len(row) < 6 {
		return candle.Candle{}, false
	}
	open, ok := asFloat(row[1])
	if !ok {
		return candle.Candle{}, false
	}
	high, ok := asFloat(row[2])
	if !ok {
		return candle.Candle{}, false
	}
	low, ok := asFloat(row[3])
	if !ok {
		return candle.Candle{}, false
	}
	closePrice, ok := asFloat(row[4])
	if !ok {
		return candle.Candle{}, false
	}
	volume, ok := asFloat(row[5])
	if !ok {
		return candle.Candle{}, false
	}
	timestamp, ok := asInt64(row[0])
	if !ok {
		return candle.Candle{}, false
	}
	return candle.NormalizeOHLC(candle.Candle{
		Timestamp: timestamp,
		Open:      open,
		High:      high,
		Low:       low,
		Close:     closePrice,
		Volume:    volume,
	}), true
}

func parseOHLCVObject(row map[string]any) (candle.Candle, bool) {
	timestamp, ok := firstInt64(row, "time", "openTime", "closeTime")
	if !ok {
		return candle.Candle{}, false
	}
	open, ok := asFloat(row["open"])
	if !ok {
		return candle.Candle{}, false
	}
	high, ok := asFloat(row["high"])
	if !ok {
		return candle.Candle{}, false
	}
	low, ok := asFloat(row["low"])
	if !ok {
		return candle.Candle{}, false
	}
	closePrice, ok := asFloat(row["close"])
	if !ok {
		return candle.Candle{}, false
	}
	volume, ok := asFloat(row["volume"])
	if !ok {
		return candle.Candle{}, false
	}
	return candle.NormalizeOHLC(candle.Candle{
		Timestamp: timestamp,
		Open:      open,
		High:      high,
		Low:       low,
		Close:     closePrice,
		Volume:    volume,
	}), true
}

func klineTakerBuy(raw any) (float64, bool) {
	row, ok := raw.([]any)
	if !ok || len(row) < 10 {
		return 0, false
	}
	return asFloat(row[9])
}

func firstInt64(row map[string]any, keys ...string) (int64, bool) {
	for _, key := range keys {
		if value, ok := asInt64(row[key]); ok && value > 0 {
			return value, true
		}
	}
	return 0, false
}
