package candle

import "time"

// Timeframe represents a candle aggregation window.
type Timeframe string

// The canonical timeframes; each is an exact multiple of the one below it.
const (
	TF5m  Timeframe = "5m"
	TF15m Timeframe = "15m"
	TF30m Timeframe = "30m"
	TF1h  Timeframe = "1h"
	TF2h  Timeframe = "2h"
	TF4h  Timeframe = "4h"
	TF1d  Timeframe = "1d"
	TF1w  Timeframe = "1w"
)

// SeedWindow returns the time window needed to fetch `depth` candles for a timeframe.
func SeedWindow(tf Timeframe, depth int) time.Duration {
	var interval time.Duration
	switch tf {
	case TF5m:
		interval = 5 * time.Minute
	case TF15m:
		interval = 15 * time.Minute
	case TF30m:
		interval = 30 * time.Minute
	case TF1h:
		interval = time.Hour
	case TF2h:
		interval = 2 * time.Hour
	case TF4h:
		interval = 4 * time.Hour
	case TF1d:
		interval = 24 * time.Hour
	case TF1w:
		interval = 7 * 24 * time.Hour
	default:
		interval = 5 * time.Minute
	}
	// 10% buffer for missing candles
	return time.Duration(float64(interval) * float64(depth) * 1.1)
}
