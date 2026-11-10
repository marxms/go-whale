package questdb

import "github.com/marxms/go-whale/internal/candle"

// bars builds an ascending run of candles ending at endTs, spaced by stepMs.
func bars(n int, endTs, stepMs int64) []candle.Candle {
	out := make([]candle.Candle, n)
	for i := 0; i < n; i++ {
		out[i] = candle.Candle{
			Timestamp: endTs - int64(n-1-i)*stepMs,
			Close:     float64(100 + i),
			Volume:    1,
		}
	}
	return out
}
