package questdb

import "github.com/marxms/go-whale/internal/candle"

// mustSeriesKey builds the typed identity the write path now takes. Tests use it so a
// malformed fixture fails loudly instead of collapsing two series into one key.
func mustSeriesKey(exchange, marketType, symbol, timeframe string) candle.SeriesKey {
	key, err := candle.SeriesKeyOf(exchange, marketType, symbol, timeframe)
	if err != nil {
		panic(err)
	}
	return key
}
