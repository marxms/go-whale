package recovery

import "github.com/marxms/go-whale/internal/candle"

// candleRejection is one candle the write boundary refused, with the validator's reason.
// The reason travels to the metric so an operator can tell a non-positive price from a bad
// timestamp without reading the venue's payload.
type candleRejection struct {
	Timestamp int64
	Reason    string
}

// prepareCandles is the single integrity gate every REST candle passes before this package
// persists it. Seed and gap recovery both call it, so no venue has to remember to normalize.
//
// Normalization runs before validation on purpose: a venue sometimes publishes a row whose
// high is below its own open and close (the real BCHUSDT 1w of 2020-01-13). Extending the
// bounds preserves the stated session; rejecting the row would leave a hole that disables
// the timeframe. Only a candle that is still impossible afterwards (non-positive price or
// timestamp, negative volume) is dropped, and it never reaches the store.
func prepareCandles(validate CandlesValidator, candles []candle.Candle, symbol, tf string) ([]candle.Candle, []candleRejection) {
	if len(candles) == 0 {
		return nil, nil
	}
	prepared := make([]candle.Candle, 0, len(candles))
	var rejected []candleRejection
	for _, c := range candles {
		c = candle.NormalizeOHLC(c)
		if ok, reason := validate.Validate(c.Timestamp, c.Open, c.High, c.Low, c.Close, c.Volume, symbol, tf); !ok {
			rejected = append(rejected, candleRejection{Timestamp: c.Timestamp, Reason: reason})
			continue
		}
		prepared = append(prepared, c)
	}
	return prepared, rejected
}
