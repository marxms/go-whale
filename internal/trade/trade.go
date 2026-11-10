// Package trade owns the canonical trade and last-trade values shared by every
// venue adapter and by the candle aggregator.
package trade

import "strings"

// Side is the canonical aggressor side of a trade.
type Side string

// Buy and Sell are the classified sides; Unknown is explicit absence of classification.
const (
	Buy     Side = "buy"
	Sell    Side = "sell"
	Unknown Side = "unknown"
)

// ParseSide maps vendor vocabulary to the canonical side. Unknown input is
// explicit absence of classification, never a default buy.
func ParseSide(raw string) Side {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "buy", "b", "bid", "long":
		return Buy
	case "sell", "s", "ask", "short":
		return Sell
	default:
		return Unknown
	}
}

func (s Side) String() string {
	return string(s)
}

// Trade is one public trade in the canonical millisecond clock.
type Trade struct {
	ID        string
	Symbol    string
	Timestamp int64 // milliseconds since Unix epoch, venue trade time
	Price     float64
	Amount    float64
	Side      Side
}

// LastTrade is the shape persisted under the last_trade key. JSON names are
// part of the storage contract and must not drift.
type LastTrade struct {
	Timestamp  int64   `json:"timestamp"`
	Price      float64 `json:"price"`
	Volume     float64 `json:"volume"`
	Side       Side    `json:"side"`
	TradeID    string  `json:"tradeId"`
	ReceivedAt int64   `json:"receivedAt"`
	LatencyMS  int64   `json:"latency"`
}
