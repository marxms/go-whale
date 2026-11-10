package market

import (
	"fmt"
	"strings"
)

// Exchange is a validated venue identifier. The zero value is invalid, and
// ParseExchange is the way to construct a non-zero value from boundary input.
//
// The set is closed on purpose: the streaming, recovery and derivatives paths
// each switch on the venue and build vendor-specific clients, so an unknown ID
// must be rejected at configuration time instead of being coerced into one of
// the known venues.
type Exchange string

// Binance and BingX are the supported venues.
const (
	Binance Exchange = "binance"
	BingX   Exchange = "bingx"
)

// ParseExchange canonicalizes a venue ID accepted by configuration. Unknown
// venues are an error; there is no default venue.
func ParseExchange(raw string) (Exchange, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case string(Binance):
		return Binance, nil
	case string(BingX):
		return BingX, nil
	default:
		return "", fmt.Errorf("market: unknown exchange %q", raw)
	}
}

func (e Exchange) String() string {
	return string(e)
}

// Valid reports whether the value is one of the venues this service knows how
// to stream. The zero value is invalid.
func (e Exchange) Valid() bool {
	return e == Binance || e == BingX
}
