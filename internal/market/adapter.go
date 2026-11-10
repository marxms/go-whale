// Package market defines the canonical market, exchange and symbol vocabulary.
package market

import "fmt"

// ExchangeAdapter translates between the canonical market type and a venue's vocabulary.
type ExchangeAdapter struct {
	exchange Exchange
}

// NewAdapter returns the translator for one venue.
func NewAdapter(exchange Exchange) *ExchangeAdapter {
	return &ExchangeAdapter{exchange: exchange}
}

// ToExchange converts the internal market type into the venue vocabulary of the
// adapter's venue.
//
// Venue terminology (confirmed against the venue documentation):
//
//	"swap"   = USDT-M perpetual contracts (Binance fapi, BingX swap)
//	"future" = COIN-M delivery/quarterly contracts (Binance dapi)
//	"spot"   = spot trading
//
// It has no default: an unsupported venue or an invalid internal market type
// is an error, because a silent fallback would route a contract to the wrong
// endpoint.
func (a *ExchangeAdapter) ToExchange(internal MarketType) (string, error) {
	if !internal.Valid() {
		return "", fmt.Errorf("market: invalid market type %q", internal)
	}
	if internal == Spot {
		return "spot", nil
	}

	// Every supported venue calls its USDT-M perpetuals "swap" in its REST API.
	// Mapping them to "future" made the symbol filter drop every perpetual
	// (their market.Type is "swap"), leaving zero candles to seed.
	switch a.exchange {
	case Binance, BingX:
		return "swap", nil
	default:
		return "", fmt.Errorf("market: unsupported exchange %q", a.exchange)
	}
}

// FromConfig canonicalizes the market-type vocabulary accepted at configuration
// boundaries. It is strict: unknown values are an error, because silently
// treating them as a future would merge different contracts under one scope.
func FromConfig(config string) (MarketType, error) {
	return ParseMarketType(config)
}
