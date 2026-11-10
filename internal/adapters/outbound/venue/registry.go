package venue

import (
	"fmt"

	"github.com/marxms/go-whale/internal/adapters/outbound/exchange"
	"github.com/marxms/go-whale/internal/market"
)

// venueScope is the registry key: one driver serves one (exchange, market type) pair.
type venueScope struct {
	exchange   market.Exchange
	marketType market.MarketType
}

// driverConstructor builds the driver for one scope over the client's shared transport.
type driverConstructor func(*Client) driver

// driverRegistry maps a scope to its constructor. It is the only place outside an adapter
// that names a venue, so a new venue is one row here plus its driver file and
// internal/recovery stays untouched.
type driverRegistry map[venueScope]driverConstructor

// registeredDrivers is the production registry. Constructors run at client construction;
// there is no init() magic and no runtime plugin loading.
func registeredDrivers() driverRegistry {
	return driverRegistry{
		{market.Binance, market.Spot}:   func(c *Client) driver { return &binanceDriver{client: c} },
		{market.Binance, market.Future}: func(c *Client) driver { return &binanceDriver{client: c} },
		{market.BingX, market.Spot}:     func(c *Client) driver { return &bingXDriver{client: c} },
		{market.BingX, market.Future}:   func(c *Client) driver { return &bingXDriver{client: c} },
	}
}

// UnknownVenueScopeError is the typed rejection of a scope with no registered driver. It
// unwraps to a permanent failure: a missing driver is a programming error, never a
// transport fault the retry policy should spend on.
type UnknownVenueScopeError struct {
	Exchange   market.Exchange
	MarketType market.MarketType
}

func (e *UnknownVenueScopeError) Error() string {
	return fmt.Sprintf("venue: no driver registered for %s %s", e.Exchange, e.MarketType)
}

// Unwrap preserves the permanent-failure classification of an unknown scope.
func (e *UnknownVenueScopeError) Unwrap() error { return exchange.ErrPermanent }

// resolve returns the driver for one scope. An unregistered scope is an error, never a
// silent fallback to another venue.
func (r driverRegistry) resolve(scope venueScope, client *Client) (driver, error) {
	constructor, ok := r[scope]
	if !ok {
		return nil, &UnknownVenueScopeError{Exchange: scope.exchange, MarketType: scope.marketType}
	}
	return constructor(client), nil
}
