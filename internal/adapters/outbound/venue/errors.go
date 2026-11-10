package venue

import (
	"fmt"

	"github.com/marxms/go-whale/internal/adapters/outbound/exchange"
	"github.com/marxms/go-whale/internal/market"
)

// CapabilityAbsentError reports that a scope does not provide a capability the caller asked
// for. It is typed so an absence can be told apart from a transport failure, and it unwraps
// to exchange.ErrPermanent so the existing retry and circuit-breaker policy keeps treating it
// as final.
type CapabilityAbsentError struct {
	Capability market.Capability
	Exchange   market.Exchange
	MarketType market.MarketType
}

func (e *CapabilityAbsentError) Error() string {
	return fmt.Sprintf("venue: %s %s does not provide %s", e.Exchange, e.MarketType, e.Capability)
}

// Unwrap preserves the permanent-failure classification of every absence.
func (e *CapabilityAbsentError) Unwrap() error { return exchange.ErrPermanent }

// unsupported builds the typed absence, records it in the metric, and logs once per
// capability per client. Replacing an unsupported path with this helper is what removes the
// silent `nil, false, nil` from the adapter.
func (c *Client) unsupported(capability market.Capability) error {
	c.metrics.capabilityAbsent(string(c.venue), string(c.marketType), string(capability))
	if _, loaded := c.absenceLogged.LoadOrStore(capability, struct{}{}); !loaded {
		c.log.Warn("venue capability absent",
			"exchange", c.venue,
			"market_type", c.marketType,
			"capability", capability)
	}
	return &CapabilityAbsentError{
		Capability: capability,
		Exchange:   c.venue,
		MarketType: c.marketType,
	}
}
