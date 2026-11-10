package market

import "sort"

// Capability names one market-data promise a venue scope makes. The matrix below is the
// single source of what each scope provides; an absent capability is declared with a
// reason instead of being discovered in production.
type Capability string

// Capability values name every promise the capability matrix can declare.
const (
	CapabilityKlineFeed           Capability = "kline_feed"
	CapabilityCVD                 Capability = "cvd"
	CapabilityFunding             Capability = "funding"
	CapabilityOpenInterest        Capability = "open_interest"
	CapabilityOpenInterestHistory Capability = "open_interest_history"
	CapabilityLongShortRatio      Capability = "long_short_ratio"
	CapabilityMarkets             Capability = "markets"
	CapabilityListingFloors       Capability = "listing_floors"
)

// Support is one matrix entry. Present=false requires a Reason: an absence that is not
// explained is indistinguishable from an oversight.
type Support struct {
	Present bool
	Reason  string
}

// Capabilities is the declared support for one (exchange, market type) scope.
type Capabilities map[Capability]Support

// Supports reports whether the scope declares the capability.
func (c Capabilities) Supports(capability Capability) bool {
	return c[capability].Present
}

// AbsenceReason returns the declared reason for an absent capability, empty when present.
func (c Capabilities) AbsenceReason(capability Capability) string {
	return c[capability].Reason
}

// DivergesFrom returns the capabilities whose presence differs from the reference scope,
// sorted so a test or an operator sees a stable list. It compares declarations, not
// fixture results: the contract harness proves the adapter against the matrix.
func (c Capabilities) DivergesFrom(reference Capabilities) []Capability {
	seen := make(map[Capability]struct{}, len(c)+len(reference))
	for capability := range c {
		seen[capability] = struct{}{}
	}
	for capability := range reference {
		seen[capability] = struct{}{}
	}
	diverged := make([]Capability, 0, len(seen))
	for capability := range seen {
		if c[capability].Present != reference[capability].Present {
			diverged = append(diverged, capability)
		}
	}
	sort.Slice(diverged, func(i, j int) bool { return diverged[i] < diverged[j] })
	return diverged
}

type capabilityScope struct {
	exchange   Exchange
	marketType MarketType
}

func present() Support             { return Support{Present: true} }
func absent(reason string) Support { return Support{Reason: reason} }

// capabilityMatrix declares the four scopes this service supports. Binance USD-M futures
// is the reference implementation: every capability it has is either present in another
// scope or declared absent there with the reason.
func capabilityMatrix() map[capabilityScope]Capabilities {
	return map[capabilityScope]Capabilities{
		{Binance, Future}: {
			CapabilityKlineFeed:           present(),
			CapabilityCVD:                 present(),
			CapabilityFunding:             present(),
			CapabilityOpenInterest:        present(),
			CapabilityOpenInterestHistory: present(),
			CapabilityLongShortRatio:      present(),
			CapabilityMarkets:             present(),
			CapabilityListingFloors:       present(),
		},
		{Binance, Spot}: {
			CapabilityKlineFeed:           absent("the kline WebSocket feed is implemented for Binance USD-M futures only"),
			CapabilityCVD:                 present(),
			CapabilityFunding:             absent("spot markets publish no funding rate"),
			CapabilityOpenInterest:        absent("spot markets publish no open interest"),
			CapabilityOpenInterestHistory: absent("spot markets publish no open interest history"),
			CapabilityLongShortRatio:      absent("spot markets publish no long/short account ratio history"),
			CapabilityMarkets:             present(),
			CapabilityListingFloors:       absent("spot markets publish no listing time usable as an engine floor"),
		},
		{BingX, Spot}: {
			CapabilityKlineFeed:           absent("BingX publishes no candle stream this service can transport"),
			CapabilityCVD:                 absent("BingX public klines carry no taker-buy volume"),
			CapabilityFunding:             absent("spot markets publish no funding rate"),
			CapabilityOpenInterest:        absent("spot markets publish no open interest"),
			CapabilityOpenInterestHistory: absent("BingX publishes no open interest history"),
			CapabilityLongShortRatio:      absent("BingX publishes no long/short account ratio history"),
			CapabilityMarkets:             present(),
			CapabilityListingFloors:       absent("BingX publishes no listing time usable as an engine floor"),
		},
		{BingX, Future}: {
			CapabilityKlineFeed:           absent("BingX publishes no candle stream this service can transport"),
			CapabilityCVD:                 absent("BingX public klines carry no taker-buy volume"),
			CapabilityFunding:             present(),
			CapabilityOpenInterest:        present(),
			CapabilityOpenInterestHistory: absent("BingX publishes no open interest history"),
			CapabilityLongShortRatio:      absent("BingX publishes no long/short account ratio history"),
			CapabilityMarkets:             present(),
			CapabilityListingFloors:       absent("BingX publishes no listing time usable as an engine floor"),
		},
	}
}

// CapabilitiesFor returns the declared matrix row for one scope. An unknown exchange or
// market type has no row: the caller gets an empty declaration, never a guessed default.
func CapabilitiesFor(exchangeID, marketType string) Capabilities {
	exchange, err := ParseExchange(exchangeID)
	if err != nil {
		return nil
	}
	mt, err := ParseMarketType(marketType)
	if err != nil {
		return nil
	}
	return capabilityMatrix()[capabilityScope{exchange, mt}]
}
