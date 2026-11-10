package market

import (
	"fmt"
	"strings"
)

// MarketType is the canonical spot/future market vocabulary.
type MarketType string

// Spot and Future are the canonical market types.
const (
	Spot   MarketType = "spot"
	Future MarketType = "future"
)

func (mt MarketType) String() string {
	return string(mt)
}

// Valid reports whether the market type is one of the canonical values.
func (mt MarketType) Valid() bool {
	return mt == Spot || mt == Future
}

// ParseMarketType canonicalizes the market vocabulary accepted by configuration
// and vendors. Unknown values are an error: silently treating them as a future
// would merge different contracts under one scope.
func ParseMarketType(raw string) (MarketType, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "spot":
		return Spot, nil
	case "swap", "perpetual", "linear", "future", "delivery":
		return Future, nil
	default:
		return "", fmt.Errorf("market: unknown market type %q", raw)
	}
}
