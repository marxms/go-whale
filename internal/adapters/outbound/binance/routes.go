package binancesdk

import (
	"time"

	"github.com/marxms/go-whale/internal/market"
)

// streamClass names the documented stream families this adapter dials. The
// route for each one is declared by the venue policy, not here: Binance USD-M
// serves kline and aggregate trade on /market and public trade on /public, and
// a connection without a route silently receives neither.
const (
	streamClassTrade = "trade"
	streamClassKline = "kline"
)

// routeFor returns the documented route prefix for one stream class. Empty means
// the venue has no route split (Binance spot) or the class is unknown.
func routeFor(exchangeID, marketType, class string) string {
	mt, err := market.ParseMarketType(marketType)
	if err != nil {
		return ""
	}
	policy, ok := market.ConnPolicyFor(exchangeID, mt)
	if !ok {
		return ""
	}
	return policy.Routes[class]
}

// readIdleFor returns the venue's documented idle deadline. Zero means the
// venue documents none, and the caller must not invent one.
func readIdleFor(exchangeID, marketType string) time.Duration {
	mt, err := market.ParseMarketType(marketType)
	if err != nil {
		return 0
	}
	policy, ok := market.ConnPolicyFor(exchangeID, mt)
	if !ok {
		return 0
	}
	return policy.ReadIdle
}
