package app

import (
	"context"
	"time"

	"github.com/marxms/go-whale/internal/market"
)

// ListingSource is the market-metadata port the subscription path consumes.
//
// It returns canonical market descriptions; the concrete adapter owns the
// vendor translation. LoadMarkets performs the venue call; Close releases the
// connection the listing read warmed.
type ListingSource interface {
	LoadMarkets(ctx context.Context) (map[string]market.MarketInfo, error)
	Close() []error
}

// loadListingMarkets reads the venue metadata through the port. The concrete
// adapter is supplied by the composition root; this path never sees its type.
func loadListingMarkets(ctx context.Context, source ListingSource) (map[string]market.MarketInfo, error) {
	return source.LoadMarkets(ctx)
}

// recordListingFloors hands the verified listing floors to the engine. The rule that
// decides which markets are unambiguous lives in internal/market.
func (a *App) recordListingFloors(exchangeID, marketType string, symbols []string, markets map[string]market.MarketInfo) {
	if a.candleEngine == nil || !market.SupportsListingFloors(exchangeID, marketType) {
		return
	}
	floors := market.VerifiedListingFloors(exchangeID, marketType, symbols, markets, time.Now().UnixMilli())
	a.candleEngine.SetListingFloors(exchangeID, marketType, symbols, floors)
}
