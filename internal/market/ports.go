package market

import "context"

// DiscoverySource is the symbol-discovery port the core consumes. The
// concrete adapter owns the venue call (liquidity ranking, pagination and
// market filtering); consumers receive canonical internal symbols in
// liquidity-descending order.
//
// It is declared here, where the vocabulary of a market already lives, so
// app and the derivatives collector depend on the port rather than on the
// outbound client that happens to implement it.
type DiscoverySource interface {
	DiscoverSymbols(ctx context.Context, exchange Exchange, marketType MarketType, limit int) ([]string, error)
}
