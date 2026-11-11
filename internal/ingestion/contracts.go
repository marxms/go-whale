package ingestion

import "context"

// TradeHandler consumes a batch of trades for one symbol.
type TradeHandler interface {
	HandleTrades(symbol string, trades any) error
	HandleTradesWithContext(ctx context.Context, exchange, marketType, symbol string, trades any) error
}
