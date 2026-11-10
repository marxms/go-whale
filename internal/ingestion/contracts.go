package ingestion

// TradeHandler consumes a batch of trades for one symbol.
type TradeHandler interface {
	HandleTrades(symbol string, trades any) error
	HandleTradesWithContext(exchange, marketType, symbol string, trades any) error
}
