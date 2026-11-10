package trade

import "context"

// LastTradeWriter persists the most recent trade for a symbol.
type LastTradeWriter interface {
	SetLastTrade(ctx context.Context, exchange, marketType, symbol string, trade Trade) error
}

// LastTradeReader serves the most recent trade for a symbol.
type LastTradeReader interface {
	GetLastTrade(ctx context.Context, exchange, marketType, symbol string) (*LastTrade, error)
}

// LastTradeStore composes both sides; prefer the narrow interfaces in new code.
type LastTradeStore interface {
	LastTradeWriter
	LastTradeReader
}
