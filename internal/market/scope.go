package market

import "fmt"

// Scope is the structured identity of one market series family: a venue, a
// market type and a symbol. It is comparable, so callers that need an identity
// key must use the value itself instead of concatenating its parts into a
// string.
type Scope struct {
	Exchange   Exchange
	MarketType MarketType
	Symbol     Symbol
}

// NewScope validates its parts and returns a scope. The zero value of every
// part is invalid, so a scope assembled from unchecked input fails here rather
// than being treated as a real market.
func NewScope(exchange Exchange, marketType MarketType, symbol Symbol) (Scope, error) {
	scope := Scope{Exchange: exchange, MarketType: marketType, Symbol: symbol}
	if err := scope.validationError(); err != nil {
		return Scope{}, err
	}
	return scope, nil
}

// Valid reports whether every part of the scope is valid.
func (s Scope) Valid() bool {
	return s.validationError() == nil
}

// String returns the boundary form of the scope. It exists for logs and
// adapters; identity comparisons must use the Scope value itself.
func (s Scope) String() string {
	if !s.Valid() {
		return ""
	}
	return s.Exchange.String() + ":" + s.MarketType.String() + ":" + s.Symbol.String()
}

func (s Scope) validationError() error {
	if !s.Exchange.Valid() {
		return fmt.Errorf("market: invalid scope: exchange %q is not a known venue", s.Exchange)
	}
	if !s.MarketType.Valid() {
		return fmt.Errorf("market: invalid scope: market type %q is not supported", s.MarketType)
	}
	if !s.Symbol.Valid() {
		return fmt.Errorf("market: invalid scope: symbol is empty or not canonical")
	}
	return nil
}
