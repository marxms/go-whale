package candle

import (
	"fmt"

	"github.com/marxms/go-whale/internal/market"
)

// SeriesKey is the structured identity of one candle series: a market scope
// plus a timeframe.
//
// It is comparable, so the core uses the value itself as a map key instead of
// concatenating exchange, market type, symbol and timeframe into a string. A
// concatenated key makes it possible to build two different identities that
// compare equal (or one identity that names two series), and it hides the
// delimiters from the type system.
type SeriesKey struct {
	Scope     market.Scope
	Timeframe market.Timeframe
}

// NewSeriesKey validates the scope and the timeframe and returns the series
// identity. The zero value of either part is invalid.
func NewSeriesKey(scope market.Scope, timeframe market.Timeframe) (SeriesKey, error) {
	if !scope.Valid() {
		return SeriesKey{}, fmt.Errorf("candle: invalid series scope")
	}
	if !timeframe.Valid() {
		return SeriesKey{}, fmt.Errorf("candle: invalid series timeframe %q", timeframe)
	}
	return SeriesKey{Scope: scope, Timeframe: timeframe}, nil
}

// SeriesKeyOf builds the identity from the four canonical strings the streaming
// boundary already carries. It exists so adapters never invent a second,
// string-shaped key beside SeriesKey.
func SeriesKeyOf(exchange, marketType, symbol, timeframe string) (SeriesKey, error) {
	venue, err := market.ParseExchange(exchange)
	if err != nil {
		return SeriesKey{}, err
	}
	mt, err := market.ParseMarketType(marketType)
	if err != nil {
		return SeriesKey{}, err
	}
	sym, err := market.ParseSymbol(symbol)
	if err != nil {
		return SeriesKey{}, err
	}
	scope, err := market.NewScope(venue, mt, sym)
	if err != nil {
		return SeriesKey{}, err
	}
	tf, err := market.ParseTimeframe(timeframe)
	if err != nil {
		return SeriesKey{}, err
	}
	return NewSeriesKey(scope, tf)
}

// Valid reports whether both parts of the series identity are valid.
func (k SeriesKey) Valid() bool {
	return k.Scope.Valid() && k.Timeframe.Valid()
}

// String returns the boundary form of the key. It is for logs and adapters;
// identity comparisons must use the SeriesKey value itself.
func (k SeriesKey) String() string {
	if !k.Valid() {
		return ""
	}
	return k.Scope.String() + ":" + k.Timeframe.String()
}
