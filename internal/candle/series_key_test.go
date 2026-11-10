package candle

import (
	"testing"

	"github.com/marxms/go-whale/internal/market"
)

func mustSeriesKeyParts(t *testing.T, exchange, marketType, symbol, timeframe string) (market.Scope, market.Timeframe) {
	t.Helper()
	venue, err := market.ParseExchange(exchange)
	if err != nil {
		t.Fatal(err)
	}
	mt, err := market.ParseMarketType(marketType)
	if err != nil {
		t.Fatal(err)
	}
	sym, err := market.ParseSymbol(symbol)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := market.NewScope(venue, mt, sym)
	if err != nil {
		t.Fatal(err)
	}
	tf, err := market.ParseTimeframe(timeframe)
	if err != nil {
		t.Fatal(err)
	}
	return scope, tf
}

func TestNewSeriesKeyValid(t *testing.T) {
	scope, tf := mustSeriesKeyParts(t, "binance", "future", "BTC/USDT", "5m")

	key, err := NewSeriesKey(scope, tf)
	if err != nil {
		t.Fatalf("NewSeriesKey: %v", err)
	}
	if !key.Valid() {
		t.Fatal("series key must be valid")
	}
	if got, want := key.String(), "binance:future:BTC/USDT:5m"; got != want {
		t.Fatalf("SeriesKey.String() = %q, want %q", got, want)
	}
}

func TestNewSeriesKeyRejectsZeroParts(t *testing.T) {
	scope, tf := mustSeriesKeyParts(t, "binance", "future", "BTC/USDT", "5m")

	if _, err := NewSeriesKey(market.Scope{}, tf); err == nil {
		t.Fatal("NewSeriesKey accepted a zero scope")
	}
	if _, err := NewSeriesKey(scope, market.Timeframe{}); err == nil {
		t.Fatal("NewSeriesKey accepted a zero timeframe")
	}
}

func TestZeroSeriesKeyIsInvalid(t *testing.T) {
	var zero SeriesKey
	if zero.Valid() {
		t.Fatal("zero SeriesKey must be invalid")
	}
	if zero.String() != "" {
		t.Fatalf("zero SeriesKey.String() = %q, want empty", zero.String())
	}
}

// TestSeriesKeyIsStructuredIdentity proves the identity is the value itself,
// not a concatenated string: two separately built keys for the same series are
// the same map key.
func TestSeriesKeyIsStructuredIdentity(t *testing.T) {
	firstScope, firstTF := mustSeriesKeyParts(t, "binance", "swap", "BTC/USDT", "5m")
	secondScope, secondTF := mustSeriesKeyParts(t, "binance", "future", "BTC/USDT", "5m")

	first, err := NewSeriesKey(firstScope, firstTF)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewSeriesKey(secondScope, secondTF)
	if err != nil {
		t.Fatal(err)
	}

	seen := map[SeriesKey]struct{}{first: {}}
	if _, ok := seen[second]; !ok {
		t.Fatal("equivalent series keys must be the same map key")
	}
}
