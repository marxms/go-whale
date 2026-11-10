package recovery

import (
	"errors"
	"fmt"
	"testing"
)

// TestToRawFuturesSymbol verifies the conversion from venue-unified/internal
// symbols to the raw Binance USDⓈ-M format the SDK klines endpoint requires.
// C1 root cause: the former venue symbol form produced "FOLKS/USDT:USDT", which
// the binance-connector-go SDK rejects with -1121 (it needs "FOLKSUSDT").
func TestToRawFuturesSymbol(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"unified linear perp", "FOLKS/USDT:USDT", "FOLKSUSDT"},
		{"unified btc perp", "BTC/USDT:USDT", "BTCUSDT"},
		{"internal pair", "BTC/USDT", "BTCUSDT"},
		{"already raw is idempotent", "BTCUSDT", "BTCUSDT"},
		{"coin-m style", "ETH/USD:ETH", "ETHUSD"},
		{"empty", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := toRawFuturesSymbol(c.in); got != c.want {
				t.Errorf("toRawFuturesSymbol(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

// TestIsInvalidSymbolError verifies that only Binance's permanent "invalid
// symbol" rejection (-1121) is classified as permanent — transient failures
// must NOT be, so they still trip the circuit and retry.
func TestIsInvalidSymbolError(t *testing.T) {
	if !isInvalidSymbolError(fmt.Errorf(`HTTP error 400: Bad Request, {"code":-1121,"msg":"Invalid symbol."}`)) {
		t.Error("expected -1121 payload to be classified as invalid symbol")
	}
	if !isInvalidSymbolError(errors.New("Invalid symbol")) {
		t.Error("expected 'Invalid symbol' text to be classified as invalid symbol")
	}
	if isInvalidSymbolError(errors.New("connection reset by peer")) {
		t.Error("transient network error must NOT be classified as permanent")
	}
	if isInvalidSymbolError(nil) {
		t.Error("nil must not be classified as permanent")
	}
}

// TestPermanentSymbolErrorIsRecognizable verifies the wrapped error the filler
// returns is detectable via errors.Is, so the manager can skip the symbol
// without tripping the shared per-exchange circuit breaker.
func TestPermanentSymbolErrorIsRecognizable(t *testing.T) {
	wrapped := fmt.Errorf("invalid binance symbol %q: %w", "FOLKSUSDT", ErrPermanentSymbol)
	if !errors.Is(wrapped, ErrPermanentSymbol) {
		t.Error("expected errors.Is(wrapped, ErrPermanentSymbol) == true")
	}
	if errors.Is(errors.New("some other error"), ErrPermanentSymbol) {
		t.Error("unrelated error must not match ErrPermanentSymbol")
	}
}
