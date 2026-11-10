package market

import "testing"

func TestParseExchangeValid(t *testing.T) {
	tests := []struct {
		raw  string
		want Exchange
	}{
		{raw: "binance", want: Binance},
		{raw: " BINANCE ", want: Binance},
		{raw: "bingx", want: BingX},
		{raw: " BingX", want: BingX},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			got, err := ParseExchange(tt.raw)
			if err != nil {
				t.Fatalf("ParseExchange(%q): %v", tt.raw, err)
			}
			if got != tt.want {
				t.Fatalf("ParseExchange(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestParseExchangeRejectsUnknown(t *testing.T) {
	for _, raw := range []string{"", "bybit", "binancee", "binance/spot", "binancex"} {
		t.Run(raw, func(t *testing.T) {
			if got, err := ParseExchange(raw); err == nil {
				t.Fatalf("ParseExchange(%q) = %q without error, want error", raw, got)
			}
		})
	}
}

func TestZeroExchangeIsInvalid(t *testing.T) {
	var zero Exchange
	if zero.Valid() {
		t.Fatal("zero Exchange must be invalid")
	}
}

func TestParseSymbolValid(t *testing.T) {
	for _, raw := range []string{"BTC/USDT", "ETH/USDC", "1000SHIB/USDT"} {
		t.Run(raw, func(t *testing.T) {
			got, err := ParseSymbol(raw)
			if err != nil {
				t.Fatalf("ParseSymbol(%q): %v", raw, err)
			}
			if got.String() != raw {
				t.Fatalf("ParseSymbol(%q) = %q, want round-trip", raw, got)
			}
		})
	}
}

func TestParseSymbolRejectsInvalid(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{name: "empty", raw: ""},
		{name: "no slash", raw: "BTCUSDT"},
		{name: "multiple slashes", raw: "BTC/USDT/ETH"},
		{name: "double slash", raw: "BTC//USDT"},
		{name: "empty base", raw: "/USDT"},
		{name: "empty quote", raw: "BTC/"},
		{name: "leading whitespace", raw: " BTC/USDT"},
		{name: "trailing whitespace", raw: "BTC/USDT "},
		{name: "internal whitespace", raw: "BTC /USDT"},
		{name: "tab", raw: "BTC/\tUSDT"},
		{name: "venue suffix", raw: "BTC/USDT:USDT"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := ParseSymbol(tt.raw); err == nil {
				t.Fatalf("ParseSymbol(%q) = %q without error, want error", tt.raw, got)
			}
		})
	}
}

func TestZeroSymbolIsInvalid(t *testing.T) {
	var zero Symbol
	if zero.Valid() {
		t.Fatal("zero Symbol must be invalid")
	}
	if zero.String() != "" {
		t.Fatalf("zero Symbol.String() = %q, want empty", zero.String())
	}
}

func TestNewScopeValid(t *testing.T) {
	exchange, err := ParseExchange("binance")
	if err != nil {
		t.Fatal(err)
	}
	marketType, err := ParseMarketType("future")
	if err != nil {
		t.Fatal(err)
	}
	symbol, err := ParseSymbol("BTC/USDT")
	if err != nil {
		t.Fatal(err)
	}

	scope, err := NewScope(exchange, marketType, symbol)
	if err != nil {
		t.Fatalf("NewScope: %v", err)
	}
	if !scope.Valid() {
		t.Fatal("scope must be valid")
	}
	if got, want := scope.String(), "binance:future:BTC/USDT"; got != want {
		t.Fatalf("Scope.String() = %q, want %q", got, want)
	}
}

func TestNewScopeRejectsZeroParts(t *testing.T) {
	exchange, _ := ParseExchange("binance")
	marketType, _ := ParseMarketType("future")
	symbol, _ := ParseSymbol("BTC/USDT")

	tests := []struct {
		name  string
		scope Scope
	}{
		{
			name:  "zero exchange",
			scope: Scope{MarketType: marketType, Symbol: symbol},
		},
		{
			name:  "zero market type",
			scope: Scope{Exchange: exchange, Symbol: symbol},
		},
		{
			name:  "zero symbol",
			scope: Scope{Exchange: exchange, MarketType: marketType},
		},
		{
			name:  "zero scope",
			scope: Scope{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewScope(tt.scope.Exchange, tt.scope.MarketType, tt.scope.Symbol); err == nil {
				t.Fatal("NewScope unexpectedly accepted a zero-valued part")
			}
			if tt.scope.Valid() {
				t.Fatal("scope with a zero-valued part must be invalid")
			}
			if tt.scope.String() != "" {
				t.Fatalf("invalid scope String() = %q, want empty", tt.scope.String())
			}
		})
	}
}

func TestZeroScopeIsInvalid(t *testing.T) {
	var zero Scope
	if zero.Valid() {
		t.Fatal("zero Scope must be invalid")
	}
	if zero.String() != "" {
		t.Fatalf("zero Scope.String() = %q, want empty", zero.String())
	}
}

// TestScopeIsStructuredIdentity proves the identity is the value itself, not a
// concatenated string: two scopes built with the canonical parts are the same map key.
func TestScopeIsStructuredIdentity(t *testing.T) {
	symbol, err := ParseSymbol("BTC/USDT")
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewScope(Binance, Future, symbol)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewScope(Binance, Future, symbol)
	if err != nil {
		t.Fatal(err)
	}

	seen := map[Scope]int{first: 1}
	if _, ok := seen[second]; !ok {
		t.Fatal("equivalent scopes must be the same map key")
	}
}
