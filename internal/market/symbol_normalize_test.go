package market

import "testing"

func TestNormalizeSymbol(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "already normalized", input: "BTC/USDT", expected: "BTC/USDT"},
		{name: "with settle suffix (venue format)", input: "BTC/USDT:USDT", expected: "BTC/USDT"},
		{name: "dash format", input: "BTC-USDT", expected: "BTC/USDT"},
		{name: "empty string", input: "", expected: ""},
		{name: "with USDC quote", input: "ETH-USDC", expected: "ETH/USDC"},
		{name: "unknown format passthrough", input: "BTCUSDT", expected: "BTCUSDT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NormalizeSymbol(tt.input); got != tt.expected {
				t.Errorf("NormalizeSymbol(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestInternalSymbolFromConcatenated(t *testing.T) {
	tests := map[string]string{
		"BTCUSDT":        "BTC/USDT",
		"ETHUSDC":        "ETH/USDC",
		"BTCUSDT_2403":   "",
		"CVCUSDTSETTLED": "",
		"BTC/USDT":       "",
		"BTC-USDT":       "",
		"":               "",
		"USDT":           "",
	}
	for in, want := range tests {
		if got := InternalSymbolFromConcatenated(in); got != want {
			t.Errorf("InternalSymbolFromConcatenated(%q) = %q, want %q", in, got, want)
		}
	}
}
