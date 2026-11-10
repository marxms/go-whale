package market

import "testing"

func TestExchangeAdapterToExchange(t *testing.T) {
	tests := []struct {
		name       string
		exchange   Exchange
		marketType MarketType
		want       string
	}{
		{name: "binance spot", exchange: Binance, marketType: Spot, want: "spot"},
		{name: "binance future", exchange: Binance, marketType: Future, want: "swap"},
		{name: "bingx spot", exchange: BingX, marketType: Spot, want: "spot"},
		{name: "bingx future", exchange: BingX, marketType: Future, want: "swap"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewAdapter(tt.exchange).ToExchange(tt.marketType)
			if err != nil {
				t.Fatalf("ToExchange: %v", err)
			}
			if got != tt.want {
				t.Fatalf("ToExchange(%q) = %q, want %q", tt.marketType, got, tt.want)
			}
		})
	}
}

func TestExchangeAdapterToExchangeRejectsInvalid(t *testing.T) {
	if _, err := NewAdapter(Binance).ToExchange(MarketType("")); err == nil {
		t.Fatal("ToExchange accepted a zero market type")
	}
	if _, err := NewAdapter(Exchange("bybit")).ToExchange(Future); err == nil {
		t.Fatal("ToExchange accepted an unsupported exchange")
	}
}
