package market

import "testing"

func TestParseMarketTypeValid(t *testing.T) {
	tests := []struct {
		raw  string
		want MarketType
	}{
		{raw: "spot", want: Spot},
		{raw: " SPOT ", want: Spot},
		{raw: "swap", want: Future},
		{raw: "perpetual", want: Future},
		{raw: "future", want: Future},
		{raw: "delivery", want: Future},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			got, err := ParseMarketType(tt.raw)
			if err != nil {
				t.Fatalf("ParseMarketType(%q): %v", tt.raw, err)
			}
			if got != tt.want {
				t.Fatalf("ParseMarketType(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestParseMarketTypeRejectsUnknown(t *testing.T) {
	for _, raw := range []string{"", "garbage", "options", "perp"} {
		t.Run(raw, func(t *testing.T) {
			if _, err := ParseMarketType(raw); err == nil {
				t.Fatalf("ParseMarketType(%q) unexpectedly succeeded", raw)
			}
		})
	}
}
