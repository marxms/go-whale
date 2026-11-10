package market

import "testing"

// TestMarketTypeStringCanonical locks the canonical string forms that the
// /health expected-scope check depends on. canonicalScope builds scopes as
// market.FromConfig(<cfg market type>).String(); if these strings ever drift
// (e.g. Future.String() stops being "future"), the health check would silently
// stop matching live scopes and report a healthy exchange as 503 — or vice
// versa. These assertions fail loudly instead.
func TestMarketTypeStringCanonical(t *testing.T) {
	if got := Future.String(); got != "future" {
		t.Errorf("Future.String() = %q, want %q", got, "future")
	}
	if got := Spot.String(); got != "spot" {
		t.Errorf("Spot.String() = %q, want %q", got, "spot")
	}
}

// TestFromConfigSwapMapsToFuture pins the config-string normalization the
// expected-scope check relies on: a config market type of "swap" (BingX
// perpetuals) must canonicalize to Future so its reported scope matches the
// rest of the app. Other accepted aliases are checked alongside it.
func TestFromConfigSwapMapsToFuture(t *testing.T) {
	tests := []struct {
		name   string
		config string
		want   MarketType
	}{
		{name: "swap maps to future", config: "swap", want: Future},
		{name: "future maps to future", config: "future", want: Future},
		{name: "perpetual maps to future", config: "perpetual", want: Future},
		{name: "linear maps to future", config: "linear", want: Future},
		{name: "delivery maps to future", config: "delivery", want: Future},
		{name: "spot maps to spot", config: "spot", want: Spot},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := FromConfig(tt.config)
			if err != nil {
				t.Fatalf("FromConfig(%q): %v", tt.config, err)
			}
			if got != tt.want {
				t.Errorf("FromConfig(%q) = %q, want %q", tt.config, got, tt.want)
			}
		})
	}
}

// TestFromConfigRejectsUnknown pins the strict contract: an unknown config
// market type must not be treated as a future, because that would merge
// different contracts under one scope.
func TestFromConfigRejectsUnknown(t *testing.T) {
	for _, raw := range []string{"", "garbage", "options", "perp"} {
		t.Run(raw, func(t *testing.T) {
			if got, err := FromConfig(raw); err == nil {
				t.Fatalf("FromConfig(%q) = %q without error, want error", raw, got)
			}
		})
	}
}
