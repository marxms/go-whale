package netdiag

import "testing"

// TestCorrelatorConcurrentDropsAreLocalNetwork reproduces the reference incident
// shape: Binance drops, then BingX drops ~30s later. The second drop — seeing a
// sibling exchange down inside the window — must classify as a shared
// local-network event, not an exchange-specific one.
func TestCorrelatorConcurrentDropsAreLocalNetwork(t *testing.T) {
	c := New()

	if got := c.Record("binance", 1_000_000); got != ScopeExchangeSpecific {
		t.Fatalf("first drop (nothing else seen): got %q, want %q", got, ScopeExchangeSpecific)
	}
	if got := c.Record("bingx", 1_030_000); got != ScopeLocalNetwork {
		t.Fatalf("concurrent drop within window: got %q, want %q", got, ScopeLocalNetwork)
	}
}

// TestCorrelatorIsolatedDropIsExchangeSpecific: a Binance-only drop with no
// sibling failing nearby stays exchange_specific.
func TestCorrelatorIsolatedDropIsExchangeSpecific(t *testing.T) {
	c := New()
	c.Record("binance", 1_000_000)
	// BingX drops 10 minutes later — well outside the 120s window.
	if got := c.Record("bingx", 1_000_000+600_000); got != ScopeExchangeSpecific {
		t.Fatalf("isolated drop: got %q, want %q", got, ScopeExchangeSpecific)
	}
}

// TestCorrelatorSameExchangeDoesNotSelfCorrelate ensures a single exchange
// reconnecting repeatedly never labels itself local_network off its own history.
func TestCorrelatorSameExchangeDoesNotSelfCorrelate(t *testing.T) {
	c := New()
	c.Record("binance", 1_000_000)
	if got := c.Record("binance", 1_010_000); got != ScopeExchangeSpecific {
		t.Fatalf("repeated same-exchange drop: got %q, want %q", got, ScopeExchangeSpecific)
	}
}

// TestCorrelatorWindowBoundary checks the edge: a sibling exactly at the window
// edge still correlates, just past it does not.
func TestCorrelatorWindowBoundary(t *testing.T) {
	c := New()
	c.Record("binance", 5_000_000)
	if got := c.Record("bingx", 5_000_000+120_000); got != ScopeLocalNetwork {
		t.Fatalf("sibling at window edge: got %q, want %q", got, ScopeLocalNetwork)
	}

	c2 := New()
	c2.Record("binance", 5_000_000)
	if got := c2.Record("bingx", 5_000_000+120_001); got != ScopeExchangeSpecific {
		t.Fatalf("sibling just past window: got %q, want %q", got, ScopeExchangeSpecific)
	}
}
