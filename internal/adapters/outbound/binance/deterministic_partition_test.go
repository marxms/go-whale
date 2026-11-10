package binancesdk

import (
	"slices"
	"testing"
)

// TestSortedSubscribedSymbolsIsDeterministic proves O7 is fixed: the reconnect
// symbol list is stable-sorted, so the same symbol set yields the same order (and
// therefore the same partition membership) on every reconnect — instead of the
// map-iteration randomization that re-shuffled which symbols a partition owns.
func TestSortedSubscribedSymbolsIsDeterministic(t *testing.T) {
	c := &SDKClient{combinedURLConnectEnabled: true, livenessLedgerEnabled: true, symbols: map[string]bool{}}
	for _, s := range []string{"SOL/USDT", "BTC/USDT", "ETH/USDT", "MANTA/USDT", "ADA/USDT", "DOGE/USDT"} {
		c.symbols[s] = true
	}

	first := c.sortedSubscribedSymbols()
	if !slices.IsSorted(first) {
		t.Fatalf("symbols not sorted: %v", first)
	}
	if len(first) != 6 {
		t.Fatalf("expected 6 symbols, got %d", len(first))
	}

	// Map iteration order varies run-to-run; sorting must make it identical every call.
	for r := 0; r < 25; r++ {
		if got := c.sortedSubscribedSymbols(); !slices.Equal(got, first) {
			t.Fatalf("non-deterministic order on call %d: %v vs %v", r, got, first)
		}
	}
}
