package redisstore

import "testing"

// TestLiveKeepSet proves the orphan-sweep retain set is the race-free union of
// manager subscriptions (scoped by prefix) and active_symbols, and never leaks
// symbols from other scopes.
func TestLiveKeepSet(t *testing.T) {
	subs := []string{
		"binance:future:BTC/USDT",
		"binance:future:ETH/USDT",
		"binance:spot:SOL/USDT", // other scope — excluded by prefix
		"bingx:future:XRP/USDT", // other scope — excluded by prefix
	}
	// PEPE is only in active_symbols (data written) — still retained.
	active := []string{"BTC/USDT", "PEPE/USDT"}

	keep := LiveKeepSet(subs, active, "binance:future:")

	for _, want := range []string{"BTC/USDT", "ETH/USDT", "PEPE/USDT"} {
		if _, ok := keep[want]; !ok {
			t.Errorf("expected %q in keep set, missing (%v)", want, keep)
		}
	}
	for _, bad := range []string{"SOL/USDT", "XRP/USDT"} {
		if _, ok := keep[bad]; ok {
			t.Errorf("cross-scope symbol %q leaked into keep set", bad)
		}
	}
	if len(keep) != 3 {
		t.Fatalf("keep size = %d, want 3 (%v)", len(keep), keep)
	}
}

// TestLiveKeepSetEmpty guards the catastrophic case: empty inputs must yield an
// empty keep set so the caller's len==0 guard skips the sweep.
func TestLiveKeepSetEmpty(t *testing.T) {
	if got := LiveKeepSet(nil, nil, "binance:future:"); len(got) != 0 {
		t.Fatalf("empty inputs should yield empty keep set, got %v", got)
	}
}
