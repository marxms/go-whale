package binancesdk

import "testing"

func TestWSBaseFromEnv(t *testing.T) {
	if got := wsBaseFromEnv("GOWHALE_TEST_UNSET_WS_URL", "wss://fallback"); got != "wss://fallback" {
		t.Fatalf("fallback = %q", got)
	}
	t.Setenv("GOWHALE_TEST_WS_URL", "wss://override")
	if got := wsBaseFromEnv("GOWHALE_TEST_WS_URL", "wss://fallback"); got != "wss://override" {
		t.Fatalf("override = %q", got)
	}
}

func TestFuturesWSDefaultsToProductionHost(t *testing.T) {
	t.Setenv("GOWHALE_BINANCE_FUTURES_WS_URL", "")
	if got := defaultFuturesWSBasePath(); got != "wss://fstream.binance.com" {
		t.Fatalf("futures WS base = %q; the default must be the production host", got)
	}
}

func TestIsTestnetWSBase(t *testing.T) {
	cases := map[string]bool{
		"wss://fstream.binancefuture.com": true,
		"wss://stream.binancefuture.com":  true,
		"wss://fstream.binance.com":       false,
		"ws://127.0.0.1:9000":             false,
	}
	for base, want := range cases {
		if got := isTestnetWSBase(base); got != want {
			t.Errorf("isTestnetWSBase(%q) = %v, want %v", base, got, want)
		}
	}
}
