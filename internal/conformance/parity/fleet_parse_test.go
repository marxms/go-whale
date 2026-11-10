//go:build integration

package parity

import "testing"

// productionSubscriptionsBody is the literal shape /api/subscriptions serves, captured from the
// running fleet. Trimmed to three symbols per market; the nesting is what matters.
const productionSubscriptionsBody = `{
  "total": 503,
  "exchanges": {
    "binance": {
      "total_symbols": 501,
      "markets": {
        "future": {"total_symbols": 500, "symbols": ["GRASS/USDT", "INJ/USDT", "ONDO/USDT"]},
        "spot":   {"total_symbols": 1,   "symbols": ["BTC/USDT"]}
      }
    },
    "bingx": {
      "total_symbols": 2,
      "markets": {"future": {"total_symbols": 2, "symbols": ["BTC/USDT", "ETH/USDT"]}}
    }
  }
}`

// TestParseFleet_ReadsTheProductionEnvelope is the test for the envelope the endpoint actually
// serves.
//
// # WHY A PARSE BUG HERE IS WORSE THAN A PARSE BUG ANYWHERE ELSE
//
// A flat `{symbols:[]}` / `{subscriptions:[]}` shape is not what the endpoint serves, and
// json.Unmarshal does not object to it: unknown keys are ignored, absent fields stay at their zero
// value, and the call returns nil error with an empty slice.
//
// sampleFleet would then take its fallback branch and test BTC, ETH and SOL — the three easiest
// symbols in the fleet — while printing "fleet unreadable (<nil>)" and continuing. PARITY_SEED and
// PARITY_SAMPLE, the two knobs whose entire purpose is to make the sample random and the failure
// replayable, would silently do nothing against production.
//
// The gate exists to catch code that reports success without doing the work. That is that defect,
// inside the gate.
func TestParseFleet_ReadsTheProductionEnvelope(t *testing.T) {
	t.Parallel()

	got, err := parseFleet([]byte(productionSubscriptionsBody))
	if err != nil {
		t.Fatalf("parseFleet returned %v on the body production actually serves — the harness "+
			"cannot sample a fleet it cannot read, and a fallback to three majors reports "+
			"coverage it does not have", err)
	}

	// Five distinct symbols across two exchanges and two markets; BTC/USDT appears in three
	// places and must be counted once.
	want := []string{"BTC/USDT", "ETH/USDT", "GRASS/USDT", "INJ/USDT", "ONDO/USDT"}
	if len(got) != len(want) {
		t.Fatalf("parsed %d symbols %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("symbol %d = %q, want %q — the slice must be SORTED, or the shuffle is not "+
				"a pure function of (fleet, seed) and PARITY_SEED cannot replay a red run",
				i, got[i], want[i])
		}
	}
}

// TestParseFleet_EmptyResultIsAnError keeps the failure loud. A shape that parses to nothing is a
// shape mismatch; returning it as an empty fleet would make the failure silent.
func TestParseFleet_EmptyResultIsAnError(t *testing.T) {
	t.Parallel()

	for name, body := range map[string]string{
		"envelope with no exchanges": `{"total":0,"exchanges":{}}`,
		"the shape that never existed": `{"symbols":["BTC/USDT"],` +
			`"subscriptions":[{"symbol":"ETH/USDT"}]}`,
		"markets present but empty": `{"exchanges":{"binance":{"markets":{"future":{"symbols":[]}}}}}`,
	} {
		if _, err := parseFleet([]byte(body)); err == nil {
			t.Errorf("%s: parsed to zero symbols with a nil error — silence here is read as "+
				"'the fleet is fine', and the harness quietly shrinks to three majors", name)
		}
	}
}
