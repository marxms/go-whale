package candle

import (
	"testing"
	"time"
)

// TestCompletenessLookbackFor_CoversAtLeastThreeBuckets is the guard for a metric that must
// distinguish "no bucket was due" from "the tier stopped writing".
//
// With a fixed 48h window, a 1w bucket (168h) has no boundary inside the window and the current
// bucket's own timestamp sits before the window opens. Every symbol therefore has zero rows in
// range, and the absent/expected alert fires permanently on a completely healthy tier. A signal
// that always fires is exactly as useless as one that never does.
//
// Three buckets is the smallest window that can tell the two states apart: one in progress, one
// just closed, and one to compare against.
func TestCompletenessLookbackFor_CoversAtLeastThreeBuckets(t *testing.T) {
	t.Parallel()

	cases := []struct {
		tf      string
		bucket  time.Duration
		wantMin time.Duration
	}{
		{"5m", 5 * time.Minute, CompletenessLookback},
		{"15m", 15 * time.Minute, CompletenessLookback},
		{"1h", time.Hour, CompletenessLookback},
		{"4h", 4 * time.Hour, CompletenessLookback},
		{"1d", 24 * time.Hour, CompletenessLookback},
		{"1w", 7 * 24 * time.Hour, 21 * 24 * time.Hour},
	}

	for _, c := range cases {
		got := CompletenessLookbackFor(c.tf)
		if got < 3*c.bucket {
			t.Errorf("%s: lookback = %v, want at least 3 buckets (%v). With fewer, a scope where "+
				"no bucket was DUE is indistinguishable from one where the tier stopped writing, "+
				"and the absent/expected alert fires forever on a healthy tier",
				c.tf, got, 3*c.bucket)
		}
		if got < c.wantMin {
			t.Errorf("%s: lookback = %v, want at least %v", c.tf, got, c.wantMin)
		}
	}
}

// TestCompletenessLookbackFor_FineTimeframesKeepTheFloor pins the other half. The floor
// exists so the scan stays on a handful of partitions and so the metric CONVERGES — a symbol
// that stops streaming must leave the window rather than report a hole forever. Scaling must
// never shrink the window below it.
func TestCompletenessLookbackFor_FineTimeframesKeepTheFloor(t *testing.T) {
	t.Parallel()

	for _, tf := range []string{"1m", "5m", "15m", "30m", "1h"} {
		if got := CompletenessLookbackFor(tf); got != CompletenessLookback {
			t.Errorf("%s: lookback = %v, want the %v floor — scaling down would shrink the scan "+
				"below the window the convergence argument depends on", tf, got, CompletenessLookback)
		}
	}
}

// TestCompletenessLookbackFor_UnparseableTimeframeFallsBackToTheFloor keeps the function
// total. An unknown timeframe must not produce a zero window, which would report every symbol
// absent — the loud version of the same lie.
func TestCompletenessLookbackFor_UnparseableTimeframeFallsBackToTheFloor(t *testing.T) {
	t.Parallel()

	for _, tf := range []string{"", "banana", "0m"} {
		if got := CompletenessLookbackFor(tf); got != CompletenessLookback {
			t.Errorf("%q: lookback = %v, want the %v floor — a zero window reports every symbol "+
				"absent", tf, got, CompletenessLookback)
		}
	}
}
