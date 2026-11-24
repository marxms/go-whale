package ingestion

import (
	"os"
	"strings"
	"testing"
	"time"
)

// TestAdaptiveDelayUsesTheFullestBuffer is the root-cause ratchet: one full symbol among idle
// ones must drive the minimum drain delay. The average of the same fleet says "idle" and is
// how the 30k cap was reached during a rally.
func TestAdaptiveDelayUsesTheFullestBuffer(t *testing.T) {
	minDelay := 50 * time.Millisecond
	maxDelay := 500 * time.Millisecond

	if got := processDelayForFill(1.0, minDelay, maxDelay); got != minDelay {
		t.Fatalf("full buffer delay = %v, want %v", got, minDelay)
	}
	if got := processDelayForFill(0.0, minDelay, maxDelay); got != maxDelay {
		t.Fatalf("empty buffer delay = %v, want %v", got, maxDelay)
	}
	mid := processDelayForFill(0.5, minDelay, maxDelay)
	if mid <= minDelay || mid >= maxDelay {
		t.Fatalf("half-full delay = %v, want between %v and %v", mid, minDelay, maxDelay)
	}

	// The manager must aggregate with max, not the average: a full symbol among empty ones
	// has to reach processDelayForFill at 1.0.
	source, err := os.ReadFile("hybrid_manager.go")
	if err != nil {
		t.Fatalf("reading hybrid_manager.go: %v", err)
	}
	text := string(source)
	if !strings.Contains(text, "processDelayForFill(maxFill") {
		t.Fatal("calculateAdaptiveDelay no longer feeds the fullest buffer to the decision")
	}
	if strings.Contains(text, "avgFill") {
		t.Fatal("calculateAdaptiveDelay still uses the average fill; a hot symbol is diluted")
	}
}
