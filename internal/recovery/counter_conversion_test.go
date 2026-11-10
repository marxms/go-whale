package recovery

import (
	"math"
	"testing"
)

// TestCounterConversionsAreBounded pins the two conversions used by the backlog counters: a
// uint64 that cannot fit in int reads as MaxInt, and a negative job count reads as zero.
func TestCounterConversionsAreBounded(t *testing.T) {
	if got := saturatingInt(math.MaxUint64); got != math.MaxInt {
		t.Fatalf("saturatingInt(MaxUint64) = %d, want %d", got, math.MaxInt)
	}
	if got := saturatingInt(7); got != 7 {
		t.Fatalf("saturatingInt(7) = %d, want 7", got)
	}
	if got := countToUint64(-1); got != 0 {
		t.Fatalf("countToUint64(-1) = %d, want 0", got)
	}
	if got := countToUint64(12); got != 12 {
		t.Fatalf("countToUint64(12) = %d, want 12", got)
	}
}
