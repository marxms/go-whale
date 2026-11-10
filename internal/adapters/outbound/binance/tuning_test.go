package binancesdk

import (
	"math"
	"testing"
)

// TestSaturatingIntClampsAtMaxInt pins the conversion used for reconnect counters: a counter
// that outgrows int on a 32-bit build must read as MaxInt, never wrap into a negative count.
func TestSaturatingIntClampsAtMaxInt(t *testing.T) {
	if got := saturatingInt(math.MaxUint64); got != math.MaxInt {
		t.Fatalf("saturatingInt(MaxUint64) = %d, want %d", got, math.MaxInt)
	}
	if got := saturatingInt(42); got != 42 {
		t.Fatalf("saturatingInt(42) = %d, want 42", got)
	}
}
