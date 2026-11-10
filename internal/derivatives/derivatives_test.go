package derivatives

import "testing"

// TestNDerivationStep_OnlyEvidencedRungsNormalize is DUR-5 and DUR-6 stated where the
// rule lives.
//
// The ladder has four rungs and exactly two of them may carry an 8h-equivalent. The
// other two mean different things — "an interval is known but not for this bucket" and
// "no interval at all" — and both answer the same way to the only question the
// consumer asks: may a normalized value exist here.
//
// The zero value is in the table on purpose. It is what a record written before the
// ladder existed carries, and a predicate written as "everything except unavailable"
// would let it through.
func TestNDerivationStep_OnlyEvidencedRungsNormalize(t *testing.T) {
	tests := []struct {
		name string
		step NDerivationStep
		want bool
	}{
		{"measured spacing is evidence", NFromMeasuredSpacing, true},
		{"metadata seed is declared, and still an interval", NFromMetadataSeed, true},
		{"inherited is an interval observed elsewhere in time", NInheritedOutsideWindow, false},
		{"unavailable is no interval at all", NUnavailable, false},
		{"the zero value names nothing", NDerivationStep(""), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.step.Normalizes(); got != tt.want {
				t.Fatalf("%q.Normalizes() = %v, want %v", tt.step, got, tt.want)
			}
		})
	}
}

// TestNDerivationStep_TheFourRungsAreDistinctOnTheWire — the rung travels as a SYMBOL
// column and as JSON, so two rungs sharing a string would be indistinguishable to every
// consumer downstream, and the fourth one exists precisely to be told apart from the
// first.
func TestNDerivationStep_TheFourRungsAreDistinctOnTheWire(t *testing.T) {
	rungs := []NDerivationStep{
		NFromMeasuredSpacing, NFromMetadataSeed, NInheritedOutsideWindow, NUnavailable,
	}
	seen := make(map[NDerivationStep]bool, len(rungs))
	for _, rung := range rungs {
		if rung == "" {
			t.Fatal("a rung serializes as the empty string, which is the zero value and names nothing")
		}
		if seen[rung] {
			t.Fatalf("rung %q is used twice — two states that no consumer can tell apart", rung)
		}
		seen[rung] = true
	}
	if len(seen) != 4 {
		t.Fatalf("the ladder has %d distinct rungs, want 4", len(seen))
	}
}
