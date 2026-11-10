package binancesdk

import (
	"testing"

	"github.com/marxms/go-whale/internal/health"
)

// TestCountVerifiedResubscribe proves O2 accounting: a symbol counts as restored
// only when it has delivered data at or after the resubscribe instant — not when
// its handler merely attached. Symbols silent since before the resubscribe (or
// never seen) are the unverified remainder.
func TestCountVerifiedResubscribe(t *testing.T) {
	l := health.New()
	const resubAt = 1000
	l.Stamp("A", 1500) // delivered after resubscribe -> verified
	l.Stamp("B", 2000) // verified
	l.Stamp("C", 1000) // exactly at resubAt -> verified (>=)
	l.Stamp("D", 900)  // last data before resubscribe -> unverified (silent since)
	// E never stamped -> unverified
	symbols := []string{"A", "B", "C", "D", "E"}

	verified, unverified := countVerifiedResubscribe(l, symbols, resubAt)
	if verified != 3 || unverified != 2 {
		t.Fatalf("verified=%d unverified=%d, want 3,2", verified, unverified)
	}
}

func TestCountVerifiedResubscribeAllLive(t *testing.T) {
	l := health.New()
	for _, s := range []string{"A", "B", "C"} {
		l.Stamp(s, 5000)
	}
	if v, u := countVerifiedResubscribe(l, []string{"A", "B", "C"}, 1000); v != 3 || u != 0 {
		t.Fatalf("verified=%d unverified=%d, want 3,0", v, u)
	}
}
