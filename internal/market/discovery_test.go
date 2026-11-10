package market

import "testing"

// TestDiscoveryHealthy proves the discovery-completeness safety floor that guards
// the two destructive paths (startup orphan reconcile + periodic derank/prune):
// a partial-but-non-empty discovered set must NOT be trusted to drive deletions.
func TestDiscoveryHealthy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		discoveredCount int
		maxAssets       int
		want            bool
	}{
		{
			// MaxAssets <= 0 is the static / explicit-symbol path: the count is exact,
			// discovery did not bound it, so it is always trusted (even at 0).
			name:            "maxAssets<=0 always healthy (static-symbol path)",
			discoveredCount: 0,
			maxAssets:       0,
			want:            true,
		},
		{
			name:            "negative maxAssets always healthy",
			discoveredCount: 3,
			maxAssets:       -1,
			want:            true,
		},
		{
			// 200 of 500 = 40% < 50% floor: the partial-API-response case. MUST be false
			// so the caller SKIPS deletions.
			name:            "below floor is unhealthy (partial response)",
			discoveredCount: 200,
			maxAssets:       500,
			want:            false,
		},
		{
			// 249 of 500 = 49.8%, floor = int(500*0.5) = 250 -> just below.
			name:            "one short of floor is unhealthy",
			discoveredCount: 249,
			maxAssets:       500,
			want:            false,
		},
		{
			name:            "exactly at floor is healthy",
			discoveredCount: 250,
			maxAssets:       500,
			want:            true,
		},
		{
			name:            "above floor is healthy",
			discoveredCount: 500,
			maxAssets:       500,
			want:            true,
		},
		{
			// More than configured is healthy.
			name:            "above maxAssets is healthy",
			discoveredCount: 502,
			maxAssets:       500,
			want:            true,
		},
		{
			name:            "zero discovered with positive maxAssets is unhealthy",
			discoveredCount: 0,
			maxAssets:       500,
			want:            false,
		},
		{
			name:            "odd maxAssets floor truncates toward zero (at floor)",
			discoveredCount: 3,
			maxAssets:       7,
			want:            true,
		},
		{
			name:            "odd maxAssets floor truncates toward zero (below floor)",
			discoveredCount: 2,
			maxAssets:       7,
			want:            false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := DiscoveryHealthy(tt.discoveredCount, tt.maxAssets); got != tt.want {
				t.Fatalf("DiscoveryHealthy(%d, %d) = %v, want %v",
					tt.discoveredCount, tt.maxAssets, got, tt.want)
			}
		})
	}
}

// TestPeriodicDerankHealthy proves the periodic path's stronger current-subscription
// signal: even when the MaxAssets floor would pass, a discovered set that collapsed
// far below the currently-subscribed count is treated as a partial response and
// blocks all removals.
func TestPeriodicDerankHealthy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		discoveredCount int
		currentCount    int
		maxAssets       int
		want            bool
	}{
		{
			name:            "collapse vs current subs blocks removals despite passing MaxAssets",
			discoveredCount: 200,
			currentCount:    500,
			maxAssets:       0, // static-symbol path: MaxAssets basis would always pass
			want:            false,
		},
		{
			name:            "steady state is healthy",
			discoveredCount: 498,
			currentCount:    500,
			maxAssets:       500,
			want:            true,
		},
		{
			name:            "empty current set makes the stronger basis a no-op (MaxAssets passes)",
			discoveredCount: 500,
			currentCount:    0,
			maxAssets:       500,
			want:            true,
		},
		{
			name:            "below MaxAssets floor blocks removals",
			discoveredCount: 100,
			currentCount:    0,
			maxAssets:       500,
			want:            false,
		},
		{
			name:            "modest genuine derank is allowed",
			discoveredCount: 450,
			currentCount:    500,
			maxAssets:       500,
			want:            true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := PeriodicDerankHealthy(tt.discoveredCount, tt.currentCount, tt.maxAssets); got != tt.want {
				t.Fatalf("PeriodicDerankHealthy(%d, %d, %d) = %v, want %v",
					tt.discoveredCount, tt.currentCount, tt.maxAssets, got, tt.want)
			}
		})
	}
}

// TestMinDiscoveryFraction pins the documented floor constant so an accidental
// change (which would weaken the data-loss guard) is caught by the test suite.
func TestMinDiscoveryFraction(t *testing.T) {
	t.Parallel()
	if MinDiscoveryFraction != 0.5 {
		t.Fatalf("MinDiscoveryFraction = %v, want 0.5 (data-loss safety floor)", MinDiscoveryFraction)
	}
}

// TestEnforceMaxAssets proves the discovery ceiling is enforced deterministically.
func TestEnforceMaxAssets(t *testing.T) {
	syms := []string{"A/USDT", "B/USDT", "C/USDT", "D/USDT", "E/USDT"}

	kept, dropped := EnforceMaxAssets(syms, 3)
	if len(kept) != 3 || dropped != 2 {
		t.Fatalf("over cap: kept=%d dropped=%d, want 3/2", len(kept), dropped)
	}

	kept, dropped = EnforceMaxAssets(syms, 5)
	if len(kept) != 5 || dropped != 0 {
		t.Errorf("at cap: kept=%d dropped=%d, want 5/0", len(kept), dropped)
	}

	kept, dropped = EnforceMaxAssets(syms, 10)
	if len(kept) != 5 || dropped != 0 {
		t.Errorf("under cap: kept=%d dropped=%d, want 5/0", len(kept), dropped)
	}

	kept, dropped = EnforceMaxAssets(syms, 0)
	if len(kept) != 5 || dropped != 0 {
		t.Errorf("max<=0: kept=%d dropped=%d, want 5/0 (cap disabled)", len(kept), dropped)
	}
}

// TestDeclaredSymbolCount: discovery ceiling wins when enabled; otherwise the
// explicit list size; zero means no expectation.
func TestDeclaredSymbolCount(t *testing.T) {
	tests := []struct {
		discovery bool
		maxAssets int
		static    int
		want      int
	}{
		{true, 500, 3, 500},
		{false, 500, 3, 3},
		{true, 0, 3, 3},
		{false, 0, 0, 0},
	}
	for _, tt := range tests {
		if got := DeclaredSymbolCount(tt.discovery, tt.maxAssets, tt.static); got != tt.want {
			t.Errorf("DeclaredSymbolCount(%v, %d, %d) = %d, want %d",
				tt.discovery, tt.maxAssets, tt.static, got, tt.want)
		}
	}
}
