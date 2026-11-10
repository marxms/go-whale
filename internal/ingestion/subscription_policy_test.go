package ingestion

import (
	"sync"
	"testing"
	"time"
)

// TestAutoSubscribeInitBudget_CoversTheMeasuredSeedCost is the guard for a budget
// sized against the subscribe handshake instead of the seed.
func TestAutoSubscribeInitBudget_CoversTheMeasuredSeedCost(t *testing.T) {
	t.Parallel()

	const (
		measuredSymbols = 200
		measuredCost    = 11*time.Minute + 42*time.Second
	)

	got := AutoSubscribeInitBudget(measuredSymbols)
	if got <= measuredCost {
		t.Fatalf("budget %v does not cover the measured seed cost %v for %d symbols — the housekeeping gate stays shut",
			got, measuredCost, measuredSymbols)
	}
	if got < measuredCost*3/2 {
		t.Errorf("budget %v leaves less than 50%% headroom over the measured %v", got, measuredCost)
	}
}

// TestAutoSubscribeInitBudget_ScalesWithFleetSize is the property a flat constant lacked.
func TestAutoSubscribeInitBudget_ScalesWithFleetSize(t *testing.T) {
	t.Parallel()

	small := AutoSubscribeInitBudget(200)
	large := AutoSubscribeInitBudget(500)
	if large <= small {
		t.Fatalf("budget did not grow with the fleet: 200 symbols -> %v, 500 -> %v", small, large)
	}
	perSymbol := 11*time.Minute + 42*time.Second
	if want := perSymbol * 500 / 200; large <= want {
		t.Errorf("budget %v for 500 symbols does not cover the extrapolated cost %v", large, want)
	}
}

// TestAutoSubscribeInitBudget_FloorProtectsSmallDeployments: a handful of symbols
// must not produce a budget so tight that a slow discovery trips it.
func TestAutoSubscribeInitBudget_FloorProtectsSmallDeployments(t *testing.T) {
	t.Parallel()

	for _, n := range []int{0, 1, 5, -3} {
		if got := AutoSubscribeInitBudget(n); got < AutoSubscribeInitFloor {
			t.Errorf("budget for %d symbols is %v, below the floor %v", n, got, AutoSubscribeInitFloor)
		}
	}
}

func TestWaitWithTimeoutCompletes(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	wg.Add(1)
	wg.Done()

	if got := WaitWithTimeout(&wg, time.Second); !got {
		t.Fatalf("WaitWithTimeout returned %v, want true for a completed WaitGroup", got)
	}
}

func TestWaitWithTimeoutTimesOut(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	wg.Add(1) // never Done — inner wg.Wait() goroutine leaks by design

	start := time.Now()
	if got := WaitWithTimeout(&wg, 50*time.Millisecond); got {
		t.Fatalf("WaitWithTimeout returned %v, want false for a never-completing WaitGroup", got)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("WaitWithTimeout took %v, want it to return promptly near the 50ms timeout", elapsed)
	}
}
