package ingestion

import (
	"sync"
	"time"
)

// Subscription admission policy owned by the ingestion layer.
//
// These numbers decide how long the boot waits for the declared fleet and how fast
// the fallback per-symbol subscribe path paces its frames. They live here because
// they are properties of subscription admission, not of the composition root.

const (
	// AutoSubscribeInitFloor keeps small deployments from getting an absurdly tight
	// budget.
	AutoSubscribeInitFloor = 4 * time.Minute
	// AutoSubscribeInitBase covers the fixed part: discovery, connect, the combined-stream
	// handshake.
	AutoSubscribeInitBase = 2 * time.Minute
	// AutoSubscribeInitPerSymbol is the measured 3,51 s/symbol with ~2,3x headroom, so a
	// slower network or a busier box does not put us back where we started.
	AutoSubscribeInitPerSymbol = 8 * time.Second

	// SubscribePacing separates per-symbol SUBSCRIBE frames on the fallback path (used
	// only when the combined-URL batch attach is unavailable). It paces FRAMES, not
	// seeds, so it stays serial while the seed runs in parallel.
	SubscribePacing = 50 * time.Millisecond

	// MinDiscoveryRefreshInterval bounds how often periodic discovery may run. A shorter
	// cadence would spend the shared REST budget on a market that does not rerank that fast.
	MinDiscoveryRefreshInterval = time.Minute

	// DefaultManagerWorkers is how many trade-processing workers the hybrid manager
	// starts. It is admission capacity, not a per-venue limit.
	DefaultManagerWorkers = 32
)

// AutoSubscribeInitBudget returns the wait bound for the declared fleet size.
//
// The budget scales with the DECLARED symbol count because the seed — not the
// subscribe handshake — is the dominant term. A flat constant silently breaks the
// moment the fleet grows.
func AutoSubscribeInitBudget(symbols int) time.Duration {
	if symbols < 0 {
		symbols = 0
	}
	budget := AutoSubscribeInitBase + time.Duration(symbols)*AutoSubscribeInitPerSymbol
	if budget < AutoSubscribeInitFloor {
		return AutoSubscribeInitFloor
	}
	return budget
}

// WaitWithTimeout blocks until wg is Done or timeout elapses, returning true if the
// WaitGroup completed first and false on timeout. The inner goroutine that calls
// wg.Wait() is intentionally left running on timeout; it completes and exits once
// the outstanding wg counter drains.
func WaitWithTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}
