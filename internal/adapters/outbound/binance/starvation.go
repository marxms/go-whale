package binancesdk

import (
	"context"
	"sync/atomic"
	"time"
)

// Starvation interlock tuning.
const (
	heartbeatInterval          = 1 * time.Second
	heartbeatGapThreshold      = 5 * time.Second // a tick gap beyond this means the process was descheduled (CPU starvation)
	starvationSuppressWindowMs = int64(20_000)   // suppress watchdog reconnects for this long after a detected deschedule
)

// starvationHeartbeat detects process-wide descheduling (CPU starvation) via a
// monotonic ticker: if a tick arrives far later than scheduled, the process could
// not run — so a "stale everywhere" ledger reading is the process, not dead
// streams. The per-partition watchdog consults this before reconnecting (D2), so a
// CPU-starved box (the O4 incident trigger) is not hit with a reconnect storm that
// only makes the starvation worse.
type starvationHeartbeat struct {
	lastStarvationAtMs atomic.Int64
}

func (h *starvationHeartbeat) markStarved(nowMs int64) { h.lastStarvationAtMs.Store(nowMs) }

// recentlyStarved reports whether a descheduling gap was observed within the
// suppression window ending at nowMs.
func (h *starvationHeartbeat) recentlyStarved(nowMs int64) bool {
	last := h.lastStarvationAtMs.Load()
	return last != 0 && nowMs-last >= 0 && nowMs-last < starvationSuppressWindowMs
}

// run ticks on a monotonic interval; a tick gap beyond heartbeatGapThreshold marks
// a starvation event. Exits on ctx cancellation. Client-scoped (starvation is
// process-wide, not per-connection), started once.
func (h *starvationHeartbeat) run(ctx context.Context) {
	t := time.NewTicker(heartbeatInterval)
	defer t.Stop()
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			if now.Sub(last) > heartbeatGapThreshold {
				h.markStarved(now.UnixMilli())
			}
			last = now
		}
	}
}
