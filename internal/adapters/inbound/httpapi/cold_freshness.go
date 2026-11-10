package httpapi

import (
	"sync/atomic"
	"time"
)

// coldFreshnessSource reports the age of the last successful cold-tier commit and whether a
// cold tier is configured at all. Defined here (not imported from engine) so the http package
// keeps depending on behavior rather than on a concrete store.
type coldFreshnessSource func() (age time.Duration, configured bool)

// coldFreshness is the registered source, or nil when no cold tier is wired.
type coldFreshnessHolder struct{ fn atomic.Value }

func (h *coldFreshnessHolder) set(fn coldFreshnessSource) { h.fn.Store(fn) }

func (h *coldFreshnessHolder) get() (coldFreshnessSource, bool) {
	fn, ok := h.fn.Load().(coldFreshnessSource)
	return fn, ok && fn != nil
}

// SetColdWriteFreshness wires the durable tier's last-commit clock into /health.
//
// WHY THIS IS THE ASSERTION /health WAS MISSING.
//
// /health has always answered a transport question — are the WebSockets connected, are the
// expected scopes live, is the fleet stale, does the symbol count match the config. Nothing
// on the probe asserted that data was being KEPT.
//
// The fact is already in memory (QuestDBStore stamps lastFlush on every successful commit),
// so the aggregation costs one RLock and one time.Since per probe.
//
// IT DEGRADES, IT DOES NOT 503. A 503 takes the container out of rotation and, under most
// supervisors, restarts it — and restarting the streamer does not fix a wedged QuestDB. It
// discards the hot tier's in-memory rings, which are the only copy of exactly the buckets the
// cold tier is failing to accept. "degraded" is the honest verdict: ingestion works,
// durability does not, keep serving, wake someone up.
func (mc *MetricsCollector) SetColdWriteFreshness(fn coldFreshnessSource) {
	mc.coldFreshness.set(fn)
}
