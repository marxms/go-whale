package recovery

// recordNoProgressSuspension records a no-progress suspension for a scope.
func (m *Manager) recordNoProgressSuspension(exchange, marketType string) {
	m.metrics.incNoProgressSuspension(exchange, marketType)
}

// recordRESTBudgetDeferred records a job deferred due to REST budget exhaustion.
func (m *Manager) recordRESTBudgetDeferred(exchange, marketType string) {
	m.metrics.incRESTBudgetDeferred(exchange, marketType)
}

// recordValidation records a validation outcome. missing is the size of the hole still open,
// in BUCKETS, as returned by Validate; it is observed only on failure, where it is the fact
// that distinguishes a blip from an outage. A failure that reports 0 buckets (the query
// errored, or the range came back empty — the validator could not measure) is counted as a
// failure but not observed, so the histogram never dilutes real hole sizes with unknowns.
func (m *Manager) recordValidation(exchange, marketType string, ok bool, missing int) {
	if ok {
		m.metrics.incValidation(exchange, marketType, "success")
		return
	}
	m.metrics.incValidation(exchange, marketType, "failed")
	m.metrics.incValidationFailed(exchange, marketType)
	if missing > 0 {
		m.metrics.observeMissingBuckets(exchange, marketType, missing)
	}
}

// targetedRepairTotal counts jobs whose EXPLICIT window was honored instead of being
// re-derived from the ring head.
//
// It exists because the alternative was invisible: a caller could name a bucket, the worker
// would silently substitute [head, now], fetch something else and report success. A settle
// that never settles is indistinguishable from one that did unless the substitution is
// counted. A rate near zero while boot-bucket settles are being enqueued means the targeted
// branch is not being taken and those enqueues are doing nothing.
