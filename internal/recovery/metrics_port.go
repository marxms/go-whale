package recovery

// Metrics is the gap-recovery observability port. The zero value is a safe no-op.
type Metrics struct {
	ValidationTotal         func(exchange, marketType, result string)
	ValidationFailed        func(exchange, marketType string)
	MissingBuckets          func(exchange, marketType string, missing int)
	PeriodicBackstopEnqueue func(exchange, marketType string)
	NoProgressTotal         func(exchange, marketType string)
	SuspendedSymbols        func(exchange, marketType string, n float64)
	RESTBudgetDeferredTotal func(exchange, marketType string)
	TargetedRepairTotal     func(exchange, marketType, timeframe string)
	OpenBucketSnapshotLagMs func(exchange, marketType, timeframe string, lagMs float64)
	EmptyCycleTotal         func(exchange, marketType string)
	RejectedCandles         func(exchange, marketType, timeframe, reason string)
}

func (m Metrics) incValidation(exchange, marketType, result string) {
	if m.ValidationTotal != nil {
		m.ValidationTotal(exchange, marketType, result)
	}
}

func (m Metrics) incValidationFailed(exchange, marketType string) {
	if m.ValidationFailed != nil {
		m.ValidationFailed(exchange, marketType)
	}
}

func (m Metrics) observeMissingBuckets(exchange, marketType string, missing int) {
	if m.MissingBuckets != nil {
		m.MissingBuckets(exchange, marketType, missing)
	}
}

// IncPeriodicBackstopEnqueue records a symbol enqueued by the periodic backstop.
func (m Metrics) IncPeriodicBackstopEnqueue(exchange, marketType string) {
	if m.PeriodicBackstopEnqueue != nil {
		m.PeriodicBackstopEnqueue(exchange, marketType)
	}
}

func (m Metrics) incNoProgressSuspension(exchange, marketType string) {
	if m.NoProgressTotal != nil {
		m.NoProgressTotal(exchange, marketType)
	}
}

func (m Metrics) setSuspendedSymbols(exchange, marketType string, n float64) {
	if m.SuspendedSymbols != nil {
		m.SuspendedSymbols(exchange, marketType, n)
	}
}

func (m Metrics) incRESTBudgetDeferred(exchange, marketType string) {
	if m.RESTBudgetDeferredTotal != nil {
		m.RESTBudgetDeferredTotal(exchange, marketType)
	}
}

func (m Metrics) incTargetedRepair(exchange, marketType, timeframe string) {
	if m.TargetedRepairTotal != nil {
		m.TargetedRepairTotal(exchange, marketType, timeframe)
	}
}

func (m Metrics) observeOpenBucketSnapshotLag(exchange, marketType, timeframe string, lagMs float64) {
	if m.OpenBucketSnapshotLagMs != nil {
		m.OpenBucketSnapshotLagMs(exchange, marketType, timeframe, lagMs)
	}
}

// incEmptyCycle records a recovery cycle that attempted a fetch and persisted nothing.
func (m Metrics) incEmptyCycle(exchange, marketType string) {
	if m.EmptyCycleTotal != nil {
		m.EmptyCycleTotal(exchange, marketType)
	}
}

// incRejectedCandle records a candle dropped at the write boundary, labeled by reason.
func (m Metrics) incRejectedCandle(exchange, marketType, timeframe, reason string) {
	if m.RejectedCandles != nil {
		m.RejectedCandles(exchange, marketType, timeframe, reason)
	}
}
