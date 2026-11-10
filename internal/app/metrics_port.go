package app

// AppMetrics is the composition root's own observability port — the metrics whose call
// sites live in this package. The zero value is a safe no-op.
type AppMetrics struct {
	CoinalyzeCycleSeconds func(seconds float64)
	CoinalyzeCycleOverrun func()
	BootBucketSettle      func(exchange, marketType, timeframe string)
	GappedBucketRepair    func(exchange, marketType, timeframe string)
}

func (m AppMetrics) observeCoinalyzeCycle(seconds float64) {
	if m.CoinalyzeCycleSeconds != nil {
		m.CoinalyzeCycleSeconds(seconds)
	}
}

func (m AppMetrics) incCoinalyzeCycleOverrun() {
	if m.CoinalyzeCycleOverrun != nil {
		m.CoinalyzeCycleOverrun()
	}
}

// IncBootBucketSettle satisfies candle/engine.RepairMetrics.
func (m AppMetrics) IncBootBucketSettle(exchange, marketType, timeframe string) {
	if m.BootBucketSettle != nil {
		m.BootBucketSettle(exchange, marketType, timeframe)
	}
}

// IncGappedBucketRepair satisfies candle/engine.RepairMetrics.
func (m AppMetrics) IncGappedBucketRepair(exchange, marketType, timeframe string) {
	if m.GappedBucketRepair != nil {
		m.GappedBucketRepair(exchange, marketType, timeframe)
	}
}
