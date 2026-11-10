package redisstore

// Metrics is the Redis derivatives adapter's observability port. The zero value is a safe
// no-op.
type Metrics struct {
	SeriesNonFiniteDiscarded func(field string)
}

func (m Metrics) incSeriesNonFiniteDiscarded(field string) {
	if m.SeriesNonFiniteDiscarded != nil {
		m.SeriesNonFiniteDiscarded(field)
	}
}
