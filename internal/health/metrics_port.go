package health

// RedisBudgetMetrics is the Redis budget monitor's observability port. The zero value is a
// safe no-op.
type RedisBudgetMetrics struct {
	Occupancy      func(ceiling string, ratio float64)
	KeysWithoutTTL func(n float64)
	KeysTotal      func(n float64)
	EvictedKeys    func(n float64)
	Alert          func(v float64)
}

func (m RedisBudgetMetrics) setOccupancy(ceiling string, ratio float64) {
	if m.Occupancy != nil {
		m.Occupancy(ceiling, ratio)
	}
}

func (m RedisBudgetMetrics) setKeysWithoutTTL(n float64) {
	if m.KeysWithoutTTL != nil {
		m.KeysWithoutTTL(n)
	}
}

func (m RedisBudgetMetrics) setKeysTotal(n float64) {
	if m.KeysTotal != nil {
		m.KeysTotal(n)
	}
}

func (m RedisBudgetMetrics) setEvictedKeys(n float64) {
	if m.EvictedKeys != nil {
		m.EvictedKeys(n)
	}
}

func (m RedisBudgetMetrics) setAlert(v float64) {
	if m.Alert != nil {
		m.Alert(v)
	}
}
