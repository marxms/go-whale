package app

import "sync"

// appTestMetrics records the App-owned observability ports. It replaces the package-level
// Prometheus vectors the tests used.
type appTestMetrics struct {
	mu sync.Mutex

	cycleSeconds []float64
	overrun      int
	bootSettle   map[string]int
	gappedRepair map[string]int
}

func newAppTestMetrics() *appTestMetrics {
	return &appTestMetrics{
		bootSettle:   map[string]int{},
		gappedRepair: map[string]int{},
	}
}

func appKey(parts ...string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += "|"
		}
		out += p
	}
	return out
}

func (m *appTestMetrics) appPort() AppMetrics {
	return AppMetrics{
		CoinalyzeCycleSeconds: func(seconds float64) {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.cycleSeconds = append(m.cycleSeconds, seconds)
		},
		CoinalyzeCycleOverrun: func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.overrun++
		},
		BootBucketSettle: func(ex, mt, tf string) {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.bootSettle[appKey(ex, mt, tf)]++
		},
		GappedBucketRepair: func(ex, mt, tf string) {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.gappedRepair[appKey(ex, mt, tf)]++
		},
	}
}

func (m *appTestMetrics) cycleCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.cycleSeconds)
}

func (m *appTestMetrics) cycleSum() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var sum float64
	for _, v := range m.cycleSeconds {
		sum += v
	}
	return sum
}
