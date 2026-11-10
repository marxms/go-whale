package collector

// collectorTestMetrics records the collector ports in memory for assertions. It replaces the
// package-level Prometheus vectors, so tests read their own state instead.
type collectorTestMetrics struct {
	feedAvailable map[string]bool
	feedRequests  map[string]float64
	points        map[string]float64
	spans         map[string]float64
	historyPages  map[string]float64
	measurements  map[string]float64
	comparisons   map[string]float64
	registrySize  float64
	callsSpent    float64
	rateRefusals  float64
	externalQuota float64
}

func newCollectorTestMetrics() *collectorTestMetrics {
	return &collectorTestMetrics{
		feedAvailable: map[string]bool{},
		feedRequests:  map[string]float64{},
		points:        map[string]float64{},
		spans:         map[string]float64{},
		historyPages:  map[string]float64{},
		measurements:  map[string]float64{},
		comparisons:   map[string]float64{},
	}
}

func ckey(parts ...string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += "|"
		}
		out += p
	}
	return out
}

func (m *collectorTestMetrics) port() Metrics {
	return Metrics{
		FeedAvailable: func(source, feed, tf string, available bool) {
			m.feedAvailable[ckey(source, feed, tf)] = available
		},
		FeedRequestTotal: func(ex, feed, tf, outcome string) {
			m.feedRequests[ckey(ex, feed, tf, outcome)]++
		},
		CollectedPoints: func(ex, feed, symbol, tf string, points float64) {
			m.points[ckey(ex, feed, symbol, tf)] = points
		},
		CollectedSpanSeconds: func(ex, feed, symbol, tf string, seconds float64) {
			m.spans[ckey(ex, feed, symbol, tf)] = seconds
		},
		HistoryPagesTotal: func(ex, feed, tf string) {
			m.historyPages[ckey(ex, feed, tf)]++
		},
		FundingIntervalComparisonTotal: func(ex, result string) {
			m.comparisons[ckey(ex, result)]++
		},
		FundingIntervalMeasurementTotal: func(ex, outcome string) {
			m.measurements[ckey(ex, outcome)]++
		},
		FundingProfileRegistryEntries: func(n float64) {
			m.registrySize = n
		},
		FundingIntervalDivergenceTotal: func(_, _, _, _ string) {},
		CoinalyzeCallsSpentTotal:       func(n float64) { m.callsSpent += n },
		CoinalyzeRateRefusalsTotal:     func(n float64) { m.rateRefusals += n },
		CoinalyzeExternalQuotaRefusalsTotal: func(n float64) {
			m.externalQuota += n
		},
	}
}
