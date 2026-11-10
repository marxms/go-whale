package ingestion

import "github.com/marxms/go-whale/internal/candle"

// testMetrics is an in-memory implementation of the ingestion Metrics port, so tests read
// their own state instead of package-level Prometheus vectors.
type testMetrics struct {
	discarded     map[string]float64
	missing       map[string]float64
	missingTail   map[string]float64
	admitted      map[string]float64
	preregDiscard map[string]bool
	preregMissing map[string]bool
	preregAdmitt  map[string]bool
	admittedGone  map[string]bool
}

func newTestMetrics() *testMetrics {
	return &testMetrics{
		discarded:     map[string]float64{},
		missing:       map[string]float64{},
		missingTail:   map[string]float64{},
		admitted:      map[string]float64{},
		preregDiscard: map[string]bool{},
		preregMissing: map[string]bool{},
		preregAdmitt:  map[string]bool{},
		admittedGone:  map[string]bool{},
	}
}

func labels(parts ...string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += "|"
		}
		out += p
	}
	return out
}

func (m *testMetrics) port() Metrics {
	return Metrics{
		TradesDiscarded: func(ex, mt, reason string) {
			m.discarded[labels(ex, mt, reason)]++
		},
		TradesMissing: func(ex, mt string, n float64) {
			m.missing[labels(ex, mt)] += n
		},
		TradesMissingTail: func(ex, mt string, n float64) {
			m.missingTail[labels(ex, mt)] += n
		},
		AdmittedVolume: func(ex, mt string, volume float64) {
			m.admitted[labels(ex, mt)] += volume
		},
		PreregisterDiscarded: func(ex, mt, reason string) {
			m.preregDiscard[labels(ex, mt, reason)] = true
		},
		PreregisterMissing: func(ex, mt string) {
			m.preregMissing[labels(ex, mt)] = true
		},
		PreregisterAdmitted: func(ex, mt string) {
			m.preregAdmitt[labels(ex, mt)] = true
		},
		ForgetAdmitted: func(ex, mt string) {
			delete(m.admitted, labels(ex, mt))
			m.admittedGone[labels(ex, mt)] = true
		},
	}
}

// handlerWithMetrics builds the handler the tests exercise, with the recording port attached.
func handlerWithMetrics(store CombinedStore, exchange, marketType string, tfs []string) (*StreamTradeHandler, *testMetrics) {
	m := newTestMetrics()
	h := NewStreamTradeHandler(store, exchange, marketType, candle.NewTimeframeRegistry(tfs))
	h.SetMetrics(m.port())
	return h, m
}
