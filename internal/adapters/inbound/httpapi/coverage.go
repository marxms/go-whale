package httpapi

import "github.com/marxms/go-whale/internal/health"

// SetConfiguredCoverage declares how many symbols each scope is CONFIGURED to stream, so
// the health aggregation can compare it against what is effectively subscribed. Scopes
// are canonicalized the same way as expected scopes, so "bingx:swap" and
// "bingx:future" collapse onto one key.
func (mc *MetricsCollector) SetConfiguredCoverage(byScope map[string]int) {
	mc.coverage.Set(byScope)
	// Publish immediately as well as on each probe, so the gauges are populated when the
	// configuration is declared, before the first /health or scrape.
	if sink, ok := mc.gaugeSink.Load().(health.GaugeSink); ok && sink.SymbolsConfigured != nil {
		configured, declared := mc.coverage.Snapshot()
		if declared {
			for scope, n := range configured {
				ex, mt := health.SplitScope(scope)
				sink.SymbolsConfigured(ex, mt, n)
			}
		}
	}
}
