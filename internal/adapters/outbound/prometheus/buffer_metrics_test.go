package prommetrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func gaugeValue(t *testing.T, registry *prometheus.Registry, name string) (float64, bool) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			return metric.GetGauge().GetValue(), true
		}
	}
	return 0, false
}

// Current fill, high-water mark, resolved cap and drain age are all exposed per
// symbol, and an unsubscribed symbol removes every one of its series.
func TestTradeBufferMetricsExposeFillCapAndDrainAge(t *testing.T) {
	registry := prometheus.NewRegistry()
	provider, err := New(registry)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	provider.Ingestion().SetTradesBuffer("binance", "future", "BTC/USDT", 7, 9, 10, 2.5)

	for name, want := range map[string]float64{
		"gowhale_stream_trades_buffer_fill":                    7,
		"gowhale_stream_trades_buffer_max_fill":                9,
		"gowhale_stream_trades_buffer_capacity":                10,
		"gowhale_stream_trades_buffer_last_drain_age_seconds":  2.5,
	} {
		got, ok := gaugeValue(t, registry, name)
		if !ok {
			t.Fatalf("%s not exposed after SetTradesBuffer", name)
		}
		if got != want {
			t.Fatalf("%s=%v, want %v", name, got, want)
		}
	}

	provider.Ingestion().DeleteTradesBuffer("binance", "future", "BTC/USDT")
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather after delete: %v", err)
	}
	for _, family := range families {
		switch family.GetName() {
		case "gowhale_stream_trades_buffer_fill", "gowhale_stream_trades_buffer_max_fill",
			"gowhale_stream_trades_buffer_capacity",
			"gowhale_stream_trades_buffer_last_drain_age_seconds":
			if len(family.GetMetric()) != 0 {
				t.Fatalf("%s still exposes %d series after DeleteTradesBuffer", family.GetName(), len(family.GetMetric()))
			}
		}
	}
}
