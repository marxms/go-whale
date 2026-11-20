package prommetrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// The cold refusal must be observable without reading logs: the drop counter, the pending depth
// and the durability false counter are the three signals the health/alarm path needs.
func TestColdDropMetricsExposed(t *testing.T) {
	reg := prometheus.NewRegistry()
	provider, err := New(reg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	provider.QuestDB().ColdPipelineDropped("binance", "future")
	provider.Engine().ColdPendingDepth("binance", "future", 3)
	provider.Engine().ColdDurableFalse("binance", "future")

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	want := map[string]float64{
		"cold_pipeline_dropped_total": 1,
		"cold_pending_depth":          3,
		"cold_durable_false_total":    1,
	}
	for _, family := range families {
		name := family.GetName()
		expected, ok := want[name]
		if !ok {
			continue
		}
		metrics := family.GetMetric()
		if len(metrics) == 0 {
			t.Fatalf("%s has no metric", name)
		}
		metric := metrics[0]
		got := 0.0
		switch {
		case metric.GetCounter() != nil:
			got = metric.GetCounter().GetValue()
		case metric.GetGauge() != nil:
			got = metric.GetGauge().GetValue()
		default:
			t.Fatalf("%s is neither a counter nor a gauge", name)
		}
		if got != expected {
			t.Fatalf("%s = %v, want %v", name, got, expected)
		}
		labels := map[string]string{}
		for _, label := range metric.GetLabel() {
			labels[label.GetName()] = label.GetValue()
		}
		if labels["exchange"] != "binance" || labels["market_type"] != "future" {
			t.Fatalf("%s labels = %v, want exchange/market_type", name, labels)
		}
		delete(want, name)
	}
	if len(want) != 0 {
		t.Fatalf("missing series: %v", want)
	}
}
