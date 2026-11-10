package prommetrics

import (
	"runtime/metrics"

	"github.com/prometheus/client_golang/prometheus"
)

// The runtime publishes GC cost and total cost as separate CPU-second counters; the fraction
// the gate names is their ratio. There is no single metric for it.
const (
	gcCPUSecondsMetric    = "/cpu/classes/gc/total:cpu-seconds"
	totalCPUSecondsMetric = "/cpu/classes/total:cpu-seconds"
)

// newGCCPUFractionCollector publishes the quantity the resource acceptance gate names.
//
// It is read at SCRAPE time through a GaugeFunc, so there is no goroutine, no ticker and
// nothing to shut down. It is CUMULATIVE since process start, not a windowed rate.
func newGCCPUFractionCollector() prometheus.Collector {
	return prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "go_gc_cpu_fraction",
		Help: "Fraction of this process's total CPU time spent in garbage collection since start, read from runtime/metrics (/gc/cpu/fraction). Cumulative, not windowed. This is the quantity the resource acceptance gate specifies; go_gc_duration_seconds_sum is stop-the-world PAUSE wall-time and is not a substitute.",
	}, readGCCPUFraction)
}

// readGCCPUFraction returns the runtime's GC CPU fraction, or 0 when the runtime does not
// publish it.
//
// Zero on absence rather than NaN: this metric is compared against an upper bound, and NaN
// would make `> 0.188` false — silently passing a gate that could not be evaluated.
func readGCCPUFraction() float64 {
	sample := []metrics.Sample{{Name: gcCPUSecondsMetric}, {Name: totalCPUSecondsMetric}}
	metrics.Read(sample)
	if sample[0].Value.Kind() != metrics.KindFloat64 || sample[1].Value.Kind() != metrics.KindFloat64 {
		return 0
	}
	total := sample[1].Value.Float64()
	if total <= 0 {
		return 0
	}
	return sample[0].Value.Float64() / total
}
