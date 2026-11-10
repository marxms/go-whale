package prommetrics

import (
	"math"
	"net/http"
	"net/http/httptest"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"

	httpapi "github.com/marxms/go-whale/internal/adapters/inbound/httpapi"
	"github.com/marxms/go-whale/internal/ingestion/streaming"
)

// TestNew_DuplicateRegistrationFails is the required proof that a second construction
// against one registry is rejected rather than silently replacing a collector.
func TestNew_DuplicateRegistrationFails(t *testing.T) {
	reg := prometheus.NewRegistry()
	if _, err := New(reg); err != nil {
		t.Fatalf("first New: %v", err)
	}
	if _, err := New(reg); err == nil {
		t.Fatal("second New against the same registry succeeded; duplicate registration must fail")
	}
}

// TestNew_TwoRegistriesAreIsolated proves the provider has no package-level/default
// registration: a series written through one provider is invisible to the other registry.
func TestNew_TwoRegistriesAreIsolated(t *testing.T) {
	reg1 := prometheus.NewRegistry()
	reg2 := prometheus.NewRegistry()
	p1, err := New(reg1)
	if err != nil {
		t.Fatalf("New reg1: %v", err)
	}
	if _, err := New(reg2); err != nil {
		t.Fatalf("New reg2: %v", err)
	}

	p1.Streaming().SetLiveness("binance", "future", true)

	found1 := gatherFamily(t, reg1, "gowhale_stream_liveness")
	if found1 == nil {
		t.Fatal("registry 1 did not expose gowhale_stream_liveness after a write")
	}
	if found2 := gatherFamily(t, reg2, "gowhale_stream_liveness"); found2 != nil {
		t.Fatal("registry 2 exposes a series written through provider 1 — the registries are not isolated")
	}
	if got := testFloat(t, found1); got != 1 {
		t.Errorf("liveness = %v, want 1", got)
	}
}

// TestNew_RegistersTheGoAndProcessCollectors keeps the go_* and process_* series that the
// historical /metrics exposed.
func TestNew_RegistersTheGoAndProcessCollectors(t *testing.T) {
	reg := prometheus.NewRegistry()
	if _, err := New(reg); err != nil {
		t.Fatalf("New: %v", err)
	}
	for _, name := range []string{
		"go_memstats_next_gc_bytes",
		"go_memstats_heap_alloc_bytes",
		"go_gc_cpu_fraction",
		"process_cpu_seconds_total",
	} {
		if gatherFamily(t, reg, name) == nil {
			t.Errorf("%s is not registered — the process collectors must remain on /metrics", name)
		}
	}
}

// TestMetricsRoute_ServesRepresentativeSeries closes the step between the registry and the
// exposition through the SAME router that production mounts.
func TestMetricsRoute_ServesRepresentativeSeries(t *testing.T) {
	reg := prometheus.NewRegistry()
	p, err := New(reg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.Streaming().SetLiveness("binance", "future", true)
	p.Binance().MessagesInTotal("binance", "future")
	p.Ingestion().TradesMissing("binance", "future", 3)
	p.Recovery().NoProgressTotal("binance", "future")

	mc := httpapi.NewMetricsCollector(&stubHybrid{})
	router := httpapi.NewRouterWithMetrics(
		httpapi.ReadPorts{Metrics: promhttp.HandlerFor(reg, promhttp.HandlerOpts{})},
		"binance", "future", mc, nil, nil, nil, nil, nil, nil, nil,
	)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics = %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"gowhale_stream_liveness",
		"gowhale_binancesdk_messages_in_total",
		"trades_missing_total",
		"gaprecovery_no_progress_total",
		"go_gc_cpu_fraction",
		"go_gc_headroom_ratio",
		"process_cpu_seconds_total",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics does not serve %q", want)
		}
	}
}

// TestNew_DefinesTheWholeMetricInventory pins the count and every name the old promauto
// definitions exposed. A missing name is a dashboard/alert that silently stops existing.
func TestNew_DefinesTheWholeMetricInventory(t *testing.T) {
	reg := prometheus.NewRegistry()
	p, err := New(reg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// HistogramVecs expose no family until a child exists; create one so the bucket
	// boundaries can be asserted.
	p.Recovery().MissingBuckets("inventory", "future", 1)
	p.Recovery().OpenBucketSnapshotLagMs("inventory", "future", "5m", 1)

	expected := []string{
		// streaming
		"gowhale_stream_liveness", "gowhale_stream_symbols_total", "gowhale_stream_symbols_stale",
		"gowhale_streaming_startup_failures_total", "gowhale_streaming_boot_seed_delivery_confirmed",
		"gowhale_streaming_boot_seed_delivery_blind",
		// binance
		"gowhale_binancesdk_goroutines_total", "gowhale_binancesdk_reconnect_attempts_total",
		"gowhale_binancesdk_reconnect_failures_total", "gowhale_binancesdk_disconnect_cause_total",
		"gowhale_binancesdk_last_message_age_seconds", "gowhale_binancesdk_partition_stale_fraction",
		"gowhale_binancesdk_health_state", "gowhale_binancesdk_messages_in_total",
		"gowhale_binancesdk_trades_dropped_total", "gowhale_binancesdk_trades_ingested_total",
		"gowhale_binancesdk_reconnect_signals_dropped_total", "gowhale_binancesdk_resubscribe_incomplete_total",
		"gowhale_binancesdk_resubscribe_unverified_total", "gowhale_binancesdk_dynamic_subscribe_deferred_total",
		"gowhale_binancesdk_starvation_suppressed_reconnect_total",
		"kline_feed_frames_total", "kline_feed_frames_rejected_total", "kline_feed_symbols_attached",
		// realtime
		"realtime_publish_dropped_total",
		// coverage
		"gowhale_symbols_configured", "gowhale_symbols_effective",
		// derivatives adapters
		"gowhale_derivatives_long_short_share_sum_violation_total",
		"gowhale_derivatives_cold_degraded_rows_total",
		"cold_pipeline_blocked", "cold_pipeline_pending_rows", "cold_seed_noop_rows_total",
		"cold_last_flush_age_seconds",
		"gowhale_derivatives_series_non_finite_discarded_total",
		// candle engine
		"candle_warm_seed_total", "candle_delta_inside_horizon_total", "engine_applied_delta_volume_total",
		"cold_missing_buckets", "cold_symbols_with_gaps", "cold_symbols_expected", "cold_symbols_absent",
		"gowhale_derivatives_durable_rows", "gowhale_derivatives_durable_missing_buckets",
		"gowhale_derivatives_durable_column_covered", "gowhale_derivatives_durable_timeframe_covered",
		"gowhale_derivatives_durable_incoherent_funding_rows", "gowhale_derivatives_durable_dimensions_measured",
		"candle_seed_ancestor_revisions_unconfirmed_total", "candle_late_update_total",
		"candle_seed_merge_total", "candle_seed_ancestor_unadmitted_evicted_series_total",
		"candle_seed_ancestor_outside_cold_retention_total", "cold_append_aborted_total",
		// derivatives collector
		"gowhale_derivatives_feed_available", "gowhale_derivatives_feed_request_total",
		"gowhale_derivatives_collected_points", "gowhale_derivatives_collected_span_seconds",
		"gowhale_derivatives_history_pages_total", "gowhale_derivatives_funding_interval_comparison_total",
		"gowhale_derivatives_funding_interval_measurement_total",
		"gowhale_derivatives_funding_profile_registry_entries",
		"gowhale_derivatives_funding_interval_divergence_total",
		"gowhale_coinalyze_calls_spent_total", "gowhale_coinalyze_rate_refusals_total",
		"gowhale_coinalyze_external_quota_refusals_total",
		// health redis budget
		"redis_budget_occupancy_ratio", "redis_budget_keys_without_ttl", "redis_budget_keys_total",
		"redis_budget_evicted_keys", "redis_budget_alert",
		// ingestion
		"trades_discarded_total", "trades_missing_total", "trades_missing_tail_total",
		"stream_admitted_volume_total",
		// netdiag
		"gowhale_netdiag_reconnect_scope_total",
		// recovery
		"gap_recovery_validation_total", "gap_recovery_validation_failed",
		"gap_recovery_missing_buckets", "gowhale_periodic_backstop_enqueue_total",
		"gaprecovery_no_progress_total", "gaprecovery_suspended_symbols",
		"gaprecovery_rest_budget_deferred_total", "gap_recovery_targeted_repair_total",
		"gowhale_seed_open_bucket_snapshot_lag_ms",
		// app
		"gowhale_coinalyze_cycle_duration_seconds", "gowhale_coinalyze_cycle_overrun_total",
		"candle_boot_bucket_settle_total", "candle_gapped_bucket_repair_total",
		// process-level custom gauges
		"go_gc_cpu_fraction", "go_gc_headroom_ratio",
	}

	got := make(map[string]bool)
	for _, name := range metricNames(p) {
		got[name] = true
	}
	for _, name := range expected {
		if !got[name] {
			t.Errorf("%s is missing from the exposition — the metric was renamed, dropped or never registered", name)
		}
	}
	if len(expected) != 90 {
		t.Fatalf("inventory has %d names, want the 90 definitions T44 moved", len(expected))
	}
	// The histogram bucket boundaries are part of the wire contract.
	assertBuckets(t, reg, "gowhale_coinalyze_cycle_duration_seconds", []float64{30, 60, 120, 240, 480, 960, 1920, 3840, 7680, 15360})
	assertBuckets(t, reg, "gap_recovery_missing_buckets", []float64{1, 4, 16, 64, 256, 1024, 4096})
	assertBuckets(t, reg, "gowhale_seed_open_bucket_snapshot_lag_ms", []float64{10, 25, 50, 100, 200, 400, 800, 1600, 3200})
}

func TestReadGCCPUFraction_ReportsTheRuntimeNumber(t *testing.T) {
	runtime.GC()
	runtime.GC()
	got := readGCCPUFraction()
	if got < 0 || got > 1 {
		t.Fatalf("fraction = %v, want a value in [0,1]", got)
	}
	if got == 0 {
		// /cpu/classes is updated on a GC cycle; after two forced collections a zero means
		// the runtime metric was not found and the gauge is publishing a placeholder.
		t.Error("fraction = 0 after two forced collections — the runtime metric was not found")
	}
	sample := []metrics.Sample{{Name: gcCPUSecondsMetric}, {Name: totalCPUSecondsMetric}}
	metrics.Read(sample)
	if sample[0].Value.Kind() != metrics.KindFloat64 || sample[1].Value.Kind() != metrics.KindFloat64 {
		t.Skip("runtime does not publish /cpu/classes in this Go version")
	}
	gc, total := sample[0].Value.Float64(), sample[1].Value.Float64()
	if total <= 0 {
		t.Fatal("total_cpu_seconds = 0")
	}
	if want := gc / total; !closeEnough(got, want) {
		t.Errorf("readGCCPUFraction() = %v, gc/total = %v — the composition changed", got, want)
	}
}

// TestGaugeFuncs_AreEvaluatedAtScrapeTime proves the two process gauges remain GaugeFuncs:
// their value is produced by Gather, and absence is published as NaN rather than zero.
func TestGaugeFuncs_AreEvaluatedAtScrapeTime(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	debug.SetMemoryLimit(math.MaxInt64)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	reg := prometheus.NewRegistry()
	if _, err := New(reg); err != nil {
		t.Fatalf("New: %v", err)
	}

	// No cold store and no GOMEMLIMIT in this binary: both gauges must say NaN, never 0.
	for _, name := range []string{"cold_last_flush_age_seconds", "go_gc_headroom_ratio"} {
		f := gatherFamily(t, reg, name)
		if f == nil {
			t.Fatalf("%s not found", name)
		}
		m := f.GetMetric()
		if len(m) == 0 {
			t.Fatalf("%s has no metric", name)
		}
		if got := m[0].GetGauge().GetValue(); !math.IsNaN(got) {
			t.Errorf("%s = %v when unconfigured; want NaN", name, got)
		}
	}
}

func closeEnough(a, b float64) bool {
	d := a - b
	if d < 0 {
		d = -d
	}
	return d < 1e-3
}

// metricNames extracts the fully-qualified names from the collectors this package built.
func metricNames(p *Provider) []string {
	seen := make(map[string]struct{})
	ch := make(chan *prometheus.Desc, 8192)
	done := make(chan struct{})
	go func() {
		for d := range ch {
			seen[d.String()] = struct{}{}
		}
		close(done)
	}()
	for _, c := range p.collectors {
		c.Describe(ch)
	}
	close(ch)
	<-done

	names := make([]string, 0, len(seen))
	const prefix = `Desc{fqName: "`
	for desc := range seen {
		rest := desc[len(prefix):]
		for i := 0; i < len(rest); i++ {
			if rest[i] == '"' {
				names = append(names, rest[:i])
				break
			}
		}
	}
	return names
}

func gatherFamily(t *testing.T, reg *prometheus.Registry, name string) *dto.MetricFamily {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() == name {
			return f
		}
	}
	return nil
}

func testFloat(t *testing.T, f *dto.MetricFamily) float64 {
	t.Helper()
	mets := f.GetMetric()
	if len(mets) == 0 {
		t.Fatal("family has no metrics")
	}
	return mets[0].GetGauge().GetValue()
}

func assertBuckets(t *testing.T, reg *prometheus.Registry, name string, want []float64) {
	t.Helper()
	f := gatherFamily(t, reg, name)
	if f == nil {
		t.Fatalf("%s not found", name)
	}
	m := f.GetMetric()
	if len(m) == 0 {
		t.Fatalf("%s has no metric", name)
	}
	got := m[0].GetHistogram().GetBucket()
	if len(got) != len(want) {
		t.Fatalf("%s has %d buckets, want %d", name, len(got), len(want))
	}
	for i := range want {
		if got[i].GetUpperBound() != want[i] {
			t.Errorf("%s bucket %d = %v, want %v", name, i, got[i].GetUpperBound(), want[i])
		}
	}
}

// Small HTTP stubs so the router can mount /metrics without a live manager.

type stubHybrid struct{}

func (stubHybrid) GetConnectionStats() []streaming.ConnectionStats {
	return nil
}
