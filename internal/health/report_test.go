package health

import (
	"math"
	"net/http"
	"strings"
	"testing"
	"time"
)

// startedAgo returns a StartedAt probe fixed at the supplied age.
func startedAgo(age time.Duration) func() time.Time {
	start := time.Now().Add(-age)
	return func() time.Time { return start }
}

func healthyConns() []Connection {
	return []Connection{
		{Exchange: "binance", MarketType: "future", Connected: true, Healthy: true, Symbols: 200},
		{Exchange: "bingx", MarketType: "swap", Connected: true, Healthy: true, Symbols: 150},
	}
}

func requireComponent(t *testing.T, r Report, name string, want ComponentState) {
	t.Helper()
	for _, c := range r.Components {
		if c.Name != name {
			continue
		}
		if c.State != want {
			t.Errorf("component %q = %q, want %q (reason: %q)", name, c.State, want, c.Reason)
		}
		return
	}
	t.Fatalf("component %q missing from report: %+v", name, r.Components)
}

func TestAggregator_StatusPaths(t *testing.T) {
	pastGrace := 2 * StartupGrace

	tests := []struct {
		name       string
		probes     Probes
		wantStatus string
		wantHTTP   int
	}{
		{
			name: "healthy with every present probe ok",
			probes: Probes{
				StartedAt:       startedAgo(pastGrace),
				Connections:     healthyConns,
				ExpectedScopes:  func() ([]string, bool) { return []string{"binance:future", "bingx:future"}, true },
				LivenessFailing: func() ([]string, bool) { return nil, true },
				ConfiguredCoverage: func() (map[string]int, bool) {
					return map[string]int{"binance:future": 200, "bingx:future": 150}, true
				},
				ColdWrite:         func() (time.Duration, bool) { return time.Second, true },
				GCHeadroom:        func() (float64, bool) { return 0.5, true },
				DerivativesPoller: func() (bool, bool) { return true, true },
				TimeframeChecksum: func() (string, bool) { return "abc", true },
			},
			wantStatus: "healthy",
			wantHTTP:   http.StatusOK,
		},
		{
			name:       "no connections is starting",
			probes:     Probes{StartedAt: startedAgo(pastGrace), Connections: func() []Connection { return nil }},
			wantStatus: "starting",
			wantHTTP:   http.StatusOK,
		},
		{
			name: "none connected is unhealthy",
			probes: Probes{
				StartedAt: startedAgo(pastGrace),
				Connections: func() []Connection {
					return []Connection{{Exchange: "binance", MarketType: "future", Connected: false}}
				},
			},
			wantStatus: "unhealthy",
			wantHTTP:   http.StatusServiceUnavailable,
		},
		{
			name: "unhealthy connection is degraded",
			probes: Probes{
				StartedAt: startedAgo(pastGrace),
				Connections: func() []Connection {
					return []Connection{{Exchange: "binance", MarketType: "future", Connected: true, Healthy: false}}
				},
			},
			wantStatus: "degraded",
			wantHTTP:   http.StatusOK,
		},
		{
			name: "missing expected scope after grace is unhealthy",
			probes: Probes{
				StartedAt:      startedAgo(pastGrace),
				Connections:    healthyConns,
				ExpectedScopes: func() ([]string, bool) { return []string{"binance:future", "coinbase:spot"}, true },
			},
			wantStatus: "unhealthy",
			wantHTTP:   http.StatusServiceUnavailable,
		},
		{
			name: "missing expected scope within grace stays lenient",
			probes: Probes{
				StartedAt:      startedAgo(time.Second),
				Connections:    healthyConns,
				ExpectedScopes: func() ([]string, bool) { return []string{"coinbase:spot"}, true },
			},
			wantStatus: "healthy",
			wantHTTP:   http.StatusOK,
		},
		{
			name: "stale fleet after grace is unhealthy",
			probes: Probes{
				StartedAt:       startedAgo(pastGrace),
				Connections:     healthyConns,
				LivenessFailing: func() ([]string, bool) { return []string{"binance:future"}, true },
			},
			wantStatus: "unhealthy",
			wantHTTP:   http.StatusServiceUnavailable,
		},
		{
			name: "coverage deficit after grace is unhealthy",
			probes: Probes{
				StartedAt:   startedAgo(pastGrace),
				Connections: healthyConns,
				ConfiguredCoverage: func() (map[string]int, bool) {
					return map[string]int{"binance:future": 500}, true
				},
			},
			wantStatus: "unhealthy",
			wantHTTP:   http.StatusServiceUnavailable,
		},
		{
			name: "coverage deficit within grace stays lenient",
			probes: Probes{
				StartedAt:   startedAgo(time.Second),
				Connections: healthyConns,
				ConfiguredCoverage: func() (map[string]int, bool) {
					return map[string]int{"binance:future": 500}, true
				},
			},
			wantStatus: "healthy",
			wantHTTP:   http.StatusOK,
		},
		{
			name: "stale cold write after grace degrades",
			probes: Probes{
				StartedAt:   startedAgo(pastGrace),
				Connections: healthyConns,
				ColdWrite:   func() (time.Duration, bool) { return 16 * time.Minute, true },
			},
			wantStatus: "degraded",
			wantHTTP:   http.StatusOK,
		},
		{
			name: "stale cold write within grace stays healthy",
			probes: Probes{
				StartedAt:   startedAgo(time.Second),
				Connections: healthyConns,
				ColdWrite:   func() (time.Duration, bool) { return 16 * time.Minute, true },
			},
			wantStatus: "healthy",
			wantHTTP:   http.StatusOK,
		},
		{
			name: "tight GC headroom degrades",
			probes: Probes{
				StartedAt:   startedAgo(pastGrace),
				Connections: healthyConns,
				GCHeadroom:  func() (float64, bool) { return 0.9, true },
			},
			wantStatus: "degraded",
			wantHTTP:   http.StatusOK,
		},
		{
			name: "dead derivatives poller degrades",
			probes: Probes{
				StartedAt:         startedAgo(pastGrace),
				Connections:       healthyConns,
				DerivativesPoller: func() (bool, bool) { return false, true },
			},
			wantStatus: "degraded",
			wantHTTP:   http.StatusOK,
		},
		{
			name: "dead poller does not soften an unhealthy status",
			probes: Probes{
				StartedAt:         startedAgo(pastGrace),
				Connections:       func() []Connection { return []Connection{{Connected: false}} },
				DerivativesPoller: func() (bool, bool) { return false, true },
			},
			wantStatus: "unhealthy",
			wantHTTP:   http.StatusServiceUnavailable,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			report := NewAggregator(tc.probes).Evaluate()
			if report.Status != tc.wantStatus {
				t.Errorf("status = %q, want %q", report.Status, tc.wantStatus)
			}
			if report.HTTPStatus != tc.wantHTTP {
				t.Errorf("HTTPStatus = %d, want %d", report.HTTPStatus, tc.wantHTTP)
			}
		})
	}
}

// TestAggregator_AbsentProbeIsNeverHealthy proves the central rule: a component that is
// not mounted/configured is reported as absent and never as a passed check.
func TestAggregator_AbsentProbeIsNeverHealthy(t *testing.T) {
	report := NewAggregator(Probes{
		Connections: healthyConns,
	}).Evaluate()

	for _, name := range []string{
		"expected_scopes", "fleet_liveness", "feed_producer", "coverage", "cold_write",
		"gc_headroom", "derivatives_poller", "timeframe_checksum",
	} {
		requireComponent(t, report, name, ComponentAbsent)
	}

	if report.Status != "healthy" {
		t.Errorf("status = %q; the present components are healthy, so the aggregate may be healthy", report.Status)
	}
	if report.DerivativesPoller {
		t.Error("derivatives_poller must be false when the probe is absent")
	}
	if report.ColdWriteAgeSeconds != nil {
		t.Error("cold_write_age_seconds must be omitted when the probe is absent")
	}
	if report.GCHeadroomRatio != nil {
		t.Error("gc_headroom_ratio must be omitted when the probe is absent")
	}
	if report.TimeframeChecksum != "" {
		t.Errorf("timeframe_checksum = %q when the probe is absent", report.TimeframeChecksum)
	}
}

// TestAggregator_AbsentConnectionsIsNotHealthy proves that even the aggregate status refuses
// to call a process healthy when it has no connection probe to read.
func TestAggregator_AbsentConnectionsIsNotHealthy(t *testing.T) {
	report := NewAggregator(Probes{}).Evaluate()
	requireComponent(t, report, "connections", ComponentAbsent)
	if report.Status == "healthy" {
		t.Fatal("absent connection probe must not produce status=healthy")
	}
	if report.Status != "starting" {
		t.Errorf("status = %q, want starting (no connections observed)", report.Status)
	}
	if NewAggregator(Probes{}).Ready() {
		t.Error("Ready must be false when the connection probe is absent")
	}
}

func TestCanonicalScope(t *testing.T) {
	tests := []struct{ in, want string }{
		{"binance:future", "binance:future"},
		{"bingx:swap", "bingx:future"},
		{"binance:spot", "binance:spot"},
		{"binance:perpetual", "binance:future"},
		{"noscope", "noscope"},
		{"", ""},
	}
	for _, tc := range tests {
		if got := CanonicalScope(tc.in); got != tc.want {
			t.Errorf("CanonicalScope(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestAggregator_CoverageDeficitFormatAndSort(t *testing.T) {
	report := NewAggregator(Probes{
		StartedAt: startedAgo(2 * StartupGrace),
		Connections: func() []Connection {
			return []Connection{
				{Exchange: "binance", MarketType: "future", Connected: true, Healthy: true, Symbols: 200},
				{Exchange: "bingx", MarketType: "swap", Connected: true, Healthy: true, Symbols: 10},
			}
		},
		ConfiguredCoverage: func() (map[string]int, bool) {
			return map[string]int{"binance:future": 500, "bingx:future": 100}, true
		},
	}).Evaluate()

	want := []string{"binance:future: 200/500", "bingx:future: 10/100"}
	if len(report.CoverageDeficits) != 2 || report.CoverageDeficits[0] != want[0] || report.CoverageDeficits[1] != want[1] {
		t.Errorf("coverage deficits = %v, want %v", report.CoverageDeficits, want)
	}
}

func TestAggregator_MissingAndStaleScopesAreSorted(t *testing.T) {
	report := NewAggregator(Probes{
		StartedAt:      startedAgo(2 * StartupGrace),
		Connections:    func() []Connection { return []Connection{{Connected: true, Healthy: true}} },
		ExpectedScopes: func() ([]string, bool) { return []string{"z:future", "a:future"}, true },
		LivenessFailing: func() ([]string, bool) {
			return []string{"z:future", "a:future"}, true
		},
	}).Evaluate()

	if len(report.MissingScopes) != 2 || report.MissingScopes[0] != "a:future" || report.MissingScopes[1] != "z:future" {
		t.Errorf("missing scopes = %v, want sorted", report.MissingScopes)
	}
	if len(report.StaleScopes) != 2 || report.StaleScopes[0] != "a:future" || report.StaleScopes[1] != "z:future" {
		t.Errorf("stale scopes = %v, want sorted", report.StaleScopes)
	}
}

func TestAggregator_GaugeSinkReceivesLivenessAndCoverage(t *testing.T) {
	type livenessCall struct {
		ex, mt string
		live   bool
	}
	var liveCalls []livenessCall
	configured := map[string]int{}
	effective := map[string]int{}

	NewAggregator(Probes{
		StartedAt:      startedAgo(time.Second),
		Connections:    healthyConns,
		ExpectedScopes: func() ([]string, bool) { return []string{"binance:future"}, true },
		ConfiguredCoverage: func() (map[string]int, bool) {
			return map[string]int{"binance:future": 500}, true
		},
		GaugeSink: func() GaugeSink {
			return GaugeSink{
				StreamLiveness: func(ex, mt string, live bool) {
					liveCalls = append(liveCalls, livenessCall{ex, mt, live})
				},
				SymbolsConfigured: func(ex, mt string, n int) { configured[ex+":"+mt] = n },
				SymbolsEffective:  func(ex, mt string, n int) { effective[ex+":"+mt] = n },
			}
		},
	}).Evaluate()

	if len(liveCalls) != 2 {
		t.Fatalf("liveness sink calls = %d, want 2 (expected scope + bingx connection)", len(liveCalls))
	}
	if configured["binance:future"] != 500 {
		t.Errorf("configured[binance:future] = %d, want 500", configured["binance:future"])
	}
	if effective["binance:future"] != 200 || effective["bingx:future"] != 150 {
		t.Errorf("effective = %v, want binance:future=200 bingx:future=150", effective)
	}
}

// TestFeedProducerDegradedOnRestOnly proves the feed_producer verdict distinguishes the
// three producers and that REST carrying the candle is degraded, never a 503: a readable
// candle is still being served, and restarting would not repair the venue.
func TestFeedProducerDegradedOnRestOnly(t *testing.T) {
	tests := []struct {
		name       string
		report     FeedProducerReport
		wantState  ComponentState
		wantReason []string
	}{
		{
			name:       "rest_total only is degraded",
			report:     FeedProducerReport{Scope: "binance:future", Producer: FeedProducerRESTTotal, SilentSeconds: 90, HaveFeed: true},
			wantState:  ComponentDegraded,
			wantReason: []string{"rest_total only", "ws_kline silent 90s"},
		},
		{
			name:       "ws_kline is ok",
			report:     FeedProducerReport{Scope: "binance:future", Producer: FeedProducerWSKline, HaveFeed: true},
			wantState:  ComponentOK,
			wantReason: []string{"ws_kline"},
		},
		{
			name:       "no producer is degraded",
			report:     FeedProducerReport{Scope: "binance:future", Producer: FeedProducerNone, SilentSeconds: 120, HaveFeed: true},
			wantState:  ComponentDegraded,
			wantReason: []string{"none", "ws_kline silent 120s"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			report := NewAggregator(Probes{
				StartedAt:   startedAgo(2 * StartupGrace),
				Connections: healthyConns,
				FeedProducers: func() ([]FeedProducerReport, bool) {
					return []FeedProducerReport{tc.report}, true
				},
			}).Evaluate()

			requireComponent(t, report, "feed_producer", tc.wantState)
			for _, c := range report.Components {
				if c.Name != "feed_producer" {
					continue
				}
				for _, want := range tc.wantReason {
					if !strings.Contains(c.Reason, want) {
						t.Errorf("feed_producer reason = %q, want it to contain %q", c.Reason, want)
					}
				}
			}
			if report.HTTPStatus != http.StatusOK {
				t.Errorf("HTTPStatus = %d; feed_producer must never turn the probe into 503", report.HTTPStatus)
			}
			if tc.wantState == ComponentDegraded && report.Status != "degraded" {
				t.Errorf("status = %q, want degraded for a rest_total-only scope", report.Status)
			}
			if tc.wantState == ComponentOK && report.Status != "healthy" {
				t.Errorf("status = %q, want healthy for a ws_kline scope", report.Status)
			}
		})
	}
}

// TestRecoveryOnlyIsDegraded proves the other half of the freshness contract: a current
// candle served by REST while the feed is silent keeps the probe at 200 and reports the
// feed_producer component degraded. Restarting would not repair the venue and would
// destroy the rings, so this must never be a 503 on its own.
func TestRecoveryOnlyIsDegraded(t *testing.T) {
	now := time.Now()
	report := NewAggregator(Probes{
		StartedAt:       startedAgo(2 * StartupGrace),
		Connections:     healthyConns,
		LivenessFailing: func() ([]string, bool) { return nil, true },
		CandleFreshness: func() ([]CandleFreshness, bool) {
			return []CandleFreshness{{
				Scope: "binance:future", Symbols: 10, Stale: 0,
				FreshestAtMs: now.Add(-time.Minute).UnixMilli(), HaveData: true,
			}}, true
		},
		FeedProducers: func() ([]FeedProducerReport, bool) {
			return []FeedProducerReport{{
				Scope: "binance:future", Producer: FeedProducerRESTTotal, SilentSeconds: 300, HaveFeed: true,
			}}, true
		},
	}).Evaluate()

	if report.Status != "degraded" || report.HTTPStatus != http.StatusOK {
		t.Fatalf("status = %q/%d, want degraded/200: recovery-only is not an outage", report.Status, report.HTTPStatus)
	}
	requireComponent(t, report, "feed_producer", ComponentDegraded)
	requireComponent(t, report, "fleet_liveness", ComponentOK)
	if len(report.StaleScopes) != 0 {
		t.Fatalf("stale scopes = %v, want none while a fresh REST candle is readable", report.StaleScopes)
	}
	if len(report.CandleFreshness) != 1 || report.CandleFreshness[0].Stale != 0 || report.CandleFreshness[0].FreshestAgoMs <= 0 {
		t.Fatalf("candle_freshness = %+v, want a fresh scope", report.CandleFreshness)
	}
}

// TestHealthStartingWithinGrace proves the startup leniency: a cold fleet during the
// grace window reports the candle component as starting and never 503.
func TestHealthStartingWithinGrace(t *testing.T) {
	report := NewAggregator(Probes{
		StartedAt:       startedAgo(time.Second),
		Connections:     healthyConns,
		LivenessFailing: func() ([]string, bool) { return []string{"binance:future"}, true },
		CandleFreshness: func() ([]CandleFreshness, bool) {
			return []CandleFreshness{{
				Scope: "binance:future", Symbols: 10, Stale: 9, HaveData: false,
			}}, true
		},
		FeedProducers: func() ([]FeedProducerReport, bool) {
			return []FeedProducerReport{{
				Scope: "binance:future", Producer: FeedProducerNone, SilentSeconds: 10, HaveFeed: true,
			}}, true
		},
	}).Evaluate()

	if report.HTTPStatus != http.StatusOK {
		t.Fatalf("HTTPStatus = %d within the startup grace, want 200", report.HTTPStatus)
	}
	if report.Status == "unhealthy" {
		t.Fatalf("status = unhealthy within the startup grace")
	}
	requireComponent(t, report, "fleet_liveness", ComponentStarting)
	if len(report.StaleScopes) != 0 {
		t.Fatalf("stale scopes within grace = %v, want none", report.StaleScopes)
	}
}

// TestHealthHealthyWithFreshCandles proves HLT-06: a fresh readable candle keeps the probe
// at 200 with the candle components in ok.
func TestHealthHealthyWithFreshCandles(t *testing.T) {
	now := time.Now()
	report := NewAggregator(Probes{
		StartedAt:       startedAgo(2 * StartupGrace),
		Connections:     healthyConns,
		LivenessFailing: func() ([]string, bool) { return nil, true },
		CandleFreshness: func() ([]CandleFreshness, bool) {
			return []CandleFreshness{{
				Scope: "binance:future", Symbols: 10, Stale: 0,
				FreshestAtMs: now.Add(-time.Minute).UnixMilli(), HaveData: true,
			}}, true
		},
		FeedProducers: func() ([]FeedProducerReport, bool) {
			return []FeedProducerReport{{
				Scope: "binance:future", Producer: FeedProducerWSKline, HaveFeed: true,
			}}, true
		},
	}).Evaluate()

	if report.Status != "healthy" || report.HTTPStatus != http.StatusOK {
		t.Fatalf("status = %q/%d, want healthy/200 with fresh candles", report.Status, report.HTTPStatus)
	}
	requireComponent(t, report, "fleet_liveness", ComponentOK)
	requireComponent(t, report, "feed_producer", ComponentOK)
}

// TestCandleFreshnessReportsStaleCount proves the published detail counts STALE SYMBOLS,
// not the age of the newest one: one leading symbol must not hide the rest of a frozen
// fleet.
func TestCandleFreshnessReportsStaleCount(t *testing.T) {
	now := time.Now()
	freshest := now.Add(-2 * time.Minute).UnixMilli()
	report := NewAggregator(Probes{
		StartedAt:       startedAgo(2 * StartupGrace),
		Connections:     healthyConns,
		LivenessFailing: func() ([]string, bool) { return nil, true },
		CandleFreshness: func() ([]CandleFreshness, bool) {
			return []CandleFreshness{
				{Scope: "binance:future", Symbols: 10, Stale: 4, FreshestAtMs: freshest, HaveData: true},
				{Scope: "bingx:future", Symbols: 5, Stale: 0, FreshestAtMs: now.UnixMilli(), HaveData: true},
			}, true
		},
	}).Evaluate()

	if len(report.CandleFreshness) != 2 {
		t.Fatalf("candle_freshness = %+v, want two scopes", report.CandleFreshness)
	}
	first := report.CandleFreshness[0]
	if first.Scope != "binance:future" || first.Symbols != 10 || first.Stale != 4 {
		t.Fatalf("candle_freshness[0] = %+v, want binance:future symbols=10 stale=4", first)
	}
	if math.Abs(first.StaleFraction-0.4) > 1e-9 {
		t.Errorf("stale_fraction = %v, want 0.4", first.StaleFraction)
	}
	if first.FreshestAgoMs < 2*time.Minute.Milliseconds()-5000 || first.FreshestAgoMs > 2*time.Minute.Milliseconds()+5000 {
		t.Errorf("freshest_ago_ms = %d, want ~%d", first.FreshestAgoMs, 2*time.Minute.Milliseconds())
	}
	if report.CandleFreshness[1].Scope != "bingx:future" {
		t.Errorf("candle_freshness not sorted by scope: %+v", report.CandleFreshness)
	}
}

func TestGCHeadroomRatio(t *testing.T) {
	sample := gcHeadroomSample{mappedInUse: 1_889_448_248, limit: 1_887_436_800, configured: true}
	ratio, configured := gcHeadroomRatio(sample)
	if !configured {
		t.Fatal("expected configured=true")
	}
	if math.Abs(ratio-1.001066) > 1e-5 {
		t.Errorf("ratio = %.6f, want 1.001066", ratio)
	}

	if ratio, configured := gcHeadroomRatio(gcHeadroomSample{configured: true, limit: 0}); configured || ratio != 0 {
		t.Errorf("non-positive limit must be unconfigured, got ratio=%v configured=%v", ratio, configured)
	}
}
