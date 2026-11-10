package httpapi

import (
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/health"
	"github.com/marxms/go-whale/internal/ingestion/streaming"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// ─── Dependency interfaces (no concrete imports) ──────────────────────────

// HybridManagerStats is the interface the metrics layer needs from HybridManager.
type HybridManagerStats interface {
	GetConnectionStats() []streaming.ConnectionStats
}

// ─── MetricsCollector ─────────────────────────────────────────────────────

// MetricsCollector owns the HTTP-side probe state and the DTOs. The aggregation itself
// lives in internal/health; this type never computes a status.
type MetricsCollector struct {
	startTime        time.Time
	hybridManager    HybridManagerStats
	derivPollerAlive *health.PollerHealth // external flag set by the restart wrapper
	tfReg            *candle.TimeframeRegistry
	expectedScopes   atomic.Value // stores []string: canonical scopes that MUST be live after grace
	fleetLiveness    atomic.Value // stores fleetLivenessSnapshot: failing scopes + per-scope detail
	feedProducers    atomic.Value // stores []health.FeedProducerReport: producer of the newest candle per scope
	coverage         health.CoverageConfig
	// coldFreshness is the durable tier's last-commit clock — the one INTEGRITY assertion on
	// a probe that otherwise measures transport only. See SetColdWriteFreshness.
	coldFreshness coldFreshnessHolder
	gaugeSink     atomic.Value // stores health.GaugeSink
	aggregator    *health.Aggregator
	log           *slog.Logger
}

// fleetLivenessSnapshot is the ONE atomic record the sampler publishes. The failing
// verdict and the freshness detail are produced and read together so /health cannot
// report a scope as fresh while the verdict calls it stale.
type fleetLivenessSnapshot struct {
	failing   []string
	freshness []health.CandleFreshness
}

// NewMetricsCollector creates a MetricsCollector over the live connection catalog.
func NewMetricsCollector(hybridManager HybridManagerStats, logs ...*slog.Logger) *MetricsCollector {
	mc := &MetricsCollector{
		startTime:     time.Now(),
		hybridManager: hybridManager,
		log:           logger.OrDiscard(logs...),
	}
	mc.aggregator = health.NewAggregator(health.Probes{
		StartedAt:      func() time.Time { return mc.startTime },
		Connections:    mc.connectionProbe,
		ExpectedScopes: mc.expectedScopesProbe,
		LivenessFailing: func() ([]string, bool) {
			v, ok := mc.fleetLiveness.Load().(fleetLivenessSnapshot)
			if !ok {
				return nil, false
			}
			return v.failing, true
		},
		CandleFreshness: func() ([]health.CandleFreshness, bool) {
			v, ok := mc.fleetLiveness.Load().(fleetLivenessSnapshot)
			if !ok {
				return nil, false
			}
			return v.freshness, true
		},
		FeedProducers: func() ([]health.FeedProducerReport, bool) {
			v, ok := mc.feedProducers.Load().([]health.FeedProducerReport)
			return v, ok
		},
		ConfiguredCoverage: mc.coverage.Snapshot,
		ColdWrite:          mc.coldWriteProbe,
		GCHeadroom:         health.GCHeadroomRatio,
		DerivativesPoller: func() (bool, bool) {
			if mc.derivPollerAlive == nil {
				return false, false
			}
			return mc.derivPollerAlive.IsAlive(), true
		},
		TimeframeChecksum: func() (string, bool) {
			if mc.tfReg == nil {
				return "", false
			}
			return mc.tfReg.Checksum(), true
		},
		GaugeSink: func() health.GaugeSink {
			s, _ := mc.gaugeSink.Load().(health.GaugeSink)
			return s
		},
	})
	return mc
}

// connectionProbe maps the hybrid manager's DTO onto the health package's capability
// type. The mapping is the only place the two vocabularies meet.
func (mc *MetricsCollector) connectionProbe() []health.Connection {
	stats := mc.hybridManager.GetConnectionStats()
	out := make([]health.Connection, 0, len(stats))
	for _, cs := range stats {
		out = append(out, health.Connection{
			Exchange:   cs.ExchangeID,
			MarketType: cs.MarketType,
			Connected:  cs.Connected,
			Healthy:    cs.Healthy,
			Symbols:    cs.Symbols,
		})
	}
	return out
}

func (mc *MetricsCollector) expectedScopesProbe() ([]string, bool) {
	v, ok := mc.expectedScopes.Load().([]string)
	return v, ok
}

func (mc *MetricsCollector) coldWriteProbe() (time.Duration, bool) {
	fn, ok := mc.coldFreshness.get()
	if !ok {
		return 0, false
	}
	return fn()
}

// SetGaugeSink wires the optional metric sink the aggregation publishes through.
func (mc *MetricsCollector) SetGaugeSink(sink health.GaugeSink) {
	mc.gaugeSink.Store(sink)
}

// SetDerivativesPollerFlag sets an external atomic flag that tracks whether
// the derivatives poller goroutine is alive. Called from main.go.
func (mc *MetricsCollector) SetDerivativesPollerFlag(flag *health.PollerHealth) {
	mc.derivPollerAlive = flag
}

// SetTimeframeRegistry sets the timeframe registry for health checksum.
func (mc *MetricsCollector) SetTimeframeRegistry(tfReg *candle.TimeframeRegistry) {
	mc.tfReg = tfReg
}

// SetFleetLiveness records one atomic sample: the canonical exchange:market_type scopes
// whose per-symbol fleet freshness is below threshold, plus the per-scope detail the
// verdict is made of. Both probes read this same snapshot, so the published freshness and
// the 503 verdict cannot disagree. The sampler applies the two-consecutive-sample
// hysteresis before calling this.
func (mc *MetricsCollector) SetFleetLiveness(failing []string, freshness []health.CandleFreshness) {
	mc.fleetLiveness.Store(fleetLivenessSnapshot{
		failing:   append([]string(nil), failing...),
		freshness: append([]health.CandleFreshness(nil), freshness...),
	})
}

// SetFeedProducers records, per canonical scope, the producer of the newest readable
// candle. A rest_total-only scope is published as degraded by the aggregation; this
// setter never decides the HTTP status.
func (mc *MetricsCollector) SetFeedProducers(reports []health.FeedProducerReport) {
	mc.feedProducers.Store(append([]health.FeedProducerReport(nil), reports...))
}

// SetExpectedScopes declares the exchange:market_type scopes that MUST have a
// healthy connected stream once the startup grace period has elapsed. Scopes
// are canonicalized via market.FromConfig so callers may pass raw config market
// types ("swap", "future", "spot") interchangeably; "binance:future" and
// "bingx:swap" both normalize into the "<id>:future" namespace. After the grace
// window, any expected scope without a healthy connected stream makes /health
// report "unhealthy"/503, so a totally dead exchange does not pass the probe.
// Passing an empty slice disables the expected-scope check (lenient mode).
func (mc *MetricsCollector) SetExpectedScopes(scopes []string) {
	canon := make([]string, 0, len(scopes))
	for _, s := range scopes {
		canon = append(canon, health.CanonicalScope(s))
	}
	mc.expectedScopes.Store(canon)
}

// ─── Health: Real health check aggregating all subsystems ─────────────────

// HealthResponse is the JSON response for /health.
type HealthResponse struct {
	Status            string                    `json:"status"` // "healthy", "degraded", "unhealthy", "starting"
	Uptime            string                    `json:"uptime"`
	Goroutines        int                       `json:"goroutines"`
	Connections       []ConnectionHealthSummary `json:"connections,omitempty"`
	DerivativesPoller bool                      `json:"derivatives_poller"` // true if the poller goroutine is alive
	TimeframeChecksum string                    `json:"timeframe_checksum,omitempty"`
	MissingScopes     []string                  `json:"missing_scopes,omitempty"`    // expected scopes with no healthy connected stream (post-grace)
	StaleScopes       []string                  `json:"stale_scopes,omitempty"`      // scopes connected but with most symbols frozen (per-symbol liveness, post-grace)
	CoverageDeficits  []string                  `json:"coverage_deficits,omitempty"` // scopes streaming fewer symbols than configured, as "scope: effective/configured"`
	// CandleFreshness is the per-scope detail behind stale_scopes: newest readable age,
	// monitored symbols and stale symbols. Additive; every field above keeps its name,
	// shape and omitempty behavior.
	CandleFreshness []health.CandleFreshnessReport `json:"candle_freshness,omitempty"`

	// ColdWriteAgeSeconds is the age of the last successful commit to the durable tier.
	// Every other field on this response answers "is data arriving?"; this one answers "is it
	// being kept?" — the question that went unasked for thirteen hours while the probe stayed
	// green. Nil (omitted) when no cold tier is configured.
	ColdWriteAgeSeconds *int64 `json:"cold_write_age_seconds,omitempty"`
	// ColdWriteStale is true when that age is past coldWriteFreshnessDegradedAfter. It
	// degrades the status; it deliberately does NOT make the probe 503 — see
	// SetColdWriteFreshness for why restarting on this makes the incident worse.
	ColdWriteStale bool `json:"cold_write_stale,omitempty"`

	// GCHeadroomRatio is mapped memory not returned to the OS over GOMEMLIMIT.
	//
	// The fields above answer "is data arriving?" and "is it being kept?". This one answers
	// "can the process still do work?" — the question that went unowned for 30.6 h while
	// 82.5% of 1.92 cores burned in GC and the probe stayed green. Nil (omitted) when no
	// memory limit is configured.
	GCHeadroomRatio *float64 `json:"gc_headroom_ratio,omitempty"`
	// GCHeadroomTight is true when that ratio exceeds gcHeadroomTightAbove. It degrades the
	// status and DELIBERATELY does not make the probe answer 503: restarting a process under
	// memory pressure only restarts the spiral from zero, at the cost of seeding again.
	GCHeadroomTight bool `json:"gc_headroom_tight,omitempty"`

	// Components is the additive per-component verdict. "absent" means the component is
	// not mounted or not configured; it is never a passed check. Every field above keeps
	// its name, shape and omitempty behavior.
	Components []health.ComponentReport `json:"components,omitempty"`
}

// ConnectionHealthSummary is a compact health summary per WebSocket connection.
type ConnectionHealthSummary struct {
	Exchange   string `json:"exchange"`
	MarketType string `json:"market_type"`
	Connected  bool   `json:"connected"`
	Healthy    bool   `json:"healthy"`
	Symbols    int    `json:"symbols"`
}

// Health serves GET /health. The aggregation lives in internal/health; this handler only
// maps its Report to the DTO and writes the status the aggregation chose.
func (mc *MetricsCollector) Health(w http.ResponseWriter, _ *http.Request) {
	report := mc.aggregator.Evaluate()

	summaries := make([]ConnectionHealthSummary, 0, len(report.Connections))
	for _, c := range report.Connections {
		summaries = append(summaries, ConnectionHealthSummary{
			Exchange:   c.Exchange,
			MarketType: c.MarketType,
			Connected:  c.Connected,
			Healthy:    c.Healthy,
			Symbols:    c.Symbols,
		})
	}

	resp := HealthResponse{
		Status:              report.Status,
		Uptime:              report.Uptime,
		Goroutines:          report.Goroutines,
		Connections:         summaries,
		DerivativesPoller:   report.DerivativesPoller,
		TimeframeChecksum:   report.TimeframeChecksum,
		MissingScopes:       report.MissingScopes,
		StaleScopes:         report.StaleScopes,
		CandleFreshness:     report.CandleFreshness,
		CoverageDeficits:    report.CoverageDeficits,
		ColdWriteAgeSeconds: report.ColdWriteAgeSeconds,
		ColdWriteStale:      report.ColdWriteStale,
		GCHeadroomRatio:     report.GCHeadroomRatio,
		GCHeadroomTight:     report.GCHeadroomTight,
		Components:          report.Components,
	}

	writeJSON(mc.log, w, report.HTTPStatus, resp)
}

// ─── Ready: Readiness probe ──────────────────────────────────────────────

// Ready serves GET /ready — returns 200 only when at least one connection is established.
func (mc *MetricsCollector) Ready(w http.ResponseWriter, _ *http.Request) {
	if mc.aggregator.Ready() {
		writeJSON(mc.log, w, http.StatusOK, map[string]string{responseKeyStatus: "ready"})
		return
	}
	writeJSON(mc.log, w, http.StatusServiceUnavailable, map[string]string{responseKeyStatus: "not_ready"})
}
