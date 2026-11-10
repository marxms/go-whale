// Package health owns the process's health aggregation. It receives small, capability-
// scoped probes and never a store or an adapter: the composition root supplies closures
// that read the live subsystems, and this package decides the status.
package health

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marxms/go-whale/internal/market"
)

// StartupGrace is the window after process start during which /health stays lenient
// (reports "starting"/200 even with missing scopes) so the paced multi-exchange startup
// does not flap the probe. After it elapses, every expected exchange:market_type scope
// must have a healthy connected stream or /health reports "unhealthy"/503.
const StartupGrace = 3 * time.Minute

// ColdWriteFreshnessDegradedAfter is the age at which /health calls the durable tier
// degraded.
//
// Derived, not picked. The cold pipeline commits on a 5s ticker and this fleet closes
// buckets continuously across hundreds of symbols x timeframes, so a healthy steady state
// keeps the last-commit age in single-digit seconds. Fifteen minutes is ~180 missed
// commits — far past any GC pause, WAL recovery, deploy restart or transient QuestDB
// hiccup, and far short of the thirteen hours the outage it guards against took. It is
// deliberately not tunable: a threshold an operator can raise is a threshold that gets
// raised the first time it fires.
const ColdWriteFreshnessDegradedAfter = 15 * time.Minute

// Connection is one small observation of a transport connection. It deliberately does not
// embed streaming.ConnectionStats: the aggregator depends on behavior, not on the
// ingestion package's DTO.
type Connection struct {
	Exchange   string
	MarketType string
	Connected  bool
	Healthy    bool
	Symbols    int
}

// ConnectionSummary is the report's per-connection view.
type ConnectionSummary struct {
	Exchange   string
	MarketType string
	Connected  bool
	Healthy    bool
	Symbols    int
}

// ComponentState is the per-component verdict. "absent" means the component is not
// mounted or not configured; it is never a passed check.
type ComponentState string

// Component states reported by /health; absent is never a passed check.
const (
	ComponentOK        ComponentState = "ok"
	ComponentDegraded  ComponentState = "degraded"
	ComponentUnhealthy ComponentState = "unhealthy"
	ComponentStarting  ComponentState = "starting"
	ComponentAbsent    ComponentState = "absent"
)

// Component names and top-level status strings published by /health. They are the wire
// contract consumed by probes and dashboards, so the aggregation spells them once.
const (
	componentConnections       = "connections"
	componentExpectedScopes    = "expected_scopes"
	componentFleetLiveness     = "fleet_liveness"
	componentFeedProducer      = "feed_producer"
	componentCoverage          = "coverage"
	componentColdWrite         = "cold_write"
	componentGCHeadroom        = "gc_headroom"
	componentDerivativesPoller = "derivatives_poller"
	componentTimeframeChecksum = "timeframe_checksum"

	statusHealthy   = "healthy"
	statusUnhealthy = "unhealthy"
	statusDegraded  = "degraded"

	reasonStartupGrace  = "startup grace"
	reasonNotConfigured = "not configured"
)

// ComponentReport names one capability and what the aggregation could conclude about it.
type ComponentReport struct {
	Name   string         `json:"name"`
	State  ComponentState `json:"state"`
	Reason string         `json:"reason,omitempty"`
}

// Feed producer values published by the feed_producer component. They answer the
// question "connected" does not: who wrote the newest candle the graph and /health can
// read for this scope?
const (
	FeedProducerWSKline   = "ws_kline"
	FeedProducerRESTTotal = "rest_total"
	FeedProducerNone      = "none"
)

// FeedProducerReport is one scope's producer observation. SilentSeconds is how long the
// venue feed has been quiet; it is meaningful when HaveFeed is true. A rest_total-only
// scope is degraded, never an outage: restarting the process does not repair the venue,
// and a readable candle is still being served.
type FeedProducerReport struct {
	Scope         string
	Producer      string
	SilentSeconds float64
	HaveFeed      bool
}

// CandleFreshnessReport is the wire detail published under candle_freshness. Stale counts
// symbols, not the age of the newest one: a single leading symbol must not mask a frozen
// fleet. FreshestAgoMs is operational detail, never the only signal.
type CandleFreshnessReport struct {
	Scope         string  `json:"scope"`
	Symbols       int     `json:"symbols"`
	Stale         int     `json:"stale"`
	StaleFraction float64 `json:"stale_fraction"`
	FreshestAgoMs int64   `json:"freshest_ago_ms"`
}

// GaugeSink is the output side of the aggregation: the two coverage gauges and the
// liveness gauge. Nil function fields are safe no-ops. A sink is not a check: its absence
// changes nothing about the reported status.
type GaugeSink struct {
	StreamLiveness    func(exchange, marketType string, live bool)
	SymbolsConfigured func(exchange, marketType string, configured int)
	SymbolsEffective  func(exchange, marketType string, effective int)
}

// Probes are the capability-scoped inputs. Nil means "not mounted / not configured"; the
// aggregation reports that component as absent and never counts it as healthy.
type Probes struct {
	// StartedAt reports the instant the process started. Nil uses the aggregator's own
	// construction time.
	StartedAt func() time.Time
	// Connections returns the current connection set.
	Connections func() []Connection
	// ExpectedScopes returns the canonical scopes that MUST be live after the grace.
	ExpectedScopes func() ([]string, bool)
	// LivenessFailing returns scopes whose per-symbol fleet liveness is below threshold.
	LivenessFailing func() ([]string, bool)
	// FeedProducers returns, per canonical scope, the producer of the newest readable
	// candle. A rest_total-only scope is degraded; only the fleet-liveness verdict, which
	// reads the same watermark, may turn it into 503.
	FeedProducers func() ([]FeedProducerReport, bool)
	// CandleFreshness returns the per-scope detail behind the fleet-liveness verdict:
	// newest readable age, monitored symbols and stale symbols.
	CandleFreshness func() ([]CandleFreshness, bool)
	// ConfiguredCoverage returns the configured symbol count per canonical scope.
	ConfiguredCoverage func() (map[string]int, bool)
	// ColdWrite reports the age of the last successful durable-tier commit.
	ColdWrite func() (age time.Duration, configured bool)
	// GCHeadroom reports mapped memory over GOMEMLIMIT.
	GCHeadroom func() (ratio float64, configured bool)
	// DerivativesPoller reports whether the derivatives poller goroutine is alive.
	DerivativesPoller func() (alive, configured bool)
	// TimeframeChecksum returns the current timeframe checksum.
	TimeframeChecksum func() (checksum string, configured bool)
	// GaugeSink returns the optional metrics sink.
	GaugeSink func() GaugeSink
}

// Report is the complete aggregation result. The HTTP layer maps it to JSON and writes
// HTTPStatus; it must not recompute either.
type Report struct {
	Status     string
	HTTPStatus int

	Uptime     string
	Goroutines int

	Connections       []ConnectionSummary
	DerivativesPoller bool
	TimeframeChecksum string
	MissingScopes     []string
	StaleScopes       []string
	CandleFreshness   []CandleFreshnessReport
	CoverageDeficits  []string

	ColdWriteAgeSeconds *int64
	ColdWriteStale      bool
	GCHeadroomRatio     *float64
	GCHeadroomTight     bool

	// Components enumerates the per-capability verdicts alongside the aggregate status.
	Components []ComponentReport
}

// Aggregator evaluates the probes. It holds no subsystem state of its own beyond the
// construction instant.
type Aggregator struct {
	probes    Probes
	startTime time.Time
}

// NewAggregator builds an aggregator over the supplied probes.
func NewAggregator(probes Probes) *Aggregator {
	return &Aggregator{probes: probes, startTime: time.Now()}
}

// Evaluate runs every probe and derives the status, the per-component states and the
// values the HTTP DTO exposes.
func (a *Aggregator) Evaluate() Report {
	now := time.Now()
	start := a.startTime
	if a.probes.StartedAt != nil {
		start = a.probes.StartedAt()
	}
	elapsed := now.Sub(start)

	components := make([]ComponentReport, 0, 8)

	// ── Connections ──────────────────────────────────────────────────────
	var conns []Connection
	if a.probes.Connections == nil {
		components = append(components, ComponentReport{Name: componentConnections, State: ComponentAbsent, Reason: reasonNotConfigured})
	} else {
		conns = a.probes.Connections()
	}

	allHealthy := true
	anyConnected := false
	summaries := make([]ConnectionSummary, 0, len(conns))
	effectiveByScope := make(map[string]int, len(conns))

	expectedScopes, expectedConfigured := []string(nil), false
	if a.probes.ExpectedScopes != nil {
		expectedScopes, expectedConfigured = a.probes.ExpectedScopes()
	}

	liveByScope := make(map[string]bool, len(conns)+len(expectedScopes))
	for _, scope := range expectedScopes {
		liveByScope[scope] = false
	}

	for _, cs := range conns {
		summaries = append(summaries, ConnectionSummary(cs))
		if cs.Connected {
			anyConnected = true
		}
		if !cs.Healthy {
			allHealthy = false
		}
		scope := CanonicalScope(cs.Exchange + ":" + cs.MarketType)
		if cs.Connected && cs.Healthy {
			liveByScope[scope] = true
		} else if _, ok := liveByScope[scope]; !ok {
			liveByScope[scope] = false
		}
		effectiveByScope[scope] += cs.Symbols
	}

	switch {
	case a.probes.Connections == nil:
		// Already reported absent; leave the component list as-is.
	case len(conns) == 0:
		components = append(components, ComponentReport{Name: componentConnections, State: ComponentStarting, Reason: "no connections yet"})
	case !anyConnected:
		components = append(components, ComponentReport{Name: componentConnections, State: ComponentUnhealthy, Reason: "no connected stream"})
	case !allHealthy:
		components = append(components, ComponentReport{Name: componentConnections, State: ComponentDegraded, Reason: "unhealthy connection"})
	default:
		components = append(components, ComponentReport{Name: componentConnections, State: ComponentOK})
	}

	// ── Liveness gauge sink ───────────────────────────────────────────────
	var sink GaugeSink
	if a.probes.GaugeSink != nil {
		sink = a.probes.GaugeSink()
	}
	for scope, live := range liveByScope {
		if sink.StreamLiveness != nil {
			ex, mt := splitScope(scope)
			sink.StreamLiveness(ex, mt, live)
		}
	}

	status := statusHealthy
	httpStatus := 200

	switch {
	case len(conns) == 0:
		status = "starting"
	case !anyConnected:
		status = statusUnhealthy
		httpStatus = 503
	case !allHealthy:
		status = statusDegraded
	}

	// ── Expected-scope enforcement ────────────────────────────────────────
	var missingScopes []string
	switch {
	case !expectedConfigured:
		components = append(components, ComponentReport{Name: componentExpectedScopes, State: ComponentAbsent, Reason: reasonNotConfigured})
	case len(expectedScopes) == 0:
		components = append(components, ComponentReport{Name: componentExpectedScopes, State: ComponentAbsent, Reason: "no expected scopes configured"})
	case elapsed <= StartupGrace:
		components = append(components, ComponentReport{Name: componentExpectedScopes, State: ComponentStarting, Reason: reasonStartupGrace})
	default:
		for _, scope := range expectedScopes {
			if !liveByScope[scope] {
				missingScopes = append(missingScopes, scope)
			}
		}
		if len(missingScopes) > 0 {
			sort.Strings(missingScopes)
			status = statusUnhealthy
			httpStatus = 503
			components = append(components, ComponentReport{Name: componentExpectedScopes, State: ComponentUnhealthy, Reason: fmt.Sprintf("%d scope(s) missing", len(missingScopes))})
		} else {
			components = append(components, ComponentReport{Name: componentExpectedScopes, State: ComponentOK})
		}
	}

	// ── Fleet liveness ────────────────────────────────────────────────────
	var staleScopes []string
	livenessConfigured := false
	if a.probes.LivenessFailing != nil {
		var failing []string
		failing, livenessConfigured = a.probes.LivenessFailing()
		if livenessConfigured && elapsed > StartupGrace && len(failing) > 0 {
			staleScopes = append(staleScopes, failing...)
			sort.Strings(staleScopes)
			status = statusUnhealthy
			httpStatus = 503
		}
	}
	switch {
	case !livenessConfigured:
		components = append(components, ComponentReport{Name: componentFleetLiveness, State: ComponentAbsent, Reason: reasonNotConfigured})
	case elapsed <= StartupGrace:
		components = append(components, ComponentReport{Name: componentFleetLiveness, State: ComponentStarting, Reason: reasonStartupGrace})
	case len(staleScopes) > 0:
		components = append(components, ComponentReport{Name: componentFleetLiveness, State: ComponentUnhealthy, Reason: fmt.Sprintf("%d stale scope(s)", len(staleScopes))})
	default:
		components = append(components, ComponentReport{Name: componentFleetLiveness, State: ComponentOK})
	}

	// ── Candle freshness detail ───────────────────────────────────────────
	var candleFreshness []CandleFreshnessReport
	if a.probes.CandleFreshness != nil {
		fresh, configured := a.probes.CandleFreshness()
		if configured {
			nowMs := now.UnixMilli()
			candleFreshness = make([]CandleFreshnessReport, 0, len(fresh))
			for _, f := range fresh {
				detail := CandleFreshnessReport{Scope: f.Scope, Symbols: f.Symbols, Stale: f.Stale}
				if f.Symbols > 0 {
					detail.StaleFraction = float64(f.Stale) / float64(f.Symbols)
				}
				if f.HaveData {
					ago := nowMs - f.FreshestAtMs
					if ago < 0 {
						ago = 0
					}
					detail.FreshestAgoMs = ago
				}
				candleFreshness = append(candleFreshness, detail)
			}
			sort.Slice(candleFreshness, func(i, j int) bool {
				return candleFreshness[i].Scope < candleFreshness[j].Scope
			})
		}
	}

	// ── Feed producer ─────────────────────────────────────────────────────
	// This component never turns the probe into 503: a live feed whose frames stopped is
	// degraded because a readable candle is still being served, and restarting would
	// destroy the rings without repairing the venue. Only an unreadable watermark past the
	// staleness threshold escalates, through the fleet-liveness verdict above.
	var feedReports []FeedProducerReport
	feedConfigured := false
	if a.probes.FeedProducers != nil {
		feedReports, feedConfigured = a.probes.FeedProducers()
	}
	feedState, feedReason := evaluateFeedProducers(feedReports, feedConfigured)
	components = append(components, ComponentReport{Name: componentFeedProducer, State: feedState, Reason: feedReason})
	if feedState == ComponentDegraded && status == statusHealthy {
		status = statusDegraded
	}

	// ── Coverage ──────────────────────────────────────────────────────────
	deficits := []string(nil)
	coverageConfigured := false
	var configuredCoverage map[string]int
	if a.probes.ConfiguredCoverage != nil {
		configuredCoverage, coverageConfigured = a.probes.ConfiguredCoverage()
	}
	for scope, effective := range effectiveByScope {
		ex, mt := splitScope(scope)
		if sink.SymbolsEffective != nil {
			sink.SymbolsEffective(ex, mt, effective)
		}
		configured, ok := configuredCoverage[scope]
		if !ok || configured <= 0 || effective >= configured {
			continue
		}
		deficits = append(deficits, fmt.Sprintf("%s: %d/%d", scope, effective, configured))
	}
	sort.Strings(deficits)
	if coverageConfigured {
		for scope, n := range configuredCoverage {
			ex, mt := splitScope(scope)
			if sink.SymbolsConfigured != nil {
				sink.SymbolsConfigured(ex, mt, n)
			}
		}
	}
	if len(deficits) > 0 && elapsed > StartupGrace {
		status = statusUnhealthy
		httpStatus = 503
	}
	switch {
	case !coverageConfigured:
		components = append(components, ComponentReport{Name: componentCoverage, State: ComponentAbsent, Reason: reasonNotConfigured})
	case elapsed <= StartupGrace:
		components = append(components, ComponentReport{Name: componentCoverage, State: ComponentStarting, Reason: reasonStartupGrace})
	case len(deficits) > 0:
		components = append(components, ComponentReport{Name: componentCoverage, State: ComponentUnhealthy, Reason: fmt.Sprintf("%d deficit(s)", len(deficits))})
	default:
		components = append(components, ComponentReport{Name: componentCoverage, State: ComponentOK})
	}

	// ── Cold-write freshness ───────────────────────────────────────────────
	var coldAge *int64
	coldStale := false
	if a.probes.ColdWrite == nil {
		components = append(components, ComponentReport{Name: componentColdWrite, State: ComponentAbsent, Reason: reasonNotConfigured})
	} else {
		age, configured := a.probes.ColdWrite()
		if !configured {
			components = append(components, ComponentReport{Name: componentColdWrite, State: ComponentAbsent, Reason: reasonNotConfigured})
		} else {
			secs := int64(age / time.Second)
			if secs < 0 {
				secs = 0
			}
			coldAge = &secs
			coldStale = age > ColdWriteFreshnessDegradedAfter
			switch {
			case elapsed <= StartupGrace:
				components = append(components, ComponentReport{Name: componentColdWrite, State: ComponentStarting, Reason: reasonStartupGrace})
			case coldStale:
				components = append(components, ComponentReport{Name: componentColdWrite, State: ComponentDegraded, Reason: "last commit is stale"})
			default:
				components = append(components, ComponentReport{Name: componentColdWrite, State: ComponentOK})
			}
		}
	}
	if coldStale && elapsed > StartupGrace && status == statusHealthy {
		status = statusDegraded
	}

	// ── GC headroom ───────────────────────────────────────────────────────
	var gcRatio *float64
	gcTight := false
	if a.probes.GCHeadroom == nil {
		components = append(components, ComponentReport{Name: componentGCHeadroom, State: ComponentAbsent, Reason: reasonNotConfigured})
	} else {
		ratio, configured := a.probes.GCHeadroom()
		if !configured {
			components = append(components, ComponentReport{Name: componentGCHeadroom, State: ComponentAbsent, Reason: reasonNotConfigured})
		} else {
			gcRatio = &ratio
			gcTight = ratio > GCHeadroomTightAbove
			if gcTight {
				components = append(components, ComponentReport{Name: componentGCHeadroom, State: ComponentDegraded, Reason: "tight headroom"})
			} else {
				components = append(components, ComponentReport{Name: componentGCHeadroom, State: ComponentOK})
			}
		}
	}
	if gcTight && status == statusHealthy {
		status = statusDegraded
	}

	// ── Derivatives poller ─────────────────────────────────────────────────
	derivPoller := false
	derivConfigured := false
	if a.probes.DerivativesPoller != nil {
		derivPoller, derivConfigured = a.probes.DerivativesPoller()
	}
	switch {
	case !derivConfigured:
		components = append(components, ComponentReport{Name: componentDerivativesPoller, State: ComponentAbsent, Reason: reasonNotConfigured})
	case !derivPoller:
		components = append(components, ComponentReport{Name: componentDerivativesPoller, State: ComponentDegraded, Reason: "poller is dead"})
	default:
		components = append(components, ComponentReport{Name: componentDerivativesPoller, State: ComponentOK})
	}
	if derivConfigured && !derivPoller && status == statusHealthy {
		status = statusDegraded
	}

	// ── Timeframe checksum ─────────────────────────────────────────────────
	checksum := ""
	if a.probes.TimeframeChecksum == nil {
		components = append(components, ComponentReport{Name: componentTimeframeChecksum, State: ComponentAbsent, Reason: reasonNotConfigured})
	} else {
		var configured bool
		checksum, configured = a.probes.TimeframeChecksum()
		if !configured {
			components = append(components, ComponentReport{Name: componentTimeframeChecksum, State: ComponentAbsent, Reason: reasonNotConfigured})
		} else {
			components = append(components, ComponentReport{Name: componentTimeframeChecksum, State: ComponentOK})
		}
	}

	return Report{
		Status:              status,
		HTTPStatus:          httpStatus,
		Uptime:              now.Sub(start).Round(time.Second).String(),
		Goroutines:          runtime.NumGoroutine(),
		Connections:         summaries,
		DerivativesPoller:   derivPoller,
		TimeframeChecksum:   checksum,
		MissingScopes:       missingScopes,
		StaleScopes:         staleScopes,
		CandleFreshness:     candleFreshness,
		CoverageDeficits:    deficits,
		ColdWriteAgeSeconds: coldAge,
		ColdWriteStale:      coldStale,
		GCHeadroomRatio:     gcRatio,
		GCHeadroomTight:     gcTight,
		Components:          components,
	}
}

// evaluateFeedProducers turns per-scope producer observations into one verdict. A scope
// fed only by REST, or with no readable candle at all, is degraded; it is deliberately
// never unhealthy, because the fleet-liveness check owns the 503 decision.
func evaluateFeedProducers(reports []FeedProducerReport, configured bool) (ComponentState, string) {
	if !configured {
		return ComponentAbsent, reasonNotConfigured
	}
	if len(reports) == 0 {
		return ComponentAbsent, "no scopes monitored"
	}

	degraded := false
	parts := make([]string, 0, len(reports))
	for _, r := range reports {
		producer := r.Producer
		if producer == "" {
			producer = FeedProducerNone
		}
		var part string
		switch producer {
		case FeedProducerWSKline:
			part = FeedProducerWSKline
		case FeedProducerRESTTotal:
			degraded = true
			part = fmt.Sprintf("%s only (ws_kline silent %s)", FeedProducerRESTTotal, feedSilence(r.SilentSeconds))
		default:
			degraded = true
			part = FeedProducerNone
			if r.HaveFeed {
				part = fmt.Sprintf("%s (ws_kline silent %s)", FeedProducerNone, feedSilence(r.SilentSeconds))
			}
		}
		if len(reports) > 1 {
			part = r.Scope + ": " + part
		}
		parts = append(parts, part)
	}
	if degraded {
		return ComponentDegraded, strings.Join(parts, "; ")
	}
	return ComponentOK, strings.Join(parts, "; ")
}

// feedSilence formats the feed's silence for the component reason.
func feedSilence(seconds float64) string {
	if seconds < 0 {
		seconds = 0
	}
	return fmt.Sprintf("%ds", int64(seconds))
}

// Ready reports whether at least one connection is connected. An absent connection probe
// is not ready: absence is not evidence of a connection.
func (a *Aggregator) Ready() bool {
	if a.probes.Connections == nil {
		return false
	}
	for _, cs := range a.probes.Connections() {
		if cs.Connected {
			return true
		}
	}
	return false
}

// CanonicalScope normalizes a raw "exchange:marketType" scope so spot/future comparison is
// exchange-agnostic. The market-type half is passed through market.FromConfig
// (swap/perpetual/future/delivery -> "future", spot -> "spot") so reported scopes (e.g.
// BingX reports "swap", Binance reports "future") and expected scopes collapse onto the
// same canonical key. A scope missing the ":marketType" half is returned unchanged.
func CanonicalScope(scope string) string {
	idx := -1
	for i := len(scope) - 1; i >= 0; i-- {
		if scope[i] == ':' {
			idx = i
			break
		}
	}
	if idx < 0 {
		return scope
	}
	marketType, err := market.FromConfig(scope[idx+1:])
	if err != nil {
		// Unknown vocabulary is left as-is instead of being coerced: it then
		// simply fails to match a canonical scope, which is the safe direction
		// for the expected-scope health check.
		return scope
	}
	return scope[:idx] + ":" + marketType.String()
}

// SplitScope splits a canonical "exchange:marketType" scope into its two halves on the
// final colon. A scope without a colon yields the whole string as the exchange and an
// empty market type.
func SplitScope(scope string) (exchange, marketType string) {
	return splitScope(scope)
}

func splitScope(scope string) (exchange, marketType string) {
	for i := len(scope) - 1; i >= 0; i-- {
		if scope[i] == ':' {
			return scope[:i], scope[i+1:]
		}
	}
	return scope, ""
}

// CoverageConfig holds the configured symbol count per canonical scope. It is probe state,
// not aggregation: the comparison lives in Evaluate.
type CoverageConfig struct {
	mu         sync.RWMutex
	configured map[string]int
	set        atomic.Bool
}

// Set replaces the configured coverage map, canonicalizing every scope.
func (c *CoverageConfig) Set(byScope map[string]int) {
	canon := make(map[string]int, len(byScope))
	for scope, n := range byScope {
		canon[CanonicalScope(scope)] = n
	}
	c.mu.Lock()
	c.configured = canon
	c.mu.Unlock()
	c.set.Store(true)
}

// Snapshot returns the configured coverage map and whether it was ever declared.
func (c *CoverageConfig) Snapshot() (map[string]int, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.set.Load() {
		return nil, false
	}
	out := make(map[string]int, len(c.configured))
	for k, v := range c.configured {
		out[k] = v
	}
	return out, true
}
