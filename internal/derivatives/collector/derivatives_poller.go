// Package collector polls derivatives feeds and records their provenance, coverage and
// label conventions.
package collector

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/market"
	"github.com/marxms/go-whale/internal/platform/config"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// DerivativesPoller polls funding rate, open interest, and long/short ratio
// for all configured exchanges/symbols and stores snapshots in Redis
// with analyzer-compatible key patterns.
type DerivativesPoller struct {
	cfg     config.DerivativesConfig
	store   pollerStore
	symbols []SymbolScope
	clients map[string]derivatives.Source // key: exchangeID
	tfReg   *candle.TimeframeRegistry

	// fundingProfiles carries the measured rung of the settlement-interval ladder between
	// polls: bootstrap measures the spacing and deposits it here. Injected, not built, because
	// it belongs to the process and this poller is rebuilt on restart. See FundingProfileRegistry.
	fundingProfiles *FundingProfileRegistry

	// The label convention is declared per endpoint in derivativesLabelDeclarations, not here:
	// a single field per poller would be a guess shared by OI, LSR and funding.

	// now is the cycle clock; tests inject a synthetic timeline, production leaves it nil.
	now func() time.Time

	// newPacer builds one cycle's admission source; tests inject it to measure pacing without
	// sleeping. Production leaves it nil and gets a time.Ticker.
	newPacer func(time.Duration) (tick <-chan time.Time, stop func())

	// sourceFactory builds ONE derivatives.Source per venue connection. It is
	// injected because this package must not import the outbound adapter; the
	// composition root supplies the venue implementation.
	sourceFactory derivatives.SourceFactory

	// discovery resolves the symbol universe for exchanges configured without
	// an explicit list. Injected for the same reason as sourceFactory.
	discovery market.DiscoverySource

	// metrics is the injected observability port. Zero value = safe no-op.
	metrics Metrics

	// catchUpChecked arms the restart catch-up once per process. The scheduler's first
	// cycle is already immediate, so a second catch-up would write another snapshot for
	// the same period.
	catchUpChecked bool

	log *slog.Logger
}

// SetMetrics injects the observability port. The composition root calls it before Start.
func (p *DerivativesPoller) SetMetrics(m Metrics) { p.metrics = m }

// pollerStore is the persistence surface the poll loop uses: the previous snapshot (for the
// open-interest delta window) and the two write paths, declared at the consumer.
type pollerStore interface {
	derivatives.Writer
	GetLatestSnapshot(ctx context.Context, exchange, marketType, symbol string) (*derivatives.DerivativesSnapshot, error)
}

// stalePruner is the optional capability that removes keys of symbols no longer active.
type stalePruner interface {
	UnlinkStaleDerivatives(ctx context.Context, exchange, marketType string, activeSymbols map[string]struct{}) int64
}

// NewDerivativesPoller creates a new derivatives poller. sourceFactory and discovery are the
// two outbound seams; a nil source is refused at Start rather than in a silent empty cycle.
func NewDerivativesPoller(
	ctx context.Context,
	cfg config.DerivativesConfig,
	store pollerStore,
	exchanges []config.ExchangeConfig,
	tfReg *candle.TimeframeRegistry,
	fundingProfiles *FundingProfileRegistry,
	sourceFactory derivatives.SourceFactory,
	discovery market.DiscoverySource,
	logs ...*slog.Logger,
) *DerivativesPoller {
	log := logger.OrDiscard(logs...)
	symbols := buildFutureSymbolScopes(ctx, exchanges, discovery, log)
	return &DerivativesPoller{
		cfg:             cfg,
		store:           store,
		symbols:         symbols,
		clients:         make(map[string]derivatives.Source),
		tfReg:           tfReg,
		fundingProfiles: fundingProfiles,
		sourceFactory:   sourceFactory,
		discovery:       discovery,
		log:             log,
	}
}

// feedNameOf names the feed for the refusal log. The venue is what identifies a feed
// here: two venues are two label conventions to declare, and "derivatives" alone would
// send a maintainer looking through all of them.
func feedNameOf(symbols []SymbolScope) string {
	if len(symbols) == 0 {
		return "derivatives"
	}
	return symbols[0].ExchangeID
}

// ─── LABEL CONVENTION, DECLARED PER ENDPOINT ─────────────────────────────────
//
// An additive feed and a point-sample feed cannot share one declared convention: for the
// additive one the two conventions predict different numbers, and for the point sample they are
// indistinguishable. The table below is the registry declaration; admitFeed reads it before any
// write.

// venueBinance is the exchange id the configuration uses and the (venue, feed) key repeats. The
// endpoints below are Binance's; a second venue declares its own rather than inheriting.
const venueBinance = "binance"

// feedNature says what kind of quantity the endpoint publishes; the consequence of the
// convention depends on it.
type feedNature uint8

const (
	// feedNatureUnspecified is the zero value and is invalid: a declaration that does not know
	// the feed's nature cannot be checked against the additive rule.
	feedNatureUnspecified feedNature = iota
	// feedNaturePointSample means one observation tied to the label instant. Measured for
	// OI/LSR: the 1d bar tracks the average of the same day and equals the 5m bucket of the
	// same timestamp. The bucket content is the same under both conventions; the convention
	// moves only the name.
	feedNaturePointSample
	// feedNatureAdditive means the field is summed over the interval (buyVol/sellVol); the two
	// conventions predict different numbers, so the declaration must carry a falsifier.
	// Measured: 53,199.80 against 53,140.53, deviation 0.111%.
	feedNatureAdditive
)

// String names the nature for a log line, like LabelConvention does for the convention.
func (n feedNature) String() string {
	switch n {
	case feedNaturePointSample:
		return "point-sample"
	case feedNatureAdditive:
		return "additive"
	default:
		return "unspecified"
	}
}

// labelDeclaration is the declaration of ONE ingested endpoint.
type labelDeclaration struct {
	// venue is the configuration's exchange id; feed is the identity the availability matrix
	// publishes and the key by which coverage is enumerated.
	venue string
	feed  derivativesFeed
	// endpoint is the path read, which is what a maintainer needs to reach the primary doc.
	endpoint string
	nature   feedNature
	label    derivatives.LabelConvention
	// evidence names the measurement backing this declaration and what it does not measure.
	evidence string
	// falsifier names the empirical test that separates start from end. Empty is allowed only
	// for a point sample; an additive feed with no falsifier is refused.
	falsifier string
}

// derivativesLabelDeclarations is the registry declaration, one row per endpoint this collector
// ingests. All current rows are point samples, so the additive rule has no instance yet; it is
// kept because that is where the two conventions diverge, which is what a future venue brings.
//
// The table governs admission. It does not govern alignment of the Binance/Redis layer, where
// upsertSeries and upsertSeriesBatch pin LabelIntervalStart; it does govern durable-tier
// alignment, where batch.Label drives coldPointOf. The residual is declared, not hidden.
func derivativesLabelDeclarations() []labelDeclaration {
	return []labelDeclaration{
		{
			venue:    venueBinance,
			feed:     feedFundingRate,
			endpoint: "/fapi/v1/fundingRate",
			nature:   feedNaturePointSample,
			label:    derivatives.LabelIntervalStart,
			evidence: "settlement is an EVENT: the row carries the instant at which the rate was settled and the value belongs to it, so the floor places the same observation in the bucket under the two nominal conventions — the same structural argument measured for point samples, applied to an event. NOT measured for this endpoint, and there is no way to measure it by sum: the rate is not an aggregate field over the interval. The interval to which the rate applies is another contract property and is not what this row declares.",
		},
		{
			venue:    venueBinance,
			feed:     feedOpenInterest,
			endpoint: "/futures/data/openInterestHist",
			nature:   feedNaturePointSample,
			label:    derivatives.LabelIntervalStart,
			evidence: "point sample MEASURED: the `1d` bar tracks the average of the SAME day (ratio 0.9927) and the sum is discarded (0.0034), and the bar carries the value at the label instant. The primary doc says 'End time of the period' for this endpoint, and the 2026-08-23 measurement that supported the start was RETRACTED by the heatmap feature validation: it does not discriminate between the two readings. The bucket content is the same under both, what changes is the NAME, and moving the series is not reversible — declared start, with the residual named.",
		},
		{
			venue:    venueBinance,
			feed:     feedLongShortRatio,
			endpoint: "/futures/data/globalLongShortAccountRatio",
			nature:   feedNaturePointSample,
			label:    derivatives.LabelIntervalStart,
			evidence: "point sample MEASURED: the `1d` bar is EXACTLY the `5m` bucket of the same timestamp (1.0859 = 1.0859), not the average of the sub-buckets — the series is one reading per boundary, not a period aggregation. The same 2026-08-23 retraction of the OI row applies here, and the two average reasons of the previous day and the same day are indistinguishable (1.0324 vs 1.0321): this endpoint is precisely what the snapshot test does NOT discriminate.",
		},
	}
}

// declarationsFor returns the feed's declarations, in table order.
func declarationsFor(feed string) []labelDeclaration {
	var out []labelDeclaration
	for _, d := range derivativesLabelDeclarations() {
		if d.venue == feed {
			out = append(out, d)
		}
	}
	return out
}

// admitDeclaration refuses a declaration that does not hold up, naming what was missing.
func admitDeclaration(d labelDeclaration) error {
	switch {
	case !d.label.Valid():
		return fmt.Errorf("label convention %s", d.label)
	case d.nature == feedNatureUnspecified:
		return fmt.Errorf("nature unspecified; a declaration that does not know the feed's nature cannot be checked against the additive rule")
	case d.evidence == "":
		return fmt.Errorf("no evidence named for the declared convention")
	case d.nature == feedNatureAdditive && d.falsifier == "":
		return fmt.Errorf("additive feed with no falsifier: the two conventions predict different sums here, so the declaration has to be separated from the alternative by measurement")
	}
	return nil
}

// admitFeed refuses a feed whose endpoints have not declared their label convention, before any
// write. The refusal lives here because the feed has a name to put in the log; a silent fallback
// inside alignment would apply the guess where nobody looks, after the ZSET was contaminated.
func (p *DerivativesPoller) admitFeed(feed string) error {
	return admitDeclarations(feed, declarationsFor(feed), p.log)
}

// admitDeclarations applies the admission rules to a declaration list; the refusal that matters
// is the one that names the endpoint.
func admitDeclarations(feed string, declared []labelDeclaration, logs ...*slog.Logger) error {
	log := logger.OrDiscard(logs...)
	if len(declared) == 0 {
		unspecified := derivatives.LabelUnspecified
		log.Error("derivatives: feed refused; no endpoint declares a label convention",
			"feed", feed, "label_convention", unspecified)
		return fmt.Errorf("derivatives: feed %q has label convention %s — no endpoint of it declares a convention; declare one per ingested endpoint", feed, unspecified)
	}
	for _, d := range declared {
		if err := admitDeclaration(d); err != nil {
			log.Error("derivatives: feed endpoint refused",
				"feed", feed, "endpoint", d.endpoint, "error", err, "nature", d.nature)
			return fmt.Errorf("derivatives: feed %q endpoint %q rejected: %w", feed, d.endpoint, err)
		}
	}
	return nil
}

// Interval is the cadence this poller is scheduled on. The scheduler owns the ticker;
// the collector owns what the configured cadence IS.
func (p *DerivativesPoller) Interval() time.Duration {
	return time.Duration(p.cfg.PollIntervalSec) * time.Second
}

// Prepare initializes the REST clients, one per venue. A false return with a nil error is a
// declared refusal to run (disabled, or no future symbols) and must not be retried; an error is a
// failed attempt, and the scheduler owns the retry clock.
func (p *DerivativesPoller) Prepare(ctx context.Context) (bool, error) {
	if !p.cfg.Enabled {
		logger.OrDiscard(p.log).Info("derivatives poller disabled by config")
		return false, nil
	}

	if len(p.symbols) == 0 {
		logger.OrDiscard(p.log).Warn("derivatives poller: no future symbols configured; skipping")
		return false, nil
	}

	if err := p.initClients(ctx); err != nil {
		return true, err
	}
	return true, nil
}

// Bootstrap publishes the feed-availability matrix and seeds historical data. It runs
// once, after the clients are ready and before the first live cycle.
func (p *DerivativesPoller) Bootstrap(ctx context.Context) {
	interval := p.Interval()
	admission, _ := admissionInterval(interval, len(p.symbols))
	logger.OrDiscard(p.log).Info("derivatives poller started",
		"symbols", len(p.symbols), "interval", interval, "admission", admission)

	// The (feed x timeframe) matrix is a startup fact: a registry includes timeframes a given
	// feed does not serve, and publishing the gap avoids inferring it from absent data.
	publishFeedAvailability(p.metrics, p.tfReg.All(), p.log)

	// Historical bootstrap runs after clients are ready, before the poll loop starts.
	p.BootstrapHistory(ctx)
}

// PollOnce runs ONE cycle and reports it. The scheduler calls it synchronously; a tick
// that arrives while it runs is coalesced by the ticker, never executed concurrently.
// The first call after a restart IS the catch-up cycle when the last snapshot is older
// than the poll period; otherwise it is the ordinary immediate first cycle.
func (p *DerivativesPoller) PollOnce(ctx context.Context) {
	if p.catchUp(ctx) {
		return
	}
	p.safePollAll(ctx)
}

// catchUp runs ONE immediate cycle when the newest persisted snapshot is older than the
// poll period, and records the period a restart left unwritten. It is armed once per
// process: the scheduler's next tick is the regular cadence, so a second catch-up would
// write a second snapshot for the same period.
//
// The snapshot the catch-up writes carries the CURRENT observed instant. The missed
// interval is recorded (gap_from_ms, gap_to_ms), never backfilled with an invented value:
// rewriting the past would need a second source, which is out of scope.
func (p *DerivativesPoller) catchUp(ctx context.Context) bool {
	if p.catchUpChecked {
		return false
	}
	p.catchUpChecked = true

	interval := p.Interval()
	if interval <= 0 || len(p.symbols) == 0 {
		return false
	}

	now := p.clock()
	var newest int64
	var found bool
	var unreadable int
	for _, scope := range p.symbols {
		previous, err := p.store.GetLatestSnapshot(ctx, scope.ExchangeID, scope.MarketType, scope.Symbol)
		if err != nil {
			unreadable++
			continue
		}
		if previous == nil {
			continue
		}
		if !found || previous.Timestamp > newest {
			newest = previous.Timestamp
			found = true
		}
	}

	if !found {
		// DRV-03: a period that cannot be found is RECORDED, not silenced.
		logger.OrDiscard(p.log).Warn("derivatives catch-up: no readable snapshot to compare; the missed period is unknown",
			"scopes", len(p.symbols), "unreadable", unreadable)
		return false
	}
	if unreadable > 0 {
		logger.OrDiscard(p.log).Warn("derivatives catch-up: part of the fleet has no readable snapshot; their missed period is unknown",
			"scopes", len(p.symbols), "unreadable", unreadable)
	}
	if now.Sub(time.UnixMilli(newest)) <= interval {
		return false
	}

	logger.OrDiscard(p.log).Warn("derivatives catch-up: missed period recorded; the first cycle writes the current snapshot",
		"gap_from_ms", newest,
		"gap_to_ms", now.UnixMilli(),
		"missed_ms", now.UnixMilli()-newest,
		"interval_ms", interval.Milliseconds())

	p.safePollAll(ctx)
	return true
}

// Close releases the REST clients. The scheduler calls it on every exit path.
func (p *DerivativesPoller) Close() {
	p.closeClients()
}

// safePollAll runs one cycle and reports it, recovering vendor panics. The outcome is a value
// the caller decides what to do with, so a test can assert on a cycle without reading logs.
func (p *DerivativesPoller) safePollAll(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			logger.OrDiscard(p.log).Error("panic recovered in derivatives pollAll", "panic", r)
		}
	}()
	reportCycle(p.pollAll(ctx), p.log)
}

// PruneStaleSymbols removes derivatives data for symbols NOT in the activeSymbols
// set using Redis UNLINK (non-blocking) instead of DEL.
func (p *DerivativesPoller) PruneStaleSymbols(ctx context.Context, exchange, marketType string, activeSymbols map[string]struct{}) int64 {
	if store, ok := p.store.(stalePruner); ok {
		return store.UnlinkStaleDerivatives(ctx, exchange, marketType, activeSymbols)
	}
	return 0
}

func (p *DerivativesPoller) initClients(ctx context.Context) error {
	if p.sourceFactory == nil {
		return fmt.Errorf("derivatives: no market-data source factory configured")
	}
	seen := make(map[string]bool)
	for _, s := range p.symbols {
		if seen[s.ExchangeID] {
			continue
		}
		seen[s.ExchangeID] = true

		client, err := p.sourceFactory(s.ExchangeID, market.Future)
		if err != nil {
			return fmt.Errorf("create REST client for %s: %w", s.ExchangeID, err)
		}

		if _, err := client.LoadMarkets(ctx); err != nil {
			_ = client.Close()
			return fmt.Errorf("load markets for %s: %w", s.ExchangeID, err)
		}

		p.clients[s.ExchangeID] = client
		logger.OrDiscard(p.log).Info("derivatives poller: REST client ready", "exchange", s.ExchangeID)
	}
	return nil
}

func (p *DerivativesPoller) closeClients() {
	for id, client := range p.clients {
		_ = client.Close()
		logger.OrDiscard(p.log).Debug("derivatives poller: closed REST client", "exchange", id)
	}
}

// minAdmissionInterval is the fastest rate at which symbols may be released into a cycle,
// regardless of configured cadence.
//
// The /futures/data family is metered by a separate, invisible pool: 1000 requests per rolling
// five minutes, weight 0, with no x-mbx header. 1000/300s is one every 300ms at the ceiling;
// this floor sits a third slower (750 per window) because bootstrap pages the same family
// unpaced. The penalty escalates from a two-minute ban to a three-day one. It is a last-resort
// clamp, not the operating point: at ~500 symbols on 300s the derived pace is ~570ms.
const minAdmissionInterval = 400 * time.Millisecond

// cycleDrainFraction is the share of the period held back from admission so work in flight when
// the last symbol enters has somewhere to finish. Pacing at exactly period/N overruns whenever
// one symbol latency exceeds period/N. One twentieth (15s of a 300s period) is an order of
// magnitude above the measured ~1.1s per symbol.
const cycleDrainFraction = 20

// pollAdmissionConcurrency caps symbols in flight. Pacing decides the request rate; this only
// has to be large enough never to decide it (16 x 570ms absorbs 9.1s of per-symbol latency).
const pollAdmissionConcurrency = 16

// admissionInterval is the spacing between symbols entering a cycle: the period left after the
// drain reserve, divided by the fleet, floored at what the venue will serve. The second return
// reports that the floor, not the configured interval, decided the pace.
func admissionInterval(cycle time.Duration, symbols int) (time.Duration, bool) {
	if symbols <= 0 {
		return minAdmissionInterval, false
	}
	admissionWindow := cycle - cycle/cycleDrainFraction
	paced := admissionWindow / time.Duration(symbols)
	if paced < minAdmissionInterval {
		return minAdmissionInterval, true
	}
	return paced, false
}

// cycleStats is what one poll cycle did. The three counts are a partition: `ok` are symbols
// written, `errors` those the venue answered and failed, and `omitted` those with no write.
type cycleStats struct {
	ok      int
	errors  int
	symbols int
	// omitted names the served symbols the cycle did not write; a counter alone cannot say
	// which symbol to look for.
	omitted []string
	// omittedN is the partition size; `omitted` is truncated at omittedNameCap.
	omittedN int
	elapsed  time.Duration
	// interval is the cadence the cycle is scheduled on.
	interval time.Duration
	// admission is the pace symbols entered at.
	admission time.Duration
	// paceFloored says the venue's request budget, not the configured interval, decided the pace.
	paceFloored bool
	// refused says the cycle never started because the feed has no declared label convention;
	// nothing was attempted, so it is not an error count.
	refused bool
	// refusal carries the cause of the refusal so it does not read as "0 ok" with no reason.
	refusal error
	// fleetErr says the partition could not be checked against the served fleet; absence of a
	// read is not absence of a symbol.
	fleetErr error
}

// omittedNameCap limits how many names the report carries; the total stays in omittedN. It is
// NOT RETENTION: it sizes nothing stored, only how many names fit in a log line.
const omittedNameCap = 20

// overran reports that the cycle outlasted its period. That is when the loop's time.Ticker
// starts dropping the ticks that fall inside it, so the real spacing becomes the cycle's
// duration instead of the configured interval.
func (s cycleStats) overran() bool { return s.interval > 0 && s.elapsed > s.interval }

// lapFactor is how many configured periods the cycle actually consumed.
func (s cycleStats) lapFactor() float64 {
	if s.interval <= 0 {
		return 0
	}
	return float64(s.elapsed) / float64(s.interval)
}

// reportCycle puts a finished cycle where an operator can see it.
func reportCycle(s cycleStats, logs ...*slog.Logger) {
	log := logger.OrDiscard(logs...)
	// Refusal comes first, with its reason: "0 ok" without cause is indistinguishable from an
	// ordinary cycle.
	if s.refused {
		log.Warn("derivatives poll refused at admission; no symbol was attempted and the served fleet has no write",
			"reason", s.refusal)
	}

	if s.paceFloored {
		log.Warn("derivatives poll pace floored by the venue budget; the cycle cannot hold the configured cadence",
			"symbols", s.symbols,
			"interval", s.interval,
			"admission_floor", minAdmissionInterval)
	}

	switch {
	case s.overran():
		log.Warn("derivatives poll overran the configured cadence",
			"counts", s.counts(),
			"elapsed", s.elapsed.Round(time.Millisecond),
			"interval", s.interval,
			"lap_factor", s.lapFactor())
	case s.errors > 0 || s.omittedN > 0:
		log.Warn("derivatives poll cycle completed with errors or omissions",
			"counts", s.counts(),
			"elapsed", s.elapsed.Round(time.Millisecond),
			"interval", s.interval,
			"admission", s.admission)
	default:
		log.Info("derivatives poll cycle completed",
			"counts", s.counts(),
			"elapsed", s.elapsed.Round(time.Millisecond),
			"interval", s.interval,
			"admission", s.admission)
	}

	// The names, not just the number: a counter hides which symbol to look for.
	if names := s.namesOmitted(); names != "" {
		log.Warn("derivatives poll: served-fleet symbols had no write in this cycle",
			"omitted", s.omittedN, "names", names)
	}
	// An unanswered read sustains no absence: "none omitted" must not mean "I did not look".
	if s.fleetErr != nil {
		log.Warn("derivatives poll: served fleet unreadable; omitted-symbol count is unknown",
			"error", s.fleetErr)
	}
}

// counts is the partition in the words an operator reads.
func (s cycleStats) counts() string {
	return fmt.Sprintf("%d ok (written), %d errors, %d without a write", s.ok, s.errors, s.omittedN)
}

// namesOmitted renders the symbols the cycle did not write, capped. It returns "" when there
// are none, so a healthy cycle prints no extra line.
func (s cycleStats) namesOmitted() string {
	if s.omittedN == 0 {
		return ""
	}
	names := s.omitted
	if len(names) > omittedNameCap {
		names = names[:omittedNameCap]
	}
	shown := strings.Join(names, ", ")
	if rest := s.omittedN - len(names); rest > 0 {
		shown += fmt.Sprintf(" (+%d not listed)", rest)
	}
	return shown
}

// clock reads the cycle's time source.
func (p *DerivativesPoller) clock() time.Time {
	if p.now != nil {
		return p.now()
	}
	return time.Now()
}

// pacer builds the admission source for one cycle.
func (p *DerivativesPoller) pacer(d time.Duration) (<-chan time.Time, func()) {
	if p.newPacer != nil {
		return p.newPacer(d)
	}
	ticker := time.NewTicker(d)
	return ticker.C, ticker.Stop
}

// pollAll runs one cycle over the whole fleet and reports what it did. A serial loop does not
// fit 300s at ~500 symbols, and a cycle that outlasts its period makes the ticker drop ticks.
// The binding limit is the long/short pool (1000 requests per rolling five minutes), so symbols
// are admitted at a fixed pace and a bounded pool overlaps their round trips behind it.
//
// fleetFor returns the scopes this cycle should poll. The list is resolved ONCE at construction,
// while the served fleet is reordered periodically, so the two diverge by one step per discovery
// cycle. It reads the same active_symbols key the read serves, and a missing or empty registry
// falls back to the declared list with a warning rather than silently polling the wrong fleet.
func (p *DerivativesPoller) fleetFor(ctx context.Context) []SymbolScope {
	reader, ok := p.store.(servedFleetReader)
	if !ok {
		return p.symbols
	}

	type feedIdentity struct{ ExchangeID, MarketType string }
	seen := make(map[feedIdentity]struct{}, 2)
	var resolved []SymbolScope

	for _, scope := range p.symbols {
		feed := feedIdentity{scope.ExchangeID, scope.MarketType}
		if _, done := seen[feed]; done {
			continue
		}
		seen[feed] = struct{}{}

		members, err := reader.ServedSymbols(ctx, scope.ExchangeID, scope.MarketType)
		if err != nil {
			logger.OrDiscard(p.log).Warn("derivatives: served fleet unreadable; using the declared list",
				"exchange", scope.ExchangeID,
				"market_type", scope.MarketType,
				"error", err,
				"declared_scopes", len(p.symbols))
			return p.symbols
		}
		for symbol := range members {
			resolved = append(resolved, SymbolScope{
				ExchangeID: scope.ExchangeID,
				MarketType: scope.MarketType,
				Symbol:     symbol,
			})
		}
	}

	if len(resolved) == 0 {
		logger.OrDiscard(p.log).Warn("derivatives: served fleet empty; using the declared list",
			"declared_scopes", len(p.symbols))
		return p.symbols
	}

	// Stable order: SMEMBERS promises none, and a random admission order makes two identical
	// cycles non-reproducible and therefore incomparable.
	sort.Slice(resolved, func(i, j int) bool { return resolved[i].Key() < resolved[j].Key() })
	return resolved
}

func (p *DerivativesPoller) pollAll(ctx context.Context) cycleStats {
	start := p.clock()
	interval := time.Duration(p.cfg.PollIntervalSec) * time.Second
	fleet := p.fleetFor(ctx)
	admission, floored := admissionInterval(interval, len(fleet))

	stats := cycleStats{
		symbols:     len(fleet),
		interval:    interval,
		admission:   admission,
		paceFloored: floored,
	}
	if len(fleet) == 0 {
		return stats
	}

	// The admission door closes before the first fetch, not after the first save: a cycle that
	// discovered the missing convention at write time would already hold a point it should not use.
	if err := p.admitFeed(feedNameOf(p.symbols)); err != nil {
		stats.refused = true
		stats.refusal = err
		// The refusal applies to the whole fleet: nothing was attempted, so the whole served
		// fleet is unwritten. The partition is still computed below.
		stats.symbols = len(fleet)
		stats.elapsed = p.clock().Sub(start)
		stats.omitted, stats.omittedN, stats.fleetErr = p.omissionsAgainstServedFleet(ctx, nil)
		return stats
	}

	tick, stopPacer := p.pacer(admission)
	defer stopPacer()

	sem := make(chan struct{}, pollAdmissionConcurrency)
	var wg sync.WaitGroup
	var okCount, errCount atomic.Int64
	// written is the set the cycle effectively wrote, by scope; the served fleet is checked
	// against it rather than against p.symbols.
	var writtenMu sync.Mutex
	written := make(map[SymbolScope]struct{}, len(fleet))

	for i, scope := range fleet {
		if i > 0 && !p.awaitAdmission(ctx, tick) {
			break
		}

		// The client map is written once by initClients before this loop starts, so reading it
		// here is safe.
		client, ok := p.clients[scope.ExchangeID]
		if !ok {
			logger.OrDiscard(p.log).Warn("derivatives: no client for exchange", "exchange", scope.ExchangeID)
			errCount.Add(1)
			continue
		}

		wg.Add(1)
		// Acquiring after the tick means a saturated pool delays admission rather than the pace;
		// the pool can slow a cycle down, never speed it past the budget.
		sem <- struct{}{}

		go func(s SymbolScope, c derivatives.Source) {
			defer wg.Done()
			defer func() { <-sem }()
			defer func() {
				if r := recover(); r != nil {
					logger.OrDiscard(p.log).Error("derivatives poll panic", "symbol", s.Symbol, "panic", r)
					errCount.Add(1)
				}
			}()

			escrito, err := p.pollSymbol(ctx, c, s)
			if err != nil {
				errCount.Add(1)
				return
			}
			if !escrito {
				// Not ok and not an error: the venue answered, but there was no write. It
				// enters the partition as absence.
				return
			}
			writtenMu.Lock()
			written[s] = struct{}{}
			writtenMu.Unlock()
			okCount.Add(1)
		}(scope, client)
	}

	wg.Wait()

	stats.ok = int(okCount.Load())
	stats.errors = int(errCount.Load())
	stats.omitted, stats.omittedN, stats.fleetErr = p.omissionsAgainstServedFleet(ctx, written)
	stats.elapsed = p.clock().Sub(start)
	return stats
}

// servedFleetReader is what the cycle counter needs from the store to check the partition; it is
// declared by its consumer, not in a shared storage interface.
type servedFleetReader interface {
	ServedSymbols(ctx context.Context, exchange, marketType string) (map[string]struct{}, error)
}

// omissionsAgainstServedFleet compares what the cycle wrote with the fleet the read serves and
// returns the unwritten symbols by name. Checking against p.symbols would compare the collector
// list with itself, since the two diverge as discovery reorders the served fleet. A nil written
// set (feed refusal) makes the whole served fleet absent, with the cycle's own reason.
func (p *DerivativesPoller) omissionsAgainstServedFleet(ctx context.Context, written map[SymbolScope]struct{}) ([]string, int, error) {
	reader, ok := p.store.(servedFleetReader)
	if !ok {
		// Without a served-fleet reader the partition cannot be checked, and saying so is the
		// only honest outcome.
		return nil, 0, fmt.Errorf("%T does not expose the served fleet", p.store)
	}

	type scopeKey struct{ ExchangeID, MarketType string }
	served := make(map[scopeKey]map[string]struct{})

	// Scopes come from the collector's own symbols: sweeping a scope it does not carry would
	// invent absence over someone else's scope.
	for _, s := range p.symbols {
		k := scopeKey{s.ExchangeID, s.MarketType}
		if _, done := served[k]; done {
			continue
		}
		members, err := reader.ServedSymbols(ctx, k.ExchangeID, k.MarketType)
		if err != nil {
			served[k] = nil
			continue
		}
		// An empty or unreadable served fleet sustains no absence; treating it as real would
		// mark the whole fleet unwritten.
		if len(members) == 0 {
			served[k] = nil
			continue
		}
		served[k] = members
	}

	var omitted []string
	var fleetErr error

	for k, members := range served {
		if members == nil {
			fleetErr = fmt.Errorf("served fleet of %s/%s unavailable in this cycle", k.ExchangeID, k.MarketType)
			continue
		}
		for sym := range members {
			scope := SymbolScope{ExchangeID: k.ExchangeID, MarketType: k.MarketType, Symbol: sym}
			if _, escreveu := written[scope]; escreveu {
				continue
			}
			omitted = append(omitted, k.ExchangeID+"/"+k.MarketType+"/"+sym)
		}
	}

	sort.Strings(omitted)
	return omitted, len(omitted), fleetErr
}

// awaitAdmission blocks until the pace releases the next symbol and reports false when the
// cycle was told to stop. A cycle spans the whole poll interval, so shutdown has to be visible
// from inside it.
func (p *DerivativesPoller) awaitAdmission(ctx context.Context, tick <-chan time.Time) bool {
	select {
	case <-ctx.Done():
		return false
	case <-tick:
		return true
	}
}

// pollSymbol fetches and writes ONE symbol and returns escrito = true only when the write
// actually happened. SaveSnapshot returns nil for a snapshot that reached no timeframe, so the
// error return alone counts attempts: that is how the cycle reported "101 ok" over symbols with
// no series. A missing write leaves as escrito = false with no error, because the venue answered.
//
// Symbols are independent at the store: every key is derived from (exchange, marketType,
// symbol). The shared funding-interval cache is guarded by its own RWMutex.
func (p *DerivativesPoller) pollSymbol(ctx context.Context, client derivatives.Source, scope SymbolScope) (escrito bool, err error) {
	snapshot, err := p.fetchDerivatives(ctx, client, scope)
	if err != nil {
		logger.OrDiscard(p.log).Warn("derivatives: fetch failed",
			"exchange", scope.ExchangeID, "symbol", scope.Symbol, "error", err)
		return false, err
	}

	// The change in open interest is measured over CONTRACTS. Notional is contracts x price, so
	// its variation is dominated by the revaluation term (correlates +0.982 with the candle
	// return, carries 51-76% of the variance). A panel ordered by notional is ordered by price.
	previous, prevErr := p.store.GetLatestSnapshot(ctx, scope.ExchangeID, scope.MarketType, scope.Symbol)
	if prevErr != nil {
		logger.OrDiscard(p.log).Debug("derivatives: previous snapshot lookup failed", "symbol", scope.Symbol, "error", prevErr)
	} else if previous != nil && previous.OpenInterestBase != nil && snapshot.OpenInterestBase != nil {
		prevOI := *previous.OpenInterestBase
		currOI := *snapshot.OpenInterestBase
		if prevOI > 0 {
			oiChangePct := ((currOI - prevOI) / prevOI) * 100
			snapshot.OIChangePct = &oiChangePct

			// The window travels with the number that needs it: measured between the two
			// open-interest readings, not between the two polls. A non-positive span is absent
			// rather than published, and a previous record without an observation stamp leaves
			// the window absent rather than falling back to timestamps.
			if previous.OpenInterestObservedAtMs != nil && snapshot.OpenInterestObservedAtMs != nil {
				if window := *snapshot.OpenInterestObservedAtMs - *previous.OpenInterestObservedAtMs; window > 0 {
					snapshot.OIChangePctWindowMs = &window
				}
			}
		}
	}

	notWritten, err := p.saveSnapshot(ctx, snapshot)
	if err != nil {
		logger.OrDiscard(p.log).Warn("derivatives: save failed",
			"exchange", scope.ExchangeID, "symbol", scope.Symbol, "error", err)
		return false, err
	}
	if len(notWritten) > 0 {
		// The write was missing, the venue was not: the absence enters the partition by name,
		// not through the error counter.
		logger.OrDiscard(p.log).Warn("derivatives: snapshot did not reach every destination",
			"exchange", scope.ExchangeID,
			"symbol", scope.Symbol,
			"missing_destinations", len(notWritten),
			"destinations", notWritten)
		return false, nil
	}

	return true, nil
}

// saveSnapshot stores the snapshot and returns the destinations that did not receive it. The
// SaveSnapshot contract does not move: a destination's refusal skips that write, while failing
// the poll would kill its siblings and repeat the whole cycle. The reporting interface is local
// to this consumer.
func (p *DerivativesPoller) saveSnapshot(ctx context.Context, snapshot *derivatives.DerivativesSnapshot) ([]string, error) {
	if reporter, ok := p.store.(cycleWriteReporter); ok {
		return reporter.SaveSnapshotForCycle(ctx, snapshot)
	}
	// Without a reporter the write is taken as done; the served-fleet check still catches the
	// symbol the cycle does not carry.
	return nil, p.store.SaveSnapshot(ctx, snapshot)
}

// cycleWriteReporter is what the cycle counter needs from the store: the destinations that did
// NOT receive the snapshot. An empty list means the write happened in full.
type cycleWriteReporter interface {
	SaveSnapshotForCycle(ctx context.Context, snapshot *derivatives.DerivativesSnapshot) ([]string, error)
}

func (p *DerivativesPoller) fetchDerivatives(ctx context.Context, client derivatives.Source, scope SymbolScope) (*derivatives.DerivativesSnapshot, error) {
	now := time.Now().UnixMilli()

	// The scope's market type is validated before any fetch; the symbol is canonical and the
	// adapter owns the venue translation.
	if _, err := market.FromConfig(scope.MarketType); err != nil {
		return nil, fmt.Errorf("derivatives: %s:%s: %w", scope.ExchangeID, scope.Symbol, err)
	}

	snapshot := &derivatives.DerivativesSnapshot{
		Exchange:   scope.ExchangeID,
		MarketType: scope.MarketType,
		Symbol:     scope.Symbol,
		Timestamp:  now,
		ObservedAt: now,
	}

	// --- Funding Rate ---
	// Also yields MarkPrice (display-only notional), InterestRate (the clamp anchor, not constant
	// across contracts) and Interval (the declared cadence, seed rung of the ladder).
	var markPrice float64
	var rawFunding, interestRate *float64
	var declaredInterval *string
	fr, err := client.FundingRate(ctx, scope.Symbol)
	if err != nil {
		logger.OrDiscard(p.log).Debug("derivatives: funding rate fetch failed", "symbol", scope.Symbol, "error", err)
	} else {
		if fr.Rate != nil {
			val := *fr.Rate
			snapshot.FundingRate = &val
			rawFunding = &val
		}
		if fr.NextRate != nil {
			val := *fr.NextRate
			snapshot.NextFundingRate = &val
		}
		if fr.MarkPrice != nil {
			markPrice = *fr.MarkPrice
		}
		if fr.InterestRate != nil {
			val := *fr.InterestRate
			interestRate = &val
		}
		if fr.Interval != nil {
			val := *fr.Interval
			declaredInterval = &val
		}
	}

	// The measured rung outranks the declared one and can only come from a series; the live poll
	// consults it and re-measures once the declared cadence has elapsed.
	p.revalidateFundingInterval(ctx, client, scope)
	measured, hasMeasured := p.fundingProfiles.measuredFor(scope)
	// Registry hits and misses are counted separately so "the deposit did not happen" is
	// distinguishable from "it happened under another key".
	if hasMeasured {
		recordFundingIntervalMeasurement(p.metrics, scope.ExchangeID, "registry_hit")
	} else {
		recordFundingIntervalMeasurement(p.metrics, scope.ExchangeID, "registry_miss")
		logger.OrDiscard(p.log).Debug("derivatives: registry has no measurement for key", "key", scope.Key())
	}

	// The only point where both rungs are in hand: the measured value wins, and the metric
	// records how often the exchange's metadata was wrong.
	recordFundingIntervalComparison(p.metrics, scope.ExchangeID, scope.Symbol, measured.Hours, hasMeasured, declaredInterval)

	applyFundingNormalization(
		snapshot,
		rawFunding,
		interestRate,
		// No per-bucket narrowing: the live read stamps the current instant, later than any
		// observation. Only the retroactive writer needs profileForBucket.
		resolveFundingInterval(measured, hasMeasured, declaredInterval),
	)

	// --- Open Interest ---
	// OpenInterestAmount, the count of contracts, is the decision quantity. The notional is kept
	// for display only: it is contracts x price, so ranking on it ranks on price.
	oi, err := client.OpenInterest(ctx, scope.Symbol)
	if err != nil {
		logger.OrDiscard(p.log).Debug("derivatives: open interest fetch failed", "symbol", scope.Symbol, "error", err)
	} else if oi.Amount != nil {
		contracts := *oi.Amount
		snapshot.OpenInterestBase = &contracts
		// Stamped in the branch that produced the reading, so the instant belongs to the
		// observation and not to the cycle; both fields stay absent when the fetch failed.
		observedAt := snapshot.Timestamp
		snapshot.OpenInterestObservedAtMs = &observedAt

		// The notional needs a price and is absent without one. The contract count above is not
		// gated on the price: a funding fetch failure must not take open interest down.
		if markPrice > 0 {
			notional := contracts * markPrice
			snapshot.OpenInterest = &notional
		}
	}

	// --- Long/Short Ratio ---
	// The adapter reads globalLongShortAccountRatio directly; this package consumes the
	// canonical point.
	lsrEntries, err := client.LongShortRatioHistory(ctx, scope.Symbol, "5m", 1, 0, 0)
	if err != nil {
		logger.OrDiscard(p.log).Debug("derivatives: implicit long/short ratio skipped", "symbol", scope.Symbol, "error", err)
	} else if len(lsrEntries) > 0 {
		entry := lsrEntries[len(lsrEntries)-1]
		if entry.Ratio > 0 {
			snapshot.LongShortRatio = &entry.Ratio
			// The shares travel only when the pair passed the identity check; a rejected pair
			// leaves both fields absent, never a zero.
			if entry.HasShares {
				snapshot.LSLongPct = &entry.LongPct
				snapshot.LSShortPct = &entry.ShortPct
			}
		}
	}

	return snapshot, nil
}

// revalidateFundingInterval re-measures one symbol's settlement interval when the declared
// cadence has elapsed. A contract that hits the funding cap moves to the 1h cadence and returns
// only after sixteen quiet periods; a stale N scales the 8h-equivalent by up to 8x. The attempt
// is stamped whether it succeeds or not; see fundingMeasurement.
func (p *DerivativesPoller) revalidateFundingInterval(ctx context.Context, client derivatives.Source, scope SymbolScope) {
	if !p.fundingProfiles.revalidationDue(scope) {
		return
	}

	// Shallow ON PURPOSE — see fundingIntervalRevalidationLimit. A deep window keeps
	// answering with the cadence that was in force before the change.
	history, err := client.FundingRateHistory(ctx, scope.Symbol, fundingIntervalRevalidationLimit)
	if err != nil {
		p.fundingProfiles.markAttempted(scope)
		recordFundingIntervalMeasurement(p.metrics, scope.ExchangeID, "fetch_error")
		logger.OrDiscard(p.log).Debug("derivatives: funding interval revalidation failed", "symbol", scope.Symbol, "error", err)
		return
	}

	settlements := fundingSettlementTimestamps(history)
	measured, ok := measureIntervalHours(settlements)
	if !ok {
		p.fundingProfiles.markAttempted(scope)
		// The two absences are different: empty history means the fetch answered with no
		// settlement, while too few instants or no known cadence means a series came back
		// without a spacing.
		outcome := "no_known_cadence"
		switch {
		case len(history) == 0:
			outcome = "empty_history"
		case len(settlements) < 2:
			outcome = "too_few_settlements"
		}
		recordFundingIntervalMeasurement(p.metrics, scope.ExchangeID, outcome)
		logger.OrDiscard(p.log).Debug("derivatives: settlement interval not measured",
			"symbol", scope.Symbol,
			"outcome", outcome,
			"history_rows", len(history),
			"settlements", len(settlements))
		return
	}
	recordFundingIntervalMeasurement(p.metrics, scope.ExchangeID, "measured")

	previous, hadPrevious := p.fundingProfiles.measuredFor(scope)
	// The window travels with the interval: this sample is the span the cadence is known
	// to have been in force over, and nothing older than it may claim to be measured.
	p.fundingProfiles.setMeasured(p.metrics, scope, measuredCadence{
		Hours:          measured,
		ObservedFromMs: observationStartMs(settlements),
	})
	if hadPrevious && previous.Hours != measured {
		// A cadence change is the event this whole mechanism exists to catch, so it is
		// logged at a level an operator actually sees rather than at Debug.
		logger.OrDiscard(p.log).Info("derivatives: settlement interval changed",
			"symbol", scope.Symbol, "previous_hours", previous.Hours, "measured_hours", measured)
	}
}

// BootstrapHistory fetches historical derivatives data on startup with the same parallel
// fan-out as candle seeding: every supported timeframe is populated unconditionally.
//
// Re-seeding merges field by field into the existing member, so a field the backfill does not
// carry survives and a field both sides carry takes the last writer by arrival order, not by
// origin. See SaveSeriesOnly for the rule in full.
//
// OI and LSR walk backward in pages of 500; funding is fetched once and fanned out to every
// timeframe. Depth is declared per timeframe and truncated by provider retention, except for
// funding, which serves history from listing. Raising the depth raises the REST bill linearly.
func (p *DerivativesPoller) BootstrapHistory(ctx context.Context) {
	if !p.cfg.Enabled {
		return
	}

	start := time.Now()

	logger.OrDiscard(p.log).Info("derivatives bootstrap started",
		"symbols", len(p.symbols),
		"timeframes", len(p.tfReg.All()),
		"limit", p.cfg.BootstrapLimit)

	if len(p.symbols) == 0 {
		return
	}

	// Parallel bootstrap with semaphore (same pattern as candle seeding)
	concurrency := p.cfg.BootstrapConcurrency
	if concurrency <= 0 {
		concurrency = 10
	}

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	var successCount, errorCount int64

	for _, scope := range p.symbols {
		client, ok := p.clients[scope.ExchangeID]
		if !ok {
			continue
		}

		wg.Add(1)
		sem <- struct{}{} // Acquire semaphore slot

		go func(s SymbolScope, c derivatives.Source) {
			defer wg.Done()
			defer func() { <-sem }()
			defer func() {
				if r := recover(); r != nil {
					logger.OrDiscard(p.log).Error("derivatives bootstrap panic", "symbol", s.Symbol, "panic", r)
					atomic.AddInt64(&errorCount, 1)
				}
			}()

			select {
			case <-ctx.Done():
				return
			default:
			}

			err := p.bootstrapSymbol(ctx, c, s)
			if err != nil {
				atomic.AddInt64(&errorCount, 1)
			} else {
				atomic.AddInt64(&successCount, 1)
			}
		}(scope, client)
	}

	wg.Wait()

	elapsed := time.Since(start)
	logger.OrDiscard(p.log).Info("derivatives bootstrap complete",
		"ok", atomic.LoadInt64(&successCount),
		"errors", atomic.LoadInt64(&errorCount),
		"elapsed", elapsed)
}

// bootstrapSymbol bootstraps every supported timeframe for one symbol. The venue adapter's
// request budget handles throttling.
func (p *DerivativesPoller) bootstrapSymbol(ctx context.Context, client derivatives.Source,
	scope SymbolScope,
) error {
	limit := int64(p.cfg.BootstrapLimit)

	if _, err := market.FromConfig(scope.MarketType); err != nil {
		return fmt.Errorf("derivatives: %s:%s: %w", scope.ExchangeID, scope.Symbol, err)
	}

	// ── 1. Fetch Funding Rate history once (settlements are events, no timeframe param) ──
	// The rows are fanned out to all timeframes via SaveSeriesOnly plus merge. Neither paginated
	// nor clamped: /fapi/v1/fundingRate serves history from listing, so the thirty-day retention
	// clamp that protects the positioning feeds would only truncate it.
	frHistory, frErr := client.FundingRateHistory(ctx, scope.Symbol, limit)
	if frErr != nil {
		logger.OrDiscard(p.log).Warn("derivatives bootstrap: funding rate history fetch failed",
			"symbol", scope.Symbol, "error", frErr)
	}

	// Settlement history is the only place the interval can be measured rather than taken on
	// the exchange's word; measure it here and hand the result to the live path. fundingInfo is
	// an exception table, not a register, and the interval changes over time.
	bootstrapSettlements := fundingSettlementTimestamps(frHistory)
	if measured, ok := measureIntervalHours(bootstrapSettlements); ok {
		// The window travels with the interval; see setMeasured.
		p.fundingProfiles.setMeasured(p.metrics, scope, measuredCadence{
			Hours:          measured,
			ObservedFromMs: observationStartMs(bootstrapSettlements),
		})
		recordFundingIntervalMeasurement(p.metrics, scope.ExchangeID, "measured_bootstrap")
		logger.OrDiscard(p.log).Debug("derivatives bootstrap: funding interval measured",
			"symbol", scope.Symbol, "hours", measured)
	} else {
		// The bootstrap deposit feeds the measured rung for the next ~5 minutes, so its failure
		// must not be silent.
		outcome := "no_known_cadence_bootstrap"
		switch {
		case frErr != nil:
			outcome = "fetch_error_bootstrap"
		case len(frHistory) == 0:
			outcome = "empty_history_bootstrap"
		case len(bootstrapSettlements) < 2:
			outcome = "too_few_settlements_bootstrap"
		}
		recordFundingIntervalMeasurement(p.metrics, scope.ExchangeID, outcome)
		logger.OrDiscard(p.log).Debug("derivatives bootstrap: settlement interval not measured",
			"symbol", scope.Symbol,
			"outcome", outcome,
			"history_rows", len(frHistory),
			"settlements", len(bootstrapSettlements))
	}

	// ── 2. Bootstrap ALL timeframes ──
	for _, tf := range p.tfReg.All() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		oiSaved := p.bootstrapOIForTimeframe(ctx, client, scope, tf)
		lsrSaved := p.bootstrapLSRForTimeframe(ctx, client, scope, tf)
		frSaved := p.fanOutFRToTimeframe(ctx, scope, frHistory, tf)

		logger.OrDiscard(p.log).Info("derivatives bootstrap: scope written",
			"exchange", scope.ExchangeID,
			"symbol", scope.Symbol,
			"timeframe", tf,
			"open_interest", oiSaved,
			"funding", frSaved,
			"long_short", lsrSaved)
	}

	return nil
}

// bootstrapOIForTimeframe walks open-interest history backward one explicit window at a time
// and writes each point to that timeframe's ZSET. It persists sumOpenInterest, the contract
// count; notional is never written here because its variation is dominated by price.
func (p *DerivativesPoller) bootstrapOIForTimeframe(ctx context.Context, client derivatives.Source,
	scope SymbolScope, timeframe string,
) int {
	return collectPaginatedHistory(
		// The cap declared for this timeframe, not the global one: a per-timeframe cap the
		// bootstrap never fills is a cap in name only.
		ctx, p.metrics, p.store, scope, feedOpenInterest, timeframe, p.cfg.DeclaredSeriesCap(timeframe), time.Now().UnixMilli(),
		func(page historyPage) ([]derivatives.OpenInterestHistoryPoint, error) {
			return client.OpenInterestHistory(ctx, scope.Symbol, timeframe, page.StartMs, page.EndMs, historyPageLimit)
		},
		func(oi derivatives.OpenInterestHistoryPoint) int64 {
			if oi.Timestamp == nil {
				return 0
			}
			return *oi.Timestamp
		},
		func(oi derivatives.OpenInterestHistoryPoint) *derivatives.DerivativesSnapshot {
			if oi.Timestamp == nil || oi.Amount == nil {
				return nil
			}
			contracts := *oi.Amount
			return &derivatives.DerivativesSnapshot{
				Exchange:         scope.ExchangeID,
				MarketType:       scope.MarketType,
				Symbol:           scope.Symbol,
				Timestamp:        *oi.Timestamp,
				OpenInterestBase: &contracts,
			}
		},
		p.log,
	)
}

// bootstrapLSRForTimeframe walks long/short ratio history backward. The timeframe is requested
// as itself: remapping "1d" to "4h" fills the daily ZSET with 4h readings aligned to daily
// buckets, a series that looks daily and is not.
func (p *DerivativesPoller) bootstrapLSRForTimeframe(ctx context.Context, client derivatives.Source,
	scope SymbolScope, timeframe string,
) int {
	return collectPaginatedHistory(
		ctx, p.metrics, p.store, scope, feedLongShortRatio, timeframe, p.cfg.DeclaredSeriesCap(timeframe), time.Now().UnixMilli(),
		func(page historyPage) ([]derivatives.LongShortRatioPoint, error) {
			return client.LongShortRatioHistory(ctx, scope.Symbol, timeframe, historyPageLimit, page.StartMs, page.EndMs)
		},
		func(entry derivatives.LongShortRatioPoint) int64 { return entry.Timestamp },
		func(entry derivatives.LongShortRatioPoint) *derivatives.DerivativesSnapshot {
			// Reject non-finite values as well as zero: NaN and +Inf compare false against zero,
			// and the backfill would store them successfully.
			if entry.Timestamp == 0 || entry.Ratio == 0 ||
				math.IsNaN(entry.Ratio) || math.IsInf(entry.Ratio, 0) {
				return nil
			}
			ratio := entry.Ratio
			snapshot := &derivatives.DerivativesSnapshot{
				Exchange:       scope.ExchangeID,
				MarketType:     scope.MarketType,
				Symbol:         scope.Symbol,
				Timestamp:      entry.Timestamp,
				LongShortRatio: &ratio,
			}
			// Same rule as the live path: a pair that failed the identity check is absent.
			if entry.HasShares {
				longPct, shortPct := entry.LongPct, entry.ShortPct
				snapshot.LSLongPct = &longPct
				snapshot.LSShortPct = &shortPct
			}
			return snapshot
		},
		p.log,
	)
}

// fanOutFRToTimeframe writes pre-fetched funding history to one timeframe ZSET. upsertSeries
// bucket-aligns each settlement, so several settlements may merge into one 1d bucket (latest
// wins) while 5m buckets stay sparse.
func (p *DerivativesPoller) fanOutFRToTimeframe(ctx context.Context, scope SymbolScope,
	frHistory []derivatives.FundingRateHistoryPoint, timeframe string,
) int {
	if frHistory == nil {
		return 0
	}

	// This path must write the whole funding group, never a bare rate: replacing only the rate
	// leaves the 8h-equivalent describing the rate it displaced. The spacing between the
	// history's own settlements is the interval, so the path measures its own rung. interestRate
	// is not supplied, so FundingAtAnchor stays absent rather than borrowing another instant.
	settlements := fundingSettlementTimestamps(frHistory)
	measuredHours, hasMeasured := measureIntervalHours(settlements)
	// nil declared rung: the venue's metadata belongs to the LIVE read, not to this
	// backfill, and the measured spacing is the stronger rung anyway.
	profile := resolveFundingInterval(
		measuredCadence{Hours: measuredHours, ObservedFromMs: observationStartMs(settlements)},
		hasMeasured, nil)

	saved := 0
	var oldest, newest int64
	for _, fr := range frHistory {
		if fr.Timestamp == nil || fr.Rate == nil {
			continue
		}
		snapshot := &derivatives.DerivativesSnapshot{
			Exchange:   scope.ExchangeID,
			MarketType: scope.MarketType,
			Symbol:     scope.Symbol,
			Timestamp:  *fr.Timestamp,
		}
		val := *fr.Rate
		snapshot.FundingRate = &val
		// Per bucket, not per series: the observation window is the history itself, and a
		// history shallower than the series it feeds is where the narrowing starts to bite.
		applyFundingNormalization(snapshot, &val, nil, profileForBucket(profile, snapshot.Timestamp))

		if err := p.store.SaveSeriesOnly(ctx, snapshot, timeframe); err != nil {
			continue
		}
		saved++
		if oldest == 0 || snapshot.Timestamp < oldest {
			oldest = snapshot.Timestamp
		}
		if snapshot.Timestamp > newest {
			newest = snapshot.Timestamp
		}
	}

	outcome := outcomePoints
	if saved == 0 {
		outcome = classifyEmptyCollection(feedFundingRate, timeframe)
	}
	recordFeedRequest(p.metrics, scope.ExchangeID, feedFundingRate, timeframe, outcome)
	recordCollectedDepth(p.metrics, scope.ExchangeID, feedFundingRate, scope.Symbol, timeframe, saved, oldest, newest)
	return saved
}
