package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/coinalyze"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// The collection cycle of the second provenance. It shares nothing with the venue poller:
// another host, budget, key family and label convention. The only meeting point is the snapshot
// field, which is why the unit of each field is the main subject of this file.
//
// Units measured side by side on the same daily bar, BTCUSDT perpetual: open interest is
// BASE_ASSET (factor 1); funding is published as PERCENT against Binance's fraction (factor
// 1/100); long/short shares are percent after the venue poller's own x100 (factor 1); the ratio
// matches. History timestamps arrive in SECONDS, not milliseconds: reading them as ms throws the
// series to 1970.
const (
	coinalyzeOpenInterestPath = "/open-interest-history"
	coinalyzeFundingPath      = "/funding-rate-history"
	coinalyzeLongShortPath    = "/long-short-ratio-history"
)

// coinalyzeFundingPercentPerUnit converts the published percent into the fraction the
// existing source writes. Measured: Coinalyze's `0.01` is Binance's `0.00010000`.
const coinalyzeFundingPercentPerUnit = 100.0

// coinalyzeShareSumTolerance bounds how far the two shares may drift from summing
// to 100 and still be accepted as a pair.
//
// It is the same identity test the existing source applies (validateShareSum), with
// the tolerance on the percent scale: there the pair is validated as a fraction with
// 1e-3, here as a percent, hence 1e-1. Accepting a pair that does not close would write
// two halves taken from different instants as if they were one snapshot.
const coinalyzeShareSumTolerance = 0.1

// coinalyzeIntervals translates the internal timeframe into the API's interval. The accepted
// list is what the API returns on a 400; there is no weekly, which is why this is a table and
// not a suffix rule. The two named timeframes also appear in the availability metrics, so the
// provider's spelling lives here once.
const (
	timeframe15m = "15m"
	timeframe30m = "30m"
)

func coinalyzeIntervals() map[string]string {
	return map[string]string{
		"1m":         "1min",
		"5m":         "5min",
		timeframe15m: "15min",
		timeframe30m: "30min",
		"1h":         "1hour",
		"2h":         "2hour",
		"4h":         "4hour",
		"6h":         "6hour",
		"12h":        "12hour",
		"1d":         "daily",
	}
}

// ErrCoinalyzeTimeframeUnsupported says the provider does not serve this timeframe.
var ErrCoinalyzeTimeframeUnsupported = errors.New("coinalyze: timeframe sem intervalo equivalente")

// CoinalyzeInterval resolves the API `interval` for an internal timeframe.
func CoinalyzeInterval(timeframe string) (string, error) {
	interval, ok := coinalyzeIntervals()[timeframe]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrCoinalyzeTimeframeUnsupported, timeframe)
	}
	return interval, nil
}

// coinalyzeSourceName names the provenance in both layers. It is the SAME string the
// key namespace and the `source` column of the durable table carry.
const coinalyzeSourceName = "coinalyze"

// CoinalyzeSeriesStore is the write port of this poller. The label convention crosses the port
// because whoever collects the feed is who knows it; fixing it in storage would make the
// declaration here decorative.
type CoinalyzeSeriesStore interface {
	// WriteBatch writes one (symbol, timeframe) into the durable tier, the only destination of
	// this collection. An error makes the caller keep the symbol unseeded, so the next cycle
	// asks for the deep window again.
	WriteBatch(ctx context.Context, batch derivatives.DerivativesBatch) error
}

// coinalyzeResolver is the symbol join, narrowed to what this cycle uses.
type coinalyzeResolver interface {
	Resolve(ctx context.Context, symbol string) (coinalyze.CoinalyzeMarket, error)
}

// coinalyzeSpender is the budget, narrowed to what this cycle uses.
type coinalyzeSpender interface {
	Do(ctx context.Context, symbols []string, call func(context.Context) error) error
}

// coinalyzeHistoryClient is the history read this cycle uses. The port lives here,
// narrowed to the single method called: the collector does not need to know the
// concrete HTTP client or the bucket that wraps it.
type coinalyzeHistoryClient interface {
	GetHistory(ctx context.Context, path string, params url.Values, out any) error
}

// CoinalyzePollerOptions configures the cycle.
type CoinalyzePollerOptions struct {
	Client     coinalyzeHistoryClient
	Catalog    coinalyzeResolver
	Budget     coinalyzeSpender
	Store      CoinalyzeSeriesStore
	Exchange   string
	MarketType string
	Symbols    []string
	// SymbolsFunc resolves the symbols in the cycle, not at construction: a fleet assembled by
	// discovery has no static list. Nil falls back to Symbols.
	SymbolsFunc func(context.Context) []string
	Timeframes  []string
	// Lookback is the depth requested per cycle.
	Lookback time.Duration
	// CycleInterval is the cadence the caller schedules this cycle on; it is the denominator of
	// the pace. Zero falls back to the provider floor, never to no pacing.
	CycleInterval time.Duration
	// FundingProfiles is the process-wide settlement-interval registry. It is injected because
	// this API returns buckets aligned to the requested timeframe, not settlements, so spacing
	// cannot be measured here. Nil degrades to "not measured" without fabricating a value.
	FundingProfiles *FundingProfileRegistry
	// SeedLedger is where the seeded mark survives restart and deploy. Nil falls back to the
	// process map. See coinalyze_seed_ledger.go.
	SeedLedger CoinalyzeSeedLedger
	Now        func() time.Time
	Log        *slog.Logger
}

// CoinalyzePoller walks symbols, spends the budget per symbol and writes into the key
// family of the second provenance.
type CoinalyzePoller struct {
	client      coinalyzeHistoryClient
	catalog     coinalyzeResolver
	budget      coinalyzeSpender
	store       CoinalyzeSeriesStore
	exchange    string
	marketType  string
	symbols     []string
	symbolsFunc func(context.Context) []string
	timeframes  []string
	lookback    time.Duration
	now         func() time.Time

	// fundingProfiles is the process settlement-interval registry. See CoinalyzePollerOptions.
	fundingProfiles *FundingProfileRegistry

	// cycleInterval is the cadence at which this cycle is scheduled. See CycleInterval.
	cycleInterval time.Duration

	// newPacer builds the admission source for one cycle — the source that releases the next
	// batch. Nil in production (a time.Ticker); tests replace it to measure the pace instead
	// of sleeping through it, as `CoinalyzeBudgetOptions.Wait` already does with the wait.
	newPacer func(time.Duration) (tick <-chan time.Time, stop func())

	// feedLabel is this feed's declaration, not a measurement. The label travels in batch.Label
	// to coldPointOf, which aligns the Timestamp by it and writes LabelConvention into the row,
	// so a divergent declaration moves the durable bucket. The per-endpoint source is
	// derivativesLabelDeclarations.
	feedLabel derivatives.LabelConvention

	// seededSymbols holds those that already received the deep pass, per symbol, not per
	// process: a process-wide boolean stayed set when the first boot cycle ran before discovery
	// populated the fleet, leaving the rest without deep history.
	seededMu      sync.Mutex
	seededSymbols map[string]struct{}

	// seedBackoff is the per-symbol backoff: consecutive failures and cycles the deep window
	// still waits. Guarded by the same mutex as the mark, since it is the same state.
	seedBackoff map[string]coinalyzeSeedFailure

	// seedLedger is the DURABLE half of the same mark. See CoinalyzeSeedLedger.
	seedLedger CoinalyzeSeedLedger

	log *slog.Logger
}

// coinalyzeSeedFailure is one symbol's backoff, counted in cycles: streak is consecutive failed
// passes, pending is cycles until the deep window is requested again.
type coinalyzeSeedFailure struct {
	streak  int
	pending int
}

// NewCoinalyzePoller assembles the cycle with defaults filled in. It performs no I/O.
func NewCoinalyzePoller(opts CoinalyzePollerOptions) *CoinalyzePoller {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Lookback <= 0 {
		opts.Lookback = 24 * time.Hour
	}
	return &CoinalyzePoller{
		client:      opts.Client,
		catalog:     opts.Catalog,
		budget:      opts.Budget,
		store:       opts.Store,
		exchange:    opts.Exchange,
		marketType:  opts.MarketType,
		symbols:     opts.Symbols,
		symbolsFunc: opts.SymbolsFunc,
		timeframes:  opts.Timeframes,
		lookback:    opts.Lookback,
		now:         opts.Now,
		// No defaults here: a locally built registry or ledger would be empty by construction,
		// and an undeclared interval falls to the provider floor inside
		// coinalyzeAdmissionInterval rather than duplicating the wiring's cadence.
		fundingProfiles: opts.FundingProfiles,
		seedLedger:      opts.SeedLedger,
		cycleInterval:   opts.CycleInterval,
		// Declared here rather than inherited from a type default. See feedLabel.
		feedLabel: derivatives.LabelIntervalStart,
		log:       logger.OrDiscard(opts.Log),
	}
}

// admitFeed refuses this feed before any write when the convention is undeclared.
func (p *CoinalyzePoller) admitFeed() error {
	if p.feedLabel.Valid() {
		return nil
	}
	logger.OrDiscard(p.log).Error("coinalyze: feed refused; label convention is not declared",
		"label_convention", p.feedLabel)
	return fmt.Errorf("coinalyze: feed has label convention %s; declare interval-start or interval-end", p.feedLabel)
}

// PollOnce runs one collection cycle over every configured symbol and timeframe. An
// unsupported timeframe or a symbol refused by denomination is a configuration finding, logged
// while the cycle continues; only a feed without a declared convention aborts.
func (p *CoinalyzePoller) PollOnce(ctx context.Context) error {
	if err := p.admitFeed(); err != nil {
		return err
	}

	configurados := p.symbolsOf(ctx)
	resolved, ids := p.resolveSymbols(ctx, configurados)
	if len(ids) == 0 {
		return nil
	}

	to := p.now().UTC()
	// A question, not a mark, and asked per symbol. The mark happens at the end, over those
	// that wrote.
	porSemear, jaSemeados := p.splitBySeeding(ctx, resolved, ids, to)

	// Backoff sits between the question and the plan: whoever failed previously requests the
	// tail this cycle instead of the deep window. The split is outside the plan because it also
	// decides who may be marked as seeded.
	semear, emRecuo := p.splitByBackoff(porSemear)
	if len(emRecuo) > 0 {
		// Warn, because the process default level is warn and an invisible backoff leaves the
		// operator asking why deep history never arrives.
		logger.OrDiscard(p.log).Warn("coinalyze: symbols in backoff; this cycle requests the tail instead of the deep window",
			"symbols", len(emRecuo),
			"sample", sampleOfSymbols(emRecuo, coinalyzeBackoffSampleLimit))
	}
	cauda := append(append([]string(nil), jaSemeados...), emRecuo...)

	// THE PLAN COMES BEFORE THE FIRST FETCH, which is why it exists: the pace is the period
	// divided by the batches the cycle WILL request, and counting after requesting is late.
	plan := p.planCycle(to, semear, cauda)
	admission, floored := coinalyzeAdmissionInterval(p.cycleInterval, len(plan))
	reportCoinalyzePace(p.log, len(plan), p.cycleInterval, admission, floored)

	tick, stopPacer := p.pacer(admission)
	defer stopPacer()

	falharam := make(map[string]struct{})
	for i, unit := range plan {
		// THE FIRST BATCH ENTERS DIRECTLY; the k-th waits for the k-th tick.
		//
		// Cancellation here RETURNS instead of leaving the loop: leaving would fall through
		// to `markSeeded` below and mark as seeded whoever was never collected, which is
		// exactly the seeding burn the code below guards against.
		if i > 0 && !p.awaitAdmission(ctx, tick) {
			return ctx.Err()
		}

		falhos, err := p.collectBatch(ctx, resolved, unit.symbols, unit.timeframe, unit.interval, unit.from, to)
		for _, symbol := range falhos {
			falharam[symbol] = struct{}{}
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			logger.OrDiscard(p.log).Warn("coinalyze batch failed",
				"symbols", len(unit.symbols), "timeframe", unit.timeframe, "error", err)
		}
	}

	// The mark comes afterwards, only over symbols that wrote, and per symbol. Marking before
	// the writes burns the seeding when the first cycle fails everywhere. A cycle-wide boolean
	// pinned the whole fleet in seeding over one persistent write error. Only an explicit error
	// counts as failure; a symbol in backoff wrote only the tail and is not seeded by it.
	p.markSeeded(semRecuo(semFalha(ids, falharam), emRecuo))

	// Repeated failure does not amplify the request: the streak and the wait advance one cycle,
	// so the deterministic plan stops repeating the same whole-window request.
	p.advanceSeedBackoff(ids, falharam)
	return nil
}

// semFalha returns, in fleet order, the symbols no timeframe reported as failed.
func semFalha(ids []string, falharam map[string]struct{}) []string {
	out := make([]string, 0, len(ids))
	for _, symbol := range ids {
		if _, ruim := falharam[symbol]; ruim {
			continue
		}
		out = append(out, symbol)
	}
	return out
}

// coinalyzeSeedBackoffSteps is how far the wait grows: 1, 2, 4 and 8 cycles.
const coinalyzeSeedBackoffSteps = 3

// coinalyzeSeedBackoffCycles returns how many cycles a symbol's deep pass waits after `streak`
// consecutive failures. The wait grows because the cause may be persistent; the eight-cycle
// ceiling reduces pressure without abandoning deep history, which is the only path its data has.
func coinalyzeSeedBackoffCycles(streak int) int {
	if streak < 1 {
		return 0
	}
	steps := streak - 1
	if steps > coinalyzeSeedBackoffSteps {
		steps = coinalyzeSeedBackoffSteps
	}
	return 1 << steps
}

// advanceSeedBackoff advances one cycle in each fleet symbol's backoff policy. A failure adds to
// the streak and sets a new wait; a success resets the streak and consumes one pending cycle, so
// a transient error cannot become permanent exclusion.
func (p *CoinalyzePoller) advanceSeedBackoff(ids []string, falharam map[string]struct{}) {
	p.seededMu.Lock()
	defer p.seededMu.Unlock()

	for _, symbol := range ids {
		estado := p.seedBackoff[symbol]
		if _, falhou := falharam[symbol]; falhou {
			estado.streak++
			estado.pending = coinalyzeSeedBackoffCycles(estado.streak)
		} else {
			estado.streak = 0
			if estado.pending > 0 {
				// The wait is consumed CYCLE BY CYCLE: the next cycle is the last deferred
				// one when `pending` reaches zero here.
				estado.pending--
			}
		}
		if estado.streak == 0 && estado.pending == 0 {
			delete(p.seedBackoff, symbol)
			continue
		}
		if p.seedBackoff == nil {
			p.seedBackoff = make(map[string]coinalyzeSeedFailure, len(ids))
		}
		p.seedBackoff[symbol] = estado
	}
}

// splitByBackoff separates who seeds now from who is in backoff. Backoff is per symbol, not per
// timeframe: the failure happens at the symbol or batch, and a symbol in backoff requests the
// tail across all timeframes of this cycle.
func (p *CoinalyzePoller) splitByBackoff(porSemear []string) (semear, adiados []string) {
	p.seededMu.Lock()
	defer p.seededMu.Unlock()

	for _, symbol := range porSemear {
		if estado, emRecuo := p.seedBackoff[symbol]; emRecuo && estado.pending > 0 {
			adiados = append(adiados, symbol)
			continue
		}
		semear = append(semear, symbol)
	}
	return semear, adiados
}

// semRecuo returns the symbols not in this cycle's backoff. It exists for the mark: a symbol in
// backoff requested only the tail, and tail depth is not seeding (see
// TestSeed_TailDepthIsNotSeeding).
func semRecuo(symbols, adiados []string) []string {
	if len(adiados) == 0 {
		return symbols
	}
	recuo := make(map[string]struct{}, len(adiados))
	for _, symbol := range adiados {
		recuo[symbol] = struct{}{}
	}
	out := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		if _, adiado := recuo[symbol]; adiado {
			continue
		}
		out = append(out, symbol)
	}
	return out
}

// coinalyzeBackoffSampleLimit is how many symbols in backoff the warning names: a fleet-wide
// outage can put hundreds in backoff, and the total is always logged.
const coinalyzeBackoffSampleLimit = 8

// sampleOfSymbols names up to `limit` symbols and summarizes the rest.
func sampleOfSymbols(symbols []string, limit int) string {
	if len(symbols) <= limit {
		return strings.Join(symbols, ", ")
	}
	return strings.Join(symbols[:limit], ", ") + fmt.Sprintf(" (+%d)", len(symbols)-limit)
}

// ─────────────────────────────────────────────────────────────────────────────
// ADMISSION PACE — the same mechanism as the venue path, in this source's unit. PollOnce runs
// synchronously inside the scheduler's ticker, so a cycle longer than the period makes the
// runtime discard ticks. The admission unit is the batch, because the batch is what becomes a
// request: the API accepts up to 20 symbols per call and charges one call per symbol.
// ─────────────────────────────────────────────────────────────────────────────

// CoinalyzeCallsPerBatch is what one batch costs per symbol: open interest, funding and
// long/short. Exported because the load arithmetic has two endpoints and one source; a copy
// here would be a second truth that diverges when a fourth quantity enters the batch.
const CoinalyzeCallsPerBatch = 3

// coinalyzeCycleDrainFraction is the share of the period withheld from admission so work in
// flight when the last batch enters has somewhere to finish.
const coinalyzeCycleDrainFraction = 20

// minCoinalyzeAdmission is the floor: 20 symbols x 3 endpoints = 60 charged calls against 40
// per minute, so 90 seconds. Asking faster speeds nothing up because the budget blocks at the
// same place; the floor exists so the second return can declare the configured cadence
// unreachable.
const minCoinalyzeAdmission = time.Minute * CoinalyzeCallsPerBatch *
	coinalyze.CoinalyzeMaxSymbolsPerRequest / coinalyze.CoinalyzeCallsPerMinute

// coinalyzeAdmissionUnit is ONE cycle entry: a batch of symbols, in a timeframe, with the
// window that batch's group requests.
type coinalyzeAdmissionUnit struct {
	timeframe string
	interval  string
	from      time.Time
	symbols   []string
}

// coinalyzeAdmissionInterval is the spacing between batches entering a cycle: what remains
// of the period after the drain reserve, divided by the batches, floored at what the
// provider serves.
//
// The second return says the FLOOR decided the pace instead of the configured interval,
// which means the configured cadence is unreachable within the source's budget.
func coinalyzeAdmissionInterval(cycle time.Duration, batches int) (time.Duration, bool) {
	if batches <= 0 {
		return minCoinalyzeAdmission, false
	}
	admissionWindow := cycle - cycle/coinalyzeCycleDrainFraction
	paced := admissionWindow / time.Duration(batches)
	if paced < minCoinalyzeAdmission {
		return minCoinalyzeAdmission, true
	}
	return paced, false
}

// reportCoinalyzePace puts the derived pace where an operator can see it.
func reportCoinalyzePace(log *slog.Logger, batches int, cycle, admission time.Duration, floored bool) {
	log = logger.OrDiscard(log)
	if batches == 0 {
		return
	}
	if floored {
		log.Warn("coinalyze cycle does not fit the configured cadence; admission is floored by the rate budget",
			"batches", batches,
			"cycle", cycle,
			"admission_floor", minCoinalyzeAdmission,
			"calls_per_minute", coinalyze.CoinalyzeCallsPerMinute,
			"admissions_duration", (minCoinalyzeAdmission * time.Duration(batches-1)).Round(time.Second))
		return
	}
	log.Info("coinalyze admission planned",
		"batches", batches,
		"cycle", cycle,
		"admission", admission.Round(time.Millisecond))
}

// planCycle assembles everything this cycle will request before the first fetch, because the
// pace needs the batch count before the first one. It drops unsupported timeframes, separates
// the window groups and slices each into batches in emission order.
func (p *CoinalyzePoller) planCycle(to time.Time, semear, cauda []string) []coinalyzeAdmissionUnit {
	var plan []coinalyzeAdmissionUnit

	for _, timeframe := range p.timeframes {
		interval, err := CoinalyzeInterval(timeframe)
		if err != nil {
			logger.OrDiscard(p.log).Warn("coinalyze timeframe ignored in this cycle",
				"timeframe", timeframe, "error", err)
			continue
		}
		// The two groups request different windows: depth travels as `from` in the query, so a
		// cycle-wide boolean made one seeding symbol drag the whole fleet into 2000 buckets.
		// The split costs one extra request per timeframe, charged per symbol.
		for _, grupo := range []struct {
			ids     []string
			seeding bool
		}{{semear, true}, {cauda, false}} {
			if len(grupo.ids) == 0 {
				continue
			}
			from := to.Add(-p.windowFor(timeframe, grupo.seeding))
			if grupo.seeding {
				logger.OrDiscard(p.log).Info("coinalyze seeding",
					"timeframe", timeframe,
					"symbols", len(grupo.ids),
					"from", from.Format("2006-01-02"))
			}
			for _, batch := range coinalyze.CoinalyzeSymbolBatches(grupo.ids) {
				plan = append(plan, coinalyzeAdmissionUnit{
					timeframe: timeframe,
					interval:  interval,
					from:      from,
					symbols:   batch,
				})
			}
		}
	}
	return plan
}

// pacer assembles one cycle's admission source.
func (p *CoinalyzePoller) pacer(d time.Duration) (<-chan time.Time, func()) {
	if p.newPacer != nil {
		return p.newPacer(d)
	}
	ticker := time.NewTicker(d)
	return ticker.C, ticker.Stop
}

// awaitAdmission blocks until the pace releases the next batch, and returns false when the
// cycle was told to stop.
//
// A cycle lasts the whole period by design, so shutdown must be visible from inside it:
// without this, a cancellation in the middle of an hourly cycle would only be answered an
// hour later.
func (p *CoinalyzePoller) awaitAdmission(ctx context.Context, tick <-chan time.Time) bool {
	select {
	case <-ctx.Done():
		return false
	case <-tick:
		return true
	}
}

// coinalyzeMaxPointsPerResponse is the point ceiling the provider returns per response,
// measured against the real API: ~2000 points regardless of the window requested. There is no
// pagination to do, and the requested window must be exactly 2000 buckets: less leaves history
// behind, more makes the coverage audit report a gap the provider never had.
const coinalyzeMaxPointsPerResponse = 2000

// windowFor returns the depth to request for a timeframe. Seeding requests the 2000 buckets;
// following cycles request only the tail, since rewriting everything would spend writes
// confirming what is already there. Seeding runs once per process, and restarting re-seeds:
// the write is idempotent and closes any hole an outage opened.
func (p *CoinalyzePoller) windowFor(timeframe string, seeding bool) time.Duration {
	bucket := time.Duration(candle.ParseTimeframeToSeconds(timeframe)) * time.Second
	if bucket <= 0 {
		return p.lookback
	}
	if seeding {
		return time.Duration(coinalyzeMaxPointsPerResponse) * bucket
	}
	// The tail covers more than one bucket on purpose: the current bucket is still filling and
	// the previous one may be revised.
	if tail := 4 * bucket; tail > p.lookback {
		return tail
	}
	return p.lookback
}

// splitBySeeding separates symbols that have not received the deep pass from those that have,
// without marking. It is separate from markSeeded because the question happens at cycle start
// and the mark at cycle end. The union of process memory and the durable-tier ledger is the
// mark; the ledger is what survives restart. See coinalyze_seed_ledger.go.
func (p *CoinalyzePoller) splitBySeeding(
	ctx context.Context,
	resolved map[string]coinalyzeTarget,
	fleet []string,
	to time.Time,
) (porSemear, jaSemeados []string) {
	// Outside the lock: the query is a database round trip and would block markSeeded.
	duravel := p.durableSeeded(ctx, resolved, fleet, to)

	p.seededMu.Lock()
	defer p.seededMu.Unlock()
	for _, symbol := range fleet {
		_, noProcesso := p.seededSymbols[symbol]
		_, noDuravel := duravel[symbol]
		if noProcesso || noDuravel {
			jaSemeados = append(jaSemeados, symbol)
			continue
		}
		porSemear = append(porSemear, symbol)
	}
	return porSemear, jaSemeados
}

// markSeeded marks the fleet as seeded and reports whether any of it was not already. A single
// new symbol requests the deep window for its batch: the rewrite is idempotent, while shallow
// history for a new symbol never arrives.
func (p *CoinalyzePoller) markSeeded(fleet []string) bool {
	p.seededMu.Lock()
	defer p.seededMu.Unlock()
	if p.seededSymbols == nil {
		p.seededSymbols = make(map[string]struct{}, len(fleet))
	}
	novo := false
	for _, symbol := range fleet {
		if _, ok := p.seededSymbols[symbol]; !ok {
			novo = true
			p.seededSymbols[symbol] = struct{}{}
		}
	}
	return novo
}

// symbolsOf resolves this cycle's fleet per cycle, since discovery re-evaluates periodically. An
// empty result falls back to the static list rather than erasing the collection.
func (p *CoinalyzePoller) symbolsOf(ctx context.Context) []string {
	if p.symbolsFunc == nil {
		return p.symbols
	}
	if vivos := p.symbolsFunc(ctx); len(vivos) > 0 {
		return vivos
	}
	return p.symbols
}

// resolveSymbols joins each configured symbol and returns the id→market map along with
// the ids in configured order.
func (p *CoinalyzePoller) resolveSymbols(ctx context.Context, configurados []string) (map[string]coinalyzeTarget, []string) {
	resolved := make(map[string]coinalyzeTarget, len(configurados))
	ids := make([]string, 0, len(configurados))

	for _, symbol := range configurados {
		market, err := p.catalog.Resolve(ctx, symbol)
		if err != nil {
			// A denomination refusal is already logged by the catalog; the others have no
			// other line and would make the symbol vanish with no visible cause.
			var denominacao *coinalyze.CoinalyzeDenominationError
			if !errors.As(err, &denominacao) {
				logger.OrDiscard(p.log).Warn("coinalyze symbol skipped in this cycle", "symbol", symbol, "error", err)
			}
			continue
		}
		resolved[market.Symbol] = coinalyzeTarget{internal: symbol, denominatedIn: market.DenominatedIn}
		ids = append(ids, market.Symbol)
	}
	return resolved, ids
}

// coinalyzeTarget links the provider id back to the internal spelling.
type coinalyzeTarget struct {
	internal      string
	denominatedIn string
}

// collectBatch spends the budget on the three quantities and writes what came back. It returns
// the symbols that did not reach storage: a collection failure takes the whole batch, while a
// write failure belongs to the symbol, and the caller decides seeding per symbol from that list.
func (p *CoinalyzePoller) collectBatch(
	ctx context.Context,
	resolved map[string]coinalyzeTarget,
	batch []string,
	timeframe, interval string,
	from, to time.Time,
) ([]string, error) {
	points := newCoinalyzePointSet(p.fundingProfileResolver(resolved), p.log)

	req := coinalyze.CoinalyzeHistoryRequest{
		Symbols:  batch,
		From:     from,
		To:       to,
		Interval: coinalyzeIntervalSpan(timeframe),
	}

	// Open interest with convert_to_usd=false. With true the provider returns notional, whose
	// variation is dominated by price; a decision axis ordered by it is ordered by price.
	oiParams := url.Values{"convert_to_usd": []string{"false"}}
	if err := p.fetchInto(ctx, coinalyzeOpenInterestPath, oiParams, req, interval,
		coinalyzeOpen, points.applyOpenInterest); err != nil {
		return batch, err
	}
	if err := p.fetchInto(ctx, coinalyzeFundingPath, nil, req, interval,
		coinalyzeClose, points.applyFunding); err != nil {
		return batch, err
	}
	if err := p.fetchLongShortInto(ctx, req, interval, points); err != nil {
		return batch, err
	}

	return p.write(ctx, resolved, points, timeframe)
}

// coinalyzeField chooses WHICH sample of the OHLC bucket enters the series.
type coinalyzeField func(coinalyze.CoinalyzeHistoryPoint) float64

// coinalyzeOpen is the sample at the START of the bucket.
func coinalyzeOpen(p coinalyze.CoinalyzeHistoryPoint) float64 { return p.O }

// coinalyzeClose is the sample at the END of the bucket.
func coinalyzeClose(p coinalyze.CoinalyzeHistoryPoint) float64 { return p.C }

// Open interest uses the bucket's OPEN and funding uses its CLOSE. Both feeds declare the same
// interval-start label, but each provider samples a different instant inside the bucket, which
// LabelConvention does not model. Measured against Binance over 30 daily buckets and three
// symbols: Coinalyze's `.o` errs 0.018% on the aligned day against 1.232% for `.c`, because
// Binance reports open interest at instant t and Coinalyze reports the OHLC of the interval
// starting at t. Funding has no such problem: `.c` aligns exactly after the unit conversion.
// This is a per-series rule, so the choice travels as a parameter.
//
// fetchInto makes one OHLC history call, audits coverage and applies each point.
func (p *CoinalyzePoller) fetchInto(
	ctx context.Context,
	path string,
	extra url.Values,
	req coinalyze.CoinalyzeHistoryRequest,
	interval string,
	pick coinalyzeField,
	apply func(symbol string, tsMs int64, value float64),
) error {
	var response []coinalyze.CoinalyzeHistorySeries

	params := coinalyzeHistoryParams(req, interval, extra)
	// The budget wraps the call and charges per symbol: the API charges one call per symbol of
	// a multi-symbol request, so counting the request as one understates the spend by up to 20x.
	err := p.budget.Do(ctx, req.Symbols, func(callCtx context.Context) error {
		response = nil
		return p.client.GetHistory(callCtx, path, params, &response)
	})
	if err != nil {
		return fmt.Errorf("coinalyze: %s: %w", path, err)
	}

	for _, audit := range coinalyze.CoinalyzeAuditHistory(req, response, p.log) {
		for _, point := range audit.Points {
			apply(audit.Symbol, coinalyzeSecondsToMs(point.T), pick(point))
		}
	}
	return nil
}

// fetchLongShortInto is the long/short call, which has its own body: `r`, `l`, `s` instead of
// `o,h,l,c`. Reusing the OHLC decoder would read the long share as the candle low.
func (p *CoinalyzePoller) fetchLongShortInto(
	ctx context.Context,
	req coinalyze.CoinalyzeHistoryRequest,
	interval string,
	points *coinalyzePointSet,
) error {
	var response []coinalyze.CoinalyzeLongShortSeries

	params := coinalyzeHistoryParams(req, interval, nil)
	err := p.budget.Do(ctx, req.Symbols, func(callCtx context.Context) error {
		response = nil
		return p.client.GetHistory(callCtx, coinalyzeLongShortPath, params, &response)
	})
	if err != nil {
		return fmt.Errorf("coinalyze: %s: %w", coinalyzeLongShortPath, err)
	}

	// Audited like the other two: iterating the response raw would skip the warning for an
	// omitted symbol and would not mark a truncated window as partial.
	for _, audit := range coinalyze.CoinalyzeAuditLongShort(req, response, p.log) {
		for _, point := range audit.Points {
			points.applyLongShort(audit.Symbol, coinalyzeSecondsToMs(point.T), point)
		}
	}
	return nil
}

// write stores one snapshot per (symbol, bucket), in ascending time order, and returns the
// symbols whose write failed along with the aggregated error.
func (p *CoinalyzePoller) write(
	ctx context.Context,
	resolved map[string]coinalyzeTarget,
	points *coinalyzePointSet,
	timeframe string,
) ([]string, error) {
	// The batch is already paid for, so it finishes: returning on the first failed write would
	// discard the other symbols' network work. Errors are aggregated because one symbol's
	// failure says nothing about the others.
	var falhas []error
	var falhos []string
	for _, symbol := range points.symbols() {
		target, ok := resolved[symbol]
		if !ok {
			// The provider answered for a symbol this cycle did not request; writing it would
			// require guessing the internal spelling.
			logger.OrDiscard(p.log).Warn("coinalyze response carried a symbol that was not in the batch",
				"symbol", symbol)
			continue
		}
		// In batch, not point by point, because the durable tier writes in batch and both layers
		// must receive the same observation. Calling the series write directly leaves
		// DerivativesTieredWriter with no caller and the durable table empty.
		batch := derivatives.DerivativesBatch{
			Timeframe:     timeframe,
			Source:        coinalyzeSourceName,
			DenominatedIn: target.denominatedIn,
			Label:         p.feedLabel,
			Snapshots:     make([]*derivatives.DerivativesSnapshot, 0, len(points.timestamps(symbol))),
		}
		for _, tsMs := range points.timestamps(symbol) {
			snapshot := points.snapshot(symbol, tsMs)
			snapshot.Exchange = p.exchange
			snapshot.MarketType = p.marketType
			snapshot.Symbol = target.internal
			batch.Snapshots = append(batch.Snapshots, snapshot)
		}
		if err := p.store.WriteBatch(ctx, batch); err != nil {
			falhas = append(falhas, fmt.Errorf("coinalyze: gravar %s %s: %w", target.internal, timeframe, err))
			falhos = append(falhos, symbol)
		}
	}
	return falhos, errors.Join(falhas...)
}

// coinalyzeHistoryParams assembles the query of a history call.
//
// `from`/`to` travel in SECONDS, the same unit in which the response's `t` comes back.
func coinalyzeHistoryParams(req coinalyze.CoinalyzeHistoryRequest, interval string, extra url.Values) url.Values {
	params := url.Values{
		"symbols":  []string{coinalyzeJoinSymbols(req.Symbols)},
		"interval": []string{interval},
		"from":     []string{strconv.FormatInt(req.From.Unix(), 10)},
		"to":       []string{strconv.FormatInt(req.To.Unix(), 10)},
	}
	for key, values := range extra {
		params[key] = values
	}
	return params
}

func coinalyzeJoinSymbols(symbols []string) string {
	out := ""
	for i, symbol := range symbols {
		if i > 0 {
			out += ","
		}
		out += symbol
	}
	return out
}

// coinalyzeSecondsToMs converts the history `t`, which comes in SECONDS.
func coinalyzeSecondsToMs(seconds int64) int64 {
	return seconds * 1000
}

// coinalyzeIntervalSpan is the bucket width, used as tolerance by the coverage audit. It
// derives from the project's timeframe registry.
func coinalyzeIntervalSpan(timeframe string) time.Duration {
	return time.Duration(candle.ParseTimeframeToSeconds(timeframe)) * time.Second
}

// coinalyzePointSet accumulates the three quantities per (symbol, bucket) before writing. They
// arrive in three responses and fall into the same bucket, so one merge suffices.
type coinalyzePointSet struct {
	log      *slog.Logger
	bySymbol map[string]map[int64]*derivatives.DerivativesSnapshot
	// profileOf resolves the settlement interval of the contract behind the provider id. It is a
	// field rather than an applyFunding parameter to keep the coinalyzeField signature shared by
	// all three quantities.
	profileOf func(providerSymbol string) fundingProfile
}

func newCoinalyzePointSet(profileOf func(providerSymbol string) fundingProfile, logs ...*slog.Logger) *coinalyzePointSet {
	if profileOf == nil {
		// Explicit nil default: with no resolver no step produced an interval, which is what
		// unavailable means.
		profileOf = func(string) fundingProfile { return fundingProfile{Step: derivatives.NUnavailable} }
	}
	return &coinalyzePointSet{
		log:       logger.OrDiscard(logs...),
		bySymbol:  make(map[string]map[int64]*derivatives.DerivativesSnapshot),
		profileOf: profileOf,
	}
}

// fundingProfileResolver links the provider id to the contract's interval profile. The key is
// the scope, never the ticker: a ticker-keyed state would merge contracts from different venues.
// The declared step does not enter, because this API publishes no settlement interval; only the
// measured step from the venue path, or unavailability, remains.
func (p *CoinalyzePoller) fundingProfileResolver(resolved map[string]coinalyzeTarget) func(string) fundingProfile {
	return func(providerSymbol string) fundingProfile {
		target, ok := resolved[providerSymbol]
		if !ok {
			return fundingProfile{Step: derivatives.NUnavailable}
		}
		scope := SymbolScope{ExchangeID: p.exchange, MarketType: p.marketType, Symbol: target.internal}
		measured, hasMeasured := p.fundingProfiles.measuredFor(scope)
		return resolveFundingInterval(measured, hasMeasured, nil)
	}
}

func (s *coinalyzePointSet) at(symbol string, tsMs int64) *derivatives.DerivativesSnapshot {
	buckets, ok := s.bySymbol[symbol]
	if !ok {
		buckets = make(map[int64]*derivatives.DerivativesSnapshot)
		s.bySymbol[symbol] = buckets
	}
	snapshot, ok := buckets[tsMs]
	if !ok {
		snapshot = &derivatives.DerivativesSnapshot{Timestamp: tsMs}
		buckets[tsMs] = snapshot
	}
	return snapshot
}

// applyOpenInterest stores the bucket's OPEN in OpenInterestBase. The asymmetry with
// applyFunding is deliberate: funding uses the close. The choice is per series, not per
// provider, and the measurement is summarized above fetchInto. It goes into the contracts field,
// not the notional one: with convert_to_usd=false the provider returns BASE_ASSET, measured
// against Binance's /fapi/v1/openInterest on the same day.
func (s *coinalyzePointSet) applyOpenInterest(symbol string, tsMs int64, openValue float64) {
	value := openValue
	s.at(symbol, tsMs).OpenInterestBase = &value
}

// applyFunding converts the published percent into the fraction the existing source writes and
// derives the eight-hour equivalent. Measured on the same bar: 0.01 here is Binance's 0.0001,
// so writing the raw percent would multiply funding by 100 with no error anywhere.
//
// The normalization operand is `value`, never `closeValue`: the two names differ by a factor of
// 100, and swapping them makes the row internally incoherent. Normalization happens once here,
// where N is at hand; a second 8/N downstream would square the factor. It is per bucket, not per
// series: seeding writes up to 2,000 retroactive buckets while N describes recent settlements,
// and profileForBucket prevents old buckets from being normalized with today's cadence.
func (s *coinalyzePointSet) applyFunding(symbol string, tsMs int64, closeValue float64) {
	value := closeValue / coinalyzeFundingPercentPerUnit
	snapshot := s.at(symbol, tsMs)
	snapshot.FundingRate = &value
	// &value is the fraction. interestRate is nil because this response does not carry the
	// contract's rate, and borrowing one from another instant would fabricate the anchor.
	applyFundingNormalization(snapshot, &value, nil, profileForBucket(s.profileOf(symbol), tsMs))
}

// applyLongShort stores the ratio and the share pair. The shares are already in the written
// unit: Coinalyze publishes percent and the existing poller multiplies Binance's fraction by
// 100 before writing, so the conversion here is the identity. The pair must sum to 100; a pair
// that does not close is two halves from different instants, and the ratio survives the refusal.
func (s *coinalyzePointSet) applyLongShort(symbol string, tsMs int64, point coinalyze.CoinalyzeLongShortPoint) {
	snapshot := s.at(symbol, tsMs)

	ratio := point.R
	snapshot.LongShortRatio = &ratio

	if abs(point.L+point.S-100) > coinalyzeShareSumTolerance {
		logger.OrDiscard(s.log).Warn("coinalyze: long/short shares rejected; they do not sum to 100",
			"symbol", symbol, "timestamp_ms", tsMs, "long", point.L, "short", point.S)
		return
	}
	long, short := point.L, point.S
	snapshot.LSLongPct = &long
	snapshot.LSShortPct = &short
}

// symbols returns the symbols seen, sorted — map order is random, and a random write order
// makes any failure irreproducible.
func (s *coinalyzePointSet) symbols() []string {
	out := make([]string, 0, len(s.bySymbol))
	for symbol := range s.bySymbol {
		out = append(out, symbol)
	}
	sort.Strings(out)
	return out
}

// timestamps returns a symbol's buckets in ASCENDING order.
func (s *coinalyzePointSet) timestamps(symbol string) []int64 {
	buckets := s.bySymbol[symbol]
	out := make([]int64, 0, len(buckets))
	for tsMs := range buckets {
		out = append(out, tsMs)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (s *coinalyzePointSet) snapshot(symbol string, tsMs int64) *derivatives.DerivativesSnapshot {
	return s.bySymbol[symbol][tsMs]
}
