package app

import (
	"context"
	"math"
	"time"

	"github.com/marxms/go-whale/internal/adapters/inbound/scheduler"
	"github.com/marxms/go-whale/internal/adapters/outbound/coinalyze"
	"github.com/marxms/go-whale/internal/candle/engine"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/derivatives/collector"
	"github.com/marxms/go-whale/internal/market"
)

// THE COINALYZE COLLECTION IS WIRED AT THE COMPOSITION ROOT.
//
// Configuring the key alone left nothing instantiating the collector: the reachability gate passed
// because tests reach everything, while `main()` called nobody — the "sensor built and cable
// unplugged" pattern. Without this wiring the whole second provenance is code that never runs: the
// long history never reaches the screen.

// THE PER-MINUTE CEILING DOES NOT LIVE HERE, and the absence is the decision. It has ONE place —
// `coinalyze.CoinalyzeCallsPerMinute` — and BOTH readings come from there: this wiring's bucket,
// which inherits it through the constructor default (`coinalyzeBudgetOptions`), and the collector's
// admission floor. Accounting is local because a 200 response from this API carries no limit header
// — the only signal is `Retry-After` on a 429.

// coinalyzeBudgetOptions is the DECLARATION of this collection's bucket.
//
// THE CEILING IS NOT DECLARED HERE, and the omission is the mechanism: `NewCoinalyzeBudget` fills
// `CallsPerMinute` with `coinalyze.CoinalyzeCallsPerMinute` when the field is absent — the bucket's
// own declaration, the same symbol the collector's admission floor reads. Naming it here would be
// the second copy the design exists to remove, and it cannot diverge because it does not exist.
//
// THE REACHABILITY RATCHET PROTECTS THE CABLE: a test asserts the CONTENT of this declaration (it
// names no ceiling) and the effective ceiling of the bucket built from it; `deadcode` without
// `-test` asserts it is still what production uses — replacing its call with an inline build makes
// this function unreachable from `main`, and the gate refuses.
func coinalyzeBudgetOptions() coinalyze.CoinalyzeBudgetOptions {
	return coinalyze.CoinalyzeBudgetOptions{}
}

// declareCoinalyzeCadence declares the cadence of the CURRENT load. The derivation and the report
// live in internal/derivatives/collector; here it only reads the fleet and the timeframes
// production uses.
//
// PER CYCLE, NOT AT STARTUP: the fleet is published by discovery AFTER boot — at the startup
// instant it is still the static one-symbol list while the screen shows thirty. Declaring the
// static fleet's cadence would declare another system's load.
func (a *App) declareCoinalyzeCadence(ctx context.Context) {
	fleet := a.coinalyzeFleet(ctx)
	if len(fleet) == 0 {
		fleet = collector.CoinalyzeSymbolsOf(a.coinalyzeScopes())
	}
	timeframes := a.durableTimeframes()
	declared := collector.CoinalyzeDeclaredCadence(collector.CoinalyzeCycleInterval, len(fleet), len(timeframes))
	collector.ReportCoinalyzeCadence(collector.CoinalyzeCycleInterval, declared, len(fleet), len(timeframes), a.log)
}

// durableTimeframes are the buckets this collector fills, DERIVED instead of typed.
//
// The literal `{"1h", "4h", "1d"}` that used to live here was a third answer to a question with two
// legitimate sources: the product's timeframe registry and what the durable source can serve. While
// it existed, the two disagreements it hid cost dearly — `1w` inside it produced a WARN every cycle
// until the LAN deploy showed the provider does not serve weekly, and `30m` outside it lost 254
// hours of series when the cap reached the only copy.
//
// THE SHORT INTERVALS NOW ENTER, AND THAT IS THE FEATURE. The argument that "the existing source
// already covers the whole window" was true about the WINDOW and false about DURABILITY: what the
// existing source covers is the hot tier, and the hot tier is what the cap erases.
//
// The reach MEASURED against the real API, on the three already covered:
//
//	1d  30 days → 2000 points (5.5 years · 67x)
//	4h  30 days → 2003 points (11 months · 11x)
//	1h  30 days → 2009 points (~3 months · 2.8x)
//
// `1w` stays OUT, and now by derivation rather than edit: the durable source does not serve it in
// any feed. It still exists on the screen through the existing source, which writes every
// timeframe; what it does not gain is deep history.
func (a *App) durableTimeframes() []string {
	return collector.DurableTimeframes(a.cfg.CandleEngine.Timeframes)
}

// coinalyzeScopes converts the configured exchanges into the collector's typed scopes.
func (a *App) coinalyzeScopes() []collector.CoinalyzeScope {
	scopes := make([]collector.CoinalyzeScope, 0, len(a.cfg.Exchanges))
	for _, ex := range a.cfg.Exchanges {
		marketType, err := market.FromConfig(ex.MarketType)
		if err != nil {
			continue
		}
		scopes = append(scopes, collector.CoinalyzeScope{
			ExchangeID:       ex.ID,
			MarketType:       marketType,
			DiscoveryEnabled: ex.Discovery.Enabled,
			MaxAssets:        ex.Discovery.MaxAssets,
			Symbols:          ex.Symbols,
		})
	}
	return scopes
}

// fleetScopes converts the configured exchanges into the collector's fleet-readiness
// scopes.
func (a *App) fleetScopes() []collector.FleetScope {
	scopes := make([]collector.FleetScope, 0, len(a.cfg.Exchanges))
	for _, ex := range a.cfg.Exchanges {
		marketType, err := market.FromConfig(ex.MarketType)
		if err != nil {
			continue
		}
		scopes = append(scopes, collector.FleetScope{
			ExchangeID:       ex.ID,
			MarketType:       marketType,
			DiscoveryEnabled: ex.Discovery.Enabled,
			MaxAssets:        ex.Discovery.MaxAssets,
		})
	}
	return scopes
}

// startCoinalyze wires and starts the second provenance, or explains why it did not.
//
// A MISSING KEY DISABLES COLLECTION, NOT THE PROCESS. The streamer serves price and derivatives
// from the existing source; refusing the whole boot for a credential of a second source would trade
// a degradation for a crash. The decision lives in the collector.
func (a *App) startCoinalyze(ctx context.Context) {
	cfg := a.cfg.Coinalyze
	admission := collector.DecideCoinalyze(cfg.Enabled, string(cfg.APIKey) != "", a.coinalyzeScopes())
	if !admission.Start {
		a.logger().Warn("coinalyze collection not started", "reason", admission.Reason)
		return
	}
	apiKey := string(cfg.APIKey)
	symbols := admission.Symbols

	// The explicit conversion is the ONLY point where the secret stops being `Secret`. The type
	// exists so an accidental `%v` prints "(redigido)" instead of the key, and leaving it in one
	// place only keeps that guarantee verifiable by reading.
	client := coinalyze.NewCoinalyzeClient(apiKey, coinalyze.WithCoinalyzeLogger(a.log))
	// THE BUDGET GETS A NAME because it is the LOCAL ACCOUNTING the cycle signal reads: how much
	// this process spent and how many refusals it took. Anonymous inside the assembly, it would
	// exist and nobody would know what it counted.
	budgetOpts := coinalyzeBudgetOptions()
	budgetOpts.Log = a.log
	budget := coinalyze.NewCoinalyzeBudget(budgetOpts)
	poller := collector.NewCoinalyzePoller(collector.CoinalyzePollerOptions{
		Client: client,
		Catalog: coinalyze.NewCoinalyzeSymbolCatalog(coinalyze.CoinalyzeSymbolCatalogOptions{
			Client:         client,
			ExchangeCode:   coinalyze.CoinalyzeBinanceCode,
			ReloadInterval: coinalyze.CoinalyzeCatalogReloadInterval,
			Log:            a.log,
		}),
		Budget: budget,
		// THE DURABLE TABLE, the ONLY destination of this collection. It is the half that answers a
		// window beyond the thirty-day cap — the reason this collection exists. Without this wiring
		// the writer had no caller and `SELECT count() FROM derivatives` returned zero with
		// collection enabled.
		//
		// The `derivatives:coinalyze:series` ZSET is gone: it was write-only (271.0 MiB, zero readers
		// proven by three lenses). With it gone, a NULL `a.qdbStore` stopped being degradation and
		// became an error — `WriteBatch` refuses the batch instead of silently discarding it,
		// because no tier is left to receive it.
		Store:       derivatives.NewDerivativesTieredWriter(a.coldDerivStore()),
		Exchange:    market.Binance.String(),
		MarketType:  market.Future.String(),
		Symbols:     symbols,
		SymbolsFunc: a.coinalyzeFleet,
		Timeframes:  a.durableTimeframes(),
		// THE CADENCE TRAVELS ALONG because it is the denominator of the admission pace. Without it
		// the collector paces at the provider floor and has nothing to compare against — the
		// mechanism would exist with the cable unplugged.
		CycleInterval: collector.CoinalyzeCycleInterval,
		// THE PROCESS'S INTERVAL PROFILE, and here it stops being a mechanism with the cable
		// unplugged. This source does not measure settlement cadence — it only has OHLC aligned to
		// the requested timeframe — so without this line every point from it would come out with
		// `n_step=unavailable` and no eight-hour equivalent, the defect measured on the LAN: half
		// the deep series arrives without a unit. The venue collector measures it, in the SAME
		// registry.
		FundingProfiles: a.fundingProfiles,
		// THE DURABLE SEEDED MARK, and it is what stops the long cycle from hurting. Without this
		// line the mechanism would exist with the cable unplugged: the mark would die with the
		// process again, and no deploy shorter than ~2 h would preserve the seed.
		SeedLedger: a.coinalyzeSeedLedger(),
		Log:        a.log,
	})

	a.logger().Info("coinalyze collection started",
		"symbols", len(symbols), "timeframes", a.durableTimeframes())

	a.discoveryWg.Add(1)
	go func() {
		defer a.discoveryWg.Done()
		spend := collector.NewCoinalyzeSpendMeter(budget, a.log)
		spend.SetMetrics(a.collectorMetrics)
		a.runCoinalyzeCycles(ctx, poller, spend)
	}()
}

// durableFillProbe assembles the fill checker, or nothing when there is no durable tier.
//
// THE GRACE PERIOD STARTS COUNTING HERE, at the instant collection turns on, and not at process
// start: collection is what fills the columns, and a clock that started before it would give it
// less time than the grace period declares.
func (a *App) durableFillProbe() *engine.DurableFillProbe {
	if a.qdbStore == nil {
		return nil
	}
	ligadaEm := time.Now()
	probe := engine.NewDurableFillProbe(engine.DurableFillProbeOptions{
		Counter:    a.qdbStore,
		Timeframes: a.durableTimeframes(),
		CycleFloor: collector.CoinalyzeCycleInterval,
		Elapsed:    func() time.Duration { return time.Since(ligadaEm) },
		// THE THREE DIMENSIONS PER-COLUMN COUNTING CANNOT SEE. Without this reader they disappear
		// from the derived label instead of becoming zero — and here the reader exists, so leaving
		// it out would be the checker promising four and measuring one.
		Spans: a.qdbStore,
		// AND THE FIFTH, which none of the others sees because it is about TWO columns of the same
		// row. Leaving it out is worse than one dimension fewer: the incoherent row adds to
		// `funding_rate_8h` fill, so the monitor rewards the defect.
		Coherence: a.qdbStore,
		Log:       a.log,
	})
	if probe != nil {
		probe.SetMetrics(a.engineMetrics)
	}
	return probe
}

// runCoinalyzeCycles runs the cycle until the context falls.
//
// A FAILING CYCLE DOES NOT STOP THE NEXT ONES: the provider is external and an outage must not cost
// the whole day's collection. The error is recorded and the next tick arrives.
//
// THE CLOCK DOES NOT LIVE HERE. The ticker, the initial wait for the fleet, the duration measurement
// and the cancellation belong to the scheduler; this method only declares what each cycle does and
// what runs after it. There is no second scheduling path: the scheduler is the only one that touches
// the clock.
func (a *App) runCoinalyzeCycles(
	ctx context.Context, poller *collector.CoinalyzePoller, spend *collector.CoinalyzeSpendMeter,
) {
	// THE FILL CHECKER MOVES WITH THE CYCLE, not with the startup.
	//
	// At the boot instant the new columns have zero rows BY DEFINITION, so a check only there would
	// report the grace period forever and never find anything. Running after each cycle, the grace
	// clock advances along with what should have filled the columns — and the question "why is this
	// field still empty" gets an answer at the moment it becomes legitimate.
	//
	// It is born on the FIRST pass, not before the fleet wait: the grace period starts counting at
	// the instant collection turns on.
	var fill *engine.DurableFillProbe

	cycle := func(ctx context.Context) error {
		if fill == nil {
			fill = a.durableFillProbe()
		}
		return poller.PollOnce(ctx)
	}

	loop := scheduler.NewLoop(scheduler.Options{
		Name:     "Coinalyze",
		Interval: collector.CoinalyzeCycleInterval,
		// The first cycle is IMMEDIATE — but only after the fleet exists.
		//
		// "Immediate" alone was not enough, and the price was MEASURED on the LAN: at the boot
		// instant discovery has not populated `active_symbols`, resolution falls back to the static
		// list, and the deep pass covered ONE symbol while the screen showed thirty. The next cycle
		// is only an hour away, so the deploy spent an hour delivering a thirtieth of the feature.
		//
		// AND IF THE CONTEXT FELL DURING THE WAIT, THERE IS NO FIRST CYCLE: the scheduler does not
		// call `Cycle`, preventing resolution, catalog and the three requests from running against
		// a dead context.
		BeforeFirst: func(ctx context.Context) bool {
			a.awaitFleet(ctx)
			if ctx.Err() != nil {
				a.logger().Info("coinalyze cycle ended while waiting for the fleet")
				return false
			}
			return true
		},
		Cycle: cycle,
		// THE SPEND MOVES WITH THE CYCLE, and the difference is taken here: the duration is already
		// measured by the scheduler, and the other two quantities — calls spent and refusals
		// received — come from the budget accounting.
		AfterCycle: func(ctx context.Context, report scheduler.Report) {
			spend.Cycle(report.Elapsed)
			a.declareCoinalyzeCadence(ctx)
			fill.Report(ctx)
			fill.ReportIntegrity(ctx)
		},
		OnCycle: func(_ context.Context, report scheduler.Report) {
			a.reportCoinalyzeCycle(report)
		},
		OnStop: func() {
			a.logger().Info("coinalyze cycle ended")
		},
		Log: a.log,
	})

	loop.Run(ctx)
}

// ─────────────────────────────────────────────────────────────────────────────
// CYCLE-DURATION SENSOR
//
// The cycle above calls `PollOnce` SYNCHRONOUSLY inside the ticker select. A cycle longer than the
// interval does not run late: the runtime does NOT queue the ticks that fall inside it, it
// DISCARDS them, and the effective spacing between cycles becomes the cycle duration. Nothing
// measured that duration — neither in log nor in metric — so the overrun was invisible by
// construction: the operator would only see it in a series that grows slower than the declared
// cadence promises.
//
// The check is a CONTINUOUS SIGNAL, not a startup check: the cost of a cycle depends on how many
// symbols are still to be seeded, which is runtime state.
// ─────────────────────────────────────────────────────────────────────────────

// reportCoinalyzeCycle puts the measured cycle where an operator sees it.
//
// THE MEASUREMENT COMES FROM THE SCHEDULER: `scheduler.Report` carries the real duration and the
// interval the cycle was scheduled at. The report stays here because the names and metrics of this
// collection belong to its wiring, and the scheduler knows no market vocabulary.
//
// THE DURATION ALWAYS COMES OUT, and the overrun comes out with the NUMBER. Measuring and reporting
// only the error would leave the originating defect intact: a cycle that overruns returns `nil`,
// because overrunning is not failing — it is fitting badly.
func (a *App) reportCoinalyzeCycle(r scheduler.Report) {
	a.metrics.observeCoinalyzeCycle(r.Elapsed.Seconds())

	switch {
	case r.Overran():
		a.metrics.incCoinalyzeCycleOverrun()
		a.logger().Warn("coinalyze cycle overran the scheduled interval",
			"elapsed", r.Elapsed.Round(time.Second),
			"interval", r.Interval,
			"lap_factor", math.Round(r.LapFactor()*100)/100)
	case r.Err != nil:
		a.logger().Warn("coinalyze cycle failed",
			"elapsed", r.Elapsed.Round(time.Second), "interval", r.Interval, "error", r.Err)
	default:
		a.logger().Info("coinalyze cycle completed",
			"elapsed", r.Elapsed.Round(time.Second), "interval", r.Interval)
	}
}

// coinalyzeSeedLedger returns where the seeded mark is derived from, or nil without a durable
// tier.
//
// An interface carrying a nil pointer is NOT nil — see coldDerivStore. Nil here is the declared
// degradation: without a durable table there is no depth to read, and the seed is valid per
// process again.
func (a *App) coinalyzeSeedLedger() collector.CoinalyzeSeedLedger {
	if a.qdbStore == nil {
		return nil
	}
	return a.qdbStore
}

// coldDerivStore returns the durable tier, or nil when QuestDB did not come up.
//
// An interface that carries a nil pointer is NOT nil, and an `if w.cold == nil` on the other side
// would not catch it — the cycle would try to write and panic. Converting only when there is a
// store is what keeps the degradation a degradation.
func (a *App) coldDerivStore() derivatives.ColdWriter {
	if a.qdbStore == nil {
		return nil
	}
	return a.qdbStore
}

// derivWindowReader assembles the window reader, or returns nil when there is no durable tier.
//
// It is what makes the long history REACH the screen: without this wiring the collection writes the
// durable table and the read keeps serving only the ZSET cap — thirty days on the daily, the
// complaint this collection exists to resolve.
func (a *App) derivWindowReader() *derivatives.DerivativesWindowReader {
	// THE TEST SEAM, and it exists because the assembly is what must be checked: without it,
	// asserting anything about the production router would require a real QuestDB.
	if a.windowOverride != nil {
		return a.windowOverride
	}
	if a.qdbStore == nil || a.derivStore == nil {
		return nil
	}
	return derivatives.NewDerivativesWindowReader(a.derivStore, a.qdbStore)
}

// fleetRegistry is where the active fleet is read from.
//
// An INTERFACE, not the `*redisstore.RedisStore` directly, because returning `nil` from either
// `coinalyzeFleet` or `fleetCounts` would return the system to "1 of 30 active" with the suite green
// AND both ratchets green. The poller half had a test; the app half could not have one because it
// required a Redis.
type fleetRegistry interface {
	GetActiveSymbols(ctx context.Context, exchange, marketType string) ([]string, error)
}

// fleetSource returns the fleet registry, or nil when there is no Redis.
func (a *App) fleetSource() fleetRegistry {
	if a.fleetOverride != nil {
		return a.fleetOverride
	}
	if a.redisStore == nil {
		// An interface carrying a nil pointer is NOT nil. See coldDerivStore.
		return nil
	}
	return a.redisStore
}

// coinalyzeFleet returns the ACTIVE futures fleet, read from the subscription registry.
//
// The LAN and production configurations assemble the fleet by DISCOVERY (`max_assets`), and
// discovery produces no `symbols:`. Reading only the static list delivered deep history for ONE
// asset while the screen showed thirty — measured on the LAN.
//
// Empty (Redis down, registry not yet populated) makes the poller fall back to the static list,
// which is the behavior of a hand-declared fleet.
func (a *App) coinalyzeFleet(ctx context.Context) []string {
	registro := a.fleetSource()
	if registro == nil {
		return nil
	}
	return collector.ReadCoinalyzeFleet(ctx, a.fleetScopes(), registro.GetActiveSymbols, a.log)
}

// awaitFleet blocks until discovery publishes, or until the ceiling.
//
// Hitting the ceiling is NOT an error — it proceeds with whatever exists, which is the configured
// list. What this wait avoids is seeding a thirtieth of the fleet and only correcting it an hour
// later.
func (a *App) awaitFleet(ctx context.Context) {
	if a.fleetSource() == nil {
		// Without a registry there is nothing to wait for: the fleet is the configured list.
		return
	}
	// Fields default to the constant: a test needs to exercise the wait in milliseconds, and what is
	// asserted is the BEHAVIOR at the target and at the ceiling, not the duration.
	teto, assentamento := a.fleetWait, a.fleetSettle
	if teto <= 0 {
		teto = collector.CoinalyzeFleetWait
	}
	if assentamento <= 0 {
		assentamento = collector.CoinalyzeFleetSettle
	}

	// THE CLOCKS BELONG TO THE SCHEDULER. Only the QUESTION lives here — "has the fleet been
	// published?" — which is this collector's policy, not scheduling.
	var counts map[string]int
	outcome := scheduler.Await(ctx, teto, assentamento, func(ctx context.Context) bool {
		counts = a.fleetCounts(ctx)
		return collector.FleetReady(a.fleetScopes(), counts)
	})

	switch outcome {
	case scheduler.AwaitReady:
		a.logger().Info("coinalyze fleet published", "counts", counts)
	case scheduler.AwaitTimedOut:
		a.logger().Warn("coinalyze discovery did not publish in time; seeding with what exists",
			"timeout", teto, "counts", counts)
	case scheduler.AwaitCancelled:
		// Shutdown during the wait is silent: the caller will declare the termination, and an extra
		// line here would only describe the shutdown itself.
	}
}

// fleetCounts reads how many futures symbols each exchange published. The rule for which scopes
// count (and how an unreadable exchange is handled) lives in the collector.
func (a *App) fleetCounts(ctx context.Context) map[string]int {
	registro := a.fleetSource()
	if registro == nil {
		return nil
	}
	return collector.FleetCounts(ctx, a.fleetScopes(), registro.GetActiveSymbols)
}
