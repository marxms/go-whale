// Package app is the composition root for the GoWhale process: it builds the object
// graph, owns the lifecycle and binds consumer ports to their adapters.
package app

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marxms/go-whale/internal/adapters/inbound/realtime"
	binancesdk "github.com/marxms/go-whale/internal/adapters/outbound/binance"
	"github.com/marxms/go-whale/internal/adapters/outbound/binancerest"
	"github.com/marxms/go-whale/internal/adapters/outbound/binancews"
	"github.com/marxms/go-whale/internal/adapters/outbound/exchange"
	"github.com/marxms/go-whale/internal/adapters/outbound/questdb"
	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/adapters/outbound/venue"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/candle/engine"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/derivatives/collector"
	"github.com/marxms/go-whale/internal/health"
	"github.com/marxms/go-whale/internal/ingestion"
	"github.com/marxms/go-whale/internal/ingestion/streaming"
	"github.com/marxms/go-whale/internal/market"
	"github.com/marxms/go-whale/internal/platform/config"
	logger "github.com/marxms/go-whale/internal/platform/logging"
	"github.com/marxms/go-whale/internal/platform/netdiag"
	"github.com/marxms/go-whale/internal/pruning"
	"github.com/marxms/go-whale/internal/recovery"
	"github.com/marxms/go-whale/internal/retention"

	httpapi "github.com/marxms/go-whale/internal/adapters/inbound/httpapi"
)

// These assertions are the compile-time proof that the adapter still implements each
// consumer-owned port.
var (
	_ ListingSource          = (*venue.Client)(nil)
	_ recovery.HistorySource = (*venue.Client)(nil)
	_ derivatives.Source     = (*venue.Client)(nil)
	_ market.DiscoverySource = venue.Discovery{}
)

// App is the top-level composition root: it holds the built graph and the seams tests need.
// Domain rules live in their owning packages.
type App struct {
	// klineFeed carries the venue's own candle; nil until the fleet is live and for venues
	// that publish no candle.
	//
	// Three goroutines touch this state, so it is never touched without klineMu: boot writes
	// it, the watchdog replaces it and discovery reads it. klineSymbols is the live fleet, not
	// the boot list, so discovery-found symbols survive a watchdog restart. klineScope is the
	// one scope this feed owns; startKlineFeed refuses any other.
	klineMu      sync.Mutex
	klineFeed    klineFeed
	newKlineFeed klineFeedFactory
	klineScope   string
	klineSymbols []string
	// klineGapHeads keeps the earliest head across partial failed replacement attempts.
	klineGapHeads klineGapHeads
	// klineBackoff spaces watchdog redials after repeated failures. The failure
	// count resets only after the feed has stayed healthy for StableAfter.
	klineBackoff      binancews.Backoff
	klineFailures     int
	klineHealthySince time.Time
	klineNextAttempt  time.Time
	// klinePendingAttach holds symbols a previous attach failed on: already subscribed and
	// seeded, so no later discovery cycle classifies them as new.
	klinePendingAttach []string
	cfg                config.Config
	// windowOverride replaces the window reader in tests. See derivWindowReader.
	windowOverride *derivatives.DerivativesWindowReader
	// fleetOverride replaces the fleet registry in tests. See fleetSource.
	fleetOverride fleetRegistry
	// fleetWait and fleetSettle override the fleet-wait timings in tests. Zero uses the
	// production constants. See awaitFleet.
	fleetWait   time.Duration
	fleetSettle time.Duration
	restBudget  *exchange.RestBudget
	redisStore  *redisstore.RedisStore
	// serverProbe reads facts only the server knows (maxmemory, policy, listpack entries); it is
	// injected because INFO memory has no double in miniredis.
	serverProbe  redisstore.RedisServerProbe
	store        *redisstore.CombinedStore
	tieredStore  *engine.TieredStore
	candleEngine *engine.CandleEngine
	qdbPipeline  *questdb.Pipeline
	qdbStore     *questdb.Store
	derivStore   *redisstore.DerivativesRedisStore
	handler      *ingestion.StreamTradeHandler
	manager      *ingestion.HybridManager
	// discovery resolves the venue symbol universe. It is a port so the core
	// does not depend on the adapter that implements the liquidity ranking.
	discovery market.DiscoverySource
	// nonCoinSymbols holds the venue markets whose underlying is TradFi (EQUITY,
	// COMMODITY, ...). They follow the venue's own trading hours, so the fleet-liveness
	// denominator excludes them; their WebSocket health and gap recovery still apply.
	nonCoinSymbols     sync.Map
	metrics            AppMetrics
	streamMetrics      streaming.Metrics
	binanceMetrics     binancesdk.Metrics
	ingestionMetrics   ingestion.Metrics
	engineMetrics      engine.Metrics
	collectorMetrics   collector.Metrics
	recoveryMetrics    recovery.Metrics
	redisMetrics       redisstore.Metrics
	questdbMetrics     questdb.Metrics
	venueMetrics       venue.Metrics
	realtimeMetrics    realtime.Metrics
	netdiagMetrics     netdiag.Metrics
	netdiagCorrelator  *netdiag.Correlator
	healthRedisMetrics health.RedisBudgetMetrics
	metricsCollector   *httpapi.MetricsCollector
	metricsHandler     http.Handler
	// livenessHysteresis requires two consecutive failing samples before a scope is
	// published as stale, so one missed read does not flip /health to 503.
	livenessHysteresis livenessHysteresis
	gapRecovery        *recovery.Manager
	derivPoller        *collector.DerivativesPoller
	// durableOrder reads the declared durable columns and the erosion-guarded ones; a test seam
	// for exercising the refusal.
	durableOrder func() (declared, guarded []string)
	// newColdTier constructs the durable store; nil in production. The seam sits at NewApp so a
	// contract error translated there has an assertion.
	newColdTier questDBConstructor
	// fundingProfiles is the process's funding-interval profile, born here because the poller is
	// recreated on restart. See collector.FundingProfileRegistry.
	fundingProfiles   *collector.FundingProfileRegistry
	derivPollerHealth *health.PollerHealth
	retentionMgr      *retention.Manager
	// gatesApproved is the license retention consults before every pass. It becomes true in
	// `Run`, after `preflightGates` approves — and never before.
	gatesApproved *atomic.Bool
	// bootMigration is the outcome of STARTUP STAGE 1 — the sole-copy migration, which runs at
	// CONSTRUCTION and not in `Run`. The rule lives in the QuestDB adapter.
	bootMigration    questdb.BootMigrationOutcome
	coldCompleteness *engine.ColdCompletenessMonitor
	// marginBrackets holds the tier-1 maintenance margin rate per symbol, reloaded on a
	// timer and served alongside the subscription catalog.
	marginBrackets    *binancerest.MarginBracketCatalog
	pruner            *pruning.Pruner
	hub               *realtime.Hub
	wsServer          *realtime.Server
	defaultExchange   string
	defaultMarketType string
	httpServer        *http.Server
	pprofServer       *http.Server
	streamCtx         context.Context
	streamCancel      context.CancelFunc
	discoveryWg       sync.WaitGroup
	backgroundWg      sync.WaitGroup
	tfReg             *candle.TimeframeRegistry

	// continuity detects a missing open-time bucket in the close stream without depending
	// on buffer/persist ordering. The rule lives in candle/engine; this field is the
	// process-lifetime instance.
	continuity     *engine.ContinuityTracker
	continuityOnce sync.Once

	// bootAt is when this process started. It identifies the one bucket per stream key that
	// cannot be assembled correctly: the bucket already open when watching began. A proxy for
	// this drifted and mis-fired the first close of every key.
	bootAt time.Time

	// realtimeProducer owns one goroutine per event channel; Shutdown must drain and stop them
	// or the queued tail is lost.
	realtimeProducer *realtime.AsyncProducer

	// shutdownOnce makes Shutdown idempotent: a second call returns the first outcome
	// instead of stopping the same stores twice.
	shutdownOnce sync.Once
	shutdownErr  error
	// shutdownTrace, when non-nil, receives the shutdown phase names in order. It is the
	// test seam that makes the ordering observable without replacing the subsystems.
	shutdownTrace *[]string

	// log is the ONE process logger, injected by cmd and passed down to every component
	// this graph constructs. A nil logger is a silent sink.
	log *slog.Logger
}

// logger returns the injected process logger, or a silent sink for a zero-value App built
// directly by a test.
func (a *App) logger() *slog.Logger { return logger.OrDiscard(a.log) }

// traceShutdown records one shutdown phase when a test is watching.
func (a *App) traceShutdown(phase string) {
	if a.shutdownTrace != nil {
		*a.shutdownTrace = append(*a.shutdownTrace, phase)
	}
}

// NewApp is the explicit graph assembly point: it builds each capability in order and hands
// the result to the lifecycle. Builders return errors instead of panicking.
func NewApp(cfg config.Config, tfReg *candle.TimeframeRegistry, log *slog.Logger, opts ...appOption) (*App, error) {
	// app is forward-declared so the OnCandleClose callback can reference the composition root;
	// it only dereferences fields at candle-close time, after construction completes.
	app := &App{
		newKlineFeed: newBinanceKlineFeed(log),
		// Once per process. See App.fundingProfiles.
		fundingProfiles: collector.NewFundingProfileRegistry(),
		log:             logger.OrDiscard(log),
	}
	for _, opt := range opts {
		opt(app)
	}

	metricsProvider, err := app.buildObservability()
	if err != nil {
		return nil, err
	}
	if err := app.buildStorage(cfg, tfReg, metricsProvider); err != nil {
		// A builder failure must not leave half a graph running: returning without releasing it
		// strands goroutines no lifecycle method can reach.
		app.releasePartiallyBuiltGraph()
		return nil, err
	}
	app.buildIngestion(cfg, tfReg)
	app.buildRecovery(cfg, tfReg)
	app.buildDerivatives()
	app.buildHTTP(cfg, tfReg, metricsProvider)

	app.cfg = cfg
	app.tfReg = tfReg
	app.bootAt = time.Now()
	app.continuity = engine.NewContinuityTracker(app.bootAt, app.metrics)

	// Every closed bucket and correction fans out through these whether or not the cold tier
	// came up; registering them inside the QuestDB branch disabled four Redis-only mechanisms
	// when QuestDB failed. See candle_sinks.go.
	app.wireCandleSinks()

	// The sole-copy migration runs after the schema and before Run.
	app.runBootMigration()

	return app, nil
}

// releasePartiallyBuiltGraph releases every resource NewApp may have created when a
// later builder refuses the boot. It is the failure-path sibling of Shutdown: Shutdown
// assumes the whole graph exists and is idempotent per process, while this runs at most
// once, on an App that will never be returned.
func (a *App) releasePartiallyBuiltGraph() {
	if a.realtimeProducer != nil {
		a.realtimeProducer.Stop()
	}
	if a.qdbPipeline != nil {
		// Construction failed before Run; the pipeline's own 30s ceiling bounds the drain.
		a.qdbPipeline.Stop(context.Background())
	}
	if a.qdbStore != nil {
		if err := a.qdbStore.Close(); err != nil {
			a.logger().Warn("questdb store close after failed construction", "error", err)
		}
	}
	if a.tieredStore != nil {
		if err := a.tieredStore.Close(); err != nil {
			a.logger().Warn("tiered store close after failed construction", "error", err)
		}
		return
	}
	if a.redisStore != nil {
		if err := a.redisStore.Close(); err != nil {
			a.logger().Warn("redis store close after failed construction", "error", err)
		}
	}
}

// wireCandleSinks registers the engine's close/revision fan-out. It is an explicit step of
// the graph assembly, not a side effect of a constructor.
func (a *App) wireCandleSinks() {
	if a.candleEngine == nil {
		return
	}
	a.candleEngine.OnCandleClose(a.onCandleClose)
	a.candleEngine.OnCandleRevised(a.onCandleRevised)
	a.candleEngine.OnSeedCandleRevised(a.onSeedCandleRevised)
	a.candleEngine.OnSeedGap(func(key engine.CompositeKey, timestamp int64) {
		if a.gapRecovery != nil {
			a.gapRecovery.EnqueueTargetedBucket(key.Exchange, key.MarketType, key.Symbol, key.Timeframe, timestamp)
		}
	})
}

// runBootMigration runs stage 1. The rule — completion criterion, per-boot ceiling,
// signal-cancellable budget and outcome translation — lives in the QuestDB adapter. This
// method decides WHEN the stage runs and turns the outcome into the boot log.
func (a *App) runBootMigration() {
	// Its own budget, cancellable by SIGTERM; the defer undoes both deadline and signal
	// registration.
	migrationCtx, stop := questdb.BootMigrationContext()
	defer stop()
	a.bootMigration = questdb.RunBootSoleCopyMigration(migrationCtx, a.soleCopyMigrationFor(a.redisStore, a.qdbStore), a.log)

	// The outcome is logged because it decides whether retention keeps the license.
	a.logger().Info("sole-copy migration at boot", "outcome", a.bootMigration)
}
