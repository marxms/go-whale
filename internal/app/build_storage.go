package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/marxms/go-whale/internal/adapters/inbound/realtime"
	prommetrics "github.com/marxms/go-whale/internal/adapters/outbound/prometheus"
	"github.com/marxms/go-whale/internal/adapters/outbound/questdb"
	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/adapters/outbound/redisevents"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/candle/engine"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/ingestion/streaming"
	"github.com/marxms/go-whale/internal/platform/config"
)

// buildStorage constructs the hot tier, the candle engine, the optional durable tier
// and the derivatives store. It is the only place where those concrete adapters meet
// the consumer-owned ports.
func (a *App) buildStorage(cfg config.Config, tfReg *candle.TimeframeRegistry, metricsProvider *prommetrics.Provider) error {
	// Redis store.
	//
	// WHAT THIS NUMBER IS: a FALLBACK, not the cap. The candle ZSET's resident retention is
	// resolved PER TIMEFRAME inside the store (`RedisStore.candleCapFor`), from
	// `candle_engine.redis_cap_by_timeframe` — a dedicated key, separate from `buffer_sizes`.
	//
	// The fallback comes from the SINGLE SOURCE (config.DefaultResidentDepth), not from a literal
	// of its own: this point used to declare 200 while production runs 800.
	candleCap := config.DefaultResidentDepth
	if v, ok := cfg.CandleEngine.BufferSizes["default"]; ok && v > 0 {
		candleCap = v
	}
	// Construction happens before Run and therefore has no caller context; the store's own
	// 5s ping ceiling bounds startup.
	redisStore, err := redisstore.NewRedisStore(context.Background(), cfg, candleCap, tfReg, a.log)
	if err != nil {
		return fmt.Errorf("failed to initialize redis store: %w", err)
	}
	a.redisStore = redisStore
	a.serverProbe = redisstore.NewRedisServerProbe(redisStore.Client())

	// Realtime outbound event producer. Publishes candle.close on the prefixed
	// Redis bus, best-effort: a publish never blocks ingestion nor fails the persistence
	// path. Async single-owner publisher: enqueue never blocks the caller, and one owner
	// goroutine per channel preserves the per-publisher ordering that
	// realtime-event-bus:11,18-21 requires.
	a.realtimeProducer = realtime.NewAsyncProducerWithLogger(
		redisevents.NewPublisher(redisStore.Client()),
		cfg.RealtimeChannelPrefix,
		func() int64 { return time.Now().UnixMilli() },
		a.realtimeMetrics,
		a.log,
	)

	// Tiered store and CandleEngine.
	//
	// The concrete composition is built here, at the root. Its fields are candle-owned
	// PORTS, so every consumer below receives only the capability it declares: ingestion
	// gets a writer, recovery a seed writer, the HTTP routes their readers.
	store := &redisstore.CombinedStore{
		Writer:     redisStore,
		Reader:     redisStore,
		Catalog:    redisStore,
		SeedWriter: redisStore,
		Redis:      redisStore,
	}
	a.store = store

	if cfg.CandleEngine.Enabled {
		a.candleEngine = engine.NewCandleEngine(cfg.CandleEngine.BufferSizes, a.log)
		a.candleEngine.SetMetrics(a.engineMetrics)
		a.tieredStore = engine.NewTieredStore(a.candleEngine, redisStore, cfg.CandleEngine, a.log)
		a.tieredStore.SetMetrics(a.engineMetrics)
		store.Writer = a.tieredStore
		store.Reader = a.tieredStore
		store.Catalog = a.tieredStore
		store.SeedWriter = a.tieredStore

		// Chart WebSocket edge. Built when BOTH the ticket secret and the gateway mint
		// token are configured, OR the explicit local escape hatch is set. Otherwise the
		// edge stays disabled (fail-closed, no idle goroutines).
		if (cfg.RealtimeWSTicketSecret != "" && cfg.RealtimeWSMintToken != "") || cfg.RealtimeWSAuthDisabled {
			if cfg.RealtimeWSAuthDisabled {
				a.logger().Warn("GOWHALE_REALTIME_WS_AUTH_DISABLED=true; the WS chart edge is open to any origin")
			}
			nowMs := func() int64 { return time.Now().UnixMilli() }
			a.hub = realtime.NewHub(a.candleEngine, nowMs,
				realtime.WithConnMaxAge(time.Duration(cfg.RealtimeWSMaxConnAgeS)*time.Second),
				realtime.WithMaxMembers(cfg.RealtimeWSMaxConnections),
				realtime.WithMaxSubscriptions(cfg.RealtimeWSMaxScopes))
			a.wsServer = realtime.NewServer(a.hub, []byte(cfg.RealtimeWSTicketSecret), []byte(cfg.RealtimeWSMintToken),
				cfg.RealtimeWSAllowedOrigins, time.Duration(cfg.RealtimeWSTicketTTLS)*time.Second, nowMs, cfg.RealtimeWSAuthDisabled)
		}

		if cfg.QuestDB.Enabled {
			qdbStore, qdbErr := a.buildColdTier(cfg.QuestDB, a.coldTierConstructor())
			if qdbErr != nil {
				// CONTRACT DIVERGENCE: the start ends here. See buildColdTier.
				return qdbErr
			}
			if qdbStore != nil {
				a.qdbStore = qdbStore
				qdbStore.SetMetrics(a.questdbMetrics)
				coldStore := qdbStore
				metricsProvider.SetColdFlushAgeSource(func() float64 {
					return time.Since(coldStore.Stats().LastFlushTime).Seconds()
				})
				a.qdbPipeline = questdb.NewPipeline(qdbStore, 16384, 5*time.Second, a.log)
				a.qdbPipeline.SetMetrics(a.questdbMetrics)
				a.qdbPipeline.Start()
				a.tieredStore.SetColdStore(qdbStore)
				// Route seed / re-seed / gap-recovery / continuity CLOSED candles to the cold tier
				// through the pipeline (single-writer-safe), retention-clamped.
				//
				// The clamp receives the SAME policy this table's retention applies, and not a
				// duration assembled here.
				coldBoundary, _ := candlesPolicyFromConfig(cfg)
				a.tieredStore.SetColdAppender(a.qdbPipeline, coldBoundary)
				// Standing cold DATA-completeness invariant (closes the +20h blind spot).
				a.coldCompleteness = engine.NewColdCompletenessMonitor(qdbStore, redisStore, tfReg, 0, a.log)
				a.coldCompleteness.SetMetrics(a.engineMetrics)
			}
		}
	}

	// Derivatives store.
	a.derivStore = redisstore.NewDerivativesRedisStore(redisStore.Client(), cfg.Derivatives, tfReg, a.log)
	a.derivStore.SetMetrics(a.redisMetrics)
	// A missing durable tier is a declared coarse absence, never a native ruler.
	// Avoid a typed nil in the reader interface and install before consumers start.
	var coarseCold derivatives.ColdReader
	if a.qdbStore != nil {
		coarseCold = a.qdbStore
	}
	a.derivStore.SetCoarseSeriesReader(derivatives.NewCoarseDerivativesHistory(coarseCold).Series)

	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// THE TWO FAILURE CLASSES OF THE DURABLE TIER
//
// Treating ANY QuestDB failure the same way — `qdbStore = nil`, a loud log, the application
// continues — is defensible for a transient unavailability.
//
// The same door swallowed a failure of the opposite nature. With `qdbStore = nil`,
// `sizing_sheet.go` returns `SoleCopyRisk` with `Evaluated` at its zero value and `gates.go`
// downgrades the verdict from `GateFatal` to `GateError` — A DDL ERROR DISABLED THE GATE THAT
// PROTECTS THE 129,190 SOLE-COPY CANDLES.
//
//	THE SEPARATION:
//
//	UNAVAILABILITY  (connection refused, timeout, WAL in recovery)
//	  → degrades loudly, as always. There is no wrong assertion about the data, only absence.
//	CONTRACT DIVERGENCE  (read partition ≠ declared, missing dedup)
//	  → FATAL for the start. The schema is wrong; continuing means writing wrong data
//	    irreversibly — and, worse, with the guard disarmed.
// ─────────────────────────────────────────────────────────────────────────────

// questDBConstructor is the seam that makes the separation testable without a live QuestDB.
// In production it is `questdb.NewStore`.
type questDBConstructor func(config.QuestDBConfig) (*questdb.Store, error)

// coldTierConstructor returns the injected seam, or the production one.
func (a *App) coldTierConstructor() questDBConstructor {
	if a.newColdTier != nil {
		return a.newColdTier
	}
	return func(cfg config.QuestDBConfig) (*questdb.Store, error) {
		return questdb.NewStore(cfg, a.log)
	}
}

// buildColdTier is the package-level form used by tests: it classifies a cold-tier
// startup failure with the zero-value metrics port. Production goes through
// App.buildColdTier so the startup-failure counter reaches the real registry.
func buildColdTier(cfg config.QuestDBConfig, construct questDBConstructor, log *slog.Logger) (*questdb.Store, error) {
	return (&App{log: log}).buildColdTier(cfg, construct)
}

// buildColdTier constructs the durable tier and CLASSIFIES the failure.
//
//	(store, nil)  the tier came up and is verified
//	(nil,   nil)  unavailability: the process continues WITHOUT a durable tier
//	(nil,   err)  contract divergence: `NewApp` returns the error and the process does not start
func (a *App) buildColdTier(cfg config.QuestDBConfig, construct questDBConstructor) (*questdb.Store, error) {
	store, err := construct(cfg)
	if err != nil {
		return nil, a.degradeOrRefuse(nil, err)
	}

	// DEDUP+WAL on the exact keys is a HARD precondition — without it, a seed/backfill rewrite
	// DUPLICATES instead of overwriting. Closure with `defer cancel()` so the context is ALWAYS
	// released.
	vErr := func() error {
		verifyCtx, verifyCancel := context.WithTimeout(context.Background(), questdb.ColdTierBudget)
		defer verifyCancel()
		return store.VerifyDedup(verifyCtx)
	}()
	if vErr != nil {
		return nil, a.degradeOrRefuse(store, fmt.Errorf(
			"DEDUP/WAL verification failed (cold disabled to avoid duplicate-prone writes): %w", vErr))
	}

	// Converge the additive columns. NON-FATAL by declared decision: the cold tier serves without
	// the column (the read degrades to the old shape and heals itself on the first miss).
	func() {
		schemaCtx, schemaCancel := context.WithTimeout(context.Background(), questdb.ColdTierBudget)
		defer schemaCancel()
		if schemaErr := store.EnsureSchema(schemaCtx); schemaErr != nil {
			a.logger().Warn("questdb EnsureSchema incomplete; cold reads continue without the missing column",
				"error", schemaErr)
		}
		// THE OTHER TABLE. `EnsureSchema` speaks of `candles` and only it; the `derivatives` table
		// has no schema mechanism, and it is the table that grows.
		store.ReportDerivativesSchema(schemaCtx)
	}()

	return store, nil
}

// degradeOrRefuse applies the classification to ONE durable-tier startup error.
//
// It returns an error only for the contract class. For the unavailability class the error is
// HANDLED here — reported loudly and counted — and it returns `nil`, which is the degradation that
// has always existed. An error is logged OR propagated, and each class does exactly one of the two.
func (a *App) degradeOrRefuse(store *questdb.Store, err error) error {
	if store != nil {
		_ = store.Close()
	}
	if errors.Is(err, questdb.ErrSchemaContract) {
		return fmt.Errorf("QuestDB: the schema DIVERGES from the contract — startup is refused instead of "+
			"proceeding with a NIL durable tier, which is the state that disarms the sole-copy guard "+
			"(sizing_sheet returns Evaluated on the zero value and the gate drops from FATAL to ERROR): %w", err)
	}
	// QuestDB unavailable. The application continues with the hot tier (Redis), but COLD durability
	// is gone: closed candles will not persist. The degradation is loud and observable through the
	// startup-failure counter.
	a.logger().Error("questdb init failed; cold storage disabled and candles will not persist", "error", err)
	a.streamMetrics.IncStartupFailure("questdb", "-", streaming.StartupStageInit)
	return nil
}
