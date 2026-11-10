package app

import (
	"context"
	"log"
	"net/http"
	_ "net/http/pprof" //nolint:gosec // G108: handlers are served only on 127.0.0.1:6060 by startPprofServer.
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/marxms/go-whale/internal/adapters/inbound/scheduler"
	"github.com/marxms/go-whale/internal/adapters/outbound/questdb"
	"github.com/marxms/go-whale/internal/adapters/outbound/venue"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/candle/engine"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/derivatives/collector"
	"github.com/marxms/go-whale/internal/ingestion"
	"github.com/marxms/go-whale/internal/market"
	"github.com/marxms/go-whale/internal/platform/config"
	"github.com/marxms/go-whale/internal/recovery"
)

// Run starts every capability the graph assembled and blocks until a termination
// signal or the caller's context is canceled, then shuts the process down in order.
func (a *App) Run(ctx context.Context) error {
	// THE STARTUP GATE, SYNCHRONOUS AND BEFORE ANYTHING STARTS. Returning the error is what
	// stops the service from OPERATING.
	if err := a.preflightGates(ctx); err != nil {
		return err
	}

	// THE RETENTION LICENSE IS BORN HERE, and not before. The order is: the license first, the
	// loop after, and both exist only on the path where the gates approved — and where the
	// migration completed.
	a.releaseRetentionUnderMigrationLicence(ctx)

	if a.candleEngine != nil {
		var coldStore candle.ColdStore
		if a.qdbStore != nil {
			coldStore = a.qdbStore
		}
		engine.WarmCandleEngine(ctx, a.candleEngine, coldStore, a.redisStore, a.cfg)
	}

	a.metricsCollector.SetDerivativesPollerFlag(a.derivPollerHealth)

	streamCtx, streamCancel := context.WithCancel(ctx)
	a.streamCtx, a.streamCancel = streamCtx, streamCancel

	if a.gapRecovery != nil {
		if a.candleEngine != nil {
			a.gapRecovery.SetTimestampProvider(a.candleEngine)
		}
		a.manager.SetGapRecovery(a.gapRecovery)
		a.gapRecovery.Start(streamCtx)
	}

	a.manager.Start(streamCtx)

	a.startGoroutineMonitor(streamCtx)

	// Periodically measure cold DATA completeness (missing buckets per scope).
	if a.coldCompleteness != nil {
		a.coldCompleteness.Start(streamCtx)
	}

	if a.hub != nil {
		a.hub.Start()
	}

	// Margin brackets feed the liquidation model's maintenance margin. The loop logs
	// and keeps the last known table on failure, so a bad fetch never stops the service.
	if a.marginBrackets != nil {
		a.marginBrackets.Start(streamCtx)
	}

	router := a.newHTTPRouter(a.derivStore, a.derivStore, a.derivStore)
	a.httpServer = a.newHTTPServer(router)
	go func() {
		log.Printf("GoWhale listening on :%s", a.cfg.Port)
		if err := a.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			a.logger().Error("http server error", "error", err)
		}
	}()

	a.startPprofServer()

	go a.autoSubscribe(streamCtx)

	a.discoveryWg = sync.WaitGroup{}
	for _, exCfg := range a.cfg.Exchanges {
		if !exCfg.Enabled || !exCfg.Discovery.Enabled || exCfg.Discovery.RefreshInterval == "" {
			continue
		}
		interval, parseErr := time.ParseDuration(exCfg.Discovery.RefreshInterval)
		if parseErr != nil || interval < ingestion.MinDiscoveryRefreshInterval {
			a.logger().Warn("invalid discovery refresh interval; skipping periodic discovery",
				"refresh_interval", exCfg.Discovery.RefreshInterval,
				"exchange", exCfg.ID,
				"market_type", exCfg.MarketType)
			continue
		}
		a.discoveryWg.Add(1)
		go func(exCfg config.ExchangeConfig, interval time.Duration) {
			defer a.discoveryWg.Done()
			a.runPeriodicDiscovery(exCfg, interval)
		}(exCfg, interval)
	}

	// Always-on gap-recovery reconciliation backstop.
	if a.gapRecovery != nil {
		a.discoveryWg.Add(1)
		go func() {
			defer a.discoveryWg.Done()
			a.runPeriodicGapReconciliation(recovery.PeriodicBackstopInterval)
		}()
	}

	// Fleet liveness has its own cadence: the gap-recovery pass runs every 30 minutes,
	// and a health signal that old cannot page anyone. The sample is a few Redis reads.
	if a.metricsCollector != nil {
		a.discoveryWg.Add(1)
		go func() {
			defer a.discoveryWg.Done()
			a.runFleetLiveness(fleetLivenessInterval)
		}()
	}

	a.derivPoller = a.newDerivPoller(streamCtx)
	a.startDerivPollerLifecycle(streamCtx)

	// THE SECOND PROVENANCE, wired here and not merely built in tests.
	a.startCoinalyze(streamCtx)

	// THE SIZING SHEET. It runs AFTER the fleet exists.
	a.discoveryWg.Add(1)
	go func() {
		defer a.discoveryWg.Done()
		a.logSizingSheet(a.streamCtx)
	}()

	// The CONTINUOUS observer answers "does the plan KEEP fitting?".
	a.discoveryWg.Add(1)
	go func() {
		defer a.discoveryWg.Done()
		a.startRedisBudgetObserver(a.streamCtx)
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(quit)
	select {
	case <-quit:
	case <-ctx.Done():
	}

	a.logger().Info("starting graceful shutdown")
	return a.Shutdown(ctx)
}

// startPprofServer binds pprof to loopback only: operators reach it via
// `docker exec ... curl localhost:6060/debug/pprof/...`, so it is never exposed on the
// host network. Handlers are registered on DefaultServeMux by the blank net/http/pprof
// import above; passing nil uses that mux.
func (a *App) startPprofServer() {
	const pprofAddr = "127.0.0.1:6060"
	a.logger().Info("pprof debug server listening", "address", pprofAddr)
	a.pprofServer = &http.Server{
		Addr:              pprofAddr,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := a.pprofServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			a.logger().Error("pprof server error", "error", err)
		}
	}()
}

// startGoroutineMonitor logs goroutine count and manager stats every 15s.
// It exits when ctx is canceled.
func (a *App) startGoroutineMonitor(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()

		var lastCount int
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				numGoroutines := runtime.NumGoroutine()

				if lastCount > 0 && numGoroutines > lastCount*2 && numGoroutines > 200 {
					a.logger().Warn("goroutine leak detected",
						"previous", lastCount, "current", numGoroutines)
				}

				a.logger().Info("goroutine monitor",
					"goroutines", numGoroutines, "hybrid_manager", a.manager.GetStats())
				lastCount = numGoroutines
			}
		}
	}()
}

// newDerivPoller builds a derivatives poller bound to the PROCESS-lifetime funding
// profile registry.
//
// Both call sites go through here — the first build and every restart — because that
// is what makes "the profile survives the restart" true.
func (a *App) newDerivPoller(ctx context.Context) *collector.DerivativesPoller {
	poller := collector.NewDerivativesPoller(
		ctx,
		a.cfg.Derivatives,
		a.derivStore,
		a.cfg.Exchanges,
		a.tfReg,
		a.fundingProfiles,
		func(exchangeID string, marketType market.MarketType) (derivatives.Source, error) {
			client, err := venue.NewClientWithOptions(exchangeID, marketType, venue.Options{
				Budget:  a.restBudget,
				Log:     a.log,
				Metrics: a.venueMetrics,
			})
			if err != nil {
				return nil, err
			}
			return client, nil
		},
		a.discovery,
		a.log,
	)
	poller.SetMetrics(a.collectorMetrics)
	return poller
}

// newDerivLoop binds one poller to the scheduler's clock and startup policy.
func (a *App) newDerivLoop(poller *collector.DerivativesPoller) *scheduler.Loop {
	return scheduler.NewLoop(scheduler.Options{
		Name:     "Derivatives poller",
		Interval: poller.Interval(),
		// One attempt per scheduler try; the scheduler's exponential backoff is 5 attempts
		// at 10s→160s.
		Startup:         poller.Prepare,
		StartupAttempts: 5,
		StartupBackoff:  10 * time.Second,
		Bootstrap:       poller.Bootstrap,
		Cycle: func(ctx context.Context) error {
			poller.PollOnce(ctx)
			return nil
		},
		Teardown: poller.Close,
		OnStop: func() {
			a.logger().Info("derivatives poller stopped; context done")
		},
		Log: a.log,
	})
}

// startDerivPollerLifecycle runs the derivatives schedule with exponential-backoff
// restart. It exits when ctx is canceled.
func (a *App) startDerivPollerLifecycle(ctx context.Context) {
	lifecycle := scheduler.NewLifecycle(scheduler.LifecycleOptions{
		Name:  "Derivatives poller",
		First: a.newDerivLoop(a.derivPoller),
		New: func() scheduler.Runner {
			// Recreate the poller with fresh state for the next attempt — fresh EXCEPT the
			// funding profile, which belongs to the process.
			poller := a.newDerivPoller(ctx)
			a.derivPoller = poller
			return a.newDerivLoop(poller)
		},
		MaxRestarts:  3,
		RestartDelay: 30 * time.Second,
		OnAlive:      a.derivPollerHealth.SetAlive,
		Log:          a.log,
	})

	a.discoveryWg.Add(1)
	go func() {
		defer a.discoveryWg.Done()
		lifecycle.Run(ctx)
	}()
}

// releaseRetentionUnderMigrationLicence is STARTUP STAGE 3, and it owes the license to STAGE 1.
//
// An unconditional `releaseRetention` erased the durable tier: the sole-copy migration wrote
// 130,284 candles and retention — a global 365-day window, `DROP PARTITION` with no timeframe
// filter — erased them on the next start.
//
// Only `INCOMPLETE` withholds the license: `not applicable` (no durable tier, no declared cell)
// and `complete` aborted nothing, and withholding it there would disable retention for every
// binary that has nothing to migrate.
func (a *App) releaseRetentionUnderMigrationLicence(ctx context.Context) {
	if a.bootMigration == questdb.BootMigrationIncomplete {
		a.logger().Error("retention has no license at this boot; the sole-copy migration did not complete",
			"outcome", a.bootMigration)
		return
	}
	a.releaseRetention(ctx)
}
