package app

import (
	"context"
	"log"
	"time"
)

// Shutdown stops the process in a fixed order and is idempotent: a second call returns
// the first outcome instead of stopping the same stores twice.
//
// The order is the one that loses the least work:
//
//  1. intake      — cancel the stream context, wait for the loops it bounds, stop the
//     live venue feed, then stop the hybrid manager;
//  2. timers      — stop the retention loop (it owns its own ticker);
//  3. producers   — drain the WS hub, gap recovery, the cold pipeline and the realtime
//     bus. DURABILITY DRAINS BEFORE TELEMETRY: an abandoned QuestDB batch
//     is a permanent hole, while the realtime bus is best-effort;
//  4. stores      — close QuestDB, then the tiered store;
//  5. HTTP        — the router and the loopback pprof server last, after nothing else
//     can write.
func (a *App) Shutdown(ctx context.Context) error {
	a.shutdownOnce.Do(func() {
		a.shutdownErr = a.shutdown(ctx)
	})
	return a.shutdownErr
}

func (a *App) shutdown(ctx context.Context) error {
	// Shutdown runs precisely because ctx was canceled, so every drain gets a detached base:
	// values propagate, cancellation does not, and each phase applies its own deadline.
	shutdownCtx := context.WithoutCancel(ctx)

	// ── 1. INTAKE ────────────────────────────────────────────────────────────────
	a.traceShutdown("intake")
	// The stream context is what every intake loop and periodic timer is bound to;
	// canceling it first is what makes the wait below terminate.
	if a.streamCancel != nil {
		a.streamCancel()
	}
	a.waitDiscovery(15 * time.Second)
	a.stopKlineFeed()
	if a.manager != nil {
		a.manager.Stop()
	}

	// ── 2. TIMERS ────────────────────────────────────────────────────────────────
	a.traceShutdown("timers")
	if a.retentionMgr != nil {
		a.retentionMgr.Stop()
	}

	// ── 3. PRODUCERS ─────────────────────────────────────────────────────────────
	a.traceShutdown("producers")
	if a.hub != nil {
		// WS is hijacked, so http.Server.Shutdown does not close it — the hub owns the
		// clean drain of every connection, bounded by this context. WithoutCancel keeps the
		// caller's values while giving shutdown its own fresh deadline: Run reaches here
		// precisely because ctx was canceled, and the drain must still get its 10s.
		stopCtx, cancel := context.WithTimeout(shutdownCtx, 10*time.Second)
		a.hub.Stop(stopCtx)
		cancel()
	}
	if a.gapRecovery != nil {
		a.gapRecovery.Stop()
	}
	// The derivatives schedule is canceled by streamCtx and waited for by discoveryWg.
	// It owns no stop channel of its own: a second shutdown path would be a second way
	// to stop the same loop.
	if a.candleEngine != nil {
		pending, evicted := a.candleEngine.SeedRepairStatus()
		if pending > 0 || evicted > 0 {
			a.logger().Error("seed ancestor repairs remain unconfirmed at shutdown",
				"pending", pending, "evicted_scopes", evicted)
		}
	}
	if a.qdbPipeline != nil {
		a.qdbPipeline.Stop(shutdownCtx)
	}
	// DURABILITY DRAINS BEFORE TELEMETRY DOES. The realtime bus is best-effort and its
	// subscribers recover from a sequence gap by design, while a QuestDB batch abandoned
	// at shutdown is a permanent hole in the only durable tier.
	if a.realtimeProducer != nil {
		a.realtimeProducer.Stop()
	}

	// ── 4. STORES ────────────────────────────────────────────────────────────────
	a.traceShutdown("stores")
	if a.qdbStore != nil {
		if err := a.qdbStore.Close(); err != nil {
			a.logger().Warn("questdb store close error", "error", err)
		}
	}
	if a.tieredStore != nil {
		if err := a.tieredStore.Close(); err != nil {
			a.logger().Warn("tiered store close error", "error", err)
		}
	} else if a.redisStore != nil {
		// With the candle engine disabled there is no tiered store to own the shared
		// Redis client, and leaving it open strands the go-redis pool/maintenance
		// goroutines past shutdown. The concrete store and the tiered store share ONE
		// client, so exactly one of them closes it.
		if err := a.redisStore.Close(); err != nil {
			a.logger().Warn("redis store close error", "error", err)
		}
	}

	// ── 5. HTTP ──────────────────────────────────────────────────────────────────
	a.traceShutdown("http")
	if a.httpServer != nil {
		httpCtx, cancel := context.WithTimeout(shutdownCtx, 10*time.Second)
		defer cancel()
		if err := a.httpServer.Shutdown(httpCtx); err != nil {
			log.Printf("http server shutdown error: %v", err)
		}
	}
	if a.pprofServer != nil {
		pprofCtx, cancel := context.WithTimeout(shutdownCtx, time.Second)
		defer cancel()
		if err := a.pprofServer.Shutdown(pprofCtx); err != nil {
			log.Printf("pprof server shutdown error: %v", err)
		}
	}

	a.logger().Info("shutdown complete")
	return nil
}

// waitDiscovery waits for the background loops tracked by discoveryWg, bounded by
// timeout. It never blocks shutdown forever on a wedged exchange loop.
func (a *App) waitDiscovery(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		a.discoveryWg.Wait()
		close(done)
	}()
	select {
	case <-done:
		a.logger().Info("all discovery goroutines finished")
	case <-time.After(timeout):
		a.logger().Warn("discovery goroutines did not finish within timeout; continuing shutdown", "timeout", timeout)
	}
}

// stopKlineFeed closes the live venue feed and releases the engine's derived scope.
//
// Closed BEFORE the manager, and closed at all: each partition holds a socket and an
// SDK read goroutine, and abandoning them on shutdown is the leak that starves a host
// across restarts. Read under klineMu like every other access: the watchdog can be
// replacing this pointer at the same instant.
func (a *App) stopKlineFeed() {
	a.klineMu.Lock()
	liveFeed := a.klineFeed
	closingScope := a.klineScope
	a.klineFeed = nil
	a.klineMu.Unlock()
	if a.candleEngine != nil && closingScope != "" {
		a.candleEngine.UnregisterDerivedScope(closingScope)
	}
	if liveFeed != nil {
		liveFeed.Stop()
	}
}
