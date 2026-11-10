package app

import (
	"context"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	binancesdk "github.com/marxms/go-whale/internal/adapters/outbound/binance"
	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/adapters/outbound/venue"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/ingestion"
	"github.com/marxms/go-whale/internal/ingestion/streaming"
	"github.com/marxms/go-whale/internal/market"
	"github.com/marxms/go-whale/internal/platform/config"
	"github.com/marxms/go-whale/internal/recovery"
)

// Subscription admission rules live in their owning packages; the functions below assemble
// the graph and translate configuration into typed inputs.

func (a *App) autoSubscribe(ctx context.Context) {
	if len(a.cfg.Exchanges) == 0 {
		a.logger().Info("no exchanges configured")
		return
	}

	var enabledExchanges []config.ExchangeConfig
	for _, exCfg := range a.cfg.Exchanges {
		if exCfg.Enabled {
			enabledExchanges = append(enabledExchanges, exCfg)
		}
	}
	if len(enabledExchanges) == 0 {
		a.logger().Info("no enabled exchanges configured")
		return
	}

	allowedScopes := make(map[string]struct{}, len(enabledExchanges))
	for _, exCfg := range enabledExchanges {
		internalMarket, err := market.FromConfig(exCfg.MarketType)
		if err != nil {
			a.logger().Error("invalid market type", "exchange", exCfg.ID, "error", err)
			continue
		}
		allowedScopes[exCfg.ID+":"+internalMarket.String()] = struct{}{}
	}

	if existingScopes, err := a.store.GetAllActiveSymbols(ctx); err == nil {
		staleScopes := make(map[string]struct{})
		for _, item := range existingScopes {
			scope := item.Exchange + ":" + item.MarketType
			if _, ok := allowedScopes[scope]; !ok {
				staleScopes[scope] = struct{}{}
			}
		}
		for scope := range staleScopes {
			parts := strings.SplitN(scope, ":", 2)
			if len(parts) != 2 {
				continue
			}
			if err := a.redisStore.ClearActiveSymbols(ctx, parts[0], parts[1]); err != nil {
				a.logger().Warn("failed to clear stale active scope", "scope", scope, "error", err)
			} else {
				a.logger().Warn("cleared stale active scope from previous runs", "scope", scope)
			}
		}
	}

	a.logger().Info("starting multi-exchange streaming", "exchanges", len(enabledExchanges))

	var subscribedMu sync.Mutex
	subscribedSymbols := make(map[string]struct{})

	var wg sync.WaitGroup
	for _, exCfg := range enabledExchanges {
		wg.Add(1)
		go func(exCfg config.ExchangeConfig) {
			defer wg.Done()
			a.logger().Info("processing exchange", "exchange", exCfg.ID, "market_type", exCfg.MarketType)

			marketType, err := market.FromConfig(exCfg.MarketType)
			if err != nil {
				a.logger().Error("invalid market type", "exchange", exCfg.ID, "error", err)
				return
			}
			exchangeID, err := market.ParseExchange(exCfg.ID)
			if err != nil {
				a.logger().Error("invalid exchange", "exchange", exCfg.ID, "error", err)
				return
			}

			// metricMarket is the canonical market-type label for startup-failure
			// metrics, matching the stream_liveness / expected-scope namespace.
			metricMarket := marketType.String()

			if err := a.redisStore.ClearActiveSymbols(ctx, exCfg.ID, exCfg.MarketType); err != nil {
				a.logger().Warn("failed to clear active symbols", "exchange", exCfg.ID, "market_type", exCfg.MarketType, "error", err)
			}

			if marketType == market.Future {
				if err := a.redisStore.ClearActiveSymbols(ctx, exCfg.ID, redisstore.LegacySwapMarketType); err != nil {
					a.logger().Warn("failed to clear ghost swap symbols", "exchange", exCfg.ID, "error", err)
				}
			}

			symbols := exCfg.Symbols
			if len(symbols) == 0 && exCfg.Discovery.Enabled {
				discovered, err := a.discovery.DiscoverSymbols(ctx, exchangeID, marketType, exCfg.Discovery.MaxAssets)
				if err != nil {
					a.logger().Error("asset discovery failed", "exchange", exCfg.ID, "error", err)
					a.streamMetrics.IncStartupFailure(exCfg.ID, metricMarket, streaming.StartupStageDiscovery)
					return
				}
				symbols = discovered
				a.logger().Info("symbols discovered", "symbols", len(symbols), "exchange", exCfg.ID, "market_type", exCfg.MarketType)
			}

			if len(symbols) == 0 {
				// Zero symbols means the scope never streams: error severity, with liveness
				// pinned to 0.
				a.logger().Error("no symbols configured; exchange will not stream", "exchange", exCfg.ID, "market_type", exCfg.MarketType)
				a.streamMetrics.IncStartupFailure(exCfg.ID, metricMarket, streaming.StartupStageNoSymbols)
				a.streamMetrics.SetLiveness(exCfg.ID, metricMarket, false)
				return
			}

			for i, symbol := range symbols {
				symbols[i] = market.NormalizeSymbol(symbol)
			}

			// Enforce discovery.max_assets here too, so the streamed set cannot exceed the
			// operator's ceiling; liquidity-DESC order drops the least-liquid tail.
			discoveredCount := len(symbols)
			symbols, dropped := market.EnforceMaxAssets(symbols, exCfg.Discovery.MaxAssets)
			if dropped > 0 {
				a.logger().Warn("discovery exceeded max_assets; enforcing ceiling",
					"exchange", exCfg.ID, "market_type", exCfg.MarketType, "symbols", discoveredCount, "max_assets", exCfg.Discovery.MaxAssets, "dropped", dropped)
			}

			// There is no QuestDB-side orphan reconcile: QuestDB 8.2.3 has no row DELETE, so
			// cold data expires via retention DROP PARTITION. Redis hot data is cleaned on
			// derank and by the PruneRedis safety net below.

			// No Budget here: the seeder reserves the klines weight before calling
			// the adapter, so metering the same request here would charge it twice.
			restClient, err := venue.NewClientWithOptions(exCfg.ID, marketType, venue.Options{
				Log:     a.log,
				Metrics: a.venueMetrics,
			})
			if err != nil {
				a.logger().Error("failed to create REST client", "exchange", exCfg.ID, "error", err)
				a.streamMetrics.IncStartupFailure(exCfg.ID, metricMarket, streaming.StartupStageRestClient)
				return
			}
			defer func() { _ = restClient.Close() }()

			mkts, err := loadListingMarkets(ctx, restClient)
			if err != nil {
				a.logger().Error("failed to load markets", "exchange", exCfg.ID, "error", err)
				a.streamMetrics.IncStartupFailure(exCfg.ID, metricMarket, streaming.StartupStageLoadMarkets)
				return
			}
			a.recordListingFloors(exCfg.ID, string(marketType), symbols, mkts)
			a.recordNonCoinSymbols(exCfg.ID, marketType.String(), mkts)

			adapter := market.NewAdapter(exchangeID)
			marketTypeFilter, err := adapter.ToExchange(marketType)
			if err != nil {
				a.logger().Error("failed to map market type", "exchange", exCfg.ID, "error", err)
				return
			}
			valid := market.ValidSymbolsFiltered(mkts, marketTypeFilter)

			// successCount counts symbols that both subscribed and seeded cleanly; seed
			// failures are tracked separately because they leave a degraded, not healthy, state.
			successCount := 0
			subscribeErrorCount := 0
			seedErrorCount := 0
			startTime := time.Now()

			// Attach the whole fleet in one transport operation before the per-symbol loop.
			// The paced per-symbol path costs subscribeMinInterval per symbol, and with seeding
			// on top the init budget expired at ~200 of 500. Streams on the handshake URL cost
			// zero inbound messages; if batching is unavailable, batchAttached stays 0.
			batchAttached := 0
			if n, batchErr := a.manager.SubscribeAll(ctx, symbols, exCfg.ID, marketType); batchErr == nil {
				batchAttached = n
				for _, symbol := range symbols {
					symKey := exCfg.ID + ":" + string(marketType) + ":" + symbol
					subscribedMu.Lock()
					subscribedSymbols[symKey] = struct{}{}
					subscribedMu.Unlock()
				}
				// Heal the hole this startup left in the open bucket, right after the fleet is
				// live. Marking must come first: under venue ownership the venue veto discards
				// the returning REST total, and HasGap is the only thing that lets the repair
				// through. See enqueueBootGap.
				//
				// Marking only klineFeedInterval leaves the coarser buckets unmarked and
				// their repair vetoed. Mark every configured timeframe of the open bucket;
				// see bootGapMarkTimeframes.
				if a.candleEngine != nil {
					marked := 0
					for _, tf := range candle.BootGapMarkTimeframes(binancesdk.KlineFeedInterval, a.cfg.CandleEngine.Timeframes) {
						marked += a.candleEngine.MarkOpenBucketGapped(exCfg.ID, string(marketType), tf, symbols)
					}
					if marked > 0 {
						a.logger().Warn("boot gap: open buckets marked incomplete", "buckets", marked, "exchange", exCfg.ID, "market_type", exCfg.MarketType)
					}
				}
				if a.gapRecovery != nil {
					recovery.EnqueueBootGap(ctx, a.gapRecovery, a.candleEngine, exCfg.ID, string(marketType), symbols, time.Now())
				}
			} else if !ingestion.IsBatchUnsupported(batchErr) {
				// A real connect failure, not "this client cannot batch": surface it and let
				// the per-symbol path below try, so one bad handshake never strands the scope.
				a.logger().Error("batch subscribe failed; falling back to per-symbol",
					"exchange", exCfg.ID, "market_type", marketType, "symbols", len(symbols), "error", batchErr)
				a.streamMetrics.IncStartupFailure(exCfg.ID, metricMarket, streaming.StartupStageSubscribe)
			}

			// The per-symbol seed runs with bounded parallelism; subscribe stays serial when
			// batching is unavailable. The serial loop measured 11m42s for 200 symbols, and the
			// concurrency is sized against the venue's request-weight budget.
			//
			// The seed may not run before the stream it is stitched to is delivering: a REST
			// snapshot and the live stream must overlap, or trades in the gap belong to neither.
			// deliveryDeadline is absolute and shared by the scope, so the boot waits at most
			// one grace; a symbol whose grace lapses is seeded blind and counted as such.
			//
			// The venue's own candle feed is independent of how the tape subscribed: it dials its
			// own sockets, so it must start outside the batch arm. It starts after discovery
			// because the symbol list is only settled here.
			a.startKlineFeed(ctx, exCfg.ID, string(marketType), symbols)

			deliveryDeadline := time.Now().Add(recovery.BootDeliveryGrace)

			// Converted behind an explicit nil check to avoid the typed-nil trap.
			var probe recovery.DeliveryProbe
			if a.handler != nil {
				probe = a.handler
			}

			// Pre-register the discard counters at zero so "not wired" is distinguishable from
			// "never fired"; a CounterVec materializes only on first increment.
			a.handler.PreregisterDiscardReasons(exCfg.ID, string(marketType))
			// The admitted/applied pair separates "trades never reached this process" from "we
			// lost them inside it". It is registered only where the tape still writes candles,
			// since startKlineFeed deleted the series for venue-owned scopes.
			if a.handler != nil && a.handler.TapeWritesCandles(exCfg.ID, string(marketType)) {
				a.handler.PreregisterAdmittedVolume(exCfg.ID, string(marketType))
			}

			// Pre-register the late-update counter so "no late updates happened" and "the late
			// branch is never reached" stop reading the same.
			a.candleEngine.PreregisterLateUpdateReasons(exCfg.ID, string(marketType), a.tfReg.All())

			// countMu guards counters written concurrently by the seed closures.
			var (
				seedGrp   errgroup.Group
				countMu   sync.Mutex
				blindSeed int
			)
			seedGrp.SetLimit(recovery.SeedConcurrency)

			for idx, symbol := range symbols {
				// A batch attach makes the per-symbol subscribe and pacing unnecessary.
				if batchAttached == 0 {
					if err := a.manager.Subscribe(ctx, symbol, exCfg.ID, marketType); err != nil {
						a.logger().Error("failed to subscribe", "symbol", symbol, "error", err)
						a.streamMetrics.IncStartupFailure(exCfg.ID, metricMarket, streaming.StartupStageSubscribe)
						countMu.Lock()
						subscribeErrorCount++
						countMu.Unlock()
						continue
					}

					symKey := exCfg.ID + ":" + string(marketType) + ":" + symbol
					subscribedMu.Lock()
					subscribedSymbols[symKey] = struct{}{}
					subscribedMu.Unlock()

					if idx < len(symbols)-1 {
						time.Sleep(ingestion.SubscribePacing)
					}
				}

				symbol := symbol
				seedGrp.Go(func() error {
					// Wait for this symbol's stream to deliver before snapshotting, so the REST
					// and live windows overlap. The scope-wide deadline bounds the wait.
					if !recovery.AwaitSymbolDelivery(ctx, probe, exCfg.ID, string(marketType), symbol, deliveryDeadline, recovery.BootDeliveryPoll) {
						countMu.Lock()
						blindSeed++
						countMu.Unlock()
					}

					// The cold writer applies its own budget; the caller's cancellation still
					// bounds the recovery.
					seedErr := recovery.SeedAllTimeframesMulti(ctx, a.recoveryMetrics, a.store, restClient, a.restBudget, exCfg.ID, marketType, valid, symbol, a.cfg.CandleEngine.BufferSizes, a.tfReg, a.log)

					countMu.Lock()
					defer countMu.Unlock()
					if seedErr != nil {
						a.logger().Error("failed to seed all timeframes", "symbol", symbol, "error", seedErr)
						a.streamMetrics.IncStartupFailure(exCfg.ID, metricMarket, streaming.StartupStageSeed)
						seedErrorCount++
						return nil // one symbol failing must not abandon the rest
					}
					// Only a clean subscribe+seed counts as a true success.
					successCount++
					return nil
				})
			}

			// Every closure swallows its own error, so this cannot be non-nil; checked anyway
			// so a future change that propagates is not ignored.
			if err := seedGrp.Wait(); err != nil {
				a.logger().Error("seed group returned", "exchange", exCfg.ID, "market_type", exCfg.MarketType, "error", err)
			}

			// Publish how many symbols were stitched and how many were not: a blind seed is the
			// remaining shape of the boot hole.
			countMu.Lock()
			blind := blindSeed
			countMu.Unlock()
			confirmed := len(symbols) - blind
			a.streamMetrics.SetBootSeedConfirmed(exCfg.ID, metricMarket, float64(confirmed))
			a.streamMetrics.SetBootSeedBlind(exCfg.ID, metricMarket, float64(blind))
			if blind > 0 {
				a.logger().Warn("boot seed: symbols seeded without confirmed stream delivery",
					"exchange", exCfg.ID, "market_type", exCfg.MarketType, "blind", blind, "symbols", len(symbols), "grace", recovery.BootDeliveryGrace)
			} else {
				a.logger().Warn("boot seed: all symbols were delivering before their snapshot",
					"exchange", exCfg.ID, "market_type", exCfg.MarketType, "symbols", len(symbols))
			}

			elapsed := time.Since(startTime)
			errorCount := subscribeErrorCount + seedErrorCount
			switch {
			case successCount == 0:
				// Nothing came up cleanly: the scope is not streaming. Error severity plus
				// liveness pinned to 0.
				a.logger().Error("all subscriptions failed; exchange not streaming",
					"exchange", exCfg.ID, "market_type", exCfg.MarketType, "errors", errorCount)
				a.streamMetrics.SetLiveness(exCfg.ID, metricMarket, false)
			case errorCount > 0:
				// Partial: some symbols are up, others failed to subscribe or recovery.
				a.logger().Warn("partial startup",
					"exchange", exCfg.ID, "market_type", exCfg.MarketType, "success", successCount, "errors", errorCount,
					"subscribe_errors", subscribeErrorCount, "seed_errors", seedErrorCount, "elapsed", elapsed)
			default:
				// Warn, not Info: production runs at log_level=warn, and this elapsed time is
				// the only number that can size autoSubscribeInitTimeout.
				a.logger().Warn("startup complete",
					"exchange", exCfg.ID, "market_type", exCfg.MarketType, "success", successCount, "errors", errorCount, "elapsed", elapsed)
			}
		}(exCfg)
	}

	// Enable snapshot flushing early, decoupled from subscription completion, so a slow exchange
	// goroutine cannot keep durability disabled process-wide.
	if a.tieredStore != nil {
		a.tieredStore.EnableSnapshots()
		a.logger().Info("snapshot flushing enabled; decoupled from subscribe completion")
	}

	declared := 0
	for _, exCfg := range enabledExchanges {
		declared += market.DeclaredSymbolCount(exCfg.Discovery.Enabled, exCfg.Discovery.MaxAssets, len(exCfg.Symbols))
	}
	budget := ingestion.AutoSubscribeInitBudget(declared)

	initComplete := ingestion.WaitWithTimeout(&wg, budget)
	if initComplete {
		a.logger().Warn("all exchanges initialized", "budget", budget, "declared_symbols", declared)
	} else {
		a.logger().Warn("autoSubscribe: init did not complete; proceeding with limited housekeeping",
			"budget", budget, "declared_symbols", declared)
		// Skip stale purges: subscribedSymbols is incomplete, so purging would delete live,
		// not-yet-subscribed symbols. Durability is already up.
		a.logger().Warn("autoSubscribe: skipping stale-symbol purge")
	}

	if initComplete && a.tieredStore != nil && len(subscribedSymbols) > 0 {
		purged := a.tieredStore.PurgeStaleSymbols(subscribedSymbols)
		if purged > 0 {
			a.logger().Info("purged stale candle engine buffers", "buffers", purged)
		}
	}

	if initComplete {
		for _, exCfg := range enabledExchanges {
			marketType, err := market.FromConfig(exCfg.MarketType)
			if err != nil {
				a.logger().Warn("skipping stale purge", "exchange", exCfg.ID, "error", err)
				continue
			}
			mt := marketType.String()

			prefix := exCfg.ID + ":" + mt + ":"
			activeSet := make(map[string]struct{})
			for key := range subscribedSymbols {
				if strings.HasPrefix(key, prefix) {
					sym := strings.TrimPrefix(key, prefix)
					activeSet[sym] = struct{}{}
				}
			}

			if len(activeSet) == 0 {
				continue
			}

			purged, err := a.redisStore.PurgeStaleSymbolData(ctx, exCfg.ID, mt, activeSet)
			if err != nil {
				a.logger().Warn("purge stale symbol data failed", "exchange", exCfg.ID, "market_type", mt, "error", err)
			} else if purged > 0 {
				a.logger().Info("purged stale redis data keys", "keys", purged, "exchange", exCfg.ID, "market_type", mt)
			}

			derivPurged := a.derivStore.PurgeStaleDerivatives(ctx, exCfg.ID, mt, activeSet)
			if derivPurged > 0 {
				a.logger().Info("purged stale derivatives keys", "keys", derivPurged, "exchange", exCfg.ID, "market_type", mt)
			}
		}

		// Global orphan pruning via Pruner (Redis UNLINK only); QuestDB data expires through
		// retention. One pass runs after the per-scope purges so it sees their result.
		if a.pruner != nil && len(subscribedSymbols) > 0 {
			uniqueSymbols := make([]string, 0, len(subscribedSymbols))
			seen := make(map[string]struct{})
			for key := range subscribedSymbols {
				parts := strings.SplitN(key, ":", 3)
				if len(parts) == 3 {
					sym := parts[2]
					if _, ok := seen[sym]; !ok {
						seen[sym] = struct{}{}
						uniqueSymbols = append(uniqueSymbols, sym)
					}
				}
			}
			if err := a.pruner.PruneRedis(ctx, uniqueSymbols); err != nil {
				a.logger().Warn("pruner redis safety-net prune failed", "error", err)
			}
		}
	}

	// Gate-independent orphan sweep, run regardless of initComplete, because the gated purge
	// above is skipped when a slow boot exceeds the init timeout.
	a.reconcileOrphanData(ctx)
}
