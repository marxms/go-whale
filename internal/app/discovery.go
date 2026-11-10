package app

// discovery.go — periodic discovery, the staleness-gated gap backstop and the orphan
// sweep. These are orchestration loops: the rules they apply (discovery completeness,
// staleness, scope conversion) live in market/recovery.
//
// The loops are launched by Run and tracked by discoveryWg, so Shutdown waits for them.

import (
	"context"
	"fmt"
	"strings"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/adapters/outbound/venue"
	"github.com/marxms/go-whale/internal/ingestion/streaming"
	"github.com/marxms/go-whale/internal/market"
	"github.com/marxms/go-whale/internal/platform/config"
	"github.com/marxms/go-whale/internal/recovery"
)

func (a *App) runPeriodicDiscovery(exCfg config.ExchangeConfig, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	marketType, err := market.FromConfig(exCfg.MarketType)
	if err != nil {
		a.logger().Error("periodic discovery disabled", "exchange", exCfg.ID, "error", err)
		return
	}
	exchangeID, err := market.ParseExchange(exCfg.ID)
	if err != nil {
		a.logger().Error("periodic discovery disabled", "exchange", exCfg.ID, "error", err)
		return
	}

	a.logger().Info("periodic discovery enabled", "exchange", exCfg.ID, "market_type", exCfg.MarketType, "interval", interval)

	for {
		select {
		case <-a.streamCtx.Done():
			return
		case <-ticker.C:
			a.logger().Info("running periodic discovery", "exchange", exCfg.ID, "market_type", exCfg.MarketType)

			var discovered []string
			var discoverErr error
			func() {
				defer func() {
					if r := recover(); r != nil {
						discoverErr = fmt.Errorf("panic in DiscoverSymbols for %s:%s: %v", exCfg.ID, exCfg.MarketType, r)
					}
				}()
				discovered, discoverErr = a.discovery.DiscoverSymbols(a.streamCtx, exchangeID, marketType, exCfg.Discovery.MaxAssets)
			}()

			if discoverErr != nil {
				a.logger().Error("periodic discovery failed", "exchange", exCfg.ID, "market_type", exCfg.MarketType, "error", discoverErr)
				continue
			}

			// Hard-enforce discovery.max_assets so periodic discovery's target set
			// (and thus the reconciled subscriptions) never exceeds the ceiling.
			// DiscoverSymbols returns liquidity-DESC, so the cap drops the least liquid.
			discoveredCount := len(discovered)
			discovered, dropped := market.EnforceMaxAssets(discovered, exCfg.Discovery.MaxAssets)
			if dropped > 0 {
				a.logger().Warn("discovery exceeded max_assets; enforcing ceiling",
					"exchange", exCfg.ID, "market_type", exCfg.MarketType, "symbols", discoveredCount, "max_assets", exCfg.Discovery.MaxAssets, "dropped", dropped)
			}

			discoveredSet := make(map[string]struct{}, len(discovered))
			for _, sym := range discovered {
				discoveredSet[market.NormalizeSymbol(sym)] = struct{}{}
			}

			prefix := exCfg.ID + ":" + string(marketType) + ":"
			currentSubs := a.manager.ListSubscriptions()
			currentSet := make(map[string]struct{})
			for _, key := range currentSubs {
				if strings.HasPrefix(key, prefix) {
					symbol := strings.TrimPrefix(key, prefix)
					currentSet[symbol] = struct{}{}
				}
			}

			// DISCOVERY-COMPLETENESS FLOOR (CRITICAL): a degraded-but-200 FetchTickers
			// response can return a VALID, non-empty, but TRUNCATED discovered set with
			// NO error. derankedSymbols = currentSet − discoveredSet, so a partial
			// discoveredSet would move genuinely-active symbols into the derank set and
			// UNSUBSCRIBE + Redis-clean them. The floor gates that destructive
			// path so a partial discovery never mass-unsubscribes live symbols.
			//
			// The floor uses TWO bases and requires BOTH to pass (logical AND), so the
			// stronger signal can only make us MORE conservative, never less:
			//   1. MaxAssets — the configured top-N ceiling (discoveryHealthy);
			//   2. the CURRENT subscription count for this scope — a partial response is
			//      almost certainly the cause when len(discovered) collapses far below
			//      what is currently live, i.e. len(discovered) < minDiscoveryFraction *
			//      len(currentSet). On a healthy first cycle currentSet may be small/0
			//      (subscribes still pacing in); the >0 guard makes that basis a no-op
			//      then, while MaxAssets still protects.
			// When NOT healthy we SKIP the ENTIRE derank+unsubscribe+prune for this
			// cycle (no removals of any kind); the new-symbol subscribe path below still
			// runs, so genuinely-new symbols are added but NOTHING is deleted on a
			// suspicious cycle.
			derankHealthy := market.PeriodicDerankHealthy(len(discoveredSet), len(currentSet), exCfg.Discovery.MaxAssets)
			if !derankHealthy {
				a.logger().Warn("periodic discovery derank/prune skipped; discovered set is below the floor",
					"exchange", exCfg.ID, "market_type", exCfg.MarketType, "symbols", len(discoveredSet))
			}

			// Snapshot flushing is paused while we mutate the active subscription
			// set (derank + purge) so a snapshot can't race a half-applied set.
			// The pause + the derank/purge work run inside this closure with a
			// defer re-enable, so snapshots are ALWAYS re-enabled at the end of
			// the region — even if a future early-return/continue is added inside
			// it. A linear DisableSnapshots()...EnableSnapshots() pair would leave
			// persistence silently disabled forever on any such early exit.
			//
			// The entire removal region is additionally gated on derankHealthy: on a
			// suspicious (partial) cycle the closure returns immediately after the
			// snapshot pause/resume bracket, performing NO unsubscribe, NO Redis
			// cleanup and NO QuestDB delete.
			var added, removed int
			func() {
				if a.tieredStore != nil {
					a.tieredStore.DisableSnapshots()
					defer a.tieredStore.EnableSnapshots()
				}

				// PARTIAL-DISCOVERY SAFETY FLOOR: skip ALL removals on a suspicious cycle.
				if !derankHealthy {
					return
				}

				var derankedSymbols []string
				for sym := range currentSet {
					if _, exists := discoveredSet[sym]; !exists {
						if err := a.manager.Unsubscribe(sym, exCfg.ID, marketType); err != nil {
							a.logger().Warn("failed to unsubscribe deranked symbol", "symbol", sym, "error", err)
						} else {
							removed++
							// The venue feed heard none of this and keeps pushing this symbol's
							// candle; UpdateCandle would recreate the buffer PurgeSymbol is about to
							// drop, and the symbol would return from the dead on all eight timeframes.
							derankedSymbols = append(derankedSymbols, sym)
							compositeKey := fmt.Sprintf("%s:%s:%s", exCfg.ID, marketType, sym)
							a.handler.PurgeSymbol(compositeKey)
							_ = a.redisStore.RemoveActiveSymbol(a.streamCtx, exCfg.ID, string(marketType), sym)
							cleaned := a.redisStore.CleanupSymbolData(a.streamCtx, exCfg.ID, string(marketType), sym)
							derivCleaned := a.derivStore.CleanupSymbolDerivatives(a.streamCtx, exCfg.ID, string(marketType), sym)
							a.logger().Info("periodic discovery: symbol unsubscribed",
								"exchange", exCfg.ID, "market_type", exCfg.MarketType, "symbol", sym, "cleaned_keys", cleaned, "deriv_keys", derivCleaned)
						}
					}
				}

				// Make the derank stick against the venue feed: mute the frames and drop the symbols
				// from the restart list, so the watchdog does not re-dial them either.
				a.dropFromKlineFeed(exCfg.ID, string(marketType), derankedSymbols)

				// Cold-store cleanup is intentionally NOT performed here. QuestDB
				// 8.2.3 has no row DELETE, so a deranked symbol's candle rows are
				// expired by retention.Manager via time-based DROP PARTITION
				// (retention_days): once deranked, the symbol stops receiving writes,
				// its partitions age past the retention window, and are dropped
				// natively. OHLCV is re-fetchable, preserving record integrity. The
				// per-symbol Redis hot-data cleanup above (active-set removal,
				// CleanupSymbolData, derivatives, handler purge) is what the dashboard
				// depends on and remains the only derank-time deletion.

				if a.tieredStore != nil && removed > 0 {
					allSubs := a.manager.ListSubscriptions()
					allowedSet := make(map[string]struct{}, len(allSubs))
					for _, key := range allSubs {
						allowedSet[key] = struct{}{}
					}
					purged := a.tieredStore.PurgeStaleSymbols(allowedSet)
					if purged > 0 {
						a.logger().Info("periodic discovery: stale candle engine buffers purged", "buffers", purged)
					}
				}

				if a.derivPoller != nil && removed > 0 {
					derivUnlinked := a.derivPoller.PruneStaleSymbols(a.streamCtx, exCfg.ID, string(marketType), discoveredSet)
					if derivUnlinked > 0 {
						a.logger().Info("periodic discovery: stale derivative keys unlinked",
							"keys", derivUnlinked, "exchange", exCfg.ID, "market_type", exCfg.MarketType)
					}
				}
			}() // snapshots re-enabled here via defer

			var newSymbols []string
			for sym := range discoveredSet {
				if _, exists := currentSet[sym]; !exists {
					newSymbols = append(newSymbols, sym)
				}
			}

			if len(newSymbols) > 0 {
				// No Budget here: the seeder reserves the klines weight before
				// calling the adapter, so metering the request here would charge it twice.
				restClient, err := venue.NewClientWithOptions(exCfg.ID, marketType, venue.Options{
					Log:     a.log,
					Metrics: a.venueMetrics,
				})
				if err != nil {
					a.logger().Error("periodic discovery: failed to create REST client", "exchange", exCfg.ID, "error", err)
				} else {
					mkts, err := loadListingMarkets(a.streamCtx, restClient)
					if err != nil {
						a.logger().Error("periodic discovery: failed to load markets", "exchange", exCfg.ID, "error", err)
					} else {
						a.recordListingFloors(exCfg.ID, string(marketType), newSymbols, mkts)
						a.recordNonCoinSymbols(exCfg.ID, marketType.String(), mkts)
						adapter := market.NewAdapter(exchangeID)
						marketTypeFilter, err := adapter.ToExchange(marketType)
						if err != nil {
							a.logger().Error("periodic discovery: failed to map market type", "exchange", exCfg.ID, "error", err)
							return
						}
						valid := market.ValidSymbolsFiltered(mkts, marketTypeFilter)

						// ATTACH BEFORE SEEDING, THE WAY BOOT DOES.
						//
						// Boot attaches the venue feed first, awaits delivery, and only then takes the
						// REST snapshot (see the ordering note at the batch-subscribe site), precisely
						// so the live window and the snapshot window OVERLAP. Subscribing and seeding
						// each symbol inside the loop, and attaching the feed only after the whole loop
						// finished, leaves no overlap.
						//
						// The tape is already silenced for the scope, so nothing covered the interval
						// between a symbol's seed and its attach. The kline stream does not backfill —
						// it pushes the CURRENT bucket only — so any 5m bucket opening and closing in
						// that window had no producer at all, and its absence then blocks the 15m
						// rebuild forever (fold.go: a child that has started and is absent blocks).
						// With K new symbols at eight REST timeframes each plus pacing, that window is
						// minutes wide.
						//
						// Subscribing is cheap and local; seeding is the slow part. So subscribe
						// everything, attach once (one dial for the batch), then recovery.
						var subscribed []string
						for _, sym := range newSymbols {
							if err := a.manager.Subscribe(a.streamCtx, sym, exCfg.ID, marketType); err != nil {
								a.logger().Warn("failed to subscribe new symbol", "symbol", sym, "error", err)
								continue
							}
							subscribed = append(subscribed, sym)
						}
						a.extendKlineFeed(exCfg.ID, string(marketType), subscribed)

						for _, sym := range subscribed {
							seedResult := recovery.SeedAllTimeframesMultiWithResult(
								a.streamCtx, a.recoveryMetrics, a.store, restClient, a.restBudget,
								exCfg.ID, marketType, valid, sym,
								a.cfg.CandleEngine.BufferSizes,
								a.tfReg,
							)

							// ROLLBACK IS A VERDICT ABOUT THE SYMBOL, so it may only fire when the
							// failure was about the symbol. A rate limit is about US: it fails
							// every symbol identically, so rolling back on it deletes the fleet.
							//
							// Measured: the 500-symbol boot burst took 4,602 HTTP 429s and this
							// line unsubscribed 117 valid symbols (500 -> 383). After the power
							// outage, with the IP still penalized, the same line cut the fleet to
							// 34. The penalty decays on its own and the next cycle re-seeds —
							// keeping the subscription costs nothing but a retry.
							switch {
							case seedResult.AllFailed && seedResult.Transient:
								// KEEP the subscription, but a kept symbol has NO history, and the
								// next discovery cycle will not re-seed it: newSymbols is
								// discovered MINUS currentSet, and this symbol just entered
								// currentSet. Without an explicit retry it stays live and empty
								// forever — "it will re-seed next cycle" is simply false.
								//
								// The periodic gap-recovery backstop is the mechanism that DOES
								// revisit an already-subscribed symbol: it enqueues whatever is
								// stale on the fastest timeframe, which an unseeded symbol always
								// is. Registering the intent explicitly is what closes the loop.
								a.markForReseed(exCfg.ID, string(marketType), sym)
								a.logger().Warn("periodic discovery: keeping symbol unseeded after a transient seed failure",
									"symbol", sym, "error", seedResult.PartialError)
							case seedResult.AllFailed:
								// ROLLBACK must undo everything Subscribe created, not just the
								// subscription. The derank path above does the full cleanup
								// (PurgeSymbol + RemoveActiveSymbol + CleanupSymbolData); calling
								// Unsubscribe alone would leave the engine buffer and the Redis
								// active-set entry behind. The screener then lists a symbol that is
								// active-but-dead, with a staleness badge that only grows.
								a.logger().Error("periodic discovery: rolling back symbol; all timeframes failed", "symbol", sym)
								_ = a.manager.Unsubscribe(sym, exCfg.ID, marketType)
								a.cleanupRolledBackSymbol(exCfg.ID, string(marketType), marketType, sym)
								// It is already ON the feed now that attaching precedes seeding, so
								// the rollback has to detach it too — otherwise the next frame
								// recreates the engine buffer this cleanup just dropped.
								a.dropFromKlineFeed(exCfg.ID, string(marketType), []string{sym})
								continue
							case seedResult.PartialError != nil:
								a.logger().Warn("periodic discovery: partial seed",
									"symbol", sym, "error", seedResult.PartialError, "succeeded", seedResult.Succeeded)
							default:
								a.logger().Info("periodic discovery: seeded all timeframes",
									"exchange", exCfg.ID, "market_type", exCfg.MarketType, "symbol", sym)
							}

							added++
							time.Sleep(50 * time.Millisecond)
						}
					}
					_ = restClient.Close()
				}
			}

			if a.streamCtx.Err() == nil {
				if members, err := a.redisStore.GetActiveSymbols(a.streamCtx, exCfg.ID, string(marketType)); err == nil {
					var reconciled int
					for _, sym := range members {
						if _, ok := discoveredSet[sym]; !ok {
							_ = a.redisStore.RemoveActiveSymbol(a.streamCtx, exCfg.ID, string(marketType), sym)
							reconciled++
						}
					}
					if reconciled > 0 {
						a.logger().Info("post-discovery reconciliation: stale symbols removed",
							"symbols", reconciled, "key_prefix", redisstore.ActiveSymbolsKeyPrefix, "exchange", exCfg.ID, "market_type", exCfg.MarketType)
					}
				}
			}

			if added > 0 || removed > 0 {
				a.logger().Info("periodic discovery complete",
					"exchange", exCfg.ID, "market_type", exCfg.MarketType, "added", added, "removed", removed, "total", len(discoveredSet))
			} else {
				a.logger().Debug("periodic discovery: no changes", "exchange", exCfg.ID, "market_type", exCfg.MarketType)
			}
		}
	}
}

// runPeriodicGapReconciliation is an always-on, staleness-gated backstop. Each
// tick it evaluates the currently-active symbols of every enabled exchange scope
// and enqueues gap recovery ONLY for symbols whose most recent persisted candle on
// the fastest timeframe is stale (see reconcileGapsOnce). Detection (per-stream
// watchdog, reconnect-triggered recovery, ingest continuity) can miss a window —
// for example a silent stall, or a reconnect that returned before enqueuing — so
// this loop guarantees those windows still heal, without the rate-limit
// amplification of re-enqueuing every symbol on every tick.
//
// The goroutine exits when a.streamCtx is canceled. It is launched from Run.
func (a *App) runPeriodicGapReconciliation(interval time.Duration) {
	if a.gapRecovery == nil {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	a.logger().Info("periodic gap reconciliation enabled", "interval", interval)

	for {
		select {
		case <-a.streamCtx.Done():
			return
		case <-ticker.C:
			subs := a.manager.ListSubscriptions()
			now := time.Now()
			a.reconcileGapsOnce(a.streamCtx, a.gapRecovery, a.consumerLatestTimestamp(), subs, now, interval)
			// Gate-independent orphan sweep on the same always-on tick: reclaim
			// no-TTL per-symbol data (candles/timeframes/derivatives:series) of
			// symbols no longer live, including pre-existing orphans the per-cycle
			// derank never sweeps and any a slow boot skipped.
			a.reconcileOrphanData(a.streamCtx)
		}
	}
}

// reconcileOrphanData deletes the no-TTL per-symbol Redis data (candles:,
// timeframes:, derivatives:series:) of any symbol NOT in the live set, for every
// enabled scope. The live set is the UNION of the manager's subscriptions and the
// authoritative active_symbols registry — race-free because a freshly subscribed
// symbol is in the manager the instant Subscribe returns, before its first data
// write reaches active_symbols, so it is never swept. This is the gate-independent
// backstop the per-cycle derank path lacks: it reclaims orphans inherited across
// restarts and any a slow boot (init timeout) skipped. Unlike the analyzer's
// indicator ZSETs, these streamer namespaces carry NO TTL, so without this sweep
// they leak permanently as the top-N reranks.
func (a *App) reconcileOrphanData(ctx context.Context) {
	if a.redisStore == nil {
		return
	}
	// globalKeep accumulates live "exchange:marketType:symbol" composites across all
	// scopes, used after the loop to bound the in-memory continuity map.
	globalKeep := make(map[string]struct{})
	for _, exCfg := range a.cfg.Exchanges {
		if !exCfg.Enabled {
			continue
		}
		marketType, err := market.FromConfig(exCfg.MarketType)
		if err != nil {
			a.logger().Warn("skipping orphan sweep", "exchange", exCfg.ID, "error", err)
			continue
		}
		mt := marketType.String()
		prefix := exCfg.ID + ":" + mt + ":"

		var active []string
		if members, err := a.redisStore.GetActiveSymbols(ctx, exCfg.ID, mt); err == nil {
			active = members
		}
		keep := redisstore.LiveKeepSet(a.manager.ListSubscriptions(), active, prefix)

		// Empty-set guard (CRITICAL): never sweep a scope whose live set is empty
		// (boot pre-seed, or a transient Redis miss) — an empty keep-set would mark
		// EVERY symbol stale and wipe the whole scope's data.
		if len(keep) == 0 {
			continue
		}
		for sym := range keep {
			globalKeep[exCfg.ID+":"+mt+":"+sym] = struct{}{}
		}

		if purged, err := a.redisStore.PurgeStaleSymbolData(ctx, exCfg.ID, mt, keep); err != nil {
			a.logger().Warn("orphan reconcile: purge stale symbol data failed", "exchange", exCfg.ID, "market_type", mt, "error", err)
		} else if purged > 0 {
			a.logger().Info("orphan reconcile: stale candle/timeframe keys purged",
				"exchange", exCfg.ID, "market_type", mt, "keys", purged)
		}
		if a.derivPoller != nil {
			if unlinked := a.derivPoller.PruneStaleSymbols(ctx, exCfg.ID, mt, keep); unlinked > 0 {
				a.logger().Info("orphan reconcile: stale derivative keys unlinked",
					"exchange", exCfg.ID, "market_type", mt, "keys", unlinked)
			}
		}
	}

	// Bound the in-memory open-time continuity map to the live set (gate-independent),
	// dropping baselines for deranked symbols the map otherwise never evicts.
	if len(globalKeep) > 0 {
		a.continuityTracker().Prune(globalKeep)
	}
}

// reconcileGapsOnce is a single staleness-gated reconciliation tick. The policy —
// fastest timeframe, fail-toward-recovery staleness, proof metric and per-scope
// enqueue — lives in recovery.Backstop; this method only converts configuration into
// typed scopes and connection stats into the disconnect predicate.
func (a *App) reconcileGapsOnce(ctx context.Context, orch recovery.Orchestrator, provider recovery.LatestTimestampProvider, subs []string, now time.Time, window time.Duration) int {
	if orch == nil || len(subs) == 0 {
		return 0
	}

	var stats []streaming.ConnectionStats
	if a.manager != nil {
		stats = a.manager.GetConnectionStats()
	}
	disconnects := make([]recovery.DisconnectStat, 0, len(stats))
	for _, s := range stats {
		disconnects = append(disconnects, recovery.DisconnectStat{
			ExchangeID:     s.ExchangeID,
			MarketType:     s.MarketType,
			LastDisconnect: s.LastDisconnect,
		})
	}

	backstop := recovery.NewBackstop(orch, provider, a.recoveryScopes(), a.timeframes(), a.recoveryMetrics, a.log)
	return backstop.ReconcileOnce(ctx, subs, now, window, recovery.ScopeDisconnectPredicate(disconnects, now.Add(-window)))
}

// recoveryScopes converts the enabled exchange configuration into the typed scopes
// the backstop evaluates. Invalid market types are skipped loudly.
func (a *App) recoveryScopes() []recovery.Scope {
	scopes := make([]recovery.Scope, 0, len(a.cfg.Exchanges))
	for _, exCfg := range a.cfg.Exchanges {
		if !exCfg.Enabled {
			continue
		}
		marketType, err := market.FromConfig(exCfg.MarketType)
		if err != nil {
			a.logger().Warn("skipping gap reconciliation", "exchange", exCfg.ID, "error", err)
			continue
		}
		scopes = append(scopes, recovery.Scope{ExchangeID: exCfg.ID, MarketType: marketType})
	}
	return scopes
}

// timeframes returns the registered timeframe list, or the configured one when the
// registry is absent (tests).
func (a *App) timeframes() []string {
	if a.tfReg != nil {
		return a.tfReg.All()
	}
	return a.cfg.CandleEngine.Timeframes
}
