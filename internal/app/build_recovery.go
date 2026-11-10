package app

import (
	"sync/atomic"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/adapters/outbound/venue"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/market"
	"github.com/marxms/go-whale/internal/platform/config"
	"github.com/marxms/go-whale/internal/pruning"
	"github.com/marxms/go-whale/internal/recovery"
	"github.com/marxms/go-whale/internal/retention"
)

// buildRecovery constructs gap recovery, the retention license and the Redis pruner.
func (a *App) buildRecovery(cfg config.Config, tfReg *candle.TimeframeRegistry) {
	// Gap recovery (Task 4.1, 4.5, 4.6).
	if cfg.CandleEngine.Enabled {
		a.gapRecovery = recovery.NewManager(recovery.DefaultStorageWriter{Store: a.store}, recovery.DefaultWorkers, tfReg, a.log)
		a.gapRecovery.SetMetrics(a.recoveryMetrics)
		// The history adapter is injected here: recovery only knows the HistorySource
		// port, so the venue adapter import stays in the composition root.
		a.gapRecovery.SetClientPool(recovery.NewLazyClientPool(func(exchangeID string, marketType market.MarketType) (recovery.HistorySource, error) {
			client, err := venue.NewClientWithOptions(exchangeID, marketType, venue.Options{
				Budget:  a.restBudget,
				Log:     a.log,
				Metrics: a.venueMetrics,
			})
			if err != nil {
				return nil, err
			}
			return client, nil
		}, a.log))
		// HistorySource-based filler (BasicOHLCV) works for all exchanges incl. BingX.
		a.gapRecovery.SetFiller(recovery.NewBasicOHLCVFiller(a.log))
		// SDK-based filler (ExtendedKlines) for Binance only (richer fields). The seed and
		// gap recovery share ONE budget because Binance meters weight per IP.
		a.gapRecovery.SetFillerForExchange(market.Binance.String(), recovery.NewSDKGapFiller(a.restBudget, a.log))
	}

	// RETENTION — ASSEMBLED HERE, STARTED IN `Run`, AND AFTER THE GATES.
	//
	// `Start()` must not run here: `preflightGates` only runs at the first instruction of `Run`,
	// so a `DROP PARTITION` fired at construction would be destruction executed by a process the
	// gates might refuse. The `GatesApproved` predicate ENFORCES the order, because it is
	// consulted before every pass.
	a.gatesApproved = &atomic.Bool{}
	if cfg.QuestDB.Enabled && a.qdbStore != nil {
		policies := retentionPoliciesFromConfig(cfg, cfg.CandleEngine.Timeframes)
		if len(policies) > 0 {
			a.retentionMgr = retention.NewManager(retention.ManagerOptions{
				Executor:      a.qdbStore,
				Policies:      policies,
				GatesApproved: a.gatesApproved.Load,
				Log:           a.log,
			})
		}
	}

	if a.redisStore != nil {
		a.pruner = pruning.NewPruner(
			redisstore.NewPrunerClient(a.redisStore, pruning.AnchorFloorKey),
			pruning.WithGatesApproved(a.gatesApproved.Load),
			pruning.WithLogger(a.log),
		)
	}
}
