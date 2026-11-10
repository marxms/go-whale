package recovery

import (
	"context"
	"log/slog"
	"time"

	"github.com/marxms/go-whale/internal/market"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

const (
	// BootGapMinWindow is the smallest hole worth a fleet-wide REST pass. Below it the
	// restart was fast enough that the open bucket is effectively intact, and enqueueing
	// would make every boot pay for nothing.
	BootGapMinWindow = 5 * time.Second

	// BootGapMaxWindow caps what this path will try to heal. Deep history is the SEED's
	// job; this exists for the minutes around a restart. Without the cap a three-day
	// outage would turn one boot into a fleet-wide backfill stampede.
	BootGapMaxWindow = 15 * time.Minute

	// FastestBootTimeframe is the timeframe consulted for persisted freshness. The
	// fastest configured frame gives the tightest estimate of where our data ends.
	FastestBootTimeframe = "5m"
)

// EnqueueBootGap heals the hole a RESTART leaves in the open bucket.
//
// A reconnect records disconnectTime and triggers gap recovery. A RESTART does not:
// the disconnect time died with the previous process. Between the last persisted
// candle and the moment the websocket reattaches, the trades exist only at the
// exchange.
//
// The window here is KNOWN rather than inferred: persisted freshness says where our
// data ends, and now is where the live stream resumes. Over-covering is safe because
// gap recovery writes the open bucket with a REST total that merges with MAX on
// volume, so a wider window costs REST weight, never correctness.
func EnqueueBootGap(
	ctx context.Context,
	orch Orchestrator,
	provider LatestTimestampProvider,
	exchangeID, marketType string,
	symbols []string,
	now time.Time,
	logs ...*slog.Logger,
) {
	log := logger.OrDiscard(logs...)
	if orch == nil || provider == nil || len(symbols) == 0 {
		return
	}

	// THE WINDOW IS DECIDED PER SYMBOL, NOT BY THE FLEET MINIMUM. One illiquid asset
	// whose last 5m candle predates the cap must not veto the honest heal for everyone.
	var (
		healable   []string
		beyondCap  []string
		widestFrom time.Time
	)
	for _, sym := range symbols {
		latest, ok := provider.GetLatestTimestamp(ctx, exchangeID, marketType, market.NormalizeSymbol(sym), FastestBootTimeframe)
		if !ok || latest <= 0 {
			// Nothing persisted for this symbol: a cold start, not a restart.
			continue
		}
		symFrom := time.UnixMilli(latest)
		symGap := now.Sub(symFrom)

		switch {
		case symGap < BootGapMinWindow:
			// Restarted fast enough that this symbol's open bucket is intact.
		case symGap > BootGapMaxWindow:
			beyondCap = append(beyondCap, sym)
		default:
			healable = append(healable, sym)
			if widestFrom.IsZero() || symFrom.Before(widestFrom) {
				widestFrom = symFrom
			}
		}
	}

	// A hole past this path's scope must stay VISIBLY OPEN rather than be papered over
	// with a healed tail, and it must not become a fleet-wide REST burst.
	if len(beyondCap) > 0 {
		log.Error("boot gap: symbols stale beyond the window this path covers; not enqueued; these holes need a deliberate paced backfill",
			"exchange", exchangeID,
			"market_type", marketType,
			"symbols", len(beyondCap),
			"max_window", BootGapMaxWindow,
			"example", beyondCap[0])
	}

	if len(healable) == 0 {
		return
	}

	log.Warn("boot gap: enqueuing restart gap",
		"exchange", exchangeID,
		"market_type", marketType,
		"symbols", len(healable),
		"total_symbols", len(symbols),
		"window", now.Sub(widestFrom).Round(time.Second),
		"beyond_cap", len(beyondCap))
	orch.EnqueueBatch(exchangeID, marketType, healable, widestFrom, now)
}
