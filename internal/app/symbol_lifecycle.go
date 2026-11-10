package app

import (
	"time"

	"github.com/marxms/go-whale/internal/market"
	"github.com/marxms/go-whale/internal/recovery"
)

// markForReseed records that a symbol is subscribed but has no seeded history, so the
// always-on gap-recovery backstop will fill it.
//
// The rate-limit KEEP path deliberately does NOT unsubscribe a symbol whose seed failed,
// because a refusal from the exchange is not a verdict about the symbol. But periodic discovery
// only seeds symbols in `discovered MINUS current`, and a kept symbol is now IN current — so it is
// never revisited.
//
// Enqueueing it for gap recovery is the honest closure: that path is already the mechanism for
// "this symbol's data is behind", it is budgeted through the shared REST limiter, and it is
// idempotent — if the symbol has meanwhile been seeded by another route, the recovery is a cheap
// no-op merge instead of a duplicate.
func (a *App) markForReseed(exchangeID, marketType, symbol string) {
	if a.gapRecovery == nil {
		a.logger().Warn("reseed: symbol kept unseeded and gap recovery is disabled",
			"exchange", exchangeID, "market_type", marketType, "symbol", symbol)
		return
	}

	now := time.Now()
	a.gapRecovery.EnqueueBatch(exchangeID, marketType,
		[]string{market.NormalizeSymbol(symbol)},
		now.Add(-recovery.ReseedRetryWindow), now)
}

// cleanupRolledBackSymbol undoes the state a subscribe created, for the case where the seed
// failed for a reason that IS about the symbol (delisted, no history) and the subscription is
// genuinely rolled back.
//
// The rollback must not call manager.Unsubscribe alone, because the derank path performs the full
// cleanup. The asymmetry would leave two pieces of state behind:
//
//   - the trade handler's per-symbol dedup window and timestamp maps, and
//   - the Redis active-symbol entry plus the symbol's hot candle keys.
//
// The Redis entry is the damaging one: the analyzer reads the active set to decide what to
// analyze, so a rolled-back symbol stays on the screener as active-but-dead, carrying a staleness
// badge that only grows. Reusing the same cleanup the derank path uses keeps the two removal
// routes from drifting.
func (a *App) cleanupRolledBackSymbol(exchangeID, marketTypeStr string, _ market.MarketType, symbol string) {
	sym := market.NormalizeSymbol(symbol)

	if a.handler != nil {
		a.handler.PurgeSymbol(exchangeID + ":" + marketTypeStr + ":" + sym)
	}
	if a.redisStore != nil {
		if err := a.redisStore.RemoveActiveSymbol(a.streamCtx, exchangeID, marketTypeStr, sym); err != nil {
			a.logger().Warn("rollback: symbol failed to leave the active set",
				"exchange", exchangeID, "market_type", marketTypeStr, "symbol", sym, "error", err)
		}
		cleaned := a.redisStore.CleanupSymbolData(a.streamCtx, exchangeID, marketTypeStr, sym)
		a.logger().Info("rollback: hot keys cleaned",
			"exchange", exchangeID, "market_type", marketTypeStr, "symbol", sym, "keys", cleaned)
	}
	if a.derivStore != nil {
		_ = a.derivStore.CleanupSymbolDerivatives(a.streamCtx, exchangeID, marketTypeStr, sym)
	}
}
