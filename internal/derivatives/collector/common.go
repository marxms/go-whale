package collector

import (
	"context"
	"log/slog"

	"github.com/marxms/go-whale/internal/market"
	"github.com/marxms/go-whale/internal/platform/config"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// NOTE: DiscoverSymbols already returns normalized symbols (BTC/USDT, never BTC/USDT:USDT).
// No post-processing needed — see normalizeSymbol() in discovery.go.

// SymbolScope identifies a unique exchange+marketType+symbol combination.
type SymbolScope struct {
	ExchangeID string
	MarketType string
	Symbol     string
}

// Key identifies a scope uniquely across exchanges and market types.
//
// The symbol alone is not unique: the same ticker exists on more than one venue and
// in more than one market type, and per-symbol state keyed by ticker would silently
// merge readings that belong to different contracts.
func (s SymbolScope) Key() string {
	return s.ExchangeID + ":" + s.MarketType + ":" + s.Symbol
}

// buildFutureSymbolScopes extracts all enabled future exchange symbols.
// When an exchange has discovery.enabled=true and no explicit symbols, it asks
// the injected market.DiscoverySource for the top-N assets by volume —
// mirroring the same discovery logic used by the WebSocket streaming in main.go.
func buildFutureSymbolScopes(ctx context.Context, exchanges []config.ExchangeConfig, discovery market.DiscoverySource, log *slog.Logger) []SymbolScope {
	log = logger.OrDiscard(log)
	var scopes []SymbolScope
	for _, ex := range exchanges {
		if !ex.Enabled {
			continue
		}
		if ex.MarketType != "future" && ex.MarketType != "swap" {
			continue
		}

		symbols := ex.Symbols

		// Resolve via discovery when no explicit symbols are configured
		if len(symbols) == 0 && ex.Discovery.Enabled {
			mt, err := market.FromConfig(ex.MarketType)
			if err != nil {
				log.Error("derivatives: invalid market type", "exchange", ex.ID, "error", err)
				continue
			}
			if discovery == nil {
				log.Error("derivatives: discovery requested without an injected source",
					"exchange", ex.ID, "market_type", ex.MarketType)
				continue
			}
			id, parseErr := market.ParseExchange(ex.ID)
			if parseErr != nil {
				log.Error("derivatives: invalid exchange for discovery", "exchange", ex.ID, "error", parseErr)
				continue
			}
			discovered, err := discovery.DiscoverSymbols(ctx, id, mt, ex.Discovery.MaxAssets)
			if err != nil {
				log.Error("derivatives: discovery failed",
					"exchange", ex.ID, "market_type", ex.MarketType, "error", err)
				continue
			}
			symbols = discovered
			log.Info("derivatives: symbols discovered",
				"symbols", len(symbols), "exchange", ex.ID, "market_type", ex.MarketType)
		}

		for _, sym := range symbols {
			scopes = append(scopes, SymbolScope{
				ExchangeID: ex.ID,
				MarketType: ex.MarketType,
				Symbol:     sym,
			})
		}
	}
	return scopes
}
