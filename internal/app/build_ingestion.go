package app

import (
	binancesdk "github.com/marxms/go-whale/internal/adapters/outbound/binance"
	"github.com/marxms/go-whale/internal/adapters/outbound/bingx"
	"github.com/marxms/go-whale/internal/adapters/outbound/exchange"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/ingestion"
	"github.com/marxms/go-whale/internal/ingestion/streaming"
	"github.com/marxms/go-whale/internal/market"
	"github.com/marxms/go-whale/internal/platform/config"
)

// buildIngestion constructs the trade handler, the venue client catalog and the
// hybrid manager, and declares the per-route limits.
func (a *App) buildIngestion(cfg config.Config, tfReg *candle.TimeframeRegistry) {
	defaultExchange := market.Binance.String()
	defaultMarketType := market.Future.String()
	if len(cfg.Exchanges) > 0 {
		for _, exCfg := range cfg.Exchanges {
			if exCfg.Enabled {
				defaultExchange = exCfg.ID
				defaultMarketType = exCfg.MarketType
				break
			}
		}
	}
	a.defaultExchange = defaultExchange
	a.defaultMarketType = defaultMarketType

	// ONE REST budget for the process: Binance meters weight per IP, so seed, gap recovery
	// and discovery must draw from the same pool. It is created here, before any consumer.
	a.restBudget = exchange.NewRestBudget()
	a.handler = ingestion.NewStreamTradeHandler(a.store, defaultExchange, defaultMarketType, tfReg, a.log)
	a.handler.SetMetrics(a.ingestionMetrics)

	// THE CATALOG IS CONSTRUCTED HERE, ONCE. It is a value whose lifetime is the
	// process's, not a package-level registry that adapters install themselves into:
	// each factory owns its per-market client/pool cache, and the manager receives the
	// whole catalog through its constructor.
	binanceFactory := binancesdk.NewClientFactory(a.log)
	binanceFactory.SetMetrics(a.binanceMetrics)
	binanceFactory.SetNetdiag(a.netdiagCorrelator)
	bingxFactory := bingx.NewClientFactory(a.log)
	bingxFactory.SetNetdiag(a.netdiagCorrelator)
	a.manager = ingestion.NewHybridManager(a.handler, ingestion.DefaultManagerWorkers, map[string]streaming.ClientFactory{
		market.Binance.String(): binanceFactory.For,
		market.BingX.String():   bingxFactory.For,
	}, a.log)
	a.manager.SetTradeBufferSize(cfg.TradeBufferSize)

	// Configure route-specific limits.
	for _, exCfg := range cfg.Exchanges {
		if !exCfg.Enabled {
			continue
		}
		marketType, err := market.FromConfig(exCfg.MarketType)
		if err != nil {
			a.logger().Warn("skipping route limits", "exchange", exCfg.ID, "error", err)
			continue
		}
		maxSymbols, maxConnections := cfg.GetExchangeLimits(exCfg.ID, exCfg.MarketType)
		a.manager.SetStreamLimits(exCfg.ID, marketType, ingestion.StreamLimits{
			MaxSymbolsPerInstance: maxSymbols,
			MaxConnectionsPerIP:   maxConnections,
		})
		a.logger().Info("route limits configured",
			"exchange", exCfg.ID,
			"market_type", exCfg.MarketType,
			"max_symbols", maxSymbols,
			"max_connections", maxConnections)
	}
}
