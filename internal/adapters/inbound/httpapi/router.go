// Package httpapi serves the streamer's read-only HTTP surface: timeseries, trades,
// subscriptions and the derivatives measurement envelope.
package httpapi

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/marxms/go-whale/internal/adapters/inbound/realtime"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	logger "github.com/marxms/go-whale/internal/platform/logging"
	"github.com/marxms/go-whale/internal/trade"
)

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// ReadPorts carries the narrow read surfaces the HTTP routes consume.
//
// Each field is a separate capability, so a route receives only the port it actually
// uses. The composition root may hand the same concrete adapter to several fields — a
// concrete type implementing several ports is expected — but no route reads through a
// composite interface.
type ReadPorts struct {
	Candles      CandleReader
	BatchCandles CandleBatchReader
	Symbols      SymbolCatalog
	// AllSymbols is optional: when nil, /api/subscriptions falls back to Symbols for
	// the default exchange/market scope.
	AllSymbols AllSymbolsCatalog
	LastTrades trade.LastTradeReader
	// Metrics is the /metrics exposition. The composition root supplies a handler backed
	// by the process's own registry; nil leaves the route unmounted rather than falling
	// back to the default gatherer.
	Metrics http.Handler
}

// MarginBracketProvider serves the tier-1 maintenance margin rate per catalog symbol for
// one exchange/market scope. Nil when no bracket source is wired; the catalog then omits
// the field rather than inventing a rate.
type MarginBracketProvider interface {
	Tier1MaintenanceMarginRates(exchange, marketType string) map[string]float64
}

// NewRouterWithMetrics creates a router with optional metrics and market data endpoints.
// When mc is non-nil, /health and /ready use real health checks.
// When wsServer is non-nil, the chart WebSocket edge is mounted.
// When brackets is non-nil, /api/subscriptions also carries the tier-1 maintenance margin
// rate of each catalog symbol.
//
// The derivatives route depends on three separate read ports — series, baseline and price
// — so that a fake implementing only what a route uses can stand in for the store. A nil
// derivReader disables the route.
//
// A NIL `derivWindow` means "no durable tier": the ZSET answers alone and the read returns,
// at most, its cap. With it, the requested window crosses the cap — which is the reason the
// second provenance exists.
func NewRouterWithMetrics(
	ports ReadPorts,
	defaultExchange string,
	defaultMarketType string,
	mc *MetricsCollector,
	derivReader derivatives.Reader,
	derivBaselines derivatives.BaselineReader,
	derivPrices derivatives.PriceReader,
	tfReg *candle.TimeframeRegistry,
	wsServer *realtime.Server,
	brackets MarginBracketProvider,
	derivWindow *derivatives.DerivativesWindowReader,
	durableTimeframes ...string,
) http.Handler {
	return NewRouterWithMetricsAndLogger(ports, defaultExchange, defaultMarketType, mc,
		derivReader, derivBaselines, derivPrices, tfReg, wsServer, brackets, derivWindow, nil,
		durableTimeframes...)
}

// NewRouterWithMetricsAndLogger is NewRouterWithMetrics with the process logger injected.
func NewRouterWithMetricsAndLogger(
	ports ReadPorts,
	defaultExchange string,
	defaultMarketType string,
	mc *MetricsCollector,
	derivReader derivatives.Reader,
	derivBaselines derivatives.BaselineReader,
	derivPrices derivatives.PriceReader,
	tfReg *candle.TimeframeRegistry,
	wsServer *realtime.Server,
	brackets MarginBracketProvider,
	derivWindow *derivatives.DerivativesWindowReader,
	log *slog.Logger,
	durableTimeframes ...string,
) http.Handler {
	r := chi.NewRouter()
	r.Use(corsMiddleware)
	handler := &Handler{
		candles:           ports.Candles,
		batchCandles:      ports.BatchCandles,
		symbols:           ports.Symbols,
		allSymbols:        ports.AllSymbols,
		lastTrades:        ports.LastTrades,
		defaultExchange:   defaultExchange,
		defaultMarketType: defaultMarketType,
		tfReg:             tfReg,
		datetimeCache:     newRFC3339Cache(),
		brackets:          brackets,
		log:               logger.OrDiscard(log),
	}

	// the chart WebSocket path chart WebSocket edge (ticket-authenticated; Origin-checked). Mounted at
	// the top level so no body/timeout middleware interferes with the upgrade.
	if wsServer != nil {
		r.Get("/api/ws", wsServer.ServeWS)
		r.Get("/api/ws-ticket", wsServer.MintTicket)
	}

	// Auth handled by nginx gateway — user ID in X-Auth-User-Id header
	r.Group(func(r chi.Router) {
		r.Get("/api/timeseries", handler.GetTimeseriesQuery)
		r.Get("/api/subscriptions", handler.GetSubscriptions)

		// Market data endpoints (derivatives)
		if derivReader != nil {
			mdHandler := NewMarketDataHandler(derivReader, derivBaselines, derivPrices, defaultExchange, defaultMarketType, tfReg, log).
				WithDerivativesWindow(derivWindow, durableTimeframes...)
			r.Get("/api/batch/derivatives-series", mdHandler.GetBatchDerivativesSeries)
		}

		// Batch API — prices + volume24h for multiple symbols
		r.Get("/api/batch/prices", handler.GetBatchPrices)
	})

	// Public routes (no auth)
	r.Get("/openapi.json", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(log, w, http.StatusOK, BuildOpenAPISpecFromRoutes(r))
	})

	if mc != nil {
		r.Get("/health", mc.Health)
		r.Head("/health", mc.Health)
		r.Get("/ready", mc.Ready)
		r.Head("/ready", mc.Ready)
		if ports.Metrics != nil {
			r.Get("/metrics", ports.Metrics.ServeHTTP)
		}
	} else {
		r.Get("/health", handler.Health)
		r.Head("/health", handler.Health)
	}

	return r
}
