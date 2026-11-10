package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	logger "github.com/marxms/go-whale/internal/platform/logging"
	"github.com/marxms/go-whale/internal/trade"
)

// JSON response keys shared by every HTTP payload. They are part of the API contract, so
// the same spelling must not drift between handlers.
const (
	responseKeyStatus = "status"
	responseKeyError  = "error"
)

// errMsgSymbolRequired is the bad-request message for a missing symbol on every
// symbol-scoped route.
const errMsgSymbolRequired = "symbol is required"

// CandleReader is the closed-candle window the timeseries route serves.
type CandleReader interface {
	GetCandlesDesc(ctx context.Context, exchange, marketType, symbol, timeframe string, limit int) ([]candle.Candle, error)
	GetCandlesRange(ctx context.Context, exchange, marketType, symbol, timeframe string, from, to time.Time, limit int) ([]candle.Candle, error)
}

// CandleBatchReader is the multi-symbol candle read /api/batch/prices performs.
type CandleBatchReader interface {
	GetBatchCandlesDesc(ctx context.Context, exchange, marketType string, reqs []candle.CandleReq) map[candle.CandleReq][]candle.Candle
}

// SymbolCatalog is the active-symbol list /api/subscriptions and /api/batch/prices read.
type SymbolCatalog interface {
	GetActiveSymbols(ctx context.Context, exchange, marketType string) ([]string, error)
}

// AllSymbolsCatalog is the richer cross-exchange catalog. It is optional: when a store
// implements it, /api/subscriptions answers every configured scope in one call; when it
// does not, the route falls back to SymbolCatalog for the default scope.
type AllSymbolsCatalog interface {
	GetAllActiveSymbols(ctx context.Context) ([]candle.SymbolInfo, error)
}

// Handler serves the timeseries, trades, subscription and health endpoints.
type Handler struct {
	candles           CandleReader
	batchCandles      CandleBatchReader
	symbols           SymbolCatalog
	allSymbols        AllSymbolsCatalog
	lastTrades        trade.LastTradeReader
	defaultExchange   string
	defaultMarketType string
	tfReg             *candle.TimeframeRegistry
	// datetimeCache memoizes per-candle RFC3339 datetime strings. Bounded
	// and handler-scoped; never nil after construction via NewRouterWithMetrics.
	datetimeCache *rfc3339Cache
	// brackets serves the tier-1 maintenance margin rate per symbol. Nil when no bracket
	// source is wired.
	brackets MarginBracketProvider
	log      *slog.Logger
}

// tier1RatesFor returns the tier-1 maintenance margin rate for exactly the symbols this
// market carries, so the catalog grows by the symbols it already serves and not by the
// exchange's full bracket table. Nil when nothing is known — an absent field says "no
// answer", which is what lets the consumer fall back instead of trusting a default.
func (h *Handler) tier1RatesFor(exchange, marketType string, symbols []string) map[string]float64 {
	if h.brackets == nil {
		return nil
	}

	table := h.brackets.Tier1MaintenanceMarginRates(exchange, marketType)
	if len(table) == 0 {
		return nil
	}

	rates := make(map[string]float64, len(symbols))
	for _, symbol := range symbols {
		if rate, ok := table[symbol]; ok {
			rates[symbol] = rate
		}
	}

	if len(rates) == 0 {
		return nil
	}
	return rates
}

// Health endpoint for monitoring.
func (h *Handler) Health(w http.ResponseWriter, _ *http.Request) {
	resp := map[string]string{responseKeyStatus: "ok"}
	if h.tfReg != nil {
		resp["timeframe_checksum"] = h.tfReg.Checksum()
	}
	writeJSON(h.log, w, http.StatusOK, resp)
}

// GetTimeseriesQuery serves one symbol/timeframe window from the candle store.
func (h *Handler) GetTimeseriesQuery(w http.ResponseWriter, r *http.Request) {
	symbol := decodeSymbol(r.URL.Query().Get("symbol"))
	timeframe := r.URL.Query().Get("timeframe")
	exchange := r.URL.Query().Get("exchange")
	marketType := r.URL.Query().Get("marketType")

	// Use defaults if not provided
	if exchange == "" {
		exchange = h.defaultExchange
	}
	if marketType == "" {
		marketType = h.defaultMarketType
	}

	h.getTimeseriesWithParams(w, r, symbol, timeframe, exchange, marketType)
}

func (h *Handler) getTimeseriesWithParams(w http.ResponseWriter, r *http.Request, symbol, timeframe, exchange, marketType string) {
	if !h.tfReg.IsSupported(timeframe) {
		// CON-06: the typed code travels BESIDE the prose, not in its place. `error` stays
		// byte for byte as it was, so no consumer that reads it breaks; `code` gives the
		// analyzer's `invalid_timeframe` (liquidation.controller.ts) its counterpart on this
		// end — the requirement asks for the code "in the analyzer and in the streamer".
		writeJSON(h.log, w, http.StatusBadRequest, map[string]string{
			responseKeyError: "unsupported timeframe",
			"code":           "invalid_timeframe",
		})
		return
	}
	if symbol == "" {
		writeJSON(h.log, w, http.StatusBadRequest, map[string]string{responseKeyError: errMsgSymbolRequired})
		return
	}

	limit := parseLimit(r, 800)
	if limit > 5000 {
		limit = 5000
	}

	query := r.URL.Query()

	// Parse optional from/to millisecond timestamps
	var from, to time.Time
	hasRange := false
	if fromStr := query.Get("from"); fromStr != "" {
		if fromMs, err := strconv.ParseInt(fromStr, 10, 64); err == nil {
			from = time.UnixMilli(fromMs)
			hasRange = true
		}
	}
	if toStr := query.Get("to"); toStr != "" {
		if toMs, err := strconv.ParseInt(toStr, 10, 64); err == nil {
			to = time.UnixMilli(toMs)
			hasRange = true
		}
	}

	var items []candle.Candle
	var err error

	if hasRange {
		items, err = h.candles.GetCandlesRange(r.Context(), exchange, marketType, symbol, timeframe, from, to, limit)
	} else {
		items, err = h.candles.GetCandlesDesc(r.Context(), exchange, marketType, symbol, timeframe, limit)
	}

	if err != nil {
		writeJSON(h.log, w, http.StatusInternalServerError, map[string]string{responseKeyError: err.Error()})
		return
	}

	// Get last trade
	lastTrade, _ := h.lastTrades.GetLastTrade(r.Context(), exchange, marketType, symbol)

	response := TimeseriesResponse{
		Symbol:    symbol,
		Timeframe: timeframe,
		Count:     len(items),
		Total:     len(items),
		Data:      h.toCandleDTO(items),
	}
	if lastTrade != nil {
		response.LastTrade = &LastTradeDTO{
			Timestamp:  lastTrade.Timestamp,
			Datetime:   time.UnixMilli(lastTrade.Timestamp).UTC().Format(time.RFC3339),
			Price:      lastTrade.Price,
			Volume:     lastTrade.Volume,
			Side:       string(lastTrade.Side),
			ReceivedAt: time.UnixMilli(lastTrade.ReceivedAt).UTC().Format(time.RFC3339),
			Latency:    lastTrade.LatencyMS,
		}
	}

	writeJSON(h.log, w, http.StatusOK, response)
}

// GetSubscriptions serves the active symbols grouped by exchange and market.
func (h *Handler) GetSubscriptions(w http.ResponseWriter, r *http.Request) {
	// Get all active symbols with exchange context. The richer catalog is an explicit
	// optional port: a store that does not provide it falls back to the default scope.
	if h.allSymbols != nil {
		symbols, err := h.allSymbols.GetAllActiveSymbols(r.Context())
		if err != nil {
			writeJSON(h.log, w, http.StatusInternalServerError, map[string]string{responseKeyError: err.Error()})
			return
		}

		// Organize subscriptions hierarchically: exchange -> market -> symbols
		exchanges := make(map[string]*ExchangeSubData)

		for _, s := range symbols {
			// Ensure exchange exists
			if exchanges[s.Exchange] == nil {
				exchanges[s.Exchange] = &ExchangeSubData{
					Markets: make(map[string]MarketSubData),
				}
			}

			exData := exchanges[s.Exchange]

			// Ensure market exists
			market, exists := exData.Markets[s.MarketType]
			if !exists {
				market = MarketSubData{
					Symbols:    make([]string, 0),
					Timeframes: h.tfReg.All(),
				}
			}

			// Add symbol to market
			market.Symbols = append(market.Symbols, s.Symbol)
			market.TotalSymbols = len(market.Symbols)
			exData.Markets[s.MarketType] = market

			// Update exchange total
			exData.TotalSymbols++
		}

		// Build response
		response := SubscriptionsResponse{
			Total:     len(symbols),
			Exchanges: make(map[string]ExchangeSubData),
		}

		// Convert pointers to values for JSON, attaching the tier-1 maintenance margin
		// rate of each market's own symbols.
		for exID, exData := range exchanges {
			for mtID, market := range exData.Markets {
				market.MaintenanceMarginRate = h.tier1RatesFor(exID, mtID, market.Symbols)
				exData.Markets[mtID] = market
			}
			response.Exchanges[exID] = *exData
		}

		writeJSON(h.log, w, http.StatusOK, response)
	} else {
		// Fallback to default exchange
		symbols, err := h.symbols.GetActiveSymbols(r.Context(), h.defaultExchange, h.defaultMarketType)
		if err != nil {
			writeJSON(h.log, w, http.StatusInternalServerError, map[string]string{responseKeyError: err.Error()})
			return
		}

		// Format for single exchange/market
		market := MarketSubData{
			TotalSymbols:          len(symbols),
			Symbols:               symbols,
			Timeframes:            h.tfReg.All(),
			MaintenanceMarginRate: h.tier1RatesFor(h.defaultExchange, h.defaultMarketType, symbols),
		}

		exData := ExchangeSubData{
			TotalSymbols: len(symbols),
			Markets: map[string]MarketSubData{
				h.defaultMarketType: market,
			},
		}

		response := SubscriptionsResponse{
			Total: len(symbols),
			Exchanges: map[string]ExchangeSubData{
				h.defaultExchange: exData,
			},
		}

		writeJSON(h.log, w, http.StatusOK, response)
	}
}

func (h *Handler) toCandleDTO(items []candle.Candle) []CandleDTO {
	out := make([]CandleDTO, 0, len(items))
	for _, c := range items {
		out = append(out, CandleDTO{
			Timestamp:        c.Timestamp,
			Datetime:         h.datetimeCache.format(c.Timestamp),
			Open:             c.Open,
			High:             c.High,
			Low:              c.Low,
			Close:            c.Close,
			Volume:           c.Volume,
			TakerBuyVolume:   c.TakerBuyVolume,
			TakerSellVolume:  c.TakerSellVolume,
			Source:           c.Source,
			AggregateMethod:  c.AggregateMethod,
			SideUnknownCount: c.SideUnknownCount,
			LateTradeCount:   c.LateTradeCount,
			HasGap:           c.HasGap,
		})
	}
	return out
}

// ── Batch API ──────────────────────────────────────────────────────────

// GetBatchPrices returns latest close price + 24h volume for multiple symbols.
// GET /api/batch/prices?exchange=binance&marketType=future&timeframe=30m&symbols=BTC/USDT,ETH/USDT
// If symbols is omitted, returns data for all active symbols.
func (h *Handler) GetBatchPrices(w http.ResponseWriter, r *http.Request) {
	exchange := r.URL.Query().Get("exchange")
	marketType := r.URL.Query().Get("marketType")
	timeframe := r.URL.Query().Get("timeframe")
	symbolsRaw := r.URL.Query().Get("symbols")

	if exchange == "" {
		exchange = h.defaultExchange
	}
	if marketType == "" {
		marketType = h.defaultMarketType
	}
	if timeframe == "" {
		timeframe = h.tfReg.Default()
	}

	var symbols []string
	if symbolsRaw != "" {
		for _, s := range strings.Split(symbolsRaw, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				symbols = append(symbols, s)
			}
		}
	} else {
		var err error
		symbols, err = h.symbols.GetActiveSymbols(r.Context(), exchange, marketType)
		if err != nil {
			writeJSON(h.log, w, http.StatusInternalServerError, map[string]string{responseKeyError: err.Error()})
			return
		}
	}

	// ── Volume24h computation ──────────────────────────────────────────────
	// We use 1h candles (24 candles = exactly 24h) instead of 5m candles because:
	//
	// COVERAGE BUG (was 5m): Redis stores at most 200 candles per timeframe.
	// 200 × 5m = 1000 min = 16.67h — never a full 24h window.
	// 1h candles: 200 stored covers ~8 days; we only need 24 → full window guaranteed.
	//
	// UNIT BUG: candle.Volume is in BASE ASSET units (e.g., BTC for BTC/USDT).
	// Summing volumes and displaying with a "$" prefix showed "188K" for BTC/USDT
	// instead of ~$14B. Fix: multiply each candle's volume by its close price
	// to convert to QUOTE CURRENCY (USDT), which is what the UI expects.
	const candles24hCount = 24 // 1h × 24 = exactly 24h
	cutoff := time.Now().UnixMilli() - 24*60*60*1000

	// Build all reads up front: 2 requests per symbol — the primary timeframe
	// (price/pct) and 1h (24h volume) — then issue them as ONE pipeline.
	reqs := make([]candle.CandleReq, 0, len(symbols)*2)
	for _, symbol := range symbols {
		// Latest 2 candles: [0]=newest, [1]=previous (for price change %)
		reqs = append(reqs, candle.CandleReq{Symbol: symbol, Timeframe: timeframe, Limit: 2})
		// 1h candles (24 = exactly 24h) for the volume window.
		reqs = append(reqs, candle.CandleReq{Symbol: symbol, Timeframe: "1h", Limit: candles24hCount})
	}

	results := h.batchCandles.GetBatchCandlesDesc(r.Context(), exchange, marketType, reqs)

	data := make(map[string]*BatchPriceEntry, len(symbols))

	for _, symbol := range symbols {
		// Latest 2 candles: [0]=newest, [1]=previous (for price change %)
		latest := results[candle.CandleReq{Symbol: symbol, Timeframe: timeframe, Limit: 2}]
		if len(latest) == 0 {
			continue
		}

		entry := &BatchPriceEntry{
			Close:     latest[0].Close,
			Timestamp: latest[0].Timestamp,
		}

		// Compute price change % vs previous candle
		if len(latest) >= 2 && latest[1].Close > 0 {
			pct := (latest[0].Close - latest[1].Close) / latest[1].Close * 100
			entry.PriceChangePct = &pct
		}

		// Sum 1h candle volumes (in USDT = Volume × Close) for a full 24h window.
		// Using 1h timeframe ensures 24 candles always covers exactly 24h with
		// our 200-candle storage (1h × 200 = ~8 days available).
		oneHourCandles := results[candle.CandleReq{Symbol: symbol, Timeframe: "1h", Limit: candles24hCount}]
		var totalVolUsdt float64
		for _, c := range oneHourCandles {
			if c.Timestamp >= cutoff {
				// Volume × Close converts base-asset units → quote currency (USDT)
				totalVolUsdt += c.Volume * c.Close
			}
		}
		entry.Volume24h = totalVolUsdt

		data[symbol] = entry
	}

	writeJSON(h.log, w, http.StatusOK, BatchPricesResponse{
		Timeframe: timeframe,
		Data:      data,
	})
}

// writeJSON is the single JSON response path for this package.
//
// The encode error is LOGGED, not returned: by the time it can fail the status line and
// headers are already on the wire, so there is no status left to change and the client
// will see a truncated body either way. Logging is the only actionable outcome — and a
// silent discard here would hide exactly the serialization bugs that produce those
// truncated responses.
func writeJSON(log *slog.Logger, w http.ResponseWriter, status int, payload any) {
	log = logger.OrDiscard(log)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Warn("http: encoding response body failed after the status was sent", responseKeyStatus, status, responseKeyError, err)
	}
}

func parseLimit(r *http.Request, fallback int) int {
	limitStr := r.URL.Query().Get("limit")
	if limitStr == "" {
		return fallback
	}
	value, err := strconv.Atoi(limitStr)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func decodeSymbol(raw string) string {
	if raw == "" {
		return raw
	}
	decoded, err := url.PathUnescape(raw)
	if err != nil {
		return raw
	}
	return decoded
}
