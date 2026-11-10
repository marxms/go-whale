package httpapi

import "time"

// TimeseriesResponse is the JSON body of one timeseries window.
type TimeseriesResponse struct {
	Symbol    string        `json:"symbol"`
	Timeframe string        `json:"timeframe"`
	Count     int           `json:"count"`
	Total     int           `json:"total"`
	LastTrade *LastTradeDTO `json:"lastTrade,omitempty"`
	Data      []CandleDTO   `json:"data"`
}

// AllTimeframesResponse groups every timeframe of one symbol.
type AllTimeframesResponse struct {
	Symbol     string                         `json:"symbol"`
	Timeframes map[string]TimeframeDataBundle `json:"timeframes"`
}

// TimeframeDataBundle is one timeframe's candles in an AllTimeframesResponse.
type TimeframeDataBundle struct {
	Count int         `json:"count"`
	Total int         `json:"total"`
	Data  []CandleDTO `json:"data"`
}

// CandleDTO is one candle as served on the JSON wire.
type CandleDTO struct {
	Timestamp int64   `json:"timestamp"`
	Datetime  string  `json:"datetime"`
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    float64 `json:"volume"`

	// Trade-signal-clarity additive aggregates. Mirrored from
	// candles.Candle so the analyzer streamer-native path receives real taker
	// volumes instead of silently falling back to clv-proxy. All omitempty so
	// existing consumers and historical candles serialize unchanged.
	// The measured pair, ALWAYS present — see candles.Candle for why omitempty is wrong
	// here: ~1% of bars have one side legitimately at zero, and with the derived fields gone
	// there is no second copy to recover an erased zero from.
	TakerBuyVolume   float64 `json:"takerBuyVolume"`
	TakerSellVolume  float64 `json:"takerSellVolume"`
	Source           string  `json:"source,omitempty"`
	AggregateMethod  string  `json:"aggregateMethod,omitempty"`
	SideUnknownCount int64   `json:"sideUnknownCount,omitempty"`
	LateTradeCount   int64   `json:"lateTradeCount,omitempty"`
	HasGap           bool    `json:"hasGap,omitempty"`
}

// TradesResponse is the JSON body of one trade window.
type TradesResponse struct {
	Symbol string     `json:"symbol"`
	Count  int        `json:"count"`
	Total  int        `json:"total"`
	Data   []TradeDTO `json:"data"`
}

// TradeDTO is one trade as served on the JSON wire.
type TradeDTO struct {
	ID        string   `json:"id"`
	Timestamp int64    `json:"timestamp"`
	Datetime  string   `json:"datetime"`
	Symbol    string   `json:"symbol"`
	Side      string   `json:"side"`
	Price     float64  `json:"price"`
	Amount    float64  `json:"amount"`
	Cost      *float64 `json:"cost"`
}

// SubscriptionsResponse is the hierarchical subscriptions structure.
type SubscriptionsResponse struct {
	Total     int                        `json:"total"`
	Exchanges map[string]ExchangeSubData `json:"exchanges"`
}

// ExchangeSubData is the subscription data per exchange.
type ExchangeSubData struct {
	TotalSymbols int                      `json:"total_symbols"`
	Markets      map[string]MarketSubData `json:"markets"`
}

// MarketSubData is the subscription data per market type.
type MarketSubData struct {
	TotalSymbols int      `json:"total_symbols"`
	Symbols      []string `json:"symbols"`
	Timeframes   []string `json:"timeframes,omitempty"`
	// MaintenanceMarginRate carries the tier-1 maintenance margin rate of each symbol in
	// Symbols, as a fraction (0.004 = 0.40%). Omitted entirely when no bracket source is
	// wired, so a consumer reads "the catalog has no answer" and falls back to its own
	// persisted value instead of to a silent default.
	MaintenanceMarginRate map[string]float64 `json:"maintenance_margin_rate,omitempty"`
}

// LastTradeDTO is the most recent trade snapshot served with a timeseries.
type LastTradeDTO struct {
	Timestamp  int64   `json:"timestamp"`
	Datetime   string  `json:"datetime"`
	Price      float64 `json:"price"`
	Volume     float64 `json:"volume"`
	Side       string  `json:"side"`
	ReceivedAt string  `json:"receivedAt"`
	Latency    int64   `json:"latency"`
}

// SubscribeResponse acknowledges a subscription request.
type SubscribeResponse struct {
	Message string `json:"message"`
	Status  string `json:"status"`
}

// ── Batch API DTOs ─────────────────────────────────────────────────────

// BatchPriceEntry represents a single symbol's latest price data.
type BatchPriceEntry struct {
	Close          float64  `json:"close"`
	Timestamp      int64    `json:"timestamp"`
	Volume24h      float64  `json:"volume24h"`
	PriceChangePct *float64 `json:"priceChangePct"` // % change vs previous candle close
}

// BatchPricesResponse is the response for GET /api/batch/prices.
type BatchPricesResponse struct {
	Timeframe string                      `json:"timeframe"`
	Data      map[string]*BatchPriceEntry `json:"data"`
}

// BatchDerivativesSeriesPoint is a flat derivatives data point for batch responses.
type BatchDerivativesSeriesPoint struct {
	Timestamp      int64    `json:"timestamp"`
	OpenInterest   *float64 `json:"openInterest,omitempty"`
	OIChangePct    *float64 `json:"oiChangePct,omitempty"`
	FundingRate    *float64 `json:"fundingRate,omitempty"`
	LongShortRatio *float64 `json:"longShortRatio,omitempty"`
}

// BatchDerivativesSeriesResponse is the response for GET /api/batch/derivatives-series.
type BatchDerivativesSeriesResponse struct {
	Timeframe string                                   `json:"timeframe"`
	Data      map[string][]BatchDerivativesSeriesPoint `json:"data"`
}

// GapCheckResult represents the result of a gap check.
type GapCheckResult struct {
	ExchangeID string
	MarketType string
	Symbol     string
	Timeframe  string
	HasGap     bool
	GapCount   int
	Gaps       []GapDetail
	CheckedAt  time.Time
}

// GapDetail represents a single detected gap.
type GapDetail struct {
	ExpectedTimestamp int64 // ms — the timestamp that should have existed
	ActualTimestamp   int64 // ms — the next actual timestamp found (0 if none)
	DurationMs        int64 // ActualTimestamp - ExpectedTimestamp
}
