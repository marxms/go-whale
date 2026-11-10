package venue

import (
	"context"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/market"
)

// driver is the request contract one (exchange, market type) scope implements. The Client
// façade resolves exactly one driver at construction and delegates every request to it, so
// porting a venue adds an implementation instead of editing a switch in the hot path.
//
// Capabilities is part of the contract because an absence must be readable from the same
// object that would answer the request: the driver declares what it cannot serve in the
// matrix and returns CapabilityAbsentError from the method.
type driver interface {
	FetchOHLCV(ctx context.Context, symbol, timeframe string, sinceMs, limit int64) ([]candle.Candle, error)
	FetchKlinesWithCVD(ctx context.Context, symbol, timeframe string, limit int) ([]candle.Candle, bool, error)
	FundingRate(ctx context.Context, symbol string) (derivatives.FundingRate, error)
	FundingRateHistory(ctx context.Context, symbol string, limit int64) ([]derivatives.FundingRateHistoryPoint, error)
	OpenInterest(ctx context.Context, symbol string) (derivatives.OpenInterest, error)
	OpenInterestHistory(ctx context.Context, symbol, timeframe string, sinceMs, untilMs, limit int64) ([]derivatives.OpenInterestHistoryPoint, error)
	LongShortRatioHistory(ctx context.Context, symbol, timeframe string, limit int64, sinceMs, endMs int64) ([]derivatives.LongShortRatioPoint, error)
	LoadMarkets(ctx context.Context) (map[string]market.MarketInfo, error)
	Capabilities() market.Capabilities
	Close() []error
}
