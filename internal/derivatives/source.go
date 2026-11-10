package derivatives

import (
	"context"

	"github.com/marxms/go-whale/internal/market"
)

// FundingRate is the canonical funding reading for one contract.
//
// Every field is optional for the same reason it is optional on the venue
// response: a missing rate is an ABSENT reading, not a rate of zero, and a
// consumer that cannot tell the two apart publishes a fabricated value.
type FundingRate struct {
	Rate         *float64
	NextRate     *float64
	MarkPrice    *float64
	InterestRate *float64
	Interval     *string
}

// FundingRateHistoryPoint is one settlement in a funding history.
//
// Timestamp and Rate stay pointers: the collector derives the settlement
// interval from the timestamps and skips rows whose instant is absent, while
// a row with an instant and no rate still contributes to the spacing. Making
// either a value type would collapse "absent" into zero and change the
// measured cadence.
type FundingRateHistoryPoint struct {
	Timestamp *int64
	Rate      *float64
}

// OpenInterest is the canonical open-interest reading for one contract.
//
// Amount is the contract count (the decision quantity). Timestamp is the
// observation instant when the venue sends one; the live poll stamps its own
// clock and ignores it.
type OpenInterest struct {
	Amount    *float64
	Timestamp *int64
}

// OpenInterestHistoryPoint is one open-interest sample in a history window.
type OpenInterestHistoryPoint struct {
	Timestamp *int64
	Amount    *float64
}

// LongShortRatioPoint is one long/short ratio sample.
//
// HasShares distinguishes "the pair was absent or failed the identity
// longAccount+shortAccount=1" from a pair of zeros, which would read as "no
// account is long".
type LongShortRatioPoint struct {
	Timestamp int64
	Ratio     float64
	LongPct   float64
	ShortPct  float64
	HasShares bool
}

// Source is the market-data port the derivatives collector consumes.
//
// It is declared here, at the consumer, and returns canonical types only: no
// vendor SDK value crosses this boundary. The concrete venue adapter owns the
// symbol translation, the retry/limit options and the error shapes.
type Source interface {
	// LoadMarkets warms and returns the venue metadata for this client. The
	// collector calls it before the first poll so symbol resolution is ready.
	LoadMarkets(ctx context.Context) (map[string]market.MarketInfo, error)

	// FundingRate returns the current funding reading for an internal
	// BASE/QUOTE symbol (BTC/USDT).
	FundingRate(ctx context.Context, symbol string) (FundingRate, error)

	// FundingRateHistory returns the most recent settlements, newest last.
	FundingRateHistory(ctx context.Context, symbol string, limit int64) ([]FundingRateHistoryPoint, error)

	// OpenInterest returns the current contract count for a symbol.
	OpenInterest(ctx context.Context, symbol string) (OpenInterest, error)

	// OpenInterestHistory returns samples inside [sinceMs, untilMs], newest
	// first as the venue serves them. untilMs is the exchange's endTime.
	OpenInterestHistory(ctx context.Context, symbol, timeframe string, sinceMs, untilMs, limit int64) ([]OpenInterestHistoryPoint, error)

	// LongShortRatioHistory returns ratio samples for a symbol. When
	// sinceMs > 0 and endMs > sinceMs, both bounds are sent; a zero window
	// asks for the newest reading only.
	LongShortRatioHistory(ctx context.Context, symbol, timeframe string, limit int64, sinceMs, endMs int64) ([]LongShortRatioPoint, error)

	// Close releases the underlying clients.
	Close() []error
}

// SourceFactory builds one Source connection for a venue scope. The
// composition root supplies the concrete adapter; the collector only sees
// this function type.
type SourceFactory func(exchangeID string, marketType market.MarketType) (Source, error)
