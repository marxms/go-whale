package derivatives

import (
	"context"
	"time"
)

// Writer persists derivatives snapshots.
type Writer interface {
	SaveSnapshot(ctx context.Context, snapshot *DerivativesSnapshot) error
	SaveSeriesOnly(ctx context.Context, snapshot *DerivativesSnapshot, timeframe string) error
}

// Reader serves the latest snapshot and per-timeframe series.
type Reader interface {
	GetLatestSnapshot(ctx context.Context, exchange, marketType, symbol string) (*DerivativesSnapshot, error)
	GetSnapshotSeries(ctx context.Context, exchange, marketType, symbol, timeframe string, limit int) ([]*DerivativesSnapshot, error)
}

// BaselineReader serves the persisted robust ruler for one series.
type BaselineReader interface {
	GetBaseline(ctx context.Context, exchange, marketType, symbol, timeframe string) (*DerivativesBaseline, error)
}

// PriceReader serves r(t) = ln(close(t)/close(t-1)) from the candle series.
type PriceReader interface {
	PriceIncrement(ctx context.Context, exchange, marketType, symbol, timeframe string, barTs int64) (increment *float64, reason string, err error)
}

// ColdWriter persists derivatives points in the durable tier. It takes the
// WHOLE slice on purpose: a one-point API is an API that will be called in a
// loop, and the loop is the write amplification the durable table cannot take.
type ColdWriter interface {
	WriteDerivativesBatch(ctx context.Context, points []DerivativesColdPoint) error
}

// ColdReader serves durable derivatives points for one scope and window,
// ascending by timestamp.
type ColdReader interface {
	QueryDerivativesRange(ctx context.Context, scope DerivativesScope, from, to time.Time) ([]*DerivativesSnapshot, error)
}
