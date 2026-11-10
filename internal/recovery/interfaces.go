package recovery

import (
	"context"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/market"
)

// HistorySource is the historical candle port recovery depends on.
//
// The symbol is ALWAYS the internal BASE/QUOTE form (BTC/USDT). The concrete
// adapter owns the venue-symbol translation, the request limits and the
// venue error shapes; no vendor type crosses this boundary.
type HistorySource interface {
	// FetchOHLCV returns OHLCV-only candles (six fields) for one window.
	// sinceMs <= 0 asks for the newest `limit` candles.
	FetchOHLCV(ctx context.Context, symbol, timeframe string, sinceMs, limit int64) ([]candle.Candle, error)

	// FetchKlinesWithCVD returns CVD-complete candles when the venue serves
	// the raw kline carrying taker-buy volume. ok=false means the venue does
	// not support it (or the response was unusable) and the caller must fall
	// back to FetchOHLCV.
	FetchKlinesWithCVD(ctx context.Context, symbol, timeframe string, limit int) ([]candle.Candle, bool, error)

	// Close releases the underlying client.
	Close() []error
}

// HistorySourceFactory builds one HistorySource for a venue scope.
type HistorySourceFactory func(exchangeID string, marketType market.MarketType) (HistorySource, error)

// Orchestrator is the public facade for gap recovery.
// Implemented by Manager.
type Orchestrator interface {
	Start(ctx context.Context)
	Stop()
	EnqueueBatch(exchangeID, marketType string, symbols []string, disconnectTime, reconnectTime time.Time)
	// EnqueueTargetedBucket asks for ONE closed bucket to be re-read. Separate from
	// EnqueueBatch because it scopes one bucket/timeframe and bypasses reconnect
	// materiality. A batch preserves its admitted window and checks materiality.
	EnqueueTargetedBucket(exchangeID, marketType, symbol, timeframe string, bucketOpenTime int64)
	ForceBackfill(req BackfillRequest) BackfillResponse
	Stats() GapRecoveryStats
	SetTimestampProvider(provider LatestTimestampProvider)
	SetFiller(filler GapFiller)
}

// ClientPool manages lazy creation and shared lifecycle of HistorySource.
type ClientPool interface {
	Get(exchangeID, marketType string) (HistorySource, error)
	CloseAll()
}

// CircuitBreaker prevents cascading failures per exchange scope.
type CircuitBreaker interface {
	IsOpen(key string) bool
	RecordFailure(key string) bool // returns true if it just opened
	RecordSuccess(key string)
}

// RetryScheduler manages deferred retry jobs with exponential backoff.
type RetryScheduler interface {
	Schedule(job BackfillRetryJob)
	// SetReenqueue wires the callback that hands a due retry job back to the
	// worker pipeline. Without it, due jobs are dropped (the N1 stranding bug).
	SetReenqueue(fn func(BackfillRetryJob))
	Start(ctx context.Context)
	Stop()
}

// Validator checks whether a backfill is complete. currentBucket is the open bucket of tf
// at validation time; the series must cover the previous closed bucket.
type Validator interface {
	Validate(ctx context.Context, exchangeID, marketType, symbol, tf string, gapStart, currentBucket int64) (bool, int)
}

// CandlesValidator validates candle data before redisstore.
type CandlesValidator interface {
	Validate(timestamp int64, open, high, low, closePrice, volume float64, symbol, tf string) (bool, string)
}

// StorageWriter seeds historical and current candle.
type StorageWriter interface {
	SeedCandlesBatch(ctx context.Context, exchange, marketType, symbol, timeframe string, candles []candle.Candle) error
	SeedCurrentBucketCandle(ctx context.Context, exchange, marketType, symbol, timeframe string, candle candle.Candle) error
	GetCandlesByScoreRange(ctx context.Context, exchange, marketType, symbol, timeframe string, minScore, maxScore int64) ([]candle.Candle, error)
	GetCandlesCount(ctx context.Context, exchange, marketType, symbol, timeframe string) (int64, error)
}

// storageSeedStore is the narrow persistence surface the default writer wraps.
type storageSeedStore interface {
	SeedCandlesBatch(ctx context.Context, exchange, marketType, symbol, timeframe string, candles []candle.Candle) error
	SeedCurrentBucketCandle(ctx context.Context, exchange, marketType, symbol, timeframe string, candle candle.Candle) error
	GetCandlesByScoreRange(ctx context.Context, exchange, marketType, symbol, timeframe string, minScore, maxScore int64) ([]candle.Candle, error)
	GetCandlesCount(ctx context.Context, exchange, marketType, symbol, timeframe string) (int64, error)
}

// DefaultStorageWriter adapts the storage seed surface to StorageWriter.
type DefaultStorageWriter struct{ Store storageSeedStore }

// SeedCandlesBatch delegates to the wrapped store.
func (w DefaultStorageWriter) SeedCandlesBatch(ctx context.Context, exchange, marketType, symbol, timeframe string, candles []candle.Candle) error {
	return w.Store.SeedCandlesBatch(ctx, exchange, marketType, symbol, timeframe, candles)
}

// SeedCurrentBucketCandle delegates to the wrapped store.
func (w DefaultStorageWriter) SeedCurrentBucketCandle(ctx context.Context, exchange, marketType, symbol, timeframe string, candle candle.Candle) error {
	return w.Store.SeedCurrentBucketCandle(ctx, exchange, marketType, symbol, timeframe, candle)
}

// GetCandlesByScoreRange delegates to the wrapped store.
func (w DefaultStorageWriter) GetCandlesByScoreRange(ctx context.Context, exchange, marketType, symbol, timeframe string, minScore, maxScore int64) ([]candle.Candle, error) {
	return w.Store.GetCandlesByScoreRange(ctx, exchange, marketType, symbol, timeframe, minScore, maxScore)
}

// GetCandlesCount delegates to the wrapped store.
func (w DefaultStorageWriter) GetCandlesCount(ctx context.Context, exchange, marketType, symbol, timeframe string) (int64, error) {
	return w.Store.GetCandlesCount(ctx, exchange, marketType, symbol, timeframe)
}

// DefaultCandlesValidator delegates to candle.ValidateCandle.
type DefaultCandlesValidator struct{}

// Validate delegates to candle.ValidateCandle.
func (DefaultCandlesValidator) Validate(timestamp int64, open, high, low, closePrice, volume float64, symbol, tf string) (bool, string) {
	return candle.ValidateCandle(timestamp, open, high, low, closePrice, volume, symbol, tf)
}
