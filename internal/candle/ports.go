package candle

import (
	"context"
	"time"
)

// CandleMergeSource defines how a write reaches an existing candle row.
type CandleMergeSource string

// Merge-source labels recorded on a candle's provenance.
const (
	CandleMergeSourceWSDelta       CandleMergeSource = "ws_delta"
	CandleMergeSourceRESTTotal     CandleMergeSource = "rest_total"
	CandleMergeSourceSnapshotTotal CandleMergeSource = "snapshot_total"
	CandleMergeSourceWSKline       CandleMergeSource = "ws_kline"
)

// CandleUpdate is one streaming update in a batch.
type CandleUpdate struct {
	Exchange   string
	MarketType string
	Symbol     string
	Timeframe  string
	Candle     Candle
	Source     CandleMergeSource
}

// SymbolInfo carries the exchange context of an active symbol.
type SymbolInfo struct {
	Exchange   string
	MarketType string
	Symbol     string
}

// CandleReq asks for one (symbol, timeframe) window in a batch read.
type CandleReq struct {
	Symbol    string
	Timeframe string
	Limit     int
}

// Writer accepts streaming candle updates.
type Writer interface {
	AddStreamingCandle(ctx context.Context, exchange, marketType, symbol, timeframe string, candle Candle) error
	AddStreamingCandlesBatch(ctx context.Context, updates []CandleUpdate) error
	ReplaceCandle(ctx context.Context, exchange, marketType, symbol, timeframe string, candle Candle) error
}

// Reader serves candle queries.
type Reader interface {
	GetCandleByTimestamp(ctx context.Context, exchange, marketType, symbol, timeframe string, timestamp int64) (*Candle, error)
	GetCandlesDesc(ctx context.Context, exchange, marketType, symbol, timeframe string, limit int) ([]Candle, error)
	GetBatchCandlesDesc(ctx context.Context, exchange, marketType string, reqs []CandleReq) map[CandleReq][]Candle
	GetCandlesAsc(ctx context.Context, exchange, marketType, symbol, timeframe string, limit int) ([]Candle, error)
	GetCandlesByScoreRange(ctx context.Context, exchange, marketType, symbol, timeframe string, minScore, maxScore int64) ([]Candle, error)
	GetCandlesCount(ctx context.Context, exchange, marketType, symbol, timeframe string) (int64, error)
	GetCandlesRange(ctx context.Context, exchange, marketType, symbol, timeframe string, from, to time.Time, limit int) ([]Candle, error)
}

// Catalog tracks which symbols and timeframes are active.
type Catalog interface {
	AddActiveSymbol(ctx context.Context, exchange, marketType, symbol string) error
	GetActiveSymbols(ctx context.Context, exchange, marketType string) ([]string, error)
	GetAllActiveSymbols(ctx context.Context) ([]SymbolInfo, error)
	AddTimeframe(ctx context.Context, exchange, marketType, symbol, timeframe string) error
	GetTimeframes(ctx context.Context, exchange, marketType, symbol string) ([]string, error)
}

// SeedWriter writes historical and current-bucket candles during seed/recovery.
type SeedWriter interface {
	SeedCandlesBatch(ctx context.Context, exchange, marketType, symbol, timeframe string, candles []Candle) error
	SeedCurrentBucketCandle(ctx context.Context, exchange, marketType, symbol, timeframe string, candle Candle) error
}

// SeedSplitter separates the cheap fold from the expensive durable write so a
// caller can interleave the open-bucket update between them.
type SeedSplitter interface {
	FoldSeedBatch(exchange, marketType, symbol, timeframe string, cs []Candle) []Candle
	PersistSeedBatch(ctx context.Context, exchange, marketType, symbol, timeframe string, merged, raw []Candle) error
}

// WriteFloor is the durable-write boundary as the candle tier consumes it.
//
// The retention POLICY lives in internal/retention, not here: it owns durable
// retention, and the engine only needs the one number that keeps a write from
// re-creating a partition retention already dropped. Keeping the dependency as
// this port makes the engine independent of the policy package and lets tests
// pass a two-line fake.
type WriteFloor interface {
	// WriteFloor returns the earliest instant a durable write may carry, and
	// whether a floor exists at all. False means "write everything".
	WriteFloor(now time.Time) (time.Time, bool)
}
