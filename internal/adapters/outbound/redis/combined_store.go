package redisstore

import (
	"context"

	"github.com/marxms/go-whale/internal/candle"
)

// CombinedStore composes the hot candle tier (TieredStore) with the Redis trade surface
// so that the composition root can hand ONE concrete value to consumers that each require
// a different capability.
//
// It embeds the candle-owned PORTS, not a composite interface: each field is a separate,
// minimal capability, so a consumer receives only the port it asked for. The concrete type
// implementing several ports is fine; no consumer may depend on this composition.
type CombinedStore struct {
	candle.Writer
	candle.Reader
	candle.Catalog
	candle.SeedWriter
	Redis *RedisStore
}

var (
	_ candle.Writer       = (*CombinedStore)(nil)
	_ candle.Reader       = (*CombinedStore)(nil)
	_ candle.Catalog      = (*CombinedStore)(nil)
	_ candle.SeedWriter   = (*CombinedStore)(nil)
	_ candle.SeedSplitter = (*CombinedStore)(nil)
)

// FoldSeedBatch forwards to the underlying store when it can split, and degrades to a no-op
// signal (nil) otherwise so the caller falls back to the single-call form.
//
// # WHY THE ASSERTION IS ON THE EMBEDDED PORT
//
// The seeder chooses the three-step order (fold, open bucket, persist) by type-asserting
// on the store it is given. A wrapper that embeds a broad interface does not promote the
// concrete engine's extra methods, so an assertion on the wrapper can fail silently.
//
// Asserting on the embedded SeedWriter port keeps the forwarding honest: any wrapper that
// fails to carry the splitter degrades here, visibly, and the compile-time assertion on
// this type still guarantees the production composition exposes it.
func (c *CombinedStore) FoldSeedBatch(exchange, marketType, symbol, timeframe string, cs []candle.Candle) []candle.Candle {
	if s, ok := c.SeedWriter.(candle.SeedSplitter); ok {
		return s.FoldSeedBatch(exchange, marketType, symbol, timeframe, cs)
	}
	return nil
}

// PersistSeedBatch forwards to the underlying store, or performs the un-split write.
func (c *CombinedStore) PersistSeedBatch(ctx context.Context, exchange, marketType, symbol, timeframe string, merged, raw []candle.Candle) error {
	if s, ok := c.SeedWriter.(candle.SeedSplitter); ok {
		return s.PersistSeedBatch(ctx, exchange, marketType, symbol, timeframe, merged, raw)
	}
	return c.SeedCandlesBatch(ctx, exchange, marketType, symbol, timeframe, raw)
}

// SetLastTrade forwards to Redis.
func (c *CombinedStore) SetLastTrade(ctx context.Context, exchange, marketType, symbol string, trade candle.Trade) error {
	return c.Redis.SetLastTrade(ctx, exchange, marketType, symbol, trade)
}

// GetLastTrade forwards to Redis.
func (c *CombinedStore) GetLastTrade(ctx context.Context, exchange, marketType, symbol string) (*candle.LastTrade, error) {
	return c.Redis.GetLastTrade(ctx, exchange, marketType, symbol)
}

// GetBatchCandlesDesc forwards to Redis. Batch price reads hit the durable ZSETs
// directly (single pipeline) rather than the hot CandleEngine; the embedded
// candle.Reader is the TieredStore whose write-through keeps Redis current.
func (c *CombinedStore) GetBatchCandlesDesc(ctx context.Context, exchange, marketType string, reqs []CandleReq) map[CandleReq][]candle.Candle {
	return c.Redis.GetBatchCandlesDesc(ctx, exchange, marketType, reqs)
}
