package ingestion

import (
	"sync"

	"github.com/marxms/go-whale/internal/candle"
)

// ohlcvcBucketPool and ohlcvcResultPool recycle the two working maps that
// candle.BuildOHLCVCInto needs, so the per-timeframe trade-batch aggregation
// does not allocate a fresh map pair on every call. NOTE: only the two map
// HEADERS are pooled — the per-bucket []Trade backing slices inside bucketPool are
// still GC-allocated per call (clear() only resets entries, it does not free them).
// That is expected; the dominant saving is the map-header alloc across the ~6000-key
// flush. StreamTradeHandler calls BuildOHLCVCPooled, the single owner of the
// get/use/clear/put cycle.
type ohlcvcPool struct {
	buckets sync.Pool
	results sync.Pool
}

func newOHLCVCPool() *ohlcvcPool {
	return &ohlcvcPool{
		buckets: sync.Pool{New: func() any { return make(map[int64][]candle.Trade) }},
		results: sync.Pool{New: func() any { return make(map[int64]candle.BucketedCandle) }},
	}
}

// BuildOHLCVCPooled aggregates trades into per-bucket candles using maps
// borrowed from package pools, invokes fn with the result map, and returns the
// maps to the pools cleared (clear(), not realloc) for reuse.
//
// The result map is only valid for the duration of fn: fn MUST consume it (copy
// the value-type BucketedCandle entries out) before returning, because the map
// is cleared and recycled immediately after. No reference into either pooled
// map is retained past this call.
//
// If fn panics, the panic propagates but the (now dirty) maps are DISCARDED —
// never returned to the pool — so a partial/inconsistent map can never be
// handed to a later call.
func (p *ohlcvcPool) build(trades []candle.Trade, timeframe string, fn func(result map[int64]candle.BucketedCandle)) {
	buckets := p.buckets.Get().(map[int64][]candle.Trade)
	result := p.results.Get().(map[int64]candle.BucketedCandle)

	// Guarded so a panic in fn discards the dirty maps instead of recycling them.
	returned := false
	defer func() {
		if !returned {
			// fn panicked (or returned abnormally): drop the maps, do not reuse.
			return
		}
		clear(buckets)
		clear(result)
		p.buckets.Put(buckets)
		p.results.Put(result)
	}()

	candle.BuildOHLCVCInto(trades, timeframe, buckets, result)
	fn(result)
	returned = true
}
