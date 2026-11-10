// Package redisstore implements the hot-tier candle, derivatives and subscription
// stores on Redis.
package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"
	logger "github.com/marxms/go-whale/internal/platform/logging"

	"github.com/redis/go-redis/v9"
)

const redisOpTimeoutV2 = 1 * time.Second // Per-pipeline timeout (500ms was too tight under pool contention with 500 assets)

// redisDefaultOpTimeout is the safety net for operations that callers invoke without a deadline.
// Prevents any Redis call from blocking a goroutine indefinitely (e.g. pool exhaustion).
const redisDefaultOpTimeout = 3 * time.Second

const (
	redisCASMaxRetries       = 5
	redisCASRetryBackoffStep = 20 * time.Millisecond
)

// retryOp executes fn up to maxAttempts times with linear backoff.
// Used for transient Redis failures (pool exhaustion, timeout spikes).
func retryOp(log *slog.Logger, label string, maxAttempts int, fn func() error) error {
	log = logger.OrDiscard(log)
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt < maxAttempts {
			backoff := time.Duration(attempt*100) * time.Millisecond
			log.Warn("redis operation failed; retrying",
				"label", label, "attempt", attempt, "max_attempts", maxAttempts, "retry_in", backoff, "error", err)
			time.Sleep(backoff)
		}
	}
	return lastErr
}

// ensureTimeout returns a context with a deadline. If the parent already has one, it is returned as-is.
// Otherwise a new child context with the given timeout is created.
// This is the single guard that prevents naked (unbounded) Redis operations.
func ensureTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := parent.Deadline(); ok {
		// Caller already set a deadline — respect it.
		return parent, func() {}
	}
	return context.WithTimeout(parent, timeout)
}

// RedisStore is the multi-exchange aware Redis store. Key format:
// candles:{exchange}:{marketType}:{symbol}:{timeframe}.
type RedisStore struct {
	client *redis.Client
	// candleCap is the fallback members-per-ZSET count for a timeframe nothing declares; the
	// effective retention is per timeframe. See candleCapFor.
	candleCap int
	// candleEngine carries the operator's retention declaration, which makes the cap per
	// timeframe.
	candleEngine config.CandleEngineConfig
	tfReg        *candle.TimeframeRegistry
	log          *slog.Logger
	// subCache memoizes GetAllActiveSymbols for this store. It is per-instance state: a cache
	// shared across stores would let one store serve data another had already invalidated.
	subCache cachedSubscriptions
}

// NewRedisStore builds the hot tier and verifies connectivity. ctx bounds the startup ping
// (5s ceiling): the composition root has no request context, while tests can pass theirs.
func NewRedisStore(ctx context.Context, cfg config.Config, candleCap int, tfReg *candle.TimeframeRegistry, logs ...*slog.Logger) (*RedisStore, error) {
	if candleCap <= 0 {
		// OVERRIDABLE DEFAULT: only reaches a caller that passes zero. Production passes
		// config.DefaultResidentDepth; 201 keeps 200 closed bars for EMA warmup with one open.
		candleCap = 201
	}
	client := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		Password:     cfg.RedisPassword,
		DB:           cfg.RedisDB,
		PoolSize:     cfg.RedisPoolSize,
		MinIdleConns: cfg.RedisMinIdleConns,
		DialTimeout:  time.Duration(cfg.RedisDialTimeoutS) * time.Second,
		ReadTimeout:  time.Duration(cfg.RedisReadTimeoutS) * time.Second,
		WriteTimeout: time.Duration(cfg.RedisWriteTimeoutS) * time.Second,
		PoolTimeout:  time.Duration(cfg.RedisPoolTimeoutS) * time.Second,
		MaxRetries:   cfg.RedisMaxRetries,
		// Make the existing per-operation/request deadlines reach socket I/O.
		ContextTimeoutEnabled: true,
	})
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		// The client is already running its pool and maintenance goroutines even though
		// the ping failed. Returning without closing it strands them for the life of the
		// process (observed by goleak in internal/app), so the failed constructor releases
		// what it created.
		_ = client.Close()
		return nil, err
	}
	log := logger.OrDiscard(logs...)
	log.Info("redis pool configured",
		"pool_size", cfg.RedisPoolSize,
		"min_idle", cfg.RedisMinIdleConns,
		"read_timeout_s", cfg.RedisReadTimeoutS,
		"write_timeout_s", cfg.RedisWriteTimeoutS,
		"max_retries", cfg.RedisMaxRetries,
		"candle_cap_fallback", candleCap)
	store := &RedisStore{client: client, candleCap: candleCap, candleEngine: cfg.CandleEngine, tfReg: tfReg, log: log}
	store.logCandleRetention()
	return store, nil
}

// candleCapFor resolves the resident retention of one timeframe's candle ZSET. The ZSET has no
// TTL, so this count is the retention, and a 1w member covers 2016x the wall time of a 5m one.
// The declaration lives in candle_engine.redis_cap_by_timeframe, separate from buffer_sizes,
// which sizes the ring and seed depth, so resident memory stays independent of chart depth.
func (s *RedisStore) candleCapFor(timeframe string) int {
	if declared := s.candleEngine.RedisCapFor(timeframe); declared > 0 {
		return declared
	}
	return s.candleCap
}

// logCandleRetention declares the resident retention per timeframe once, at construction, the
// same way the derivatives store declares its own. An undeclared retention is a decision
// nobody can notice going wrong.
func (s *RedisStore) logCandleRetention() {
	if s.tfReg == nil {
		return
	}
	for _, tf := range s.tfReg.All() {
		members := s.candleCapFor(tf)
		horizonH := float64(int64(members)*candle.ParseTimeframeToSeconds(tf)) / 3600.0
		origin := "candle_engine.buffer_sizes (no dedicated entry)"
		if s.candleEngine.RedisCapByTimeframe[tf] > 0 {
			origin = "candle_engine.redis_cap_by_timeframe"
		}
		logger.OrDiscard(s.log).Info("candles: resident retention",
			"timeframe", tf, "members", members, "horizon_hours", horizonH, "origin", origin)
	}
}

// Close releases the underlying Redis client. The owner of the store is
// responsible for calling it exactly once at shutdown.
func (s *RedisStore) Close() error {
	return s.client.Close()
}

// Client returns the underlying Redis client for sharing with other stores.
func (s *RedisStore) Client() *redis.Client {
	return s.client
}

// ============================================================================
// KEY GENERATION - Multi-Exchange Format
// ============================================================================

func candlesKeyV2(exchange, marketType, symbol, timeframe string) string {
	return fmt.Sprintf("candles:%s:%s:%s:%s", exchange, marketType, symbol, timeframe)
}

func activeSymbolsKeyV2(exchange, marketType string) string {
	return fmt.Sprintf("active_symbols:%s:%s", exchange, marketType)
}

// scopeRegistryKey is a SET that tracks all "exchange:marketType" scopes that have active symbols.
// Used by GetAllActiveSymbols to avoid SCAN across the entire keyspace.
const scopeRegistryKey = "active_symbol_scopes"

func timeframesKeyV2(exchange, marketType, symbol string) string {
	return fmt.Sprintf("timeframes:%s:%s:%s", exchange, marketType, symbol)
}

func lastTradeKeyV2(exchange, marketType, symbol string) string {
	return fmt.Sprintf("last_trade:%s:%s:%s", exchange, marketType, symbol)
}

// ============================================================================
// STREAMING OPERATIONS
// ============================================================================

// AddStreamingCandle merges a live candle into the hot tier under a CAS upsert.
func (s *RedisStore) AddStreamingCandle(ctx context.Context, exchange, marketType, symbol, timeframe string, bar candle.Candle) error {
	key := candlesKeyV2(exchange, marketType, symbol, timeframe)
	err := s.upsertCandleWithCAS(ctx, key, timeframe, bar.Timestamp, func(current *candle.Candle, now int64) candle.Candle {
		if current == nil {
			merged := bar
			merged.CreatedAt = now
			merged.LastUpdate = now
			return merged
		}

		if s.isCandleClosed(bar.Timestamp, timeframe) {
			return mergeClosedCandleBySource(*current, bar, CandleMergeSourceWSDelta, now)
		}

		return mergeOpenCandleBySource(*current, bar, CandleMergeSourceWSDelta, now)
	}, true)
	if err != nil {
		return err
	}

	_ = s.AddActiveSymbol(ctx, exchange, marketType, symbol)
	_ = s.AddTimeframe(ctx, exchange, marketType, symbol, timeframe)
	return nil
}

// AddStreamingCandlesBatch applies a batch of live updates; retries stay at per-key CAS so a partially applied batch is never doubled.
func (s *RedisStore) AddStreamingCandlesBatch(ctx context.Context, updates []CandleUpdate) error {
	if len(updates) == 0 {
		return nil
	}

	// Keep batch semantics deterministic: retries happen at per-key CAS level.
	// Retrying an already partially-applied ws_delta batch can double-apply volumes.
	return s.addStreamingCandlesBatchOnce(ctx, updates)
}

func (s *RedisStore) addStreamingCandlesBatchOnce(ctx context.Context, updates []CandleUpdate) error {
	// Aggregate duplicates
	type candleKey struct {
		exchange   string
		marketType string
		symbol     string
		timeframe  string
		timestamp  int64
		source     CandleMergeSource
	}
	aggregated := make(map[candleKey]candle.Candle)

	for _, update := range updates {
		source := normalizeCandleMergeSource(update.Source)
		key := candleKey{
			exchange:   update.Exchange,
			marketType: update.MarketType,
			symbol:     update.Symbol,
			timeframe:  update.Timeframe,
			timestamp:  update.Candle.Timestamp,
			source:     source,
		}

		if existing, found := aggregated[key]; found {
			now := time.Now().UnixMilli()
			aggregated[key] = mergeOpenCandleBySource(existing, update.Candle, source, now)
		} else {
			aggregated[key] = update.Candle
		}
	}

	for ck, candleToUpdate := range aggregated {
		ck := ck
		candleToUpdate := candleToUpdate
		redisKey := candlesKeyV2(ck.exchange, ck.marketType, ck.symbol, ck.timeframe)

		err := s.upsertCandleWithCAS(ctx, redisKey, ck.timeframe, candleToUpdate.Timestamp, func(current *candle.Candle, now int64) candle.Candle {
			if current == nil {
				merged := candleToUpdate
				merged.CreatedAt = now
				merged.LastUpdate = now
				return merged
			}

			if s.isCandleClosed(candleToUpdate.Timestamp, ck.timeframe) {
				return mergeClosedCandleBySource(*current, candleToUpdate, ck.source, now)
			}

			return mergeOpenCandleBySource(*current, candleToUpdate, ck.source, now)
		}, true)
		if err != nil {
			return err
		}
	}

	// Update metadata
	seen := make(map[string]struct{})
	for ck := range aggregated {
		metaKey := fmt.Sprintf("%s:%s:%s", ck.exchange, ck.marketType, ck.symbol)
		if _, ok := seen[metaKey]; !ok {
			seen[metaKey] = struct{}{}
			_ = s.AddActiveSymbol(ctx, ck.exchange, ck.marketType, ck.symbol)
		}
		tfKey := metaKey + ":" + ck.timeframe
		if _, ok := seen[tfKey]; !ok {
			seen[tfKey] = struct{}{}
			_ = s.AddTimeframe(ctx, ck.exchange, ck.marketType, ck.symbol, ck.timeframe)
		}
	}

	return nil
}

// SeedCandlesBatch seeds historical candles for one series, merging over existing bars.
func (s *RedisStore) SeedCandlesBatch(ctx context.Context, exchange, marketType, symbol, timeframe string, candlesToSeed []candle.Candle) error {
	if len(candlesToSeed) == 0 {
		return nil
	}

	key := candlesKeyV2(exchange, marketType, symbol, timeframe)
	now := time.Now().UnixMilli()
	timeframeSecs := candle.ParseTimeframeToSeconds(timeframe)
	timeframeMs := timeframeSecs * 1000

	// Read all existing
	readPipe := s.client.Pipeline()
	for _, candle := range candlesToSeed {
		score := strconv.FormatInt(candle.Timestamp, 10)
		readPipe.ZRangeByScore(ctx, key, &redis.ZRangeBy{
			Min:    score,
			Max:    score,
			Offset: 0,
			Count:  1,
		})
	}

	tctx, cancel := context.WithTimeout(ctx, redisOpTimeoutV2*2)
	readResults, err := readPipe.Exec(tctx)
	cancel()
	if err != nil {
		return err
	}

	// Merge and write
	writePipe := s.client.Pipeline()

	for i, bar := range candlesToSeed {
		bucketClosed := (now - bar.Timestamp) >= timeframeMs

		var existing []string
		if readResults[i].Err() == nil {
			if cmd, ok := readResults[i].(*redis.StringSliceCmd); ok {
				existing, _ = cmd.Result()
			}
		}

		var merged candle.Candle
		if len(existing) > 0 {
			current, decErr := decodeCandleMember(existing[0])
			if decErr != nil {
				return decErr
			}

			if bucketClosed {
				// Preserve every field of the existing candle; update only OHLCV.
				merged = current
				merged.Open = bar.Open
				merged.High = maxFloatV2(current.High, bar.High)
				merged.Low = minFloatV2(current.Low, bar.Low)
				merged.Close = bar.Close
				// A MAX here is a ratchet: an inflated stored value survives every later
				// repair, and this path is where the exchange's own repairs arrive. Where the
				// incoming record is the exchange's final word it settles instead — see
				// settlesClosedBucket.
				merged.Volume = maxFloatV2(current.Volume, bar.Volume)
				if settlesClosedBucket(bar, CandleMergeSource(bar.Source)) {
					merged.Volume = bar.Volume
					merged.High = bar.High
					merged.Low = bar.Low
				}
				merged.LastUpdate = now
				// A closed-bucket REST kline carries definitive order-flow totals; prefer it
				// over a CVD-void row, while the > 0 gate avoids zeroing a live-rich one.
				if bar.TakerBuyVolume > 0 || bar.TakerSellVolume > 0 {
					merged.TakerBuyVolume = bar.TakerBuyVolume
					merged.TakerSellVolume = bar.TakerSellVolume
				}
			} else {
				// Open bucket: preserve the existing open/close, update high/low/volume.
				merged = current
				merged.High = maxFloatV2(current.High, bar.High)
				merged.Low = minFloatV2(current.Low, bar.Low)
				merged.Volume = maxFloatV2(current.Volume, bar.Volume)
				merged.LastUpdate = now
			}
		} else {
			merged = bar
			merged.CreatedAt = now
			merged.LastUpdate = now
			// Tag the backfill origin for auditing.
			merged.Source = "gap-recovery"
		}

		payload, err := encodeCandleMember(merged)
		if err != nil {
			return err
		}

		mergedScore := strconv.FormatInt(merged.Timestamp, 10)
		writePipe.ZRemRangeByScore(ctx, key, mergedScore, mergedScore)
		writePipe.ZAdd(ctx, key, redis.Z{Score: float64(merged.Timestamp), Member: payload})
	}

	// The seed leg also respects retention; without this cap every boot restores buffer_sizes
	// members per key. What protects history is the order: TieredStore.PersistSeedBatch writes
	// the durable tier first, so what the cap removes here is already cold. ReplaceCandle below
	// is the remaining exception, writing one bucket with no cold counterpart.
	writePipe.ZRemRangeByRank(ctx, key, 0, -(int64(s.candleCapFor(timeframe)) + 1))

	tctx, cancel = context.WithTimeout(ctx, redisOpTimeoutV2*2)
	_, err = writePipe.Exec(tctx)
	cancel()
	if err != nil {
		return err
	}

	_ = s.AddActiveSymbol(ctx, exchange, marketType, symbol)
	_ = s.AddTimeframe(ctx, exchange, marketType, symbol, timeframe)

	logger.OrDiscard(s.log).Debug("seed candles batch persisted",
		"exchange", exchange, "market_type", marketType, "symbol", symbol,
		"timeframe", timeframe, "candles", len(candlesToSeed))
	return nil
}

// SeedCurrentBucketCandle seeds the open bucket from a REST total.
func (s *RedisStore) SeedCurrentBucketCandle(ctx context.Context, exchange, marketType, symbol, timeframe string, bar candle.Candle) error {
	key := candlesKeyV2(exchange, marketType, symbol, timeframe)
	err := s.upsertCandleWithCAS(ctx, key, timeframe, bar.Timestamp, func(current *candle.Candle, now int64) candle.Candle {
		if current == nil {
			merged := bar
			merged.CreatedAt = now
			merged.LastUpdate = now
			return merged
		}
		return mergeOpenCandleBySource(*current, bar, CandleMergeSourceRESTTotal, now)
	}, false)
	if err != nil {
		return err
	}

	_ = s.AddActiveSymbol(ctx, exchange, marketType, symbol)
	_ = s.AddTimeframe(ctx, exchange, marketType, symbol, timeframe)
	return nil
}

// ============================================================================
// QUERY OPERATIONS
// ============================================================================

// ReplaceCandle overwrites the bar at its timestamp on the backfill path.
func (s *RedisStore) ReplaceCandle(ctx context.Context, exchange, marketType, symbol, timeframe string, candle candle.Candle) error {
	key := candlesKeyV2(exchange, marketType, symbol, timeframe)
	now := time.Now().UnixMilli()
	if candle.CreatedAt == 0 {
		candle.CreatedAt = now
	}
	candle.LastUpdate = now

	payload, err := encodeCandleMember(candle)
	if err != nil {
		return err
	}

	score := strconv.FormatInt(candle.Timestamp, 10)
	pipe := s.client.Pipeline()
	pipe.ZRemRangeByScore(ctx, key, score, score)
	pipe.ZAdd(ctx, key, redis.Z{Score: float64(candle.Timestamp), Member: payload})
	// ReplaceCandle is used by reconnection_sync (backfill) — it does not apply a cap.

	tctx, cancel := context.WithTimeout(ctx, redisOpTimeoutV2)
	_, err = pipe.Exec(tctx)
	cancel()
	if err != nil {
		return err
	}

	_ = s.AddActiveSymbol(ctx, exchange, marketType, symbol)
	_ = s.AddTimeframe(ctx, exchange, marketType, symbol, timeframe)
	return nil
}

// GetCandleByTimestamp returns the bar at one timestamp, or nil when absent.
func (s *RedisStore) GetCandleByTimestamp(ctx context.Context, exchange, marketType, symbol, timeframe string, timestamp int64) (*candle.Candle, error) {
	key := candlesKeyV2(exchange, marketType, symbol, timeframe)
	score := strconv.FormatInt(timestamp, 10)
	tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	items, err := s.client.ZRangeByScore(tctx, key, &redis.ZRangeBy{
		Min:    score,
		Max:    score,
		Offset: 0,
		Count:  1,
	}).Result()
	cancel()
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, nil
	}
	c, err := decodeCandleMember(items[0])
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// dedupCandlesByTimestamp removes duplicate timestamps from a candle slice,
// keeping the first occurrence (which is the latest write for desc-sorted data).
func dedupCandlesByTimestamp(cs []candle.Candle) []candle.Candle {
	if len(cs) <= 1 {
		return cs
	}
	seen := make(map[int64]struct{}, len(cs))
	result := make([]candle.Candle, 0, len(cs))
	for _, c := range cs {
		if _, ok := seen[c.Timestamp]; ok {
			continue
		}
		seen[c.Timestamp] = struct{}{}
		result = append(result, c)
	}
	return result
}

// GetCandlesDesc returns up to limit candles newest-first.
func (s *RedisStore) GetCandlesDesc(ctx context.Context, exchange, marketType, symbol, timeframe string, limit int) ([]candle.Candle, error) {
	if limit <= 0 {
		limit = 100
	}
	key := candlesKeyV2(exchange, marketType, symbol, timeframe)
	tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	data, err := s.client.ZRevRange(tctx, key, 0, int64(limit-1)).Result()
	cancel()
	if err != nil {
		return nil, err
	}
	decoded, err := decodeCandlesV2(data)
	if err != nil {
		return nil, err
	}
	return dedupCandlesByTimestamp(decoded), nil
}

// GetBatchCandlesDesc reads many (symbol, timeframe) candle windows in one pipeline. Requests
// that fail are skipped, matching the batch handler's per-symbol continue semantics; the Exec
// error is non-fatal because go-redis returns it whenever any queued command fails.
func (s *RedisStore) GetBatchCandlesDesc(ctx context.Context, exchange, marketType string, reqs []CandleReq) map[CandleReq][]candle.Candle {
	out := make(map[CandleReq][]candle.Candle, len(reqs))
	if len(reqs) == 0 {
		return out
	}

	tctx, cancel := ensureTimeout(ctx, redisOpTimeoutV2)
	defer cancel()

	pipe := s.client.Pipeline()
	cmds := make([]*redis.StringSliceCmd, len(reqs))
	for i, rq := range reqs {
		lim := rq.Limit
		if lim <= 0 {
			lim = 100
		}
		key := candlesKeyV2(exchange, marketType, rq.Symbol, rq.Timeframe)
		cmds[i] = pipe.ZRevRange(tctx, key, 0, int64(lim-1))
	}

	// Exec returns err != nil if ANY command failed (incl. redis.Nil).
	// NON-FATAL: do not return early — inspect each command result individually.
	_, _ = pipe.Exec(tctx)

	for i, rq := range reqs {
		data, cerr := cmds[i].Result()
		if cerr != nil {
			// Includes redis.Nil (missing key): skip this request, keep the rest.
			continue
		}
		decoded, derr := decodeCandlesV2(data)
		if derr != nil {
			continue
		}
		out[rq] = dedupCandlesByTimestamp(decoded)
	}

	return out
}

// GetCandlesAsc returns up to limit candles oldest-first.
func (s *RedisStore) GetCandlesAsc(ctx context.Context, exchange, marketType, symbol, timeframe string, limit int) ([]candle.Candle, error) {
	if limit <= 0 {
		limit = 100
	}
	key := candlesKeyV2(exchange, marketType, symbol, timeframe)
	tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	data, err := s.client.ZRange(tctx, key, 0, int64(limit-1)).Result()
	cancel()
	if err != nil {
		return nil, err
	}
	decoded, err := decodeCandlesV2(data)
	if err != nil {
		return nil, err
	}
	return dedupCandlesByTimestamp(decoded), nil
}

// GetCandlesByScoreRange returns candles whose score falls in the range.
func (s *RedisStore) GetCandlesByScoreRange(ctx context.Context, exchange, marketType, symbol, timeframe string, minScore, maxScore int64) ([]candle.Candle, error) {
	key := candlesKeyV2(exchange, marketType, symbol, timeframe)
	tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	data, err := s.client.ZRangeByScore(tctx, key, &redis.ZRangeBy{
		// Range bounds intentionally keep fmt.Sprintf — this is a READ path
		// (per-gap-check, internal/recovery), not the per-candle write hot path
		// that strconv.FormatInt targets. Not a missed sweep.
		Min: fmt.Sprintf("%d", minScore),
		Max: fmt.Sprintf("%d", maxScore),
	}).Result()
	cancel()
	if err != nil {
		return nil, err
	}
	decoded, err := decodeCandlesV2(data)
	if err != nil {
		return nil, err
	}
	return dedupCandlesByTimestamp(decoded), nil
}

// GetCandlesCount returns the number of candles in one series.
func (s *RedisStore) GetCandlesCount(ctx context.Context, exchange, marketType, symbol, timeframe string) (int64, error) {
	key := candlesKeyV2(exchange, marketType, symbol, timeframe)
	tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	defer cancel()
	return s.client.ZCard(tctx, key).Result()
}

// GetCandlesRange is not implemented for RedisStore.
// Range queries are handled by TieredStore (RedisStore + QuestDBStore combined).
func (s *RedisStore) GetCandlesRange(_ context.Context, _, _, _, _ string, _, _ time.Time, _ int) ([]candle.Candle, error) {
	return nil, errors.New("GetCandlesRange not implemented for RedisStore; use TieredStore")
}

// ============================================================================
// SYMBOL/TIMEFRAME TRACKING
// ============================================================================

// AddActiveSymbol records a symbol in its scope and invalidates the subscription cache.
func (s *RedisStore) AddActiveSymbol(ctx context.Context, exchange, marketType, symbol string) error {
	key := activeSymbolsKeyV2(exchange, marketType)
	tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	err := s.client.SAdd(tctx, key, symbol).Err()
	cancel()
	if err == nil {
		s.invalidateSubscriptionsCache()
		// Register this exchange:marketType scope so GetAllActiveSymbols can skip SCAN
		scope := fmt.Sprintf("%s:%s", exchange, marketType)
		regCtx, regCancel := ensureTimeout(ctx, redisDefaultOpTimeout)
		_ = s.client.SAdd(regCtx, scopeRegistryKey, scope).Err()
		regCancel()
	}
	return err
}

// RemoveActiveSymbol drops a symbol from its scope and invalidates the subscription cache.
func (s *RedisStore) RemoveActiveSymbol(ctx context.Context, exchange, marketType, symbol string) error {
	key := activeSymbolsKeyV2(exchange, marketType)
	tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	err := s.client.SRem(tctx, key, symbol).Err()
	cancel()
	if err == nil {
		s.invalidateSubscriptionsCache()
	}
	return err
}

// ClearActiveSymbols deletes every active symbol of a scope and deregisters it.
func (s *RedisStore) ClearActiveSymbols(ctx context.Context, exchange, marketType string) error {
	key := activeSymbolsKeyV2(exchange, marketType)
	tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	err := s.client.Del(tctx, key).Err()
	cancel()
	if err == nil {
		s.invalidateSubscriptionsCache()
		// Deregister this scope from the registry
		scope := fmt.Sprintf("%s:%s", exchange, marketType)
		regCtx, regCancel := ensureTimeout(ctx, redisDefaultOpTimeout)
		_ = s.client.SRem(regCtx, scopeRegistryKey, scope).Err()
		regCancel()
	}
	return err
}

// GetActiveSymbols returns the symbols recorded for one scope.
func (s *RedisStore) GetActiveSymbols(ctx context.Context, exchange, marketType string) ([]string, error) {
	key := activeSymbolsKeyV2(exchange, marketType)
	tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	defer cancel()
	return s.client.SMembers(tctx, key).Result()
}

// cachedSubscriptions is the short-lived cache for GetAllActiveSymbols.
type cachedSubscriptions struct {
	data      []SymbolInfo
	timestamp time.Time
	mu        sync.RWMutex
}

// NOT RETENTION: validity of an in-process subscription cache, not a stored data window.
const cacheTTL = 10 * time.Second

func (s *RedisStore) invalidateSubscriptionsCache() {
	s.subCache.mu.Lock()
	defer s.subCache.mu.Unlock()
	s.subCache.data = nil
	s.subCache.timestamp = time.Time{}
}

// GetAllActiveSymbols returns all active symbols across exchanges, cached for a short TTL.
func (s *RedisStore) GetAllActiveSymbols(ctx context.Context) ([]SymbolInfo, error) {
	// Check cache first
	s.subCache.mu.RLock()
	if time.Since(s.subCache.timestamp) < cacheTTL {
		data := s.subCache.data
		s.subCache.mu.RUnlock()
		logger.OrDiscard(s.log).Debug("get all active symbols: cache hit", "symbols", len(data))
		return data, nil
	}
	s.subCache.mu.RUnlock()

	// Cache miss - fetch from Redis using scope registry (no SCAN needed)
	logger.OrDiscard(s.log).Debug("get all active symbols: cache miss; fetching from redis via registry")

	tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	defer cancel()

	// 1. Read the scope registry: O(1) lookup, 1 round-trip
	scopes, err := s.client.SMembers(tctx, scopeRegistryKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("failed to read scope registry: %w", err)
	}

	if len(scopes) == 0 {
		logger.OrDiscard(s.log).Debug("get all active symbols: no registered scopes; returning empty")
		s.subCache.mu.Lock()
		s.subCache.data = nil
		s.subCache.timestamp = time.Now()
		s.subCache.mu.Unlock()
		return nil, nil
	}

	// 2. Build keys from scopes and pipeline SMEMBERS (1 round-trip for all scopes)
	pipe := s.client.Pipeline()
	cmds := make(map[string]*redis.StringSliceCmd, len(scopes))

	for _, scope := range scopes {
		key := fmt.Sprintf("active_symbols:%s", scope)
		cmds[scope] = pipe.SMembers(tctx, key)
	}

	_, err = pipe.Exec(tctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("failed to pipeline SMEMBERS: %w", err)
	}

	// 3. Collect results
	allSymbols := make([]SymbolInfo, 0, len(scopes)*50)

	for scope, cmd := range cmds {
		symbols, err := cmd.Result()
		if err != nil {
			continue
		}

		parts := strings.SplitN(scope, ":", 2)
		if len(parts) != 2 {
			continue
		}

		for _, symbol := range symbols {
			allSymbols = append(allSymbols, SymbolInfo{
				Exchange:   parts[0],
				MarketType: parts[1],
				Symbol:     symbol,
			})
		}
	}

	// Update cache
	s.subCache.mu.Lock()
	s.subCache.data = allSymbols
	s.subCache.timestamp = time.Now()
	s.subCache.mu.Unlock()

	logger.OrDiscard(s.log).Debug("get all active symbols: fetched and cached",
		"symbols", len(allSymbols), "scopes", len(scopes))
	return allSymbols, nil
}

// AddTimeframe records a timeframe as active for one symbol.
func (s *RedisStore) AddTimeframe(ctx context.Context, exchange, marketType, symbol, timeframe string) error {
	key := timeframesKeyV2(exchange, marketType, symbol)
	tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	defer cancel()
	return s.client.SAdd(tctx, key, timeframe).Err()
}

// GetTimeframes returns the supported timeframes recorded for one symbol.
func (s *RedisStore) GetTimeframes(ctx context.Context, exchange, marketType, symbol string) ([]string, error) {
	key := timeframesKeyV2(exchange, marketType, symbol)
	tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	values, err := s.client.SMembers(tctx, key).Result()
	cancel()
	if err != nil {
		return nil, err
	}
	filtered := make([]string, 0, len(values))
	for _, tf := range values {
		if s.tfReg.IsSupported(tf) {
			filtered = append(filtered, tf)
		}
	}
	return filtered, nil
}

// ============================================================================
// TRADE TRACKING
// ============================================================================

// SetLastTrade stores the most recent trade of a symbol with a one-hour TTL.
func (s *RedisStore) SetLastTrade(ctx context.Context, exchange, marketType, symbol string, trade candle.Trade) error {
	now := time.Now().UnixMilli()
	last := candle.LastTrade{
		Timestamp:  trade.Timestamp,
		Price:      trade.Price,
		Volume:     trade.Amount,
		Side:       trade.Side,
		TradeID:    trade.ID,
		ReceivedAt: now,
		LatencyMS:  now - trade.Timestamp,
	}
	payload, err := json.Marshal(last)
	if err != nil {
		return err
	}
	tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	defer cancel()
	return s.client.Set(tctx, lastTradeKeyV2(exchange, marketType, symbol), payload, time.Hour).Err()
}

// GetLastTrade returns the most recent stored trade, or nil when none exists.
func (s *RedisStore) GetLastTrade(ctx context.Context, exchange, marketType, symbol string) (*candle.LastTrade, error) {
	tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	data, err := s.client.Get(tctx, lastTradeKeyV2(exchange, marketType, symbol)).Bytes()
	cancel()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var last candle.LastTrade
	if err := json.Unmarshal(data, &last); err != nil {
		return nil, err
	}
	return &last, nil
}

// ============================================================================
// HELPERS
// ============================================================================

// decodeCandlesV2 decodes members of the candle ZSET in either of the two encodings.
func decodeCandlesV2(data []string) ([]candle.Candle, error) {
	if len(data) == 0 {
		return []candle.Candle{}, nil
	}
	out := make([]candle.Candle, 0, len(data))
	for _, item := range data {
		candle, err := decodeCandleMember(item)
		if err != nil {
			return nil, err
		}
		out = append(out, candle)
	}
	return out, nil
}

func maxFloatV2(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func minFloatV2(a, b float64) float64 {
	if a == 0 {
		return b
	}
	if b == 0 {
		return a
	}
	if a < b {
		return a
	}
	return b
}

// isCandleClosed checks if a candle bucket is now closed (past its timeframe duration).
func (s *RedisStore) isCandleClosed(timestamp int64, timeframe string) bool {
	now := time.Now().Unix() * 1000                                    // Current time in milliseconds
	bucketDuration := candle.ParseTimeframeToSeconds(timeframe) * 1000 // Convert to ms
	isClosed := now > (timestamp + bucketDuration)
	logger.OrDiscard(s.log).Debug("is candle closed check",
		"timestamp", timestamp,
		"timeframe", timeframe,
		"now", now,
		"bucket_duration", bucketDuration,
		"closed", isClosed)
	return isClosed
}

func firstNonZeroV2(a, b float64) float64 {
	if a != 0 {
		return a
	}
	return b
}

func firstNonZeroIntV2(a, b int64) int64 {
	if a != 0 {
		return a
	}
	return b
}

// normalizeCandleMergeSource maps string source tags to canonical merge strategies. Gap-recovery
// sources are REST totals, so a merge with a streaming delta takes max, never a sum.
func normalizeCandleMergeSource(source CandleMergeSource) CandleMergeSource {
	s := string(source)
	switch {
	case s == string(CandleMergeSourceRESTTotal), s == string(CandleMergeSourceSnapshotTotal):
		return source
	case s == string(CandleMergeSourceWSDelta):
		return source
	// The venue's own candle is a total and must be recognized as one; the default maps
	// anything unknown to ws_delta, which would accumulate it.
	case s == string(CandleMergeSourceWSKline):
		return source
	// gap-recovery and gap-recovery-extended carry total volume from the REST API; treating
	// them as deltas inflated volume (observed 4.4x).
	case strings.HasPrefix(s, "gap-recovery"):
		return CandleMergeSourceRESTTotal
	default:
		return CandleMergeSourceWSDelta
	}
}

func isTotalMergeSource(source CandleMergeSource) bool {
	s := normalizeCandleMergeSource(source)
	return s == CandleMergeSourceRESTTotal || s == CandleMergeSourceSnapshotTotal ||
		s == CandleMergeSourceWSKline
}

// settlesClosedBucket reports whether an incoming record is the exchange's final word on a
// bucket and therefore replaces what we hold instead of competing in a MAX.
//
// Mixing rules produced internally impossible bars (buy + sell exceeding volume) and made an
// inflation permanent, because every later repair re-elected the larger stored number. Our value
// is a reconstruction, so only replacement converges.
//
// Both halves of the test are load-bearing: authority comes from declared provenance, never from
// the numbers, and the record must have measured the order flow. A total without taker fields
// falls back to the conservative union. Both call sites reach this only after classifying the
// bucket as closed.
func settlesClosedBucket(incoming candle.Candle, source CandleMergeSource) bool {
	if !isTotalMergeSource(source) {
		return false
	}
	return incoming.TakerBuyVolume+incoming.TakerSellVolume > 0
}

func mergeOpenCandleBySource(current, incoming candle.Candle, source CandleMergeSource, now int64) candle.Candle {
	normalized := normalizeCandleMergeSource(source)

	openMerged := firstNonZeroV2(current.Open, incoming.Open)
	if normalized == CandleMergeSourceRESTTotal {
		openMerged = firstNonZeroV2(incoming.Open, current.Open)
	}

	// Once the venue has spoken for a bucket, nobody else writes it. The rule must hold here
	// because /api/timeseries is a merged read that favors the hot copy, so a correct engine
	// value could be shadowed by a stale hot one.
	if current.Source == string(CandleMergeSourceWSKline) &&
		incoming.Source != string(CandleMergeSourceWSKline) {
		return current
	}

	volumeMerged := current.Volume + incoming.Volume
	if isTotalMergeSource(normalized) {
		volumeMerged = maxFloatV2(current.Volume, incoming.Volume)
	}

	// A SnapshotTotal carries the engine's cumulative bucket candle — authoritative totals for
	// the taker pair, not per-trade deltas — so it must replace, never add. Adding re-summed the
	// total per tick and produced takerBuy+takerSell >> volume. Replacement restores the
	// invariant, mirroring mergeClosedCandleBySource.
	if normalized == CandleMergeSourceSnapshotTotal {
		snapSource := current.Source
		if incoming.Source != "" {
			snapSource = incoming.Source
		}
		snapMethod := current.AggregateMethod
		if incoming.AggregateMethod != "" {
			snapMethod = incoming.AggregateMethod
		}
		return candle.Candle{
			Timestamp:        incoming.Timestamp,
			Open:             openMerged,
			High:             maxFloatV2(current.High, incoming.High),
			Low:              minFloatV2(current.Low, incoming.Low),
			Close:            incoming.Close,
			Volume:           volumeMerged,
			CreatedAt:        firstNonZeroIntV2(current.CreatedAt, now),
			LastUpdate:       now,
			TakerBuyVolume:   incoming.TakerBuyVolume,
			TakerSellVolume:  incoming.TakerSellVolume,
			Source:           snapSource,
			AggregateMethod:  snapMethod,
			SideUnknownCount: incoming.SideUnknownCount,
			LateTradeCount:   incoming.LateTradeCount,
			HasGap:           current.HasGap || incoming.HasGap,
		}
	}

	// The whole and its parts must obey the same rule. Volume MAX-merges for a declared total
	// while the taker pair accumulated unconditionally, producing parts larger than the whole
	// (volume 100 with buy+sell 200). When a declared total loses the volume election, its
	// taker pair must not be added on top.
	takerBuyVolume := current.TakerBuyVolume + incoming.TakerBuyVolume
	takerSellVolume := current.TakerSellVolume + incoming.TakerSellVolume
	if isTotalMergeSource(normalized) {
		takerBuyVolume, takerSellVolume = incoming.TakerBuyVolume, incoming.TakerSellVolume
		if current.Volume >= incoming.Volume {
			takerBuyVolume, takerSellVolume = current.TakerBuyVolume, current.TakerSellVolume
		}
		if takerBuyVolume == 0 && takerSellVolume == 0 {
			takerBuyVolume, takerSellVolume = current.TakerBuyVolume, current.TakerSellVolume
			if takerBuyVolume == 0 && takerSellVolume == 0 {
				takerBuyVolume, takerSellVolume = incoming.TakerBuyVolume, incoming.TakerSellVolume
			}
		}
	}
	sideUnknownCount := current.SideUnknownCount + incoming.SideUnknownCount
	lateTradeCount := current.LateTradeCount + incoming.LateTradeCount

	sourceMerged := current.Source
	if incoming.Source != "" {
		sourceMerged = incoming.Source
	}
	aggregateMethod := current.AggregateMethod
	if incoming.AggregateMethod != "" {
		aggregateMethod = incoming.AggregateMethod
	}
	hasGap := current.HasGap || incoming.HasGap

	return candle.Candle{
		Timestamp:  incoming.Timestamp,
		Open:       openMerged,
		High:       maxFloatV2(current.High, incoming.High),
		Low:        minFloatV2(current.Low, incoming.Low),
		Close:      incoming.Close,
		Volume:     volumeMerged,
		CreatedAt:  firstNonZeroIntV2(current.CreatedAt, now),
		LastUpdate: now,

		TakerBuyVolume:   takerBuyVolume,
		TakerSellVolume:  takerSellVolume,
		Source:           sourceMerged,
		AggregateMethod:  aggregateMethod,
		SideUnknownCount: sideUnknownCount,
		LateTradeCount:   lateTradeCount,
		HasGap:           hasGap,
	}
}

func mergeClosedCandleBySource(current, incoming candle.Candle, source CandleMergeSource, now int64) candle.Candle {
	normalized := normalizeCandleMergeSource(source)

	openMerged := firstNonZeroV2(current.Open, incoming.Open)
	if normalized == CandleMergeSourceRESTTotal {
		openMerged = firstNonZeroV2(incoming.Open, current.Open)
	}

	// Closed candles are authoritative per source: incoming wins. Volume follows the same rule
	// as the aggregates below, since adopting the order flow while keeping a larger volume
	// yields parts that exceed the whole. See settlesClosedBucket.
	volumeMerged := maxFloatV2(current.Volume, incoming.Volume)
	if settlesClosedBucket(incoming, source) {
		volumeMerged = incoming.Volume
	}

	// The pair follows the same > 0 gate SeedCandlesBatch applies: an incoming record that
	// measured no order flow may not blank a live-rich row.
	takerBuy, takerSell := incoming.TakerBuyVolume, incoming.TakerSellVolume
	if takerBuy+takerSell <= 0 {
		takerBuy, takerSell = current.TakerBuyVolume, current.TakerSellVolume
	}

	return candle.Candle{
		Timestamp:  incoming.Timestamp,
		Open:       openMerged,
		High:       maxFloatV2(current.High, incoming.High),
		Low:        minFloatV2(current.Low, incoming.Low),
		Close:      incoming.Close,
		Volume:     volumeMerged,
		CreatedAt:  firstNonZeroIntV2(current.CreatedAt, now),
		LastUpdate: now,

		TakerBuyVolume:   takerBuy,
		TakerSellVolume:  takerSell,
		Source:           incoming.Source,
		AggregateMethod:  incoming.AggregateMethod,
		SideUnknownCount: incoming.SideUnknownCount,
		LateTradeCount:   incoming.LateTradeCount,
		HasGap:           incoming.HasGap,
	}
}

func (s *RedisStore) upsertCandleWithCAS(
	ctx context.Context,
	key string,
	timeframe string,
	timestamp int64,
	mergeFn func(current *candle.Candle, now int64) candle.Candle,
	applyCap bool,
) error {
	// strconv.FormatInt avoids reflection and allocation on the per-candle hot write path.
	score := strconv.FormatInt(timestamp, 10)
	var lastErr error

	for attempt := 1; attempt <= redisCASMaxRetries; attempt++ {
		watchCtx, watchCancel := ensureTimeout(ctx, redisDefaultOpTimeout)
		err := s.client.Watch(watchCtx, func(tx *redis.Tx) error {
			readCtx, readCancel := ensureTimeout(watchCtx, redisDefaultOpTimeout)
			existing, err := tx.ZRangeByScore(readCtx, key, &redis.ZRangeBy{
				Min:   score,
				Max:   score,
				Count: 1,
			}).Result()
			readCancel()
			if err != nil && !errors.Is(err, redis.Nil) {
				return err
			}

			var current *candle.Candle
			if len(existing) > 0 {
				parsed, decErr := decodeCandleMember(existing[0])
				if decErr != nil {
					// Continuing would overwrite a candle this binary does not understand,
					// possibly one written by a newer image.
					return decErr
				}
				current = &parsed
			}

			now := time.Now().UnixMilli()
			merged := mergeFn(current, now)
			payload, err := encodeCandleMember(merged)
			if err != nil {
				return err
			}

			writeCtx, writeCancel := ensureTimeout(watchCtx, redisOpTimeoutV2)
			defer writeCancel()

			_, err = tx.TxPipelined(writeCtx, func(pipe redis.Pipeliner) error {
				pipe.ZRemRangeByScore(writeCtx, key, score, score)
				pipe.ZAdd(writeCtx, key, redis.Z{Score: float64(merged.Timestamp), Member: payload})
				if applyCap {
					pipe.ZRemRangeByRank(writeCtx, key, 0, -(int64(s.candleCapFor(timeframe)) + 1))
				}
				return nil
			})
			return err
		}, key)
		watchCancel()

		if err == nil {
			return nil
		}

		if errors.Is(err, redis.TxFailedErr) {
			lastErr = err
			if attempt < redisCASMaxRetries {
				time.Sleep(time.Duration(attempt) * redisCASRetryBackoffStep)
				continue
			}
			break
		}

		return err
	}

	if lastErr == nil {
		lastErr = redis.TxFailedErr
	}
	return fmt.Errorf("redis CAS upsert failed for %s@%d after %d attempts: %w", key, timestamp, redisCASMaxRetries, lastErr)
}

// ============================================================================
// DATA LIFECYCLE CLEANUP
// ============================================================================

// scanAndDelete finds all keys matching a glob pattern using SCAN and deletes them in batches.
// Returns the total number of keys deleted.
func (s *RedisStore) scanAndDelete(ctx context.Context, pattern string) int64 {
	var totalDeleted int64
	var cursor uint64

	for {
		tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
		keys, nextCursor, err := s.client.Scan(tctx, cursor, pattern, 100).Result()
		cancel()
		if err != nil {
			logger.OrDiscard(s.log).Warn("scan and delete: scan failed", "pattern", pattern, "error", err)
			break
		}

		if len(keys) > 0 {
			tctx, cancel = ensureTimeout(ctx, redisDefaultOpTimeout)
			n, err := s.client.Del(tctx, keys...).Result()
			cancel()
			if err != nil {
				logger.OrDiscard(s.log).Warn("scan and delete: batch delete failed", "error", err)
			} else {
				totalDeleted += n
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return totalDeleted
}

// CleanupSymbolData deletes all Redis keys of a symbol deranked by discovery: its candle
// ZSETs per timeframe, its timeframe set and its last trade.
func (s *RedisStore) CleanupSymbolData(ctx context.Context, exchange, marketType, symbol string) int64 {
	var totalDeleted int64

	// 1. Delete exact keys (timeframes + last_trade)
	exactKeys := []string{
		timeframesKeyV2(exchange, marketType, symbol),
		lastTradeKeyV2(exchange, marketType, symbol),
	}
	tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
	n, err := s.client.Del(tctx, exactKeys...).Result()
	cancel()
	if err != nil {
		logger.OrDiscard(s.log).Warn("cleanup symbol data: exact key delete failed",
			"exchange", exchange, "market_type", marketType, "symbol", symbol, "error", err)
	} else {
		totalDeleted += n
	}

	// 2. Delete candle ZSETs (one per timeframe — glob pattern)
	candlePattern := fmt.Sprintf("candles:%s:%s:%s:*", exchange, marketType, symbol)
	totalDeleted += s.scanAndDelete(ctx, candlePattern)

	return totalDeleted
}

// PurgeStaleSymbolData removes all data keys for symbols NOT in the activeSymbols set.
// Called on startup after discovery completes, to purge leftovers from previous runs.
// Returns total keys deleted.
func (s *RedisStore) PurgeStaleSymbolData(ctx context.Context, exchange, marketType string, activeSymbols map[string]struct{}) (int64, error) {
	staleSymbols := make(map[string]struct{})

	// Scan candle keys to find stale symbols
	// Key format: candles:{exchange}:{marketType}:{symbol}:{timeframe}
	candlePattern := fmt.Sprintf("candles:%s:%s:*", exchange, marketType)
	var cursor uint64
	for {
		tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
		keys, nextCursor, err := s.client.Scan(tctx, cursor, candlePattern, 200).Result()
		cancel()
		if err != nil {
			return 0, fmt.Errorf("PurgeStaleSymbolData: SCAN candles: %w", err)
		}
		for _, key := range keys {
			// candles:{exchange}:{marketType}:{symbol}:{timeframe}
			parts := strings.SplitN(key, ":", 5)
			if len(parts) >= 4 {
				sym := parts[3]
				if _, active := activeSymbols[sym]; !active {
					staleSymbols[sym] = struct{}{}
				}
			}
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	// Also scan timeframes: and last_trade: for symbols not caught by candle patterns
	for _, prefix := range []string{"timeframes", "last_trade"} {
		pattern := fmt.Sprintf("%s:%s:%s:*", prefix, exchange, marketType)
		cursor = 0
		for {
			tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
			keys, nextCursor, err := s.client.Scan(tctx, cursor, pattern, 200).Result()
			cancel()
			if err != nil {
				break
			}
			for _, key := range keys {
				// {prefix}:{exchange}:{marketType}:{symbol}
				parts := strings.SplitN(key, ":", 4)
				if len(parts) >= 4 {
					sym := parts[3]
					if _, active := activeSymbols[sym]; !active {
						staleSymbols[sym] = struct{}{}
					}
				}
			}
			cursor = nextCursor
			if cursor == 0 {
				break
			}
		}
	}

	if len(staleSymbols) == 0 {
		logger.OrDiscard(s.log).Info("purge stale symbol data: no stale symbols found",
			"exchange", exchange, "market_type", marketType)
		return 0, nil
	}

	logger.OrDiscard(s.log).Info("purge stale symbol data: stale symbols found",
		"exchange", exchange, "market_type", marketType, "symbols", len(staleSymbols))

	var totalDeleted int64
	for sym := range staleSymbols {
		n := s.CleanupSymbolData(ctx, exchange, marketType, sym)
		totalDeleted += n
	}

	logger.OrDiscard(s.log).Info("purge stale symbol data: keys deleted",
		"exchange", exchange,
		"market_type", marketType,
		"deleted_keys", totalDeleted,
		"symbols", len(staleSymbols))
	return totalDeleted, nil
}

// ============================================================================
// CANDLE KEY ENUMERATION — for migrating what exists only in Redis
// ============================================================================

// CandleKeyRef identifies ONE `candles:{ex}:{mt}:{symbol}:{tf}` key.
type CandleKeyRef struct {
	Exchange   string
	MarketType string
	Symbol     string
	Timeframe  string
}

// OldestCandleTimestamps returns, per candle key of the timeframe, the lowest resident score —
// the oldest candle the hot tier still holds. It is the hot half of the single-copy guard and
// reads only the frontier via ZRANGE 0 0 WITHSCORES, all keys in one pipeline. A missing key
// does not enter the map: absence is not zero.
func (s *RedisStore) OldestCandleTimestamps(ctx context.Context, timeframe string) (map[CandleKeyRef]int64, error) {
	refs, err := s.ScanCandleKeys(ctx, timeframe)
	if err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		return map[CandleKeyRef]int64{}, nil
	}

	pipe := s.client.Pipeline()
	cmds := make([]*redis.ZSliceCmd, len(refs))
	for i, ref := range refs {
		key := candlesKeyV2(ref.Exchange, ref.MarketType, ref.Symbol, ref.Timeframe)
		cmds[i] = pipe.ZRangeWithScores(ctx, key, 0, 0)
	}

	tctx, cancel := ensureTimeout(ctx, redisOpTimeoutV2)
	// A partial frontier cannot certify the absence of a single copy; redis.Nil is legitimate
	// absence, so the commands are still checked.
	_, execErr := pipe.Exec(tctx)
	cancel()
	if execErr != nil && !errors.Is(execErr, redis.Nil) {
		return nil, fmt.Errorf("read oldest candle frontier %s: %w", timeframe, execErr)
	}

	out := make(map[CandleKeyRef]int64, len(refs))
	for i, ref := range refs {
		members, cerr := cmds[i].Result()
		if cerr != nil && !errors.Is(cerr, redis.Nil) {
			return nil, fmt.Errorf("read oldest candle frontier %v: %w", ref, cerr)
		}
		if errors.Is(cerr, redis.Nil) || len(members) == 0 {
			continue
		}
		out[ref] = int64(members[0].Score)
	}
	return out, nil
}

// ScanCandleKeys returns the candle keys of one timeframe by walking the keyspace. It scans
// rather than deriving from the active fleet because the data most at risk belongs to symbols
// that left active_symbols. The positional parse, not the glob, guarantees the key shape.
func (s *RedisStore) ScanCandleKeys(ctx context.Context, timeframe string) ([]CandleKeyRef, error) {
	pattern := fmt.Sprintf("candles:*:*:*:%s", timeframe)
	var out []CandleKeyRef
	var cursor uint64
	seen := make(map[string]struct{})

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		tctx, cancel := ensureTimeout(ctx, redisDefaultOpTimeout)
		keys, next, err := s.client.Scan(tctx, cursor, pattern, 200).Result()
		cancel()
		if err != nil {
			return nil, fmt.Errorf("storage: scan %s: %w", pattern, err)
		}
		for _, key := range keys {
			// SCAN may return the same key on different pages — that is its guarantee.
			if _, repetida := seen[key]; repetida {
				continue
			}
			seen[key] = struct{}{}
			ref, ok := ParseCandleKey(key)
			if !ok || ref.Timeframe != timeframe {
				continue
			}
			out = append(out, ref)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return out, nil
}

// ParseCandleKey parses candles:{ex}:{mt}:{symbol}:{tf} positionally. It is exported because
// the migration lives in another package, and a second copy of the parse would let the writer
// and the sweeper disagree.
func ParseCandleKey(key string) (CandleKeyRef, bool) {
	parts := strings.Split(key, ":")
	if len(parts) != 5 || parts[0] != "candles" {
		return CandleKeyRef{}, false
	}
	for _, p := range parts[1:] {
		if p == "" {
			return CandleKeyRef{}, false
		}
	}
	return CandleKeyRef{Exchange: parts[1], MarketType: parts[2], Symbol: parts[3], Timeframe: parts[4]}, true
}
