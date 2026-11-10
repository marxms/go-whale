package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// Compile-time checks that TieredStore satisfies each candle-owned port.
var (
	_ candle.Writer       = (*TieredStore)(nil)
	_ candle.Reader       = (*TieredStore)(nil)
	_ candle.Catalog      = (*TieredStore)(nil)
	_ candle.SeedWriter   = (*TieredStore)(nil)
	_ candle.SeedSplitter = (*TieredStore)(nil)
)

// TieredStore implements the candle-owned ports by combining the in-memory CandleEngine with a
// RedisStore written through by periodic snapshots. redisTier is the Redis surface it owns,
// declared here so tests can fake only what the tier calls.
type redisTier interface {
	AddActiveSymbol(ctx context.Context, exchange, marketType, symbol string) error
	AddTimeframe(ctx context.Context, exchange, marketType, symbol, timeframe string) error
	GetActiveSymbols(ctx context.Context, exchange, marketType string) ([]string, error)
	GetAllActiveSymbols(ctx context.Context) ([]candle.SymbolInfo, error)
	GetBatchCandlesDesc(ctx context.Context, exchange, marketType string, reqs []candle.CandleReq) map[candle.CandleReq][]candle.Candle
	GetCandlesByScoreRange(ctx context.Context, exchange, marketType, symbol, timeframe string, minScore, maxScore int64) ([]candle.Candle, error)
	GetTimeframes(ctx context.Context, exchange, marketType, symbol string) ([]string, error)
	ReplaceCandle(ctx context.Context, exchange, marketType, symbol, timeframe string, c candle.Candle) error
	SeedCandlesBatch(ctx context.Context, exchange, marketType, symbol, timeframe string, candles []candle.Candle) error
	SeedCurrentBucketCandle(ctx context.Context, exchange, marketType, symbol, timeframe string, c candle.Candle) error
	WriteSnapshotBatch(ctx context.Context, updates []candle.CandleUpdate) error
	Close() error
}

// TieredStore is the candle store the rest of the process sees: the in-memory engine is primary and Redis and the cold tier are persistence.
type TieredStore struct {
	engine       *CandleEngine
	redis        redisTier
	cold         candle.ColdStore    // optional durable cold store for historical reads
	coldAppender candle.ColdAppender // optional: routes seed/backfill CLOSED candles to cold (durability)
	// coldBoundary is the retention policy of the same table the appender writes; the write
	// clamp comes from it, so changing the policy changes both in one place.
	coldBoundary  candle.WriteFloor
	writeThrough  bool
	snapshotTick  time.Duration
	snapshotReady atomic.Bool // gates flushSnapshot until autoSubscribe purges stale buffers
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	metrics       Metrics
	log           *slog.Logger
}

// SetMetrics injects the observability port. The composition root calls it before the store
// starts writing.
func (ts *TieredStore) SetMetrics(m Metrics) { ts.metrics = m }

// NewTieredStore creates a TieredStore that fronts CandleEngine with RedisStore write-through.
// When cfg.RedisWriteThrough is true, a background goroutine periodically flushes
// dirty candles from CandleEngine to RedisStore.
func NewTieredStore(engine *CandleEngine, redis redisTier, cfg config.CandleEngineConfig, logs ...*slog.Logger) *TieredStore {
	ctx, cancel := context.WithCancel(context.Background())

	interval := time.Duration(cfg.SnapshotIntervalMS) * time.Millisecond
	if interval <= 0 {
		interval = 30 * time.Second
	}

	log := logger.OrDiscard(logs...)
	if len(logs) == 0 && engine != nil {
		log = logger.OrDiscard(engine.log)
	}
	ts := &TieredStore{
		engine:       engine,
		redis:        redis,
		writeThrough: cfg.RedisWriteThrough,
		snapshotTick: interval,
		ctx:          ctx,
		cancel:       cancel,
		log:          log,
	}

	if ts.writeThrough {
		ts.wg.Add(1)
		go ts.snapshotLoop()
	}

	return ts
}

// ============================================================================
// SNAPSHOT GOROUTINE
// ============================================================================

// snapshotLoop periodically flushes CandleEngine buffers to RedisStore.
func (ts *TieredStore) snapshotLoop() {
	defer ts.wg.Done()
	ticker := time.NewTicker(ts.snapshotTick)
	defer ticker.Stop()

	for {
		select {
		case <-ts.ctx.Done():
			return
		case <-ticker.C:
			ts.flushSnapshot(ts.ctx)
		}
	}
}

// EnableSnapshots allows flushSnapshot to persist data. Call after autoSubscribe
// has finished and PurgeStaleSymbols has cleaned warm-cache remnants, so that
// flushSnapshot only writes currently-subscribed symbols to Redis.
func (ts *TieredStore) EnableSnapshots() {
	ts.snapshotReady.Store(true)
}

// DisableSnapshots prevents flushSnapshot from writing to Redis.
// Used during periodic discovery to prevent snapshot from re-adding deranked symbols.
func (ts *TieredStore) DisableSnapshots() {
	ts.snapshotReady.Store(false)
}

// snapshotFlushDepth is a documentation canary for the one-newest-candle-per-key assumption
// behind the zero-alloc PeekLatestCandle fast path. Tests fail if the value or behavior changes.
const snapshotFlushDepth = 1

// buildSnapshotUpdate constructs the per-key update written by the snapshot flush; it is pure
// so tests can assert it byte-identical to the GetCandlesDesc(limit=1) path.
func buildSnapshotUpdate(key CompositeKey, c candle.Candle) redisstore.CandleUpdate {
	return redisstore.CandleUpdate{
		Exchange:   key.Exchange,
		MarketType: key.MarketType,
		Symbol:     key.Symbol,
		Timeframe:  key.Timeframe,
		Candle:     c,
		Source:     redisstore.CandleMergeSourceSnapshotTotal,
	}
}

// flushSnapshot writes the newest candle of each engine buffer to Redis. Each key uses the
// zero-alloc PeekLatestCandle rather than allocating a full SliceDesc and discarding all but
// the newest.
func (ts *TieredStore) flushSnapshot(ctx context.Context) {
	if !ts.snapshotReady.Load() {
		return
	}

	keys := ts.engine.BufferKeys()

	if len(keys) == 0 {
		return
	}

	updates := make([]redisstore.CandleUpdate, 0, len(keys))

	for _, key := range keys {
		latest, ok := ts.engine.PeekLatestCandle(key.Exchange, key.MarketType, key.Symbol, key.Timeframe)
		if !ok {
			continue
		}
		updates = append(updates, buildSnapshotUpdate(key, latest))
	}

	if len(updates) == 0 {
		return
	}

	writeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Blind pipelined write: the engine is the authority, so re-reading what this loop wrote is
	// thousands of round trips for no information.
	if err := ts.redis.WriteSnapshotBatch(writeCtx, updates); err != nil {
		logger.OrDiscard(ts.log).Warn("tiered store snapshot flush failed", "error", err)
	} else {
		logger.OrDiscard(ts.log).Debug("tiered store snapshot flushed",
			"updates", len(updates), "keys", len(keys))
	}
}

// PurgeStaleSymbols removes engine buffers for "exchange:marketType:symbol" keys not allowed.
func (ts *TieredStore) PurgeStaleSymbols(allowedSymbols map[string]struct{}) int {
	return ts.engine.PurgeStaleSymbols(allowedSymbols)
}

// ============================================================================
// STREAMING OPERATIONS → CandleEngine primary
// ============================================================================

// AddStreamingCandle applies one live candle to the in-memory engine.
func (ts *TieredStore) AddStreamingCandle(_ context.Context, exchange, marketType, symbol, timeframe string, candle candle.Candle) error {
	ts.engine.UpdateCandle(exchange, marketType, symbol, timeframe, candle)
	return nil
}

// AddStreamingCandlesBatch applies live candle updates to the in-memory engine, carrying the
// batch's declared merge source onto each candle.
func (ts *TieredStore) AddStreamingCandlesBatch(_ context.Context, updates []redisstore.CandleUpdate) error {
	for i := range updates {
		u := &updates[i]
		// Carry the declared merge source ONTO the candle. The engine dispatches its
		// late-update merge on Candle.Source, and this is the only place the batch's
		// Source would otherwise be dropped — leaving every live delta unlabelled and
		// therefore MAX-merged (under-counting) instead of accumulated.
		c := u.Candle
		if c.Source == "" && u.Source != "" {
			c.Source = string(u.Source)
		}
		ts.engine.UpdateCandle(u.Exchange, u.MarketType, u.Symbol, u.Timeframe, c)
	}
	return nil
}

// ============================================================================
// SEED OPERATIONS → CandleEngine + RedisStore simultaneously
// ============================================================================

// FoldSeedBatch folds a batch into the ring and returns what the ring now holds for those
// buckets, touching no durable tier. It is split from PersistSeedBatch so the seeder can fold,
// apply the open bucket, then persist without losing live deltas during the blocking write.
// The returned slice is the merged view, so a kline without taker data cannot zero a measured pair.
func (ts *TieredStore) FoldSeedBatch(exchange, marketType, symbol, timeframe string, seedCandles []candle.Candle) []candle.Candle {
	return ts.engine.SeedCandles(exchange, marketType, symbol, timeframe, seedCandles)
}

// PersistSeedBatch writes an already-folded batch to the durable tiers. They deliberately get
// different slices: cold gets the merged view, because ILP has no NULL and a kline without taker
// data would replace a measured pair with 0/0; Redis gets the raw batch, which already protects
// the pair itself. Handing the union to Redis would forge authority: it carries the batch's
// rest_total label over values reconstructed locally.
func (ts *TieredStore) PersistSeedBatch(ctx context.Context, exchange, marketType, symbol, timeframe string, merged, raw []candle.Candle) error {
	protected, err := ts.protectSeedTaker(ctx, exchange, marketType, symbol, timeframe, merged)
	if err != nil {
		return err // No degraded replacement reaches either durable tier after a failed read.
	}
	repairErr := ts.admitSeedRepairs(ctx, exchange, marketType, symbol, timeframe, protected)
	// A failed ancestor admission must not suppress the independent hot write.
	var coldErr error
	if repairErr == nil {
		coldErr = ts.appendClosedToCold(ctx, exchange, marketType, symbol, timeframe, protected)
	}
	redisErr := ts.redis.SeedCandlesBatch(ctx, exchange, marketType, symbol, timeframe, raw)
	if repairErr != nil {
		return repairErr
	}
	if coldErr != nil {
		return fmt.Errorf("%w: %w", redisstore.ErrSeedColdAdmissionUnavailable, coldErr)
	}
	return redisErr
}

func (ts *TieredStore) admitSeedRepairs(ctx context.Context, exchange, marketType, symbol, timeframe string, protected []candle.Candle) error {
	// Admit retained ancestor repairs through the same bounded cold writer; this is queue
	// admission, not WAL confirmation.
	repairs, err := ts.engine.pendingSeedRepairs(exchange, marketType, symbol, timeframe, protected)
	if err != nil {
		return err
	}
	for _, repair := range repairs {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%w: seed ancestor repair not admitted: %w", redisstore.ErrSeedColdAdmissionUnavailable, err)
		}
		if floor, clamped := ts.coldWriteFloor(time.Now()); clamped && repair.candle.Timestamp < floor.UnixMilli() {
			// The cold tier deliberately excludes this timestamp; do not recreate an expired
			// partition or report its absence as an admitted write.
			if err := ts.engine.finishSeedRepair(repair); err != nil {
				return err
			}
			ts.metrics.incSeedRepairOutsideRetention()
			continue
		}
		if ts.coldAppender == nil {
			return fmt.Errorf("%w: seed ancestor repairs remain unconfirmed: cold writer unavailable", redisstore.ErrSeedColdAdmissionUnavailable)
		}
		if err := ts.coldAppender.EnqueueClosedBlocking(ctx, repair.key.Exchange, repair.key.MarketType, repair.key.Symbol, repair.key.Timeframe, repair.candle); err != nil {
			return fmt.Errorf("%w: seed ancestor repair not admitted: %w", redisstore.ErrSeedColdAdmissionUnavailable, err)
		}
		if err := ts.engine.finishSeedRepair(repair); err != nil {
			return err
		}
	}
	return nil
}

// protectSeedTaker consults history only when an OHLCV-only bucket has no measured counterpart
// in the folded batch, protecting the persisted slice itself rather than the capped ring.
func (ts *TieredStore) protectSeedTaker(ctx context.Context, exchange, marketType, symbol, timeframe string, merged []candle.Candle) ([]candle.Candle, error) {
	if ts.cold == nil || ts.coldAppender == nil {
		return merged, nil
	}
	now := time.Now()
	var cutoff int64
	if floor, clamped := ts.coldWriteFloor(now); clamped {
		cutoff = floor.UnixMilli()
	}
	needsHistory := func(c candle.Candle) bool {
		return c.Timestamp >= cutoff && c.Volume > 0 && c.TakerBuyVolume == 0 && c.TakerSellVolume == 0
	}
	var minTs, maxTs int64
	found := false
	for _, c := range merged {
		if !needsHistory(c) {
			continue
		}
		if !found || c.Timestamp < minTs {
			minTs = c.Timestamp
		}
		if !found || c.Timestamp > maxTs {
			maxTs = c.Timestamp
		}
		found = true
	}
	if !found {
		return merged, nil
	}
	tfMs, ok := candle.ParseTimeframeMs(timeframe)
	if !ok {
		return nil, fmt.Errorf("protect seed taker: invalid timeframe %q", timeframe)
	}
	// QueryRange returns DESC: the whole interval, including intermediate buckets, must fit its
	// limit even when the batch requests only its endpoints.
	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	history, err := ts.cold.QueryRange(readCtx, exchange, marketType, symbol, timeframe,
		time.UnixMilli(minTs), time.UnixMilli(maxTs), int(candle.ExpectedBuckets(minTs, maxTs, tfMs)))
	if err != nil {
		return nil, fmt.Errorf("protect seed taker for %s:%s:%s:%s: %w: %w", exchange, marketType, symbol, timeframe, redisstore.ErrSeedHistoryUnavailable, err)
	}
	byTs := make(map[int64]candle.Candle, len(history))
	for _, c := range history {
		byTs[c.Timestamp] = c
	}
	var protected []candle.Candle
	for i, c := range merged {
		stored, exists := byTs[c.Timestamp]
		if !needsHistory(c) || !exists || stored.TakerBuyVolume+stored.TakerSellVolume <= 0 {
			continue
		}
		if protected == nil {
			protected = append([]candle.Candle(nil), merged...)
		}
		protected[i] = candle.UnionSeedInto(stored, c, tfMs, now.UnixMilli())
	}
	if protected == nil {
		return merged, nil // Confirmed empty/unmeasured history still accepts new buckets.
	}
	ts.engine.SeedCandles(exchange, marketType, symbol, timeframe, protected)
	return protected, nil
}

// SeedCandlesBatch folds a historical run into the engine and persists it to the Redis and cold
// tiers.
func (ts *TieredStore) SeedCandlesBatch(ctx context.Context, exchange, marketType, symbol, timeframe string, seedCandles []candle.Candle) error {
	// The taker pair is per bucket, so there is no level to reconstruct. Fold verbatim on an
	// empty key, union otherwise, so a repair adds missed buckets without overwriting live
	// detail. SeedCandles does not reorder the slice; the tiers below consume the same one.
	merged := ts.FoldSeedBatch(exchange, marketType, symbol, timeframe, seedCandles)
	return ts.PersistSeedBatch(ctx, exchange, marketType, symbol, timeframe, merged, seedCandles)
}

// SeedCurrentBucketCandle seeds the open bucket in both the engine and Redis.
func (ts *TieredStore) SeedCurrentBucketCandle(ctx context.Context, exchange, marketType, symbol, timeframe string, candle candle.Candle) error {
	ts.engine.SeedCurrentBucketCandle(exchange, marketType, symbol, timeframe, candle)
	return ts.redis.SeedCurrentBucketCandle(ctx, exchange, marketType, symbol, timeframe, candle)
}

// ============================================================================
// UPDATE OPERATIONS → CandleEngine primary
// ============================================================================

// ReplaceCandle overwrites one bar in the engine and, when write-through is on, in Redis.
func (ts *TieredStore) ReplaceCandle(ctx context.Context, exchange, marketType, symbol, timeframe string, candle candle.Candle) error {
	ts.engine.UpdateCandle(exchange, marketType, symbol, timeframe, candle)
	if ts.writeThrough {
		// Authoritative replace: write immediately to Redis.
		return ts.redis.ReplaceCandle(ctx, exchange, marketType, symbol, timeframe, candle)
	}
	return nil
}

// ============================================================================
// QUERY OPERATIONS → CandleEngine only (fast in-memory reads)
// ============================================================================

// GetCandleByTimestamp returns one bar from the in-memory engine.
func (ts *TieredStore) GetCandleByTimestamp(_ context.Context, exchange, marketType, symbol, timeframe string, timestamp int64) (*candle.Candle, error) {
	c := ts.engine.GetCandleByTimestamp(exchange, marketType, symbol, timeframe, timestamp)
	return c, nil
}

// GetCandlesDesc returns the newest limit bars, reconciling with the cold tier when it is fresher.
func (ts *TieredStore) GetCandlesDesc(ctx context.Context, exchange, marketType, symbol, timeframe string, limit int) ([]candle.Candle, error) {
	hotCandles := ts.engine.GetCandlesDesc(exchange, marketType, symbol, timeframe, limit)

	// The hot tier can freeze at stream death while the cold tier keeps advancing via backfill.
	// When cold is strictly newer, merge its recent window so the freshest across tiers wins;
	// otherwise the hot-only fast path is preserved.
	if ts.cold != nil {
		if merged, ok := ts.reconcileFreshest(ctx, exchange, marketType, symbol, timeframe, limit, hotCandles); ok {
			return merged, nil
		}
	}

	if len(hotCandles) >= limit || ts.cold == nil {
		return hotCandles, nil
	}

	// Need more data from cold storage
	needed := limit - len(hotCandles)

	var to time.Time
	if len(hotCandles) > 0 {
		oldestHot := hotCandles[len(hotCandles)-1] // DESC order, last = oldest
		// The boundary is exclusive: the cold query is `timestamp <= to`, and including the hot
		// tier's oldest bucket makes it die in the dedup merge. Stepping back one millisecond is
		// exact for both a lagging and a current cold tier.
		to = time.UnixMilli(oldestHot.Timestamp - 1)
	} else {
		to = time.Now()
	}

	coldCandles, err := ts.cold.QueryRange(ctx, exchange, marketType, symbol, timeframe, coldReadFrom(to, timeframe, needed), to, needed)
	if err != nil {
		logger.OrDiscard(ts.log).Warn("cold store query failed; returning hot data only", "error", err)
		return hotCandles, nil
	}

	return mergeCandles(hotCandles, coldCandles), nil
}

// GetBatchCandlesDesc delegates the single-pipeline batch read to RedisStore; the batch-prices
// endpoint reads the durable ZSETs directly, kept current by write-through.
func (ts *TieredStore) GetBatchCandlesDesc(ctx context.Context, exchange, marketType string, reqs []redisstore.CandleReq) map[redisstore.CandleReq][]candle.Candle {
	return ts.redis.GetBatchCandlesDesc(ctx, exchange, marketType, reqs)
}

// reconcileFreshest returns the freshest candles across both tiers when cold is strictly newer
// than hot (frozen hot, backfilled cold). It returns ok=false when hot is freshest, the cold peek
// is empty, or on any cold error, preserving the fast path. mergeCandles dedups hot-first.
func (ts *TieredStore) reconcileFreshest(ctx context.Context, exchange, marketType, symbol, timeframe string, limit int, hotCandles []candle.Candle) ([]candle.Candle, bool) {
	now := time.Now()

	// Cheap peek: the single newest cold row (QueryRange is ORDER BY timestamp DESC).
	coldNewest, err := ts.cold.QueryRange(ctx, exchange, marketType, symbol, timeframe, coldReadFrom(now, timeframe, 1), now, 1)
	if err != nil || len(coldNewest) == 0 {
		return nil, false
	}

	var hotNewestTs int64
	if len(hotCandles) > 0 {
		hotNewestTs = hotCandles[0].Timestamp // DESC order, first = newest
	}
	if coldNewest[0].Timestamp <= hotNewestTs {
		return nil, false // hot is freshest — fast path
	}

	// Hot is stale: pull the cold recent window and merge so the freshest wins.
	recentCold, err := ts.cold.QueryRange(ctx, exchange, marketType, symbol, timeframe, coldReadFrom(now, timeframe, limit), now, limit)
	if err != nil {
		return nil, false
	}
	merged := mergeCandles(hotCandles, recentCold)
	if len(merged) > limit {
		merged = merged[:limit]
	}
	return merged, true
}

// GetCandlesAsc returns the oldest limit bars from the engine.
func (ts *TieredStore) GetCandlesAsc(_ context.Context, exchange, marketType, symbol, timeframe string, limit int) ([]candle.Candle, error) {
	return ts.engine.GetCandlesAsc(exchange, marketType, symbol, timeframe, limit), nil
}

// GetCandlesByScoreRange returns every candle in the inclusive window, ascending, merging the hot
// ZSET with the durable tier. A hot ZSET capped below the window cannot satisfy the validator's
// requirement that the first member sits exactly on gapStart, so cold completes it; mergeCandles
// sorts DESC, so the order is restored before returning.
func (ts *TieredStore) GetCandlesByScoreRange(ctx context.Context, exchange, marketType, symbol, timeframe string, minScore, maxScore int64) ([]candle.Candle, error) {
	hot, err := ts.redis.GetCandlesByScoreRange(ctx, exchange, marketType, symbol, timeframe, minScore, maxScore)
	if err != nil {
		return nil, err
	}
	if ts.cold == nil {
		return hot, nil
	}

	from, to, wantBuckets, ok := coldScoreWindow(minScore, maxScore, timeframe)
	if !ok || len(hot) >= wantBuckets {
		// No bucket arithmetic, or the hot tier already holds every bucket: nothing to add.
		return hot, nil
	}

	cold, err := ts.cold.QueryRange(ctx, exchange, marketType, symbol, timeframe, from, to, wantBuckets)
	if err != nil {
		logger.OrDiscard(ts.log).Warn("cold store score-range query failed; returning hot data only", "error", err)
		return hot, nil
	}

	return ascendingByTimestamp(mergeCandles(hot, cold)), nil
}

// coldScoreWindow turns a score range into the bounds and row budget of the cold read that
// completes it. The budget must cover every bucket: a DESC query with a short LIMIT drops the
// oldest rows, exactly the head the validator checks. The lower bound clamps to retention.
func coldScoreWindow(minScore, maxScore int64, timeframe string) (from, to time.Time, buckets int, ok bool) {
	tf, parsed := parseTimeframeDuration(timeframe)
	if !parsed || maxScore < minScore {
		return time.Time{}, time.Time{}, 0, false
	}

	to = time.UnixMilli(maxScore)
	from = time.UnixMilli(minScore)
	if earliest := to.Add(-coldReadMaxLookback); from.Before(earliest) {
		from = earliest
	}

	return from, to, int(to.Sub(from)/tf) + 1, true
}

// ascendingByTimestamp reverses a descending slice in place.
func ascendingByTimestamp(cs []candle.Candle) []candle.Candle {
	for i, j := 0, len(cs)-1; i < j; i, j = i+1, j-1 {
		cs[i], cs[j] = cs[j], cs[i]
	}
	return cs
}

// GetCandlesCount returns the engine's resident bar count.
func (ts *TieredStore) GetCandlesCount(_ context.Context, exchange, marketType, symbol, timeframe string) (int64, error) {
	return int64(ts.engine.CandleCount(exchange, marketType, symbol, timeframe)), nil
}

// GetCandlesRange returns candles within an explicit time range, merging hot and cold.
func (ts *TieredStore) GetCandlesRange(ctx context.Context, exchange, marketType, symbol, timeframe string, from, to time.Time, limit int) ([]candle.Candle, error) {
	hotCandles := ts.engine.GetCandlesDesc(exchange, marketType, symbol, timeframe, limit)

	var filteredHot []candle.Candle
	for _, c := range hotCandles {
		ct := time.UnixMilli(c.Timestamp)
		if (from.IsZero() || !ct.Before(from)) && (to.IsZero() || !ct.After(to)) {
			filteredHot = append(filteredHot, c)
		}
	}

	if len(filteredHot) >= limit || ts.cold == nil {
		if len(filteredHot) > limit {
			filteredHot = filteredHot[:limit]
		}
		return filteredHot, nil
	}

	// The same exclusive boundary as GetCandlesDesc: passing the caller's `to` would make the
	// cold query return the newest rows, exactly what the hot tier covers, and the dedup merge
	// would discard them. The boundary is the oldest of the served range.
	coldTo := to
	if len(filteredHot) > 0 {
		oldestHot := filteredHot[len(filteredHot)-1] // DESC order, last = oldest
		coldTo = time.UnixMilli(oldestHot.Timestamp - 1)
	} else if coldTo.IsZero() {
		// A range without a ceiling is open, not empty: a zero time serializes to year 1 and the
		// cold query returns zero rows. A real caller-supplied `to` still rules.
		coldTo = time.Now()
	}

	coldCandles, err := ts.cold.QueryRange(ctx, exchange, marketType, symbol, timeframe, from, coldTo, limit-len(filteredHot))
	if err != nil {
		logger.OrDiscard(ts.log).Warn("cold store range query failed; returning hot data only", "error", err)
		return filteredHot, nil
	}

	return mergeCandles(filteredHot, coldCandles), nil
}

// ============================================================================
// METADATA OPERATIONS → RedisStore direct delegation
// ============================================================================

// AddActiveSymbol records a symbol in the Redis active set.
func (ts *TieredStore) AddActiveSymbol(ctx context.Context, exchange, marketType, symbol string) error {
	return ts.redis.AddActiveSymbol(ctx, exchange, marketType, symbol)
}

// GetActiveSymbols returns the active symbols of one scope.
func (ts *TieredStore) GetActiveSymbols(ctx context.Context, exchange, marketType string) ([]string, error) {
	return ts.redis.GetActiveSymbols(ctx, exchange, marketType)
}

// GetAllActiveSymbols returns every active symbol across scopes.
func (ts *TieredStore) GetAllActiveSymbols(ctx context.Context) ([]redisstore.SymbolInfo, error) {
	return ts.redis.GetAllActiveSymbols(ctx)
}

// AddTimeframe records a supported timeframe for one symbol.
func (ts *TieredStore) AddTimeframe(ctx context.Context, exchange, marketType, symbol, timeframe string) error {
	return ts.redis.AddTimeframe(ctx, exchange, marketType, symbol, timeframe)
}

// GetTimeframes returns the supported timeframes recorded for one symbol.
func (ts *TieredStore) GetTimeframes(ctx context.Context, exchange, marketType, symbol string) ([]string, error) {
	return ts.redis.GetTimeframes(ctx, exchange, marketType, symbol)
}

// ============================================================================
// LIFECYCLE
// ============================================================================

// SetColdStore attaches an optional cold store for historical reads.
func (ts *TieredStore) SetColdStore(cold candle.ColdStore) {
	ts.cold = cold
}

// SetColdAppender wires the durability pipeline so seed/backfill closed candles reach the cold
// tier. The boundary is the retention policy itself, not a standalone duration that could drift;
// a zeroed policy means no clamp. Call during construction, before seed goroutines start and not
// concurrently with active seed traffic.
func (ts *TieredStore) SetColdAppender(a candle.ColdAppender, boundary candle.WriteFloor) {
	ts.coldAppender = a
	ts.coldBoundary = boundary
}

// coldWriteFloor reads the boundary without assuming it was injected: a nil interface would
// panic on the first seed path. A store without a durable tier writes everything, the same as a
// zeroed policy.
func (ts *TieredStore) coldWriteFloor(now time.Time) (time.Time, bool) {
	if ts.coldBoundary == nil {
		return time.Time{}, false
	}
	return ts.coldBoundary.WriteFloor(now)
}

// appendClosedToCold routes closed seed/backfill candles to the durable tier, idempotent via
// QuestDB DEDUP and clamped to retention. No-op without an appender; the open bucket stays
// hot-only. Every in-retention candle reaches the writer, including OHLCV-only buckets, because
// PersistSeedBatch already protects missing taker measurements. An abort leaves later candles
// unadmitted; admitted entries belong to the pipeline and must not be replayed.
func (ts *TieredStore) appendClosedToCold(ctx context.Context, exchange, marketType, symbol, timeframe string, cs []candle.Candle) error {
	if ts.coldAppender == nil {
		return nil
	}
	var cutoff int64
	if floor, clamped := ts.coldWriteFloor(time.Now()); clamped {
		cutoff = floor.UnixMilli()
	}
	appended := 0
	for i := range cs {
		if cutoff > 0 && cs[i].Timestamp < cutoff {
			continue // would re-create a partition retention already dropped
		}
		if err := ts.coldAppender.EnqueueClosedBlocking(ctx, exchange, marketType, symbol, timeframe, cs[i]); err != nil {
			ts.metrics.incColdAppendAborted()
			logger.OrDiscard(ts.log).Warn("tiered store: cold append aborted",
				"exchange", exchange,
				"market_type", marketType,
				"symbol", symbol,
				"timeframe", timeframe,
				"timestamp", cs[i].Timestamp,
				"appended", appended,
				"error", err)
			// Surface the partial admission; this does not promise an automatic retry.
			return fmt.Errorf("cold append aborted for %s:%s:%s:%s after %d candles: %w",
				exchange, marketType, symbol, timeframe, appended, err)
		}
		appended++
	}
	return nil
}

// Close flushes a final snapshot (if write-through is enabled), stops the
// snapshot goroutine, and closes the underlying RedisStore.
func (ts *TieredStore) Close() error {
	if ts.writeThrough {
		ts.flushSnapshot(ts.ctx)
	}
	ts.cancel()
	ts.wg.Wait()
	return ts.redis.Close()
}

// ============================================================================
// MERGE HELPERS
// ============================================================================

// mergeCandles merges hot and cold candles, deduplicating by timestamp.
// Hot candles take precedence. Result is sorted descending by timestamp.
func mergeCandles(hot, cold []candle.Candle) []candle.Candle {
	seen := make(map[int64]struct{}, len(hot))
	result := make([]candle.Candle, 0, len(hot)+len(cold))

	for _, c := range hot {
		seen[c.Timestamp] = struct{}{}
		result = append(result, c)
	}

	for _, c := range cold {
		if _, exists := seen[c.Timestamp]; !exists {
			result = append(result, c)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp > result[j].Timestamp
	})

	return result
}

// WriteClosedToHot persists a just-closed bucket to the hot tier. The periodic snapshot writes
// only the newest candle per key, so a bucket that rolls between sweeps would lose every later
// trade. Best-effort: a failure is logged, never propagated into the close path, because the
// durable tier already holds the authoritative copy.
func (ts *TieredStore) WriteClosedToHot(key CompositeKey, closed candle.Candle) {
	if !ts.writeThrough || ts.redis == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ts.ctx, 10*time.Second)
	defer cancel()

	if err := ts.redis.WriteSnapshotBatch(ctx, []redisstore.CandleUpdate{
		buildSnapshotUpdate(key, closed),
	}); err != nil {
		logger.OrDiscard(ts.log).Warn("tiered store: closed-bucket hot write failed",
			"exchange", key.Exchange,
			"market_type", key.MarketType,
			"symbol", key.Symbol,
			"timeframe", key.Timeframe,
			"error", err)
	}
}
