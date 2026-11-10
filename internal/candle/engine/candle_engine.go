// Package engine owns the in-memory candle engine, its durable tier and the probes that
// keep both aligned.
package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
	"unsafe"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// defaultBufferSize is the ring depth when nothing declares one; it reads the single fallback so
// one quantity cannot acquire several values. See internal/platform/config/declared_depth.go.
const defaultBufferSize = config.DefaultResidentDepth

// CandleEngine manages in-memory candle buffers with merge logic and close detection.
type CandleEngine struct {
	mu          sync.RWMutex
	buffers     map[CompositeKey]*candle.RingBuffer[candle.Candle]
	bufferSizes map[string]int // per-TF sizes; "default" key fallback
	onClose     func(key CompositeKey, candle candle.Candle)

	// redisStore is an optional fallback for GetLatestTimestamp when the
	// in-memory buffer is empty or missing. Set via SetRedisStore().
	redisStore RedisLatestTimestampStore

	// gapRecoveredBuckets tracks buckets written from a REST total so a later payload with
	// unknown provenance cannot be summed onto it.
	// Key: "exchange:marketType:symbol:tf:timestamp".
	gapRecoveredBuckets map[string]struct{}

	// onRevised is fired when an already-closed bucket is corrected in place. See
	// OnCandleRevised for why it is not the close callback.
	onRevised     func(key CompositeKey, candle candle.Candle)
	onSeedRevised func(key CompositeKey, candle candle.Candle)
	onSeedGap     func(key CompositeKey, timestamp int64)
	derivedScopes map[string]string
	listingFloors map[string]int64
	seedDirty     map[CompositeKey]map[int64]uint64
	seedDirtyLost map[CompositeKey]bool
	seedRevision  uint64

	log *slog.Logger

	// metrics is the injected observability port. Zero value = safe no-op.
	metrics Metrics
}

// SetMetrics injects the observability port. The composition root calls it before the
// engine starts accepting candles.
func (ce *CandleEngine) SetMetrics(m Metrics) { ce.metrics = m }

// ForgetAppliedDeltaVolume removes this scope's series entirely when the venue takes ownership of
// its candle. Deleting, not zeroing: absent means "not measured here", while zero would be a false
// measurement.
func (ce *CandleEngine) ForgetAppliedDeltaVolume(exchange, marketType string, timeframes []string) {
	for _, tf := range timeframes {
		ce.metrics.deleteAppliedDeltaVolume(exchange, marketType, tf)
	}
}

// RedisLatestTimestampStore is the minimal interface needed for Redis fallback
// in GetLatestTimestamp. Implemented by *redisstore.RedisStore.
type RedisLatestTimestampStore interface {
	GetCandlesDesc(ctx context.Context, exchange, marketType, symbol, timeframe string, limit int) ([]candle.Candle, error)
}

// NewCandleEngine creates an engine with per-timeframe buffer sizes.
// The map may contain a "default" key used as fallback.
func NewCandleEngine(bufferSizes map[string]int, logs ...*slog.Logger) *CandleEngine {
	if bufferSizes == nil {
		bufferSizes = map[string]int{"default": defaultBufferSize}
	}
	return &CandleEngine{
		log:                 logger.OrDiscard(logs...),
		buffers:             make(map[CompositeKey]*candle.RingBuffer[candle.Candle]),
		bufferSizes:         bufferSizes,
		gapRecoveredBuckets: make(map[string]struct{}),
		derivedScopes:       make(map[string]string),
		listingFloors:       make(map[string]int64),
		seedDirty:           make(map[CompositeKey]map[int64]uint64),
		seedDirtyLost:       make(map[CompositeKey]bool),
	}
}

// SetListingFloors records independently verified listing instants for selected symbols.
// Missing or invalid entries revoke prior evidence rather than guessing from a ring.
func (ce *CandleEngine) SetListingFloors(exchange, marketType string, symbols []string, floors map[string]int64) {
	ce.mu.Lock()
	defer ce.mu.Unlock()
	now := time.Now().UnixMilli()
	for _, symbol := range symbols {
		key := exchange + ":" + marketType + ":" + symbol
		if floor := floors[symbol]; floor > 0 && floor <= now {
			ce.listingFloors[key] = floor
		} else {
			delete(ce.listingFloors, key)
		}
	}
}

// RegisterDerivedScope records the explicitly configured native feed interval.
func (ce *CandleEngine) RegisterDerivedScope(exchange, marketType, nativeTF string) {
	ce.mu.Lock()
	defer ce.mu.Unlock()
	ce.derivedScopes[exchange+":"+marketType] = nativeTF
}

// UnregisterDerivedScope forgets the derived buffers and native interval of one scope.
func (ce *CandleEngine) UnregisterDerivedScope(scope string) {
	ce.mu.Lock()
	defer ce.mu.Unlock()
	delete(ce.derivedScopes, scope)
}

// OnCandleClose registers a callback fired when a candle bucket closes.
func (ce *CandleEngine) OnCandleClose(fn func(key CompositeKey, candle candle.Candle)) {
	ce.mu.Lock()
	ce.onClose = fn
	ce.mu.Unlock()
}

// OnCandleRevised registers a callback fired when an already-closed bucket is corrected. The
// periodic snapshot writes only the newest bucket per key, so without this outlet the durable
// tier, Redis, the realtime bus and the WS hub keep the pre-correction value.
//
// It is separate from OnCandleClose: continuity records every close as the new baseline before
// judging a gap, and re-firing a close for an older bucket would drag that baseline backwards
// and read as an out-of-order frame.
func (ce *CandleEngine) OnCandleRevised(fn func(key CompositeKey, candle candle.Candle)) {
	ce.mu.Lock()
	ce.onRevised = fn
	ce.mu.Unlock()
}

// OnSeedCandleRevised publishes seed repairs without the ordinary cold enqueue.
// PersistSeedBatch separately admits each retained repair once with its context.
func (ce *CandleEngine) OnSeedCandleRevised(fn func(key CompositeKey, candle candle.Candle)) {
	ce.mu.Lock()
	defer ce.mu.Unlock()
	ce.onSeedRevised = fn
}

// OnSeedGap requests confirmation when a settled historical seed first marks a gap.
func (ce *CandleEngine) OnSeedGap(fn func(key CompositeKey, timestamp int64)) {
	ce.mu.Lock()
	defer ce.mu.Unlock()
	ce.onSeedGap = fn
}

// bufferSize returns the configured size for a timeframe.
func (ce *CandleEngine) bufferSize(tf string) int {
	if sz, ok := ce.bufferSizes[tf]; ok && sz > 0 {
		return sz
	}
	if sz, ok := ce.bufferSizes["default"]; ok && sz > 0 {
		return sz
	}
	return defaultBufferSize
}

// getOrCreate returns the buffer for the key, creating one if needed.
// Caller MUST hold the write lock.
func (ce *CandleEngine) getOrCreate(key CompositeKey) *candle.RingBuffer[candle.Candle] {
	if buf, ok := ce.buffers[key]; ok {
		return buf
	}
	buf := candle.NewRingBuffer[candle.Candle](ce.bufferSize(key.Timeframe))
	ce.buffers[key] = buf
	return buf
}

// OldestBucketTimestamp reports the oldest bucket held for a stream, and whether there is one.
// The fold uses it to tell a missing child from one that never existed, such as a symbol listed
// mid-week with no earlier 1d buckets.
func (ce *CandleEngine) OldestBucketTimestamp(exchange, marketType, symbol, tf string) (int64, bool) {
	ce.mu.RLock()
	defer ce.mu.RUnlock()

	buf, ok := ce.buffers[CompositeKey{exchange, marketType, symbol, tf}]
	if !ok {
		return 0, false
	}
	oldest, ok := buf.Oldest()
	if !ok {
		return 0, false
	}
	return oldest.Timestamp, true
}

// MarkOpenBucketGapped stamps each symbol's newest bucket as known-incomplete and reports how many
// it marked.
//
// Venue frames carry a running total, so an interruption spanning a bucket's close is permanently
// lossy: the venue never resends a finished bucket. Marking records that knowledge and unlocks the
// targeted re-read and the veto exception in candle.VenueHasSpoken. Symbols must be the exact
// strings the feed publishes under; this engine normalizes nothing.
func (ce *CandleEngine) MarkOpenBucketGapped(exchange, marketType, tf string, symbols []string) int {
	ce.mu.Lock()
	defer ce.mu.Unlock()

	marked := 0
	for _, sym := range symbols {
		buf, ok := ce.buffers[CompositeKey{exchange, marketType, sym, tf}]
		if !ok {
			continue
		}
		buf.UpdateLatest(func(latest *candle.Candle) {
			if !latest.HasGap {
				latest.HasGap = true
				marked++
			}
		})
	}
	return marked
}

// UpdateCandle merges a single candle into the stream's buffer. Same-bucket updates merge by the
// declared source, a newer timestamp closes the previous bucket, and an older one is a historical
// repair. A bucket previously written from a REST total uses the source-dispatched merge so a
// retry cannot double-count.
func (ce *CandleEngine) UpdateCandle(exchange, marketType, symbol, tf string, c candle.Candle) {
	if c.Source == string(candle.CandleMergeSourceWSKline) && c.Origin == candle.OriginUnknown {
		c.Origin = candle.OriginNative
	}
	key := CompositeKey{exchange, marketType, symbol, tf}
	now := time.Now().UnixMilli()

	var closeFn func()
	var revisedFn func()

	ce.mu.Lock()

	buf := ce.getOrCreate(key)
	latest, hasLatest := buf.PeekLatest()

	if !hasLatest {
		// First candle — just push.
		if c.CreatedAt == 0 {
			c.CreatedAt = now
		}
		c.LastUpdate = now
		// A first push of a live delta is applied volume too; omitting it would manufacture a
		// one-batch shortfall per (symbol, timeframe).
		if c.Source != "" && !candle.IsTotalSource(c.Source) {
			ce.metrics.addAppliedDeltaVolume(exchange, marketType, tf, c.Volume)
		}
		buf.Push(c)
		ce.mu.Unlock()
		return
	}

	switch {
	case c.Timestamp == latest.Timestamp:
		// A gap-recovered bucket already holds a REST total that includes these trades.
		bucketKey := fmt.Sprintf("%s:%s:%s:%s:%d", exchange, marketType, symbol, tf, c.Timestamp)
		_, gapRecovered := ce.gapRecoveredBuckets[bucketKey]

		// The owner path: a venue candle replaces, and only a newer reading on the venue's own
		// clock may replace. See candle.MergeOpenVenueFrame.
		if c.Source == string(candle.CandleMergeSourceWSKline) {
			merged, applied := candle.MergeOpenVenueFrame(latest, c, now)
			if !applied {
				ce.mu.Unlock()
				return
			}
			buf.SetLatest(merged)
			ce.mu.Unlock()
			return
		}

		// Once the venue has spoken for a bucket, nobody else writes it. The rule is narrow, so
		// historical seeding still works, and it is asymmetric on purpose: a venue candle may
		// replace a seeded total.
		if candle.VenueHasSpoken(latest, c) {
			ce.mu.Unlock()
			return
		}

		declaredTotal := c.Source != "" && candle.IsTotalSource(c.Source)

		// An explicit increment is an increment whatever happened to the bucket earlier. The
		// gap-recovered marker only has authority over payloads with unknown provenance;
		// branching on magnitude would freeze a bucket for the rest of its window.
		declaredDelta := c.Source != "" && !candle.IsTotalSource(c.Source)
		replaceRatherThanAccumulate := (gapRecovered && !declaredDelta) || declaredTotal

		// The cutoff. An exchange total reaches to the parse instant (FetchedAt, an upper bound on
		// its true horizon); our accumulation reaches to its newest trade (LastTradeAt).
		// RequestedAt supplies the lower bound, so a delta no newer than the request is already
		// inside the total. The upper end has no symmetric remedy: the windows overlap and the
		// bucket records how far it reaches, not where it began, so max() is the only safe
		// election — a deficit is visible, inflation is permanent.
		supersede := replaceRatherThanAccumulate

		// A delta whose newest trade predates the request that produced the standing total is
		// wholly contained by it; it is dropped and counted.
		if declaredDelta && candle.DeltaInsideTotalHorizon(latest, c) {
			ce.metrics.incDeltaInsideHorizon(exchange, marketType, tf)
			// Only the flow is a duplicate; the marks travel with the record. See
			// candle.MergeDroppedDeltaMarks.
			if c.HasGap || c.SideUnknownCount > 0 {
				buf.SetLatest(candle.MergeDroppedDeltaMarks(latest, c, now))
			}
			// ce.mu is released by hand on every exit path here, not by defer: a bare return
			// would leak it and deadlock the engine on the next update.
			ce.mu.Unlock()
			return
		}

		// A total is elected, never added; the trio is elected atomically from one record; and
		// only a declared total may restate the open. See candle.MergeOpenDelta.
		merged := candle.MergeOpenDelta(latest, c, now, supersede, declaredTotal)
		if !supersede {
			ce.metrics.addAppliedDeltaVolume(exchange, marketType, tf, c.Volume)
		}
		// Overwrite the latest slot in-place.
		buf.SetLatest(merged)

	case c.Timestamp > latest.Timestamp:
		// New bucket — the previous candle is now closed.
		// Copy closed candle data and defer callback to after lock release.
		if ce.onClose != nil {
			closedCopy := latest
			closeFn = func() { ce.onClose(key, closedCopy) }
		}
		// Clean up the gap-recovery marker for the old bucket (prevents a memory leak).
		oldBucketKey := fmt.Sprintf("%s:%s:%s:%s:%d", exchange, marketType, symbol, tf, latest.Timestamp)
		delete(ce.gapRecoveredBuckets, oldBucketKey)
		if c.CreatedAt == 0 {
			c.CreatedAt = now
		}
		c.LastUpdate = now
		// The candle is pushed as measured; order flow is per bucket and has no cross-bucket
		// level. Opening a new bucket applies this delta's volume just as accumulating does.
		if c.Source != "" && !candle.IsTotalSource(c.Source) {
			ce.metrics.addAppliedDeltaVolume(exchange, marketType, tf, c.Volume)
		}
		buf.Push(c)

	default:
		// Historical candle (c.Timestamp < latest.Timestamp): the bucket already rolled.
		// The branch is chosen by the declared Source, never by magnitude.
		if corrected := ce.mergeHistorical(key, buf, c, now); corrected != nil && ce.onRevised != nil {
			revisedCopy := *corrected
			revisedFn = func() { ce.onRevised(key, revisedCopy) }
		}
	}

	ce.mu.Unlock()

	// Fire callbacks OUTSIDE the lock to prevent deadlock. Every sink on the other end does
	// I/O — QuestDB, Redis, the realtime bus — and this lock serializes every reader of all
	// ~4,000 buffers.
	if closeFn != nil {
		closeFn()
	}
	if revisedFn != nil {
		revisedFn()
	}
}

// SeedCurrentBucketCandle merges a REST-sourced candle into the current open bucket. REST
// candles carry the total bucket volume, so this MAX-merges rather than sums, and then marks
// the bucket gap-recovered so unknown-provenance payloads cannot be summed onto it.
func (ce *CandleEngine) SeedCurrentBucketCandle(exchange, marketType, symbol, tf string, c candle.Candle) {
	key := CompositeKey{exchange, marketType, symbol, tf}
	now := time.Now().UnixMilli()

	ce.mu.Lock()
	defer ce.mu.Unlock()

	buf := ce.getOrCreate(key)
	latest, hasLatest := buf.PeekLatest()

	if !hasLatest {
		if c.CreatedAt == 0 {
			c.CreatedAt = now
		}
		c.LastUpdate = now
		buf.Push(c)
		return
	}

	switch {
	case c.Timestamp == latest.Timestamp:
		// Same bucket: MAX merge, since REST candles carry totals, not deltas. The venue owns
		// this bucket and gap recovery has nothing to add, so a bare return is correct here —
		// this function releases the lock with defer, unlike its sibling in UpdateCandle.
		if candle.VenueHasSpoken(latest, c) {
			return
		}

		// As in UpdateCandle: the trio is elected atomically, a total is elected rather than
		// added, and a merge never subtracts local bookkeeping. See candle.MergeOpenSnapshot.
		buf.SetLatest(candle.MergeOpenSnapshot(latest, c, now))

	case c.Timestamp > latest.Timestamp:
		// New bucket. The candle is pushed as received; no cross-bucket level is carried here.
		if c.CreatedAt == 0 {
			c.CreatedAt = now
		}
		c.LastUpdate = now
		buf.Push(c)

	default:
		// Historical — delegate to the source-dispatched merge.
		ce.mergeHistorical(key, buf, c, now)
	}

	// Mark this bucket as gap-recovered so a later unknown-provenance payload cannot be summed.
	bucketKey := fmt.Sprintf("%s:%s:%s:%s:%d", exchange, marketType, symbol, tf, c.Timestamp)
	ce.gapRecoveredBuckets[bucketKey] = struct{}{}
}

func (ce *CandleEngine) mergeHistorical(key CompositeKey, buf *candle.RingBuffer[candle.Candle], c candle.Candle, now int64) *candle.Candle {
	for i := 0; i < buf.Len(); i++ {
		stored, _ := buf.At(i)
		if stored.Timestamp == c.Timestamp {
			reason := lateReasonAdditive
			if candle.IsTotalSource(c.Source) {
				reason = lateReasonMaxTotal
			}
			merged := candle.MergeLateIntoClosed(stored, c, now)
			buf.SetAt(i, merged)
			ce.metrics.incLateUpdate(key.Exchange, key.MarketType, key.Timeframe, reason)
			// The repair has to LEAVE MEMORY. Returned rather than published here because the
			// caller holds the global lock and every sink on the other end does I/O.
			return &merged
		}
	}
	// Candle not found — insert at the correct sorted position. Since the buffer is ordered
	// oldest→newest and this timestamp < latest, the ring is rebuilt with the new candle in place.
	insertAt, evictedPrefix := candle.InsertCandleSorted(buf, c, now)

	// Count what survived the trim, not what entered: a late update older than the full ring is
	// inserted and sliced straight back off. "Inserted" means the bucket exists and the
	// continuity check can make it durable; an evicted bucket needs gap recovery.
	reason := lateReasonInserted
	if insertAt < evictedPrefix {
		reason = lateReasonEvicted
	}
	ce.metrics.incLateUpdate(key.Exchange, key.MarketType, key.Timeframe, reason)

	// An inserted bucket is one the sinks never saw, not a correction. Continuity makes it durable
	// through a targeted backfill, so publishing a revision here would race that writer.
	return nil
}

// UpdateCandlesBatch merges multiple candle. They are applied sequentially.
func (ce *CandleEngine) UpdateCandlesBatch(exchange, marketType, symbol, tf string, batch []candle.Candle) {
	for i := range batch {
		ce.UpdateCandle(exchange, marketType, symbol, tf, batch[i])
	}
}

// SeedCandles folds historical candles (oldest-first) into the key's buffer. It is monotonic in
// information: an empty key takes the batch verbatim, while an existing buffer is unioned bucket
// by bucket, never replaced or discarded. Verbatim matters for the warm-boot path, where storage
// returns exactly what the live path measured, zeros included.
func (ce *CandleEngine) SeedCandles(exchange, marketType, symbol, tf string, seedData []candle.Candle) []candle.Candle {
	return ce.seedCandles(exchange, marketType, symbol, tf, seedData, true)
}

// LoadStoredCandles admits stored history without a batch to persist afterward, sharing the fold
// with SeedCandles.
func (ce *CandleEngine) LoadStoredCandles(exchange, marketType, symbol, tf string, seedData []candle.Candle) {
	ce.seedCandles(exchange, marketType, symbol, tf, seedData, false)
}

func (ce *CandleEngine) seedCandles(exchange, marketType, symbol, tf string, seedData []candle.Candle, persistenceResult bool) []candle.Candle {
	key := CompositeKey{exchange, marketType, symbol, tf}

	ce.mu.Lock()

	// A seed folds into the buffer and never replaces it: keeping the buffer would drop the
	// batch's interior buckets, replacing it would drop live detail and older buckets. A union
	// only takes the MAX, so where an authoritative closed record exists it settles the bucket
	// instead. See candle.IsAuthoritativeClosedTotal.
	tfMs, _ := candle.ParseTimeframeMs(tf)
	seedNow := time.Now().UnixMilli()
	var before map[int64]candle.Candle
	if persistenceResult && (ce.derivedScopes[exchange+":"+marketType] != "" || ce.onSeedGap != nil) {
		before = make(map[int64]candle.Candle)
		if existing := ce.buffers[key]; existing != nil {
			for _, c := range existing.Slice() {
				before[c.Timestamp] = c
			}
		}
	}
	var stats candle.SeedFoldStats
	if existing, ok := ce.buffers[key]; ok && existing.Len() > 0 && len(seedData) > 0 {
		nativeTF := ce.derivedScopes[exchange+":"+marketType]
		nativeMs, _ := candle.ParseTimeframeMs(nativeTF)
		allowUnknown := nativeMs > 0 && tfMs > nativeMs
		stats = candle.MergeBatchIntoBuffer(existing, seedData, tfMs, seedNow, allowUnknown)
	} else {
		buf := candle.NewRingBuffer[candle.Candle](ce.bufferSize(tf))
		for i := range seedData {
			buf.Push(seedData[i])
		}
		ce.buffers[key] = buf
	}

	// What the tiers persist is what the ring holds. Handing them the raw batch let a REST kline
	// with no taker split overwrite a measured pair with 0/0 in a table that dedups and has no
	// NULL. Returning the union removes that class.
	var merged []candle.Candle
	var affected []int64
	var newGaps []int64
	gapFn := ce.onSeedGap
	if persistenceResult {
		merged = candle.MergedForBatch(ce.buffers[key], seedData)
		if before != nil {
			incoming := make(map[int64]candle.Candle, len(seedData))
			for _, c := range seedData {
				incoming[c.Timestamp] = c
			}
			seenGaps := make(map[int64]struct{})
			for _, c := range merged {
				old, exists := before[c.Timestamp]
				if ce.derivedScopes[exchange+":"+marketType] != "" && (!exists || !candle.SameFoldValues(old, c) || old.HasGap != c.HasGap) {
					affected = append(affected, c.Timestamp)
				}
				in := incoming[c.Timestamp]
				if exists && !old.HasGap && c.HasGap && old.Volume > in.Volume && !in.HasGap && in.Source == string(candle.CandleMergeSourceRESTTotal) && candle.IsAuthoritativeClosedTotal(in, tfMs, seedNow) {
					if _, duplicate := seenGaps[c.Timestamp]; !duplicate {
						newGaps = append(newGaps, c.Timestamp)
						seenGaps[c.Timestamp] = struct{}{}
					}
				}
			}
		}
	}

	ce.mu.Unlock()
	if len(affected) > 0 {
		ce.repairSeedAncestors(exchange, marketType, symbol, tf, affected)
	}
	if gapFn != nil {
		for _, ts := range newGaps {
			gapFn(key, ts)
		}
	}

	// Reporting happens outside the critical section, which is why the unlock above is explicit:
	// reportSeedFold writes counters and can emit a synchronous log line, which under the lock
	// would block every buffer reader.
	ce.reportSeedFold(key, stats)
	return merged
}

// Snapshot returns a copy of all buffer data, keyed by CompositeKey.
// Safe for concurrent use — data is copied under RLock.
func (ce *CandleEngine) Snapshot() map[CompositeKey][]candle.Candle {
	ce.mu.RLock()
	defer ce.mu.RUnlock()
	result := make(map[CompositeKey][]candle.Candle, len(ce.buffers))
	for k, buf := range ce.buffers {
		result[k] = buf.Slice()
	}
	return result
}

// GetCandlesAsc returns up to limit candles oldest → newest.
func (ce *CandleEngine) GetCandlesAsc(exchange, marketType, symbol, tf string, limit int) []candle.Candle {
	key := CompositeKey{exchange, marketType, symbol, tf}

	ce.mu.RLock()
	defer ce.mu.RUnlock()

	buf, ok := ce.buffers[key]
	if !ok {
		return nil
	}
	asc := buf.Slice()
	if limit > 0 && limit < len(asc) {
		asc = asc[len(asc)-limit:]
	}
	return asc
}

// GetCandleByTimestamp returns a single candle matching the exact timestamp, or nil.
func (ce *CandleEngine) GetCandleByTimestamp(exchange, marketType, symbol, tf string, timestamp int64) *candle.Candle {
	key := CompositeKey{exchange, marketType, symbol, tf}

	ce.mu.RLock()
	defer ce.mu.RUnlock()

	buf, ok := ce.buffers[key]
	if !ok {
		return nil
	}
	// Scanned in place: Slice() would copy the whole ring, and the fold path performs 24 lookups
	// per frame at 800 buckets.
	for i := 0; i < buf.Len(); i++ {
		if c, _ := buf.At(i); c.Timestamp == timestamp {
			cp := c
			return &cp
		}
	}
	return nil
}

// SetRedisStore sets the optional Redis fallback for GetLatestTimestamp.
func (ce *CandleEngine) SetRedisStore(store RedisLatestTimestampStore) {
	ce.mu.Lock()
	defer ce.mu.Unlock()
	ce.redisStore = store
}

// GetLatestTimestamp returns the newest candle timestamp for the key, falling back to Redis when
// the buffer is empty or missing. It returns (0, false) when neither has data.
func (ce *CandleEngine) GetLatestTimestamp(ctx context.Context, exchange, marketType, symbol, tf string) (int64, bool) {
	key := CompositeKey{exchange, marketType, symbol, tf}
	ce.mu.RLock()
	buf, ok := ce.buffers[key]
	if ok {
		latest, exists := buf.PeekLatest()
		if exists {
			ce.mu.RUnlock()
			return latest.Timestamp, true
		}
	}
	redisStore := ce.redisStore
	ce.mu.RUnlock()

	// The caller's context bounds the fallback read; the 3s ceiling is its own upper bound.
	if redisStore != nil {
		readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		candles, err := redisStore.GetCandlesDesc(readCtx, exchange, marketType, symbol, tf, 1)
		if err == nil && len(candles) > 0 {
			return candles[0].Timestamp, true
		}
	}

	return 0, false
}

// BufferKeys returns all active composite keys.
func (ce *CandleEngine) BufferKeys() []CompositeKey {
	ce.mu.RLock()
	defer ce.mu.RUnlock()
	keys := make([]CompositeKey, 0, len(ce.buffers))
	for k := range ce.buffers {
		keys = append(keys, k)
	}
	return keys
}

// CandleCount returns the number of candles for a given key.
func (ce *CandleEngine) CandleCount(exchange, marketType, symbol, tf string) int {
	key := CompositeKey{exchange, marketType, symbol, tf}

	ce.mu.RLock()
	defer ce.mu.RUnlock()

	buf, ok := ce.buffers[key]
	if !ok {
		return 0
	}
	return buf.Len()
}

// PeekLatestCandle returns the newest candle without allocating, for the snapshot flush
// fast-path; it avoids the SliceDesc allocation that dominated alloc_space.
func (ce *CandleEngine) PeekLatestCandle(exchange, marketType, symbol, tf string) (candle.Candle, bool) {
	key := CompositeKey{exchange, marketType, symbol, tf}

	ce.mu.RLock()
	defer ce.mu.RUnlock()

	buf, ok := ce.buffers[key]
	if !ok {
		return candle.Candle{}, false
	}
	return buf.PeekLatest()
}

// GetCandlesDesc returns up to limit candles newest → oldest.
func (ce *CandleEngine) GetCandlesDesc(exchange, marketType, symbol, tf string, limit int) []candle.Candle {
	key := CompositeKey{exchange, marketType, symbol, tf}

	ce.mu.RLock()
	defer ce.mu.RUnlock()

	buf, ok := ce.buffers[key]
	if !ok {
		return nil
	}
	desc := buf.SliceDesc()
	if limit > 0 && limit < len(desc) {
		desc = desc[:limit]
	}
	return desc
}

// Stats returns runtime metrics.
func (ce *CandleEngine) Stats() EngineStats {
	ce.mu.RLock()
	defer ce.mu.RUnlock()

	stats := EngineStats{
		TotalBuffers: len(ce.buffers),
		BuffersByTF:  make(map[string]int),
	}
	for key, buf := range ce.buffers {
		stats.TotalCandles += buf.Len()
		stats.BuffersByTF[key.Timeframe]++
	}
	// Rough estimate: each Candle is ~64 bytes (8 float64 fields).
	candleSize := int64(unsafe.Sizeof(candle.Candle{}))
	stats.MemoryEstimateKB = (int64(stats.TotalCandles) * candleSize) / 1024
	return stats
}

// PurgeStaleSymbols removes buffer entries whose "exchange:marketType:symbol" is not in the
// allowed set, returning the number purged.
func (ce *CandleEngine) PurgeStaleSymbols(allowedSymbols map[string]struct{}) int {
	ce.mu.Lock()
	defer ce.mu.Unlock()

	var purged int
	for key := range ce.buffers {
		symKey := key.Exchange + ":" + key.MarketType + ":" + key.Symbol
		if _, ok := allowedSymbols[symKey]; !ok {
			delete(ce.buffers, key)
			purged++
		}
	}
	for key := range ce.listingFloors {
		if _, ok := allowedSymbols[key]; !ok {
			delete(ce.listingFloors, key)
		}
	}
	for key, pending := range ce.seedDirty {
		symKey := key.Exchange + ":" + key.MarketType + ":" + key.Symbol
		if _, ok := allowedSymbols[symKey]; !ok {
			if len(pending) > 0 {
				ce.noteSeedRepairEvictionLocked(key)
			}
			delete(ce.seedDirty, key)
			delete(ce.seedDirtyLost, key)
		}
	}

	// Also clean up gap-recovery markers for purged symbols.
	for bucketKey := range ce.gapRecoveredBuckets {
		// bucketKey format: "exchange:marketType:symbol:tf:timestamp"
		// Extract "exchange:marketType:symbol" (first 3 colon-separated parts)
		parts := bucketKey
		colonCount := 0
		for i, ch := range parts {
			if ch == ':' {
				colonCount++
				if colonCount == 3 {
					symKey := parts[:i]
					if _, ok := allowedSymbols[symKey]; !ok {
						delete(ce.gapRecoveredBuckets, bucketKey)
					}
					break
				}
			}
		}
	}

	return purged
}
