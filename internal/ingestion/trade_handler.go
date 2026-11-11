// Package ingestion owns trade ingestion, subscription management and the hand-off to
// persistence.
package ingestion

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/ingestion/streaming"
	logger "github.com/marxms/go-whale/internal/platform/logging"
	"github.com/marxms/go-whale/internal/trade"
)

// CandleBatchWriter admits aggregated candle batches.
type CandleBatchWriter interface {
	AddStreamingCandlesBatch(ctx context.Context, updates []candle.CandleUpdate) error
}

// CombinedStore composes only the capabilities this handler uses.
type CombinedStore interface {
	CandleBatchWriter
	trade.LastTradeWriter
}

// Compile-time check: StreamTradeHandler implements TradeHandler.
var _ TradeHandler = (*StreamTradeHandler)(nil)

// StreamTradeHandler turns trade batches into candles and persists them: filter already-seen
// trades, build OHLCVC per timeframe, then persist as a batch.
type StreamTradeHandler struct {
	store             CombinedStore
	defaultExchange   string
	defaultMarketType string
	tfReg             *candle.TimeframeRegistry
	// ohlcvc recycles the two working maps used by the per-timeframe aggregation.
	ohlcvc *ohlcvcPool
	// persistence accumulates failure counters that the write paths log and tests inspect.
	persistence *streaming.PersistenceObservability
	// seen tracks which trade ids were aggregated per symbol, not merely the largest: the SDK
	// dispatches messages on separate goroutines, so arrival order is not id order and a
	// watermark silently discarded unseen trades.
	seen                    map[string]*seenWindow
	lastTradeTimestamp      map[string]int64 // symbol -> last processed timestamp (for optimization)
	lastDuplicateWarnAt     map[string]time.Time
	suppressedDuplicateWarn map[string]int
	lastMissingWarnAt       map[string]time.Time
	suppressedMissingWarn   map[string]int
	mu                      sync.RWMutex
	duplicateWarnInterval   time.Duration

	persistFailures   int64
	lastTradeFailures int64

	// sync.Pool for deduplication maps — avoids an allocation per batch
	seenPool sync.Pool

	// venueOwned names the scopes whose candles come from the venue's own kline stream rather
	// than from aggregating this tape. It is topology, not a flag. The handler still admits,
	// deduplicates and serves /api/trades for those scopes; it just stops being a second writer.
	//
	// It is keyed by timeframe, not by scope: keying on exchange:marketType would stop the tape
	// writing all timeframes while the venue feed covers exactly one.
	venueOwned map[string]struct{}

	// metrics is the injected observability port. The zero value is a safe no-op.
	metrics Metrics

	log *slog.Logger
}

// SetMetrics injects the ingestion observability port. The composition root calls it before
// the handler starts processing trades.
func (h *StreamTradeHandler) SetMetrics(m Metrics) { h.metrics = m }

// MarkVenueOwnedCandles declares that a scope's candles come from the venue's kline stream.
//
// Called at wiring time from whichever adapters actually have a kline feed, so the set describes
// the topology that was built rather than an intention.
func (h *StreamTradeHandler) MarkVenueOwnedCandles(exchange, marketType, timeframe string) {
	h.mu.Lock()
	if h.venueOwned == nil {
		h.venueOwned = make(map[string]struct{})
	}
	h.venueOwned[exchange+":"+marketType+":"+timeframe] = struct{}{}
	h.mu.Unlock()
}

// tapeStillWritesCandles reports whether any configured timeframe in this scope is still built
// from the tape.
func (h *StreamTradeHandler) tapeStillWritesCandles(exchange, marketType string) bool {
	for _, tf := range h.tfReg.All() {
		if !h.candlesAreVenueOwned(exchange, marketType, tf) {
			return true
		}
	}
	return false
}

// TapeWritesCandles reports whether the tape is still a candle producer for this scope, so the
// pre-registration call site asks the same question the counting path asks.
func (h *StreamTradeHandler) TapeWritesCandles(exchange, marketType string) bool {
	return h.tapeStillWritesCandles(exchange, marketType)
}

func (h *StreamTradeHandler) candlesAreVenueOwned(exchange, marketType, timeframe string) bool {
	h.mu.RLock()
	_, ok := h.venueOwned[exchange+":"+marketType+":"+timeframe]
	h.mu.RUnlock()
	return ok
}

// NewStreamTradeHandler builds the trade-to-candle handler over the combined store.
func NewStreamTradeHandler(store CombinedStore, defaultExchange, defaultMarketType string, tfReg *candle.TimeframeRegistry, logs ...*slog.Logger) *StreamTradeHandler {
	h := &StreamTradeHandler{
		store:                   store,
		defaultExchange:         defaultExchange,
		defaultMarketType:       defaultMarketType,
		tfReg:                   tfReg,
		seen:                    make(map[string]*seenWindow),
		lastTradeTimestamp:      make(map[string]int64),
		lastDuplicateWarnAt:     make(map[string]time.Time),
		suppressedDuplicateWarn: make(map[string]int),
		lastMissingWarnAt:       make(map[string]time.Time),
		suppressedMissingWarn:   make(map[string]int),
		duplicateWarnInterval:   30 * time.Second,
		ohlcvc:                  newOHLCVCPool(),
		persistence:             streaming.NewPersistenceObservability(),
		log:                     logger.OrDiscard(logs...),
	}
	h.seenPool.New = func() interface{} {
		return make(map[string]struct{}, 64)
	}
	return h
}

// PersistenceObservability exposes the accumulator the write paths record into.
func (h *StreamTradeHandler) PersistenceObservability() *streaming.PersistenceObservability {
	return h.persistence
}

// PurgeSymbol removes all tracking state for the given compositeKey (exchange:marketType:symbol).
// Must be called when a symbol is unsubscribed to prevent memory leaks.
func (h *StreamTradeHandler) PurgeSymbol(compositeKey string) {
	h.mu.Lock()
	delete(h.seen, compositeKey)
	delete(h.lastTradeTimestamp, compositeKey)
	delete(h.lastDuplicateWarnAt, compositeKey)
	delete(h.suppressedDuplicateWarn, compositeKey)
	delete(h.lastMissingWarnAt, compositeKey)
	delete(h.suppressedMissingWarn, compositeKey)
	h.mu.Unlock()
}

// DeliveringCount reports how many symbols have had at least one trade admitted into the
// aggregator for the scope. A subscribe frame returning without error only says the venue
// accepted the request, not that bytes are arriving; lastTradeTimestamp is written by every
// ingest path that actually feeds the aggregator, and a non-zero entry cannot survive a boot.
func (h *StreamTradeHandler) DeliveringCount(exchange, marketType string, symbols []string) int {
	if len(symbols) == 0 {
		return 0
	}
	prefix := exchange + ":" + marketType + ":"

	h.mu.RLock()
	defer h.mu.RUnlock()

	n := 0
	for _, sym := range symbols {
		if h.lastTradeTimestamp[prefix+sym] > 0 {
			n++
		}
	}
	return n
}

// Delivering reports whether a single stream has admitted at least one trade. Same signal as
// DeliveringCount, for the per-symbol decision.
func (h *StreamTradeHandler) Delivering(exchange, marketType, symbol string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.lastTradeTimestamp[exchange+":"+marketType+":"+symbol] > 0
}

// tradeGap says the exchange issued aggregate trades this stream never received, and brackets in
// time where they sat. The payload is gone, so ingestion can only mark the under-counted buckets
// and let an authoritative record settle them. The bracket spans the previous and current batch
// ends; a false positive costs one redundant REST re-read, a false negative a permanent error.
type tradeGap struct {
	loTS int64
	hiTS int64
	n    int64
}

func (g tradeGap) empty() bool { return g.n <= 0 }

// covers reports whether this bucket may hold any of the trades that never arrived.
func (g tradeGap) covers(bucketTs int64, timeframe string) bool {
	if g.empty() {
		return false
	}
	lo := g.loTS
	if lo <= 0 || lo > g.hiTS {
		lo = g.hiTS
	}
	return candle.BucketTimestamp(lo, timeframe) <= bucketTs &&
		bucketTs <= candle.BucketTimestamp(g.hiTS, timeframe)
}

// crossedBucketBoundary reports whether the batch moved the stream into a new bucket of the
// finest configured timeframe. Boundaries are aligned, so crossing any coarser one crosses this
// one; a zero timestamp means the first batch and nothing has rolled yet.
func (h *StreamTradeHandler) crossedBucketBoundary(prevTS, newTS int64) bool {
	if prevTS <= 0 || newTS <= 0 || newTS < prevTS {
		return false
	}
	tf := h.finestTimeframe()
	if tf == "" {
		return false
	}
	return candle.BucketTimestamp(prevTS, tf) != candle.BucketTimestamp(newTS, tf)
}

// finestTimeframe returns the shortest configured timeframe, computed from the registry rather
// than hardcoded: this handler must not assume "5m" is the floor, and a config that adds 1m
// would otherwise leave every 1m bucket judged on the coarser clock.
func (h *StreamTradeHandler) finestTimeframe() string {
	best, bestSecs := "", int64(0)
	for _, tf := range h.tfReg.All() {
		secs := candle.ParseTimeframeToSeconds(tf)
		if secs <= 0 {
			continue
		}
		if bestSecs == 0 || secs < bestSecs {
			best, bestSecs = tf, secs
		}
	}
	return best
}

// reportMissing publishes an incomplete tape: a counter always, a log line at most once per
// duplicateWarnInterval per stream, since this fires fleet-wide at once.
func (h *StreamTradeHandler) reportMissing(exchange, marketType, compositeKey string, n int64) {
	if n <= 0 {
		return
	}
	h.metrics.addMissing(exchange, marketType, float64(n))

	now := time.Now()
	h.mu.Lock()
	if now.Sub(h.lastMissingWarnAt[compositeKey]) < h.duplicateWarnInterval {
		h.suppressedMissingWarn[compositeKey]++
		h.mu.Unlock()
		return
	}
	suppressed := h.suppressedMissingWarn[compositeKey]
	h.suppressedMissingWarn[compositeKey] = 0
	h.lastMissingWarnAt[compositeKey] = now
	h.mu.Unlock()

	logger.OrDiscard(h.log).Warn("incomplete tape: aggregate trades never arrived; the affected bucket is under-counted and marked for an authoritative re-read at close",
		"key", compositeKey,
		"missing_trades", n,
		"suppressed", suppressed)
}

// HandleTradesWithContext processes trades with the caller's context and explicit
// exchange/marketType. This is the preferred method for multi-exchange support.
func (h *StreamTradeHandler) HandleTradesWithContext(ctx context.Context, exchange, marketType, symbol string, trades any) error {
	if trades == nil {
		return nil
	}

	// Detect the trade type
	switch t := trades.(type) {
	case []trade.Trade:
		// Native client trades (BingX, Binance)
		return h.handleDomainTradesWithContext(ctx, exchange, marketType, symbol, t)
	default:
		// Map-shaped trades (fallback)
		return h.handleMapTradesWithContext(ctx, exchange, marketType, symbol, trades)
	}
}

// HandleTrades is backward compatible and uses the default exchange/marketType.
// Accepts: []trade.Trade or []map[string]interface{}.
func (h *StreamTradeHandler) HandleTrades(symbol string, trades any) error {
	return h.HandleTradesWithContext(context.Background(), h.defaultExchange, h.defaultMarketType, symbol, trades)
}

// handleDomainTradesWithContext processes native-client trades with explicit context.
func (h *StreamTradeHandler) handleDomainTradesWithContext(ctx context.Context, exchange, marketType, symbol string, tradeList []trade.Trade) error {
	if len(tradeList) == 0 {
		return nil
	}

	// Use composite key for multi-exchange deduplication
	compositeKey := fmt.Sprintf("%s:%s:%s", exchange, marketType, symbol)
	logger.OrDiscard(h.log).Debug("trade handler: batch received from native client",
		"key", compositeKey, "trades", len(tradeList))

	// The admission decision is made under the lock in one pass, so the window cannot be read
	// stale; only the decision is serialized.
	h.mu.Lock()
	lastTS := h.lastTradeTimestamp[compositeKey]
	window := h.seen[compositeKey]
	if window == nil {
		// Whether ids are a dense sequence is a property of the protocol, not of the batch.
		window = &seenWindow{contiguous: sequenceIsContiguous(exchange)}
		h.seen[compositeKey] = window
	}

	candleTrades := make([]candle.Trade, 0, len(tradeList))
	var newLastTS int64
	// The quantity actually admitted, summed at the three admission points; its engine partner
	// says how much reached a candle. See admittedVolume.
	var admitted float64

	for _, trade := range tradeList {
		// Convert trade.Trade to candle.Trade
		normalized := candle.Trade{
			ID:        trade.ID,
			Timestamp: trade.Timestamp,
			Price:     trade.Price,
			Amount:    trade.Amount,
			Side:      trade.Side,
		}

		// Skip trades far older than the last seen timestamp, counted: this and the id check are
		// the only places a trade can vanish.
		if normalized.Timestamp < lastTS-60000 {
			h.metrics.incDiscard(exchange, marketType, "stale_timestamp")
			continue
		}

		// No trade ID at all (kept as a safety net)
		if normalized.ID == "" {
			candleTrades = append(candleTrades, normalized)
			admitted += normalized.Amount
			if normalized.Timestamp > newLastTS {
				newLastTS = normalized.Timestamp
			}
			continue
		}

		// A non-numeric id is passed through: dropping what we cannot deduplicate is worse than
		// the duplicate risk.
		tradeIDInt, err := strconv.ParseInt(normalized.ID, 10, 64)
		if err != nil {
			candleTrades = append(candleTrades, normalized)
			admitted += normalized.Amount
			if normalized.Timestamp > newLastTS {
				newLastTS = normalized.Timestamp
			}
			continue
		}

		// Read the reason BEFORE admit, because admit mutates the window it is asked about.
		stale := window.beyondWindow(tradeIDInt)
		if !window.admit(tradeIDInt) {
			// Two rejections, two names: a duplicate costs nothing, while an id past the window
			// is a trade dropped from the candle and must be visible.
			reason := "duplicate_id"
			if stale {
				reason = "beyond_window"
			}
			h.metrics.incDiscard(exchange, marketType, reason)
			continue
		}

		candleTrades = append(candleTrades, normalized)
		admitted += normalized.Amount
		if normalized.Timestamp > newLastTS {
			newLastTS = normalized.Timestamp
		}
	}

	// Update last trade ID and timestamp
	if newLastTS > 0 {
		h.lastTradeTimestamp[compositeKey] = newLastTS
	}
	// One verdict per batch, under the same lock that made the admission decisions.
	missing := window.confirmMissing()

	// A boundary crossing is the last instant the mark can still reach the bucket. confirmMissing
	// withholds judgement on the newest ids, so a bucket damaged in its last trades would close
	// declaring itself intact. This batch rolls the bucket, so the tail is judged here; the
	// reorder margin is dropped only for this call.
	var tailMissing int64
	if h.crossedBucketBoundary(lastTS, newLastTS) {
		tailMissing = window.confirmTail()
		missing += tailMissing
	}

	gap := tradeGap{loTS: lastTS, hiTS: newLastTS, n: missing}
	h.mu.Unlock()

	// Only where the tape still writes a candle — see admittedVolume. Counting it for a
	// venue-owned scope makes its partner read as a total loss forever.
	if admitted > 0 && h.tapeStillWritesCandles(exchange, marketType) {
		h.metrics.addAdmitted(exchange, marketType, admitted)
	}
	h.reportMissing(exchange, marketType, compositeKey, gap.n)
	// Attributed separately so the boundary verdict is FALSIFIABLE. These are the losses that
	// close a bucket silently when the verdict arrives after the roll, when HasGap can no longer
	// be raised on the candle that carried them.
	if tailMissing > 0 {
		h.metrics.addMissingTail(exchange, marketType, float64(tailMissing))
	}

	if len(candleTrades) == 0 {
		return nil
	}

	return h.processCandleTrades(ctx, exchange, marketType, symbol, candleTrades, gap)
}

// handleMapTradesWithContext processes map-shaped trades with explicit exchange/marketType
// context.
func (h *StreamTradeHandler) handleMapTradesWithContext(ctx context.Context, exchange, marketType, symbol string, trades any) error {
	tradeList, err := normalizeTrades(trades)
	if err != nil {
		logger.OrDiscard(h.log).Error("trade handler: normalize trades failed",
			"exchange", exchange, "market_type", marketType, "symbol", symbol, "error", err)
		return err
	}
	if len(tradeList) == 0 {
		return nil
	}

	compositeKey := fmt.Sprintf("%s:%s:%s", exchange, marketType, symbol)
	logger.OrDiscard(h.log).Debug("trade handler: batch received as maps",
		"key", compositeKey, "trades", len(tradeList))

	// The admission decision is made under the lock in one pass, so the window cannot be read
	// stale; only the decision is serialized.
	h.mu.Lock()
	lastTS := h.lastTradeTimestamp[compositeKey]
	window := h.seen[compositeKey]
	if window == nil {
		// Whether ids are a dense sequence is a property of the protocol, not of the batch.
		window = &seenWindow{contiguous: sequenceIsContiguous(exchange)}
		h.seen[compositeKey] = window
	}

	candleTrades := make([]candle.Trade, 0, len(tradeList))
	var newLastTS int64
	// The quantity actually taken in, summed at the ONLY three places a trade can be admitted.
	// Its partner in the engine says how much of it reached a candle — see admittedVolume.
	var admitted float64

	for _, trade := range tradeList {
		normalized, ok := toCandleTrade(trade)
		if !ok {
			// Counted, not skipped in silence: an unmetered drop would make a missing trade
			// indistinguishable from one that never existed.
			h.metrics.incDiscard(exchange, marketType, "unparseable")
			continue
		}

		if normalized.Timestamp < lastTS-60000 {
			h.metrics.incDiscard(exchange, marketType, "stale_timestamp")
			continue
		}

		if normalized.ID == "" {
			candleTrades = append(candleTrades, normalized)
			admitted += normalized.Amount
			if normalized.Timestamp > newLastTS {
				newLastTS = normalized.Timestamp
			}
			continue
		}

		tradeIDInt, err := strconv.ParseInt(normalized.ID, 10, 64)
		if err != nil {
			candleTrades = append(candleTrades, normalized)
			admitted += normalized.Amount
			if normalized.Timestamp > newLastTS {
				newLastTS = normalized.Timestamp
			}
			continue
		}

		if !window.admit(tradeIDInt) {
			h.metrics.incDiscard(exchange, marketType, "duplicate_id")
			continue
		}

		candleTrades = append(candleTrades, normalized)
		if normalized.Timestamp > newLastTS {
			newLastTS = normalized.Timestamp
		}
	}

	if newLastTS > 0 {
		h.lastTradeTimestamp[compositeKey] = newLastTS
	}
	gap := tradeGap{loTS: lastTS, hiTS: newLastTS, n: window.confirmMissing()}
	h.mu.Unlock()

	// Only where the tape still writes a candle — see admittedVolume. Counting it for a
	// venue-owned scope makes its partner read as a total loss forever.
	if admitted > 0 && h.tapeStillWritesCandles(exchange, marketType) {
		h.metrics.addAdmitted(exchange, marketType, admitted)
	}
	h.reportMissing(exchange, marketType, compositeKey, gap.n)

	if len(candleTrades) == 0 {
		return nil
	}

	var totalVol float64
	for _, t := range candleTrades {
		totalVol += t.Amount
	}
	logger.OrDiscard(h.log).Info("trade handler: new trades admitted",
		"key", compositeKey, "trades", len(candleTrades), "total_volume", totalVol)

	return h.processCandleTrades(ctx, exchange, marketType, symbol, candleTrades, gap)
}

// processCandleTrades generates candles and MARKS the buckets whose tape arrived incomplete.
func (h *StreamTradeHandler) processCandleTrades(ctx context.Context, exchange, marketType, symbol string, candleTrades []candle.Trade, gap tradeGap) error {
	// No trades, nothing to do: without this guard the last-trade write indexes an empty slice.
	if len(candleTrades) == 0 {
		return nil
	}

	compositeKey := fmt.Sprintf("%s:%s:%s", exchange, marketType, symbol)

	// DEBUG: check for duplicate trades inside the batch (same timestamp+price+amount)
	if len(candleTrades) > 1 {
		seenInBatch := h.seenPool.Get().(map[string]struct{})
		clear(seenInBatch)
		dupesInBatch := 0
		for _, t := range candleTrades {
			var key string
			if t.ID != "" {
				key = t.ID
			} else {
				key = fmt.Sprintf("%d_%.8f_%.8f", t.Timestamp, t.Price, t.Amount)
			}
			if _, seen := seenInBatch[key]; seen {
				dupesInBatch++
				logger.OrDiscard(h.log).Debug("trade handler: duplicate trade inside the batch",
					"key", compositeKey,
					"timestamp", t.Timestamp,
					"price", t.Price,
					"amount", t.Amount)
			} else {
				seenInBatch[key] = struct{}{}
			}
		}
		h.seenPool.Put(seenInBatch)
	}

	// Where the venue owns the candle, this path does not write one. Admission, deduplication
	// and the delivery signal still run, and SetLastTrade still feeds /api/trades; only the
	// second write is skipped. One owner per quantity removes the fight instead of refereeing it.

	// 2. BuildOHLCVC for ALL timeframes and accumulate updates.
	// Pre-allocation: estimate = timeframes × buckets (~len(trades) / 2 for 1m)
	capEstimate := len(h.tfReg.All()) * (len(candleTrades)/2 + 1)
	if capEstimate < len(h.tfReg.All()) {
		capEstimate = len(h.tfReg.All())
	}
	updates := make([]candle.CandleUpdate, 0, capEstimate)

	// Buckets must reach the engine oldest-first, and map iteration is randomized: a batch
	// straddling a boundary could open the newer bucket first, closing the older one short and
	// then repairing it as a late update. The scratch slice is reused across timeframes.
	bucketTimestamps := make([]int64, 0, len(candleTrades)/2+1)

	for _, tf := range h.tfReg.All() {
		// The venue owns THIS timeframe: skip it, and only it. Every timeframe the venue does not
		// publish keeps its live producer, which is the tape.
		if h.candlesAreVenueOwned(exchange, marketType, tf) {
			continue
		}
		// BuildOHLCVC over the trades. The pooled map is consumed into updates before it is
		// reclaimed, so no pooled reference is retained.
		h.ohlcvc.build(candleTrades, tf, func(ohlcvc map[int64]candle.BucketedCandle) {
			bucketTimestamps = bucketTimestamps[:0]
			for timestamp := range ohlcvc {
				bucketTimestamps = append(bucketTimestamps, timestamp)
			}
			slices.Sort(bucketTimestamps)

			for _, timestamp := range bucketTimestamps {
				bucketCandle := ohlcvc[timestamp]
				bar := candle.Candle{
					Timestamp:        timestamp,
					Open:             bucketCandle.Open,
					High:             bucketCandle.High,
					Low:              bucketCandle.Low,
					Close:            bucketCandle.Close,
					Volume:           bucketCandle.Volume,
					TakerBuyVolume:   bucketCandle.TakerBuyVolume,
					TakerSellVolume:  bucketCandle.TakerSellVolume,
					SideUnknownCount: bucketCandle.SideUnknownCount,
					// How far this measurement reaches: the open-bucket merge compares it against
					// a total's FetchedAt to decide whether the total already contains this delta.
					LastTradeAt: bucketCandle.LastTradeAt,
					// HasGap is the only truthful mark this path can raise for data it never
					// received; every engine branch already OR-merges it and carries it to the
					// close callback, which is the moment a correction is cheap and authoritative.
					HasGap: gap.covers(timestamp, tf),
				}

				updates = append(updates, candle.CandleUpdate{
					Exchange:   exchange,   // Use the exchange from the context
					MarketType: marketType, // Use the marketType from the context
					Symbol:     symbol,
					Timeframe:  tf,
					Candle:     bar,
					Source:     candle.CandleMergeSourceWSDelta,
				})
			}
		})
	}

	// The batch is conditional; the last trade is not. Once the venue owns every timeframe,
	// updates is empty and an early return would freeze /api/trades. A critical persistence
	// failure still returns before the last-trade write.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if len(updates) > 0 {
		if err := h.store.AddStreamingCandlesBatch(ctx, updates); err != nil {
			event := h.persistence.Record(streaming.PersistenceOperationBatchWrite, compositeKey, err)
			atomic.AddInt64(&h.persistFailures, 1)
			logger.OrDiscard(h.log).Error("trade handler: critical persistence failure; stream loop continues",
				"key", compositeKey, "kind", event.Kind, "class", event.Class, "error", err)
			return streaming.NewCriticalPersistenceError(event, err)
		}
	}

	lastTrade := candleTrades[len(candleTrades)-1]
	if err := h.store.SetLastTrade(ctx, exchange, marketType, symbol, lastTrade); err != nil {
		event := h.persistence.Record(streaming.PersistenceOperationLastTradeWrite, compositeKey, err)
		atomic.AddInt64(&h.lastTradeFailures, 1)
		logger.OrDiscard(h.log).Warn("trade handler: non-critical persistence failure",
			"key", compositeKey, "kind", event.Kind, "class", event.Class, "error", err)
	}

	return nil
}

// Shutdown does nothing: this handler owns no workers.
func (h *StreamTradeHandler) Shutdown() {
	logger.OrDiscard(h.log).Info("trade handler shutdown")
}

// normalizeTrades converts the generic trade batch into maps. The streaming path is handled
// before this function; the fallback exists for clients that deliver the raw shape.
func normalizeTrades(trades any) ([]map[string]interface{}, error) {
	switch t := trades.(type) {
	case []map[string]interface{}:
		return t, nil

	case []interface{}:
		result := make([]map[string]interface{}, 0, len(t))
		for _, item := range t {
			if m, ok := item.(map[string]interface{}); ok {
				result = append(result, m)
			}
		}
		return result, nil

	default:
		return nil, fmt.Errorf("unexpected trades type: %T", trades)
	}
}

// toCandleTrade converts a generic map into candle.Trade.
func toCandleTrade(raw map[string]interface{}) (candle.Trade, bool) {
	timestamp, ok := toInt64(raw["timestamp"])
	if !ok || timestamp == 0 {
		return candle.Trade{}, false
	}

	price, ok := toFloat64(raw["price"])
	if !ok || price <= 0 {
		return candle.Trade{}, false
	}

	amount, ok := toFloat64(raw["amount"])
	if !ok || amount <= 0 {
		return candle.Trade{}, false
	}

	side, _ := raw["side"].(string)
	if side == "" {
		side = string(trade.Unknown)
	}

	id, _ := raw["id"].(string)

	return candle.Trade{
		ID:        id,
		Timestamp: timestamp,
		Price:     price,
		Amount:    amount,
		Side:      trade.ParseSide(side),
	}, true
}

func toInt64(val interface{}) (int64, bool) {
	switch v := val.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case float64:
		return int64(v), true
	case float32:
		return int64(v), true
	case *int64:
		if v != nil {
			return *v, true
		}
		return 0, false
	default:
		return 0, false
	}
}

func toFloat64(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case *float64:
		if v != nil {
			return *v, true
		}
		return 0, false
	default:
		return 0, false
	}
}
