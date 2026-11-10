// Package recovery fills historical candle gaps and seeds new symbols, isolated from the
// streaming internals.
package recovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/market"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// Gap-recovery defaults: workers, queue bounds, fetch timeout and retry policy.
const (
	DefaultWorkers      = 10
	DefaultQueueSize    = 8224
	DefaultPendingLimit = 8224
	DefaultFetchTimeout = 30 * time.Second
	DefaultMinDelay     = 2 * time.Second
	// TargetedSettleDelay is the wait before fetching a bucket that has just closed, so the
	// exchange's kline has settled before we adopt it as authoritative.
	TargetedSettleDelay  = 15 * time.Second
	DefaultMaxDelay      = 30 * time.Second
	DefaultDelayFraction = 0.8
	DefaultPromoteBatch  = 128
	RetryInitialDelay    = 5 * time.Second
	RetryMaxDelay        = 5 * time.Minute
	RetryBackoffFactor   = 2.0
	RetryMaxAttempts     = 12
	RetryAlertThreshold  = 10 * time.Minute
)

// reconnectGapMaterialityFraction gates per-timeframe recovery: the gap must span at least
// this fraction of a bucket. A 33s reconnect gap is immaterial to a 1d/1w candle and would turn
// into a retry storm on every fleet reconnect.
const reconnectGapMaterialityFraction = 0.10

// Manager orchestrates gap recovery using extracted subsystems.
type Manager struct {
	queue           *CoalescingQueue
	clientPool      ClientPool
	cb              CircuitBreaker
	retry           RetryScheduler
	writer          StorageWriter
	validator       Validator
	candlesVal      CandlesValidator
	filler          GapFiller
	exchangeFillers map[string]GapFiller // per-exchange strategy
	targetedMu      sync.Mutex
	targetedSeen    map[string]int64 // key -> last bucket enqueued, one per symbol/timeframe
	provider        LatestTimestampProvider

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	enqueued  atomic.Uint64
	processed atomic.Uint64
	dropped   atomic.Uint64
	errors    atomic.Uint64
	backlogHW atomic.Uint64

	log *slog.Logger

	maxWorkers int
	tfReg      *candle.TimeframeRegistry

	// No-progress breaker and aggregate REST budget; both are always active.
	breaker *noProgressBreaker
	budget  *restBudget

	// metrics is the injected observability port. Zero value = safe no-op.
	metrics Metrics
}

// SetMetrics injects the observability port. The composition root calls it before Start.
func (m *Manager) SetMetrics(metrics Metrics) { m.metrics = metrics }

// NewManager creates a Manager with all subsystems wired.
func NewManager(store StorageWriter, maxWorkers int, tfReg *candle.TimeframeRegistry, logs ...*slog.Logger) *Manager {
	if maxWorkers <= 0 {
		maxWorkers = DefaultWorkers
	}
	log := logger.OrDiscard(logs...)
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		queue:           NewCoalescingQueue(DefaultQueueSize, DefaultPendingLimit),
		clientPool:      NewLazyClientPool(nil, log),
		cb:              NewSimpleCircuitBreaker(log),
		retry:           NewMemoryRetryScheduler(log),
		writer:          store,
		validator:       NewRedisValidator(store, log),
		candlesVal:      DefaultCandlesValidator{},
		filler:          NewNoOpFiller(log), // safe fallback until SetFiller/SetFillerForExchange
		exchangeFillers: make(map[string]GapFiller),
		targetedSeen:    make(map[string]int64),
		ctx:             ctx,
		cancel:          cancel,
		maxWorkers:      maxWorkers,
		tfReg:           tfReg,
		breaker:         newNoProgressBreaker(DefaultNoProgressK, DefaultNoProgressBackoff, DefaultFreshnessGraceMs),
		budget:          newRESTBudget(DefaultRESTBudgetPerSec, DefaultRESTBudgetBurst),
		log:             log,
	}
	// Wire the retry scheduler back into the worker pipeline so deferred jobs are retried
	// rather than logged and dropped.
	m.retry.SetReenqueue(m.reenqueueRetryJob)
	return m
}

// reenqueueRetryJob converts a due retry entry back into a RecoveryJob, preserving the attempt
// count so RetryMaxAttempts is honored. The whole-symbol job re-reconciles all timeframes.
func (m *Manager) reenqueueRetryJob(rj BackfillRetryJob) {
	job := RecoveryJob{
		ExchangeID:     rj.ExchangeID,
		MarketType:     rj.MarketType,
		Symbol:         rj.Symbol,
		DisconnectTime: time.Now(),
		ReconnectTime:  time.Now(),
		RetryAttempt:   rj.Attempt,
		window:         rj.window,
	}
	if job.window.startMs > 0 {
		job.DisconnectTime = time.UnixMilli(job.window.startMs)
		job.ReconnectTime = job.DisconnectTime.Add(job.window.gapDuration)
	}
	// A targeted repair stays targeted across the retry: the generic rebuild sets both times
	// to now, making the materiality gate skip every timeframe and the attempts no-ops.
	if rj.TargetedBucket > 0 && rj.TargetedTimeframe != "" {
		job.TargetedTimeframe = rj.TargetedTimeframe
		job.TargetedBucket = rj.TargetedBucket
		at := time.UnixMilli(rj.TargetedBucket)
		job.DisconnectTime, job.ReconnectTime = at, at
		if m.queue.EnqueuePriority(job) == enqueueResultQueued {
			m.enqueued.Add(1)
		} else {
			m.dropped.Add(1)
		}
		return
	}
	switch m.queue.Enqueue(job) {
	case enqueueResultQueued:
		m.enqueued.Add(1)
	case enqueueResultCoalesced:
		// folded into an existing pending entry for this symbol — fine
	default:
		m.dropped.Add(1)
	}
	m.queue.Promote(DefaultPromoteBatch)
}

// SetTimestampProvider configures the optional provider per-symbol gap calculation.
func (m *Manager) SetTimestampProvider(p LatestTimestampProvider) { m.provider = p }

// SetFiller configures the gap filling strategy (Basic / Extended / Fallback).
func (m *Manager) SetFiller(f GapFiller) { m.filler = f }

// SetClientPool replaces the lazy client pool. The composition root uses it to
// inject the outbound adapter factory; recovery itself never imports one.
func (m *Manager) SetClientPool(pool ClientPool) { m.clientPool = pool }

// SetFillerForExchange registers a per-exchange gap filling strategy.
// When a job's ExchangeID matches, that filler is used; otherwise the default SetFiller is used.
func (m *Manager) SetFillerForExchange(exchangeID string, f GapFiller) {
	if m.exchangeFillers == nil {
		m.exchangeFillers = make(map[string]GapFiller)
	}
	m.exchangeFillers[exchangeID] = f
}

// Start launches the worker pool. ctx is the process lifetime context; Stop cancels the
// derived run context, so both the caller's cancellation and Stop terminate the workers.
func (m *Manager) Start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	oldCancel := m.cancel
	m.ctx, m.cancel = runCtx, cancel
	oldCancel()

	for i := 0; i < m.maxWorkers; i++ {
		m.wg.Add(1)
		go m.worker(runCtx, i)
	}
	m.retry.Start(runCtx)
	logger.OrDiscard(m.log).Info("gap recovery manager started", "workers", m.maxWorkers)
}

// Stop cancels workers and closes shared resources.
func (m *Manager) Stop() {
	m.cancel()
	m.wg.Wait()
	m.retry.Stop()
	m.clientPool.CloseAll()
	logger.OrDiscard(m.log).Info("gap recovery manager stopped")
}

// Stats returns current statistics.
func (m *Manager) Stats() GapRecoveryStats {
	queued := m.queue.Len()
	pending := m.queue.PendingLen()
	return GapRecoveryStats{
		Queued:               queued,
		Pending:              pending,
		Backlog:              queued + pending,
		BacklogHighWatermark: saturatingInt(m.backlogHW.Load()),
		Enqueued:             m.enqueued.Load(),
		Coalesced:            m.queue.coalesced.Load(),
		Retried:              m.queue.retried.Load(),
		Failed:               m.queue.failed.Load(),
		Processed:            m.processed.Load(),
		Dropped:              m.dropped.Load(),
		Errors:               m.errors.Load(),
		Workers:              m.maxWorkers,
	}
}

// EnqueueTargetedBucket asks for one closed bucket to be re-read from the exchange. A targeted
// repair names a bucket and bypasses the reconnect materiality gate; conflating it with a bulk
// job made the boot-bucket settle fetch something else and still report success.
func (m *Manager) EnqueueTargetedBucket(exchangeID, marketType, symbol, timeframe string, bucketOpenTime int64) {
	if bucketOpenTime <= 0 {
		return
	}
	// ONE SETTLE PER SYMBOL, TIMEFRAME AND BUCKET. A restart marks the scope's open
	// buckets incomplete; the close of each then asks for a targeted repair. Repeating
	// the same bucket across restarts only burns REST weight: the read is idempotent.
	seenKey := exchangeID + ":" + marketType + "|" + market.NormalizeSymbol(symbol) + "|" + timeframe
	m.targetedMu.Lock()
	if m.targetedSeen == nil {
		m.targetedSeen = make(map[string]int64)
	}
	if last, ok := m.targetedSeen[seenKey]; ok && last == bucketOpenTime {
		m.targetedMu.Unlock()
		m.dropped.Add(1)
		return
	}
	if len(m.targetedSeen) >= 8192 {
		clear(m.targetedSeen)
	}
	m.targetedSeen[seenKey] = bucketOpenTime
	m.targetedMu.Unlock()
	at := time.UnixMilli(bucketOpenTime)
	job := RecoveryJob{
		ExchangeID:     exchangeID,
		MarketType:     marketType,
		Symbol:         symbol,
		DisconnectTime: at,
		ReconnectTime:  at,
		// Not DefaultMinDelay: a just-closed kline settles for up to ~5s, and fetching earlier
		// installed a value shorter than the live reconstruction. Must stay at or above
		// candle.KlineSettleGraceMs, or the authority guard rejects every record.
		Delay:             TargetedSettleDelay,
		TargetedTimeframe: timeframe,
		TargetedBucket:    bucketOpenTime,
	}
	res := m.queue.EnqueuePriority(job)
	if res == enqueueResultQueued {
		m.enqueued.Add(1)
	} else {
		m.dropped.Add(1)
	}
	// Enqueue and execution are logged separately: only that distinguishes "never asked for"
	// from "asked for and never ran". Debug because a boot produces ~500 of them.
	logger.OrDiscard(m.log).Debug("gap recovery targeted enqueue",
		"exchange", exchangeID,
		"market_type", marketType,
		"timeframe", timeframe,
		"bucket", bucketOpenTime,
		"result", res)
	m.queue.Promote(DefaultPromoteBatch)
	m.updateBacklogHW()
}

// EnqueueBatch admits a recovery window for all supplied symbols of an exchange.
func (m *Manager) EnqueueBatch(exchangeID, marketType string, symbols []string, disconnectTime, reconnectTime time.Time) {
	if len(symbols) == 0 {
		return
	}
	gapDuration := reconnectTime.Sub(disconnectTime)
	delay := time.Duration(float64(gapDuration) * DefaultDelayFraction)
	if delay < DefaultMinDelay {
		delay = DefaultMinDelay
	}
	if delay > DefaultMaxDelay {
		delay = DefaultMaxDelay
	}

	logger.OrDiscard(m.log).Warn("gap recovery enqueuing batch",
		"exchange", exchangeID,
		"market_type", marketType,
		"symbols", len(symbols),
		"gap", gapDuration.Round(time.Second),
		"delay", delay)

	enqueued, coalesced, failed := 0, 0, 0
	for _, sym := range symbols {
		job := RecoveryJob{
			ExchangeID:     exchangeID,
			MarketType:     marketType,
			Symbol:         sym,
			DisconnectTime: disconnectTime,
			ReconnectTime:  reconnectTime,
			Delay:          delay,
			window:         recoveryWindow{startMs: disconnectTime.UnixMilli(), gapDuration: gapDuration},
		}
		switch m.queue.Enqueue(job) {
		case enqueueResultQueued:
			enqueued++
			m.enqueued.Add(1)
		case enqueueResultCoalesced:
			coalesced++
		default:
			failed++
			m.dropped.Add(1)
		}
	}
	m.queue.Promote(DefaultPromoteBatch)
	m.updateBacklogHW()
	logger.OrDiscard(m.log).Warn("gap recovery batch enqueued",
		"jobs", enqueued,
		"exchange", exchangeID,
		"market_type", marketType,
		"coalesced", coalesced,
		"failed", failed)
}

// ForceBackfill enqueues manual backfill jobs.
func (m *Manager) ForceBackfill(req BackfillRequest) BackfillResponse {
	if len(req.Symbols) == 0 {
		return BackfillResponse{Message: "no symbols provided"}
	}
	timeframes := req.Timeframes
	if len(timeframes) == 0 {
		timeframes = m.tfReg.All()
	}
	disconnectTime := time.UnixMilli(req.SinceMs)
	reconnectTime := time.Now()
	if req.UntilMs > 0 {
		reconnectTime = time.UnixMilli(req.UntilMs)
	}

	enqueued, failed := 0, 0
	for _, sym := range req.Symbols {
		job := RecoveryJob{
			ExchangeID:     req.ExchangeID,
			MarketType:     req.MarketType,
			Symbol:         sym,
			DisconnectTime: disconnectTime,
			ReconnectTime:  reconnectTime,
			Delay:          0,
			window:         recoveryWindow{startMs: req.SinceMs, gapDuration: reconnectTime.Sub(disconnectTime)},
		}
		switch m.queue.Enqueue(job) {
		case enqueueResultQueued:
			enqueued++
			m.enqueued.Add(1)
		case enqueueResultCoalesced:
			enqueued++
		default:
			failed++
			m.dropped.Add(1)
		}
	}
	m.queue.Promote(DefaultPromoteBatch)
	m.updateBacklogHW()
	return BackfillResponse{
		Enqueued:   enqueued,
		Failed:     failed,
		Symbols:    req.Symbols,
		Timeframes: timeframes,
		Message:    fmt.Sprintf("force backfill enqueued %d jobs", enqueued),
	}
}

// worker processes recovery jobs from the queue.
func (m *Manager) worker(ctx context.Context, id int) {
	defer m.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		// Priority is checked on its own first: a plain select picks uniformly among ready
		// cases, dissolving the priority into a coin flip.
		case job, ok := <-m.queue.ReadPriority():
			if !ok {
				return
			}
			m.safeProcess(ctx, id, job)
		default:
			select {
			case <-ctx.Done():
				return
			case job, ok := <-m.queue.ReadPriority():
				if !ok {
					return
				}
				m.safeProcess(ctx, id, job)
			case job, ok := <-m.queue.Read():
				if !ok {
					return
				}
				m.safeProcess(ctx, id, job)
				m.queue.Promote(DefaultPromoteBatch)
			}
		}
	}
}

// safeProcess wraps processing with panic recovery and circuit breaker.
func (m *Manager) safeProcess(ctx context.Context, workerID int, job RecoveryJob) {
	defer func() {
		if r := recover(); r != nil {
			m.errors.Add(1)
			m.cb.RecordFailure(job.ExchangeID + ":" + job.MarketType)
			logger.OrDiscard(m.log).Error("gap recovery worker panic recovered",
				"worker", workerID,
				"exchange", job.ExchangeID,
				"market_type", job.MarketType,
				"symbol", job.Symbol,
				"panic", r)
		}
	}()
	m.process(ctx, workerID, job)
}

func (m *Manager) scheduleRetry(job RecoveryJob, tf string, sinceMs int64, fetchLimit int) {
	m.retry.Schedule(BackfillRetryJob{
		ExchangeID: job.ExchangeID, MarketType: job.MarketType, Symbol: job.Symbol,
		Timeframe: tf, SinceMs: sinceMs, FetchLimit: fetchLimit,
		Attempt: job.RetryAttempt, window: job.window,
		TargetedTimeframe: job.TargetedTimeframe, TargetedBucket: job.TargetedBucket,
	})
}

func (m *Manager) process(ctx context.Context, workerID int, job RecoveryJob) {
	if job.Delay > 0 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(job.Delay):
		}
	}

	client, err := m.clientPool.Get(job.ExchangeID, job.MarketType)
	if err != nil {
		m.errors.Add(1)
		logger.OrDiscard(m.log).Error("gap recovery worker: failed to get REST client",
			"worker", workerID, "exchange", job.ExchangeID, "market_type", job.MarketType, "error", err)
		return
	}

	ctx, cancel := context.WithTimeout(ctx, DefaultFetchTimeout)
	defer cancel()

	internalSymbol := market.NormalizeSymbol(job.Symbol)
	mt, err := market.FromConfig(job.MarketType)
	if err != nil {
		m.errors.Add(1)
		logger.OrDiscard(m.log).Error("gap recovery worker: invalid market type",
			"worker", workerID, "exchange", job.ExchangeID, "market_type", job.MarketType, "error", err)
		return
	}

	if m.cb.IsOpen(job.ExchangeID + ":" + job.MarketType) {
		logger.OrDiscard(m.log).Warn("gap recovery worker: circuit open; deferring job",
			"worker", workerID, "exchange", job.ExchangeID, "market_type", job.MarketType)
		m.scheduleRetry(job, "", 0, 0)
		return
	}

	internalMarketType := mt.String()

	// Skip symbols the no-progress breaker has suspended: they ran K cycles without
	// advancing freshness, so retrying now only burns REST weight.
	targetedJob := job.TargetedBucket > 0 && job.TargetedTimeframe != ""
	progressKey := job.ExchangeID + ":" + job.MarketType + "|" + internalSymbol
	if !targetedJob && m.breaker.isSuspended(progressKey) {
		logger.OrDiscard(m.log).Warn("gap recovery worker: symbol suspended by the no-progress breaker; skipping cycle",
			"worker", workerID, "exchange", job.ExchangeID, "market_type", internalMarketType, "symbol", job.Symbol)
		m.processed.Add(1)
		return
	}

	successCount, errorCount, backfilled := 0, 0, 0
	emptyFills := 0
	completedTargeted := false
	// bestFreshness is the newest persisted timestamp this cycle achieves; the breaker
	// compares it against the stored watermark.
	var bestFreshness int64

	for _, tf := range m.tfReg.All() {
		if ctx.Err() != nil {
			break
		}
		if targetedJob && tf != job.TargetedTimeframe {
			continue
		}
		// Canonical bucket boundary, Monday-aligned for 1w. Plain integer division would be
		// Thursday-epoch-aligned and misclassify the open 1w candle.
		nowMs := time.Now().UnixMilli()
		currentBucket := candle.BucketTimestamp(nowMs, tf)

		// A targeted interior repair names one older bucket; bulk recovery derives from the live
		// head. The intent travels in an explicit field, not in a coincidence of timestamps.
		targeted := targetedJob

		// The persisted head is the only freshness input: a buffer read would mask the
		// pre-persistence loss this recovery exists to close.
		head := int64(0)
		if !targeted && m.provider != nil {
			if latestTs, ok := m.provider.GetLatestTimestamp(ctx, job.ExchangeID, internalMarketType, internalSymbol, tf); ok && latestTs > 0 {
				head = latestTs
				if latestTs > bestFreshness {
					bestFreshness = latestTs // baseline: freshness already on record
				}
			}
		}

		sinceMs, gapStart, fetchLimit := deriveWindow(job, tf, head, nowMs)
		if fetchLimit == 0 {
			continue
		}
		if targeted {
			// Counted after the fetch, not here: this branch measures that the window was
			// chosen, not that the repair executed.
			logger.OrDiscard(m.log).Debug("gap recovery targeted repair",
				"exchange", job.ExchangeID,
				"market_type", internalMarketType,
				"timeframe", tf,
				"bucket", job.TargetedBucket)
		} else {
			straddled := candle.BucketTimestamp(job.DisconnectTime.UnixMilli(), tf)
			crossedBoundary := candle.BucketTimestamp(job.DisconnectTime.UnixMilli(), tf) !=
				candle.BucketTimestamp(job.ReconnectTime.UnixMilli(), tf)
			if job.window.startMs == 0 && crossedBoundary && fetchLimit == 2 &&
				sinceMs == straddled && gapStart == straddled &&
				gapStart != candle.BucketTimestamp(head, tf) {
				logger.OrDiscard(m.log).Info("gap recovery worker: silence spanned the close of a bucket; repairing that bucket only",
					"worker", workerID,
					"exchange", job.ExchangeID,
					"symbol", job.Symbol,
					"timeframe", tf,
					"bucket", straddled)
			} else if fetchLimit > 2 {
				logger.OrDiscard(m.log).Info("gap recovery worker: per-symbol gap",
					"worker", workerID,
					"exchange", job.ExchangeID,
					"symbol", job.Symbol,
					"timeframe", tf,
					"gap", time.Duration(nowMs-sinceMs)*time.Millisecond,
					"fetch_limit", fetchLimit,
					"since", sinceMs)
			}
		}

		req := FillRequest{
			ExchangeID:    job.ExchangeID,
			MarketType:    job.MarketType,
			Symbol:        internalSymbol,
			Timeframe:     tf,
			SinceMs:       sinceMs,
			FetchLimit:    fetchLimit,
			GapStart:      gapStart,
			CurrentBucket: currentBucket,
		}
		if targeted {
			req.UntilMs = gapStart
		}

		// Select the filler per exchange (ExtendedKlines for Binance, Basic for others).
		activeFiller := m.filler
		if activeFiller == nil {
			activeFiller = NewNoOpFiller()
		}
		if f, ok := m.exchangeFillers[job.ExchangeID]; ok {
			activeFiller = f
		}

		// Cap the aggregate recovery rate so recovery cannot trip the venue's request budget;
		// when exhausted, the timeframe is deferred to retry.
		if !m.budget.allow() {
			m.recordRESTBudgetDeferred(job.ExchangeID, internalMarketType)
			logger.OrDiscard(m.log).Warn("gap recovery worker: REST budget exhausted; deferring",
				"worker", workerID,
				"exchange", job.ExchangeID,
				"market_type", internalMarketType,
				"symbol", job.Symbol,
				"timeframe", tf)
			m.scheduleRetry(job, tf, sinceMs, fetchLimit)
			continue
		}

		// Captured before the call: the exchange cannot have answered before we asked, so a live
		// delta carrying only older trades is already inside the total this response installs.
		// See candle.Candle.RequestedAt.
		requestedAt := time.Now().UnixMilli()
		result, err := activeFiller.Fill(ctx, client, req)
		if err == nil && len(result.Historical) == 0 && result.Current == nil {
			emptyFills++
		}
		if err == nil && targeted {
			// The fetch returned. This is the earliest point at which "the repair ran" is true.
			m.metrics.incTargetedRepair(job.ExchangeID, internalMarketType, tf)
		}
		if err != nil {
			errorCount++
			// Permanent per-symbol errors (e.g. invalid/delisted symbol) must not
			// trip the shared per-exchange circuit nor be retried — otherwise one
			// bad symbol disables gap recovery for the whole scope (C1).
			if errors.Is(err, ErrPermanentSymbol) {
				logger.OrDiscard(m.log).Warn("gap recovery worker: skipping permanent error",
					"worker", workerID,
					"filler", activeFiller.Name(),
					"exchange", job.ExchangeID,
					"market_type", internalMarketType,
					"symbol", job.Symbol,
					"timeframe", tf,
					"error", err)
				continue
			}
			m.cb.RecordFailure(job.ExchangeID + ":" + job.MarketType)
			logger.OrDiscard(m.log).Warn("gap recovery worker: filler failed",
				"worker", workerID,
				"filler", activeFiller.Name(),
				"exchange", job.ExchangeID,
				"market_type", internalMarketType,
				"symbol", job.Symbol,
				"timeframe", tf,
				"error", err)
			m.scheduleRetry(job, tf, sinceMs, fetchLimit)
			continue
		}

		seeded := false
		targetedWriteFailed := false
		var historicalCandles []candle.Candle
		fetchedHistorical := result.Historical
		if targeted {
			filtered := make([]candle.Candle, 0, len(fetchedHistorical))
			for _, c := range fetchedHistorical {
				if c.Timestamp == gapStart {
					filtered = append(filtered, c)
				}
			}
			fetchedHistorical = filtered
		}
		historicalCandles, rejected := prepareCandles(m.candlesVal, fetchedHistorical, job.Symbol, tf)
		for _, r := range rejected {
			m.metrics.incRejectedCandle(job.ExchangeID, internalMarketType, tf, r.Reason)
			logger.OrDiscard(m.log).Warn("gap recovery worker: invalid candle; skipping",
				"worker", workerID,
				"exchange", job.ExchangeID,
				"market_type", internalMarketType,
				"symbol", job.Symbol,
				"timeframe", tf,
				"timestamp", r.Timestamp,
				"reason", r.Reason)
		}

		// History first, then the open bucket: a reader arriving mid-repair sees the series grow
		// backwards into place instead of a floating head, and the cumulative chain stays
		// monotonic. Provenance is declared on both: the engine settles against an authoritative
		// record only when it can see that the record is authoritative, which is what makes
		// drift self-healing. fetchedAt is captured once and shared by every candle of the
		// response.
		fetchedAt := time.Now().UnixMilli()
		for i := range historicalCandles {
			// Stamped where the response is labeled, not where it is merged: an unstamped total
			// is refused authority and fails safe to the union.
			historicalCandles[i].Source = string(candle.CandleMergeSourceRESTTotal)
			historicalCandles[i].FetchedAt = fetchedAt
			historicalCandles[i].RequestedAt = requestedAt
		}
		if len(historicalCandles) > 0 {
			if err := m.writer.SeedCandlesBatch(ctx, job.ExchangeID, internalMarketType, internalSymbol, tf, historicalCandles); err != nil {
				errorCount++
				targetedWriteFailed = targeted
				logger.OrDiscard(m.log).Warn("gap recovery worker: backfill failed",
					"worker", workerID,
					"exchange", job.ExchangeID,
					"market_type", internalMarketType,
					"symbol", job.Symbol,
					"timeframe", tf,
					"candles", len(historicalCandles),
					"error", err)
			} else {
				backfilled += len(historicalCandles)
			}
		}

		if result.Current != nil && !targeted {
			current, currentRejected := prepareCandles(m.candlesVal, []candle.Candle{*result.Current}, job.Symbol, tf)
			for _, r := range currentRejected {
				m.metrics.incRejectedCandle(job.ExchangeID, internalMarketType, tf, r.Reason)
				logger.OrDiscard(m.log).Warn("gap recovery worker: invalid current candle; skipping",
					"worker", workerID,
					"exchange", job.ExchangeID,
					"market_type", internalMarketType,
					"symbol", job.Symbol,
					"timeframe", tf,
					"timestamp", r.Timestamp,
					"reason", r.Reason)
			}
			if len(current) == 1 {
				c := current[0]
				// Stamped like its siblings. On the open bucket both fields are load-bearing: without
				// Source the merge cannot tell a total from an increment, and without FetchedAt the
				// cutoff that prevents double-counting live deltas is inert. This is the reconnect
				// path, so the error would be fleet-wide.
				c.Source = string(candle.CandleMergeSourceRESTTotal)
				c.FetchedAt = fetchedAt
				c.RequestedAt = requestedAt
				if err := m.writer.SeedCurrentBucketCandle(ctx, job.ExchangeID, internalMarketType, internalSymbol, tf, c); err != nil {
					errorCount++
					logger.OrDiscard(m.log).Warn("gap recovery worker: store failed",
						"worker", workerID,
						"exchange", job.ExchangeID,
						"market_type", internalMarketType,
						"symbol", job.Symbol,
						"timeframe", tf,
						"timestamp", c.Timestamp,
						"error", err)
				} else {
					seeded = true
					// Seeding the open bucket advances persisted freshness to the
					// current bucket — the signal the no-progress breaker tracks.
					if c.Timestamp > bestFreshness {
						bestFreshness = c.Timestamp
					}
				}
			}
		}

		if targeted {
			// An interior repair has no obligation to close unrelated holes. Compare the values
			// the filler measured and require a successful write admission; neither claims the
			// cold WAL has applied the row.
			stored, queryErr := m.writer.GetCandlesByScoreRange(ctx, job.ExchangeID, internalMarketType, internalSymbol, tf, gapStart, gapStart)
			fetchedTarget := len(historicalCandles) > 0
			matchesTarget := false
			if fetchedTarget {
				incoming := historicalCandles[len(historicalCandles)-1]
				for _, c := range stored {
					if sameTargetedValues(c, incoming, result.TakerVolumesAuthoritative) {
						matchesTarget = true
						break
					}
				}
			}
			validTarget := queryErr == nil && matchesTarget && fetchedTarget && !targetedWriteFailed
			if queryErr != nil {
				logger.OrDiscard(m.log).Warn("gap recovery worker: targeted validation query failed",
					"worker", workerID,
					"exchange", job.ExchangeID,
					"market_type", internalMarketType,
					"symbol", job.Symbol,
					"timeframe", tf,
					"bucket", gapStart,
					"error", queryErr)
			}
			missing := 0
			if !validTarget {
				missing = 1
				m.scheduleRetry(job, tf, sinceMs, fetchLimit)
			} else {
				completedTargeted = true
			}
			m.recordValidation(job.ExchangeID, internalMarketType, validTarget, missing)
		} else if len(historicalCandles) > 0 || seeded {
			// The missing count is the only number that says how bad the remaining hole is;
			// without it every failure looked identical.
			ok, missing := m.validator.Validate(ctx, job.ExchangeID, internalMarketType, internalSymbol, tf, gapStart, currentBucket)
			m.recordValidation(job.ExchangeID, internalMarketType, ok, missing)
			if !ok {
				logger.OrDiscard(m.log).Warn("gap recovery: backfill validation failed; buckets still missing",
					"exchange", job.ExchangeID,
					"market_type", internalMarketType,
					"symbol", job.Symbol,
					"timeframe", tf,
					"missing", missing)
				m.scheduleRetry(job, tf, sinceMs, fetchLimit)
			}
		}

		if seeded {
			successCount++
		}
	}

	// A cycle that wrote nothing is not a success, and it is not retried either: scheduling a
	// retry here is what turns a silent venue into a retry loop. Count it and let the breaker
	// decide when the symbol is suspended.
	if !targetedJob && emptyFills > 0 && backfilled == 0 && successCount == 0 && errorCount == 0 {
		m.metrics.incEmptyCycle(job.ExchangeID, internalMarketType)
		logger.OrDiscard(m.log).Warn("gap recovery worker: cycle wrote no candles; not retrying",
			"worker", workerID,
			"exchange", job.ExchangeID,
			"market_type", internalMarketType,
			"symbol", job.Symbol)
	}

	if (successCount > 0 || completedTargeted) && errorCount == 0 {
		m.cb.RecordSuccess(job.ExchangeID + ":" + job.MarketType)
	}
	m.processed.Add(1)

	// Feed the cycle's best freshness to the no-progress breaker; K cycles without advancing
	// suspends the symbol and emits the metric.
	if !targetedJob {
		if m.breaker.observe(progressKey, bestFreshness) == progressSuspended {
			m.recordNoProgressSuspension(job.ExchangeID, internalMarketType)
			logger.OrDiscard(m.log).Warn("gap recovery worker: symbol suspended after no-progress cycles",
				"worker", workerID,
				"exchange", job.ExchangeID,
				"market_type", internalMarketType,
				"symbol", job.Symbol,
				"cycles", DefaultNoProgressK,
				"freshness", bestFreshness)
		}
		m.metrics.setSuspendedSymbols(job.ExchangeID, internalMarketType, float64(m.breaker.suspendedCount()))
	}

	if backfilled > 0 {
		logger.OrDiscard(m.log).Warn("gap recovery worker: backfill completed with errors",
			"worker", workerID,
			"exchange", job.ExchangeID,
			"market_type", internalMarketType,
			"symbol", job.Symbol,
			"backfilled", backfilled,
			"seeded", successCount,
			"timeframes", len(m.tfReg.All()),
			"errors", errorCount)
	}
	if errorCount > 0 {
		m.errors.Add(1)
		logger.OrDiscard(m.log).Warn("gap recovery worker: partial timeframe restore",
			"worker", workerID,
			"exchange", job.ExchangeID,
			"market_type", internalMarketType,
			"symbol", job.Symbol,
			"seeded", successCount,
			"timeframes", len(m.tfReg.All()),
			"errors", errorCount)
	} else {
		logger.OrDiscard(m.log).Debug("gap recovery worker: timeframes restored",
			"worker", workerID,
			"exchange", job.ExchangeID,
			"market_type", internalMarketType,
			"symbol", job.Symbol,
			"seeded", successCount,
			"timeframes", len(m.tfReg.All()))
	}
}

func sameTargetedValues(stored, incoming candle.Candle, takerAuthoritative bool) bool {
	if stored.Timestamp != incoming.Timestamp ||
		stored.Open != incoming.Open || stored.High != incoming.High ||
		stored.Low != incoming.Low || stored.Close != incoming.Close ||
		stored.Volume != incoming.Volume {
		return false
	}
	return !takerAuthoritative ||
		(stored.TakerBuyVolume == incoming.TakerBuyVolume && stored.TakerSellVolume == incoming.TakerSellVolume)
}

func (m *Manager) updateBacklogHW() {
	backlog := countToUint64(m.queue.Len()) + countToUint64(m.queue.PendingLen())
	for {
		current := m.backlogHW.Load()
		if backlog <= current {
			return
		}
		if m.backlogHW.CompareAndSwap(current, backlog) {
			return
		}
	}
}
