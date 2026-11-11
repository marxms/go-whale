package recovery

import (
	"context"
	"log/slog"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// retryTickInterval is how often processLoop drains due jobs.
const retryTickInterval = 5 * time.Second

// MemoryRetryScheduler implements RetryScheduler with in-memory exponential backoff.
// It is NOT persistent across restarts — acceptable for gap recovery because
// persistent gaps are re-discovered on startup by the startup backfill scanner.
type MemoryRetryScheduler struct {
	mu        sync.Mutex
	queue     []BackfillRetryJob
	pending   atomic.Bool            // whether processLoop is running
	reenqueue func(BackfillRetryJob) // hands due jobs back to the worker pipeline (N1 fix)

	ctx    context.Context
	cancel context.CancelFunc
	log    *slog.Logger
}

// SetReenqueue wires the callback used to push a due retry job back into the
// worker pipeline. Set by the Manager; without it, due jobs would be dropped.
func (r *MemoryRetryScheduler) SetReenqueue(fn func(BackfillRetryJob)) {
	r.mu.Lock()
	r.reenqueue = fn
	r.mu.Unlock()
}

// NewMemoryRetryScheduler creates a new in-memory retry scheduler.
func NewMemoryRetryScheduler(logs ...*slog.Logger) *MemoryRetryScheduler {
	return &MemoryRetryScheduler{log: logger.OrDiscard(logs...)}
}

// Start records the context used to stop the processing loop. Guarded by r.mu
// because processLoop (a separate goroutine) reads it.
func (r *MemoryRetryScheduler) Start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	r.mu.Lock()
	oldCancel := r.cancel
	r.ctx = runCtx
	r.cancel = cancel
	// A job scheduled before Start would otherwise sit in the queue until the
	// next Schedule; kick the loop now that a tracked context exists.
	kick := len(r.queue) > 0 && r.pending.CompareAndSwap(false, true)
	r.mu.Unlock()
	if oldCancel != nil {
		oldCancel()
	}
	if kick {
		go r.processLoop()
	}
}

// Stop halts the retry loop (idempotent). The loop reads the context stored by
// Start, so canceling it is what makes Stop a real stop rather than a no-op.
func (r *MemoryRetryScheduler) Stop() {
	r.mu.Lock()
	cancel := r.cancel
	r.cancel = nil
	r.ctx = nil
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Schedule adds a failed job to the retry queue with exponential backoff.
func (r *MemoryRetryScheduler) Schedule(job BackfillRetryJob) {
	r.mu.Lock()
	defer r.mu.Unlock()

	job.Attempt++
	if job.Attempt > RetryMaxAttempts {
		logger.OrDiscard(r.log).Error("gap recovery job exceeded max retry attempts; gap may remain open",
			"exchange", job.ExchangeID,
			"market_type", job.MarketType,
			"symbol", job.Symbol,
			"timeframe", job.Timeframe,
			"max_attempts", RetryMaxAttempts)
		return
	}

	delay := time.Duration(float64(RetryInitialDelay) * math.Pow(RetryBackoffFactor, float64(job.Attempt-1)))
	if delay > RetryMaxDelay {
		delay = RetryMaxDelay
	}
	job.NextRetryAt = time.Now().Add(delay)

	r.queue = append(r.queue, job)
	logger.OrDiscard(r.log).Warn("gap recovery retry scheduled",
		"attempt", job.Attempt,
		"max_attempts", RetryMaxAttempts,
		"exchange", job.ExchangeID,
		"market_type", job.MarketType,
		"symbol", job.Symbol,
		"timeframe", job.Timeframe,
		"retry_in", delay)

	if r.pending.CompareAndSwap(false, true) {
		go r.processLoop()
	}
}

// processLoop drains due jobs every tick and re-enqueues them via the wired
// callback, exiting when the queue empties (a later Schedule restarts it).
func (r *MemoryRetryScheduler) processLoop() {
	r.mu.Lock()
	ctx := r.ctx
	if ctx == nil {
		// Not started yet: never run untracked by the caller's context. Start or
		// the next Schedule restarts the loop.
		r.pending.Store(false)
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.pending.Store(false)
		r.mu.Unlock()
	}()

	ticker := time.NewTicker(retryTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.processReadyOnce(time.Now())
			r.mu.Lock()
			empty := len(r.queue) == 0
			r.mu.Unlock()
			if empty {
				return
			}
		}
	}
}

// processReadyOnce drains every job whose NextRetryAt has elapsed and hands it
// back to the worker pipeline via the re-enqueue callback (the N1 fix; was
// log-and-drop). Due jobs are coalesced by symbol so a symbol with several
// deferred timeframes re-enters the whole-symbol pipeline once per cycle (keeping
// the highest attempt), avoiding N×timeframe amplification.
func (r *MemoryRetryScheduler) processReadyOnce(now time.Time) {
	r.mu.Lock()
	reenqueue := r.reenqueue
	var remaining []BackfillRetryJob
	dedup := make(map[string]BackfillRetryJob)
	var order []string
	for _, job := range r.queue {
		if !now.After(job.NextRetryAt) {
			remaining = append(remaining, job)
			continue
		}
		key := job.ExchangeID + ":" + job.MarketType + "|" + job.Symbol
		if job.TargetedBucket > 0 && job.TargetedTimeframe != "" {
			key += "|" + job.TargetedTimeframe + "|" + strconv.FormatInt(job.TargetedBucket, 10)
		}
		if existing, ok := dedup[key]; ok {
			window := existing.window.merge(job.window)
			if job.Attempt > existing.Attempt {
				existing = job
			}
			existing.window = window
			dedup[key] = existing
		} else {
			dedup[key] = job
			order = append(order, key)
		}
	}
	r.queue = remaining
	r.mu.Unlock()

	for _, key := range order {
		job := dedup[key]
		if time.Since(job.NextRetryAt) > RetryAlertThreshold {
			logger.OrDiscard(r.log).Error("gap recovery job stuck unresolved",
				"exchange", job.ExchangeID,
				"market_type", job.MarketType,
				"symbol", job.Symbol,
				"timeframe", job.Timeframe,
				"threshold", RetryAlertThreshold)
		}
		if reenqueue != nil {
			reenqueue(job)
		} else {
			logger.OrDiscard(r.log).Warn("gap recovery retry job ready but no re-enqueue is wired",
				"exchange", job.ExchangeID,
				"market_type", job.MarketType,
				"symbol", job.Symbol,
				"timeframe", job.Timeframe)
		}
	}
}
