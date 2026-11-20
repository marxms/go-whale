package engine

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

const (
	defaultHotWriterCapacity = 4096
	defaultHotWriterInterval = 50 * time.Millisecond
)

// hotWrite is one pending closed-bucket snapshot. The key is the coalescing identity; the
// candle is the LATEST value seen for that key, never a delta.
type hotWrite struct {
	key    CompositeKey
	candle candle.Candle
	atMs   int64
}

// HotFlush is the owner's write. It runs on the HotWriter goroutine, never on the reader.
type HotFlush func(ctx context.Context, entries []hotWrite) error

// HotWriterStats is what the health/metrics read. Depth counts pending plus in-flight entries.
type HotWriterStats struct {
	Coalesced        int64
	Dropped          int64
	Failed           int64
	Depth            int
	OldestPendingAge time.Duration
}

// HotWriter is a bounded, single-owner write-behind queue for the hot tier.
//
// # WHY THE READER ONLY ENQUEUES
//
// The close path runs on a partition's reader goroutine. A Redis write there turns a slow Redis
// into a silent partition: the reader stops reading frames while it waits. Enqueue never does I/O;
// the owner goroutine drains the queue in batches.
//
// # WHY COALESCE INSTEAD OF DROP
//
// The hot write is a snapshot-TOTAL, so the newest value for a key subsumes every earlier one.
// Replacing the pending entry is therefore lossless for the hot tier; only a NEW key arriving when
// the queue is full is dropped, and that drop is counted.
type HotWriter struct {
	flush    HotFlush
	capacity int
	interval time.Duration

	mu             sync.Mutex
	pending        map[CompositeKey]hotWrite
	inFlight       int
	inFlightOldest int64

	coalesced  atomic.Int64
	dropped    atomic.Int64
	failed     atomic.Int64
	stopped    atomic.Bool
	dropLogged atomic.Bool
	startOnce  sync.Once
	// log names the FIRST drop. A counter nobody reads is how a drop stays silent.
	log *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wake   chan struct{}
	wg     sync.WaitGroup
}

// HotWriterOption tunes the queue without opening a second clock.
type HotWriterOption func(*HotWriter)

// WithHotWriterCapacity bounds the number of DISTINCT keys pending or in flight.
func WithHotWriterCapacity(n int) HotWriterOption {
	return func(w *HotWriter) {
		if n > 0 {
			w.capacity = n
		}
	}
}

// WithHotWriterInterval sets the maximum delay between drains.
func WithHotWriterInterval(d time.Duration) HotWriterOption {
	return func(w *HotWriter) {
		if d > 0 {
			w.interval = d
		}
	}
}

// WithHotWriterLogger names the first drop. Without it the drop is only a counter.
func WithHotWriterLogger(log *slog.Logger) HotWriterOption {
	return func(w *HotWriter) { w.log = log }
}

// NewHotWriter builds the writer. The owner starts on the first admission, so an App that never
// closes a bucket carries no idle goroutine; the caller must Stop it.
func NewHotWriter(flush HotFlush, opts ...HotWriterOption) *HotWriter {
	w := &HotWriter{
		flush:    flush,
		capacity: defaultHotWriterCapacity,
		interval: defaultHotWriterInterval,
		pending:  make(map[CompositeKey]hotWrite),
		wake:     make(chan struct{}, 1),
	}
	for _, opt := range opts {
		opt(w)
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	return w
}

// Enqueue admits a closed-bucket snapshot without doing I/O. It returns false only when a NEW
// key arrives while the bounded queue is full; an existing key is coalesced and admitted.
func (w *HotWriter) Enqueue(key CompositeKey, c candle.Candle) bool {
	w.mu.Lock()
	if w.stopped.Load() {
		w.mu.Unlock()
		w.dropped.Add(1)
		w.noteFirstDrop(key, "stopped")
		return false
	}
	_, exists := w.pending[key]
	if !exists && len(w.pending)+w.inFlight >= w.capacity {
		w.mu.Unlock()
		w.dropped.Add(1)
		w.noteFirstDrop(key, "capacity")
		return false
	}
	if exists {
		w.coalesced.Add(1)
	}
	w.pending[key] = hotWrite{key: key, candle: c, atMs: time.Now().UnixMilli()}
	// Start under the lock so the WaitGroup Add cannot race the Stop's Wait.
	w.startOnce.Do(func() {
		w.wg.Add(1)
		go w.run()
	})
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
	return true
}

// noteFirstDrop emits exactly one line for the first drop of the process. The rest stay in the
// counter: a log per dropped bar would itself become the stall.
func (w *HotWriter) noteFirstDrop(key CompositeKey, reason string) {
	if !w.dropLogged.CompareAndSwap(false, true) {
		return
	}
	logger.OrDiscard(w.log).Warn("hot writer: dropped a closed bucket (first occurrence)",
		"exchange", key.Exchange,
		"market_type", key.MarketType,
		"symbol", key.Symbol,
		"timeframe", key.Timeframe,
		"reason", reason,
		"capacity", w.capacity)
}

// Stats reports the counters and the age of the oldest pending/in-flight entry.
func (w *HotWriter) Stats() HotWriterStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	oldest := w.inFlightOldest
	for _, entry := range w.pending {
		if oldest == 0 || entry.atMs < oldest {
			oldest = entry.atMs
		}
	}
	age := time.Duration(0)
	if oldest > 0 {
		age = time.Since(time.UnixMilli(oldest))
	}
	return HotWriterStats{
		Coalesced:        w.coalesced.Load(),
		Dropped:          w.dropped.Load(),
		Failed:           w.failed.Load(),
		Depth:            len(w.pending) + w.inFlight,
		OldestPendingAge: age,
	}
}

// Stop drains once and joins the owner. A blocked owner is bounded by ctx.
func (w *HotWriter) Stop(ctx context.Context) error {
	w.mu.Lock()
	if w.stopped.Swap(true) {
		w.mu.Unlock()
		return nil
	}
	w.cancel()
	w.mu.Unlock()
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *HotWriter) run() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-w.ctx.Done():
			w.flushPending(context.Background())
			return
		case <-ticker.C:
			// The batch context is NOT the loop's: Stop cancels the loop, and canceling an
			// in-flight Redis batch would lose it after the entries were already dequeued.
			w.flushPending(context.Background())
		case <-w.wake:
			w.flushPending(context.Background())
		}
	}
}

func (w *HotWriter) flushPending(ctx context.Context) {
	w.mu.Lock()
	if len(w.pending) == 0 {
		w.mu.Unlock()
		return
	}
	batch := make([]hotWrite, 0, len(w.pending))
	var oldest int64
	for _, entry := range w.pending {
		batch = append(batch, entry)
		if oldest == 0 || entry.atMs < oldest {
			oldest = entry.atMs
		}
	}
	w.pending = make(map[CompositeKey]hotWrite, w.capacity)
	w.inFlight = len(batch)
	w.inFlightOldest = oldest
	w.mu.Unlock()

	err := w.flush(ctx, batch)

	w.mu.Lock()
	w.inFlight = 0
	w.inFlightOldest = 0
	w.mu.Unlock()
	if err != nil {
		w.failed.Add(1)
	}
}

// NewHotWriter builds a writer whose flush is the store's own snapshot batch. The writeThrough
// guard keeps a disabled hot tier a no-op instead of an error on every close.
func (ts *TieredStore) NewHotWriter(opts ...HotWriterOption) *HotWriter {
	return NewHotWriter(func(parent context.Context, entries []hotWrite) error {
		if !ts.writeThrough || ts.redis == nil || len(entries) == 0 {
			return nil
		}
		ctx, cancel := context.WithTimeout(parent, 10*time.Second)
		defer cancel()
		updates := make([]redisstore.CandleUpdate, 0, len(entries))
		for _, entry := range entries {
			updates = append(updates, buildSnapshotUpdate(entry.key, entry.candle))
		}
		return ts.redis.WriteSnapshotBatch(ctx, updates)
	}, opts...)
}
