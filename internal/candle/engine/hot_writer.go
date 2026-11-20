package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marxms/go-whale/internal/candle"
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

	coalesced atomic.Int64
	dropped   atomic.Int64
	failed    atomic.Int64
	stopped   atomic.Bool

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

// NewHotWriter starts the single owner goroutine. The caller must Stop it.
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
	w.wg.Add(1)
	go w.run()
	return w
}

// Enqueue admits a closed-bucket snapshot without doing I/O. It returns false only when a NEW
// key arrives while the bounded queue is full; an existing key is coalesced and admitted.
func (w *HotWriter) Enqueue(key CompositeKey, c candle.Candle) bool {
	if w.stopped.Load() {
		w.dropped.Add(1)
		return false
	}
	w.mu.Lock()
	_, exists := w.pending[key]
	if !exists && len(w.pending)+w.inFlight >= w.capacity {
		w.mu.Unlock()
		w.dropped.Add(1)
		return false
	}
	if exists {
		w.coalesced.Add(1)
	}
	w.pending[key] = hotWrite{key: key, candle: c, atMs: time.Now().UnixMilli()}
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
	return true
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
	if w.stopped.Swap(true) {
		return nil
	}
	w.cancel()
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
			w.flushPending(w.ctx)
		case <-w.wake:
			w.flushPending(w.ctx)
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
