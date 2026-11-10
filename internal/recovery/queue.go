package recovery

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// EnqueueResult tells the caller how Enqueue placed a job.
type EnqueueResult uint8

const (
	enqueueResultQueued EnqueueResult = iota
	enqueueResultCoalesced
	enqueueResultFailed
)

// CoalescingQueue holds a bounded channel plus a deferred pending map.
// When the channel is full, jobs are coalesced (overwritten per symbol).
type CoalescingQueue struct {
	// priority carries TARGETED single-bucket repairs, which workers drain before the bulk
	// channel.
	//
	// The bulk channel is FIFO, and the boot-gap sweep puts the whole fleet on it at start-up.
	// Measured in production: ~10 jobs a minute drained with ~500 sweep jobs ahead, so a repair
	// enqueued when the boot bucket closed would not be attempted for ~50 minutes. For a
	// 5-minute bucket that is not a repair, it is an autopsy — every consumer downstream has
	// long since read the wrong value.
	//
	// Priority rather than more workers because the two differ in KIND: a targeted repair is
	// one bucket and one page, while a sweep job walks every timeframe. Letting the cheap,
	// time-critical work overtake the bulk work costs nothing and is the whole reason the
	// distinction exists.
	priority chan RecoveryJob

	maxPending int
	queue      chan RecoveryJob
	pending    map[string]RecoveryJob
	order      []string
	mu         sync.Mutex

	enqueued  atomic.Uint64
	coalesced atomic.Uint64
	failed    atomic.Uint64
	retried   atomic.Uint64
	backlogHW atomic.Uint64
}

// saturatingInt converts a monotonic counter to int without wrapping on 32-bit builds.
func saturatingInt(v uint64) int {
	if v > math.MaxInt {
		return math.MaxInt
	}
	return int(v)
}

// countToUint64 converts a job count to uint64, treating a negative value as zero.
func countToUint64(n int) uint64 {
	if n < 0 {
		return 0
	}
	return uint64(n)
}

// NewCoalescingQueue creates a queue with the given channel and pending bounds.
func NewCoalescingQueue(chanSize, maxPending int) *CoalescingQueue {
	return &CoalescingQueue{
		queue: make(chan RecoveryJob, chanSize),
		// Bounded well below the bulk channel: a targeted repair is one per key per boot, so a
		// backlog here would mean the trigger has started firing repeatedly — a condition to
		// drop loudly rather than buffer silently.
		priority:   make(chan RecoveryJob, 4096),
		maxPending: maxPending,
		pending:    make(map[string]RecoveryJob),
	}
}

// Enqueue tries to push the job onto the channel. If the channel is full it
// attempts bounded retries with linear backoff, then falls back to pending.
func (q *CoalescingQueue) Enqueue(job RecoveryJob) EnqueueResult {
	const maxAttempts = 3
	const backoffStep = 10 // ms

	for attempt := 0; attempt < maxAttempts; attempt++ {
		select {
		case q.queue <- job:
			q.enqueued.Add(1)
			return enqueueResultQueued
		default:
			if attempt < maxAttempts-1 {
				q.retried.Add(1)
				// small sleep — callers already handle ctx cancellation externally
				sleepMs(backoffStep * (attempt + 1))
			}
		}
	}

	if q.coalescePending(job) {
		q.coalesced.Add(1)
		return enqueueResultCoalesced
	}
	q.failed.Add(1)
	return enqueueResultFailed
}

// Pop removes and returns the oldest pending job (FIFO).
func (q *CoalescingQueue) Pop() (RecoveryJob, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.order) > 0 {
		key := q.order[0]
		q.order = q.order[1:]
		job, ok := q.pending[key]
		if !ok {
			continue
		}
		delete(q.pending, key)
		return job, true
	}
	return RecoveryJob{}, false
}

// PushFront puts a job back at the front of the pending list.
func (q *CoalescingQueue) PushFront(job RecoveryJob) {
	q.mu.Lock()
	defer q.mu.Unlock()

	key := job.ExchangeID + ":" + job.MarketType + "|" + job.Symbol
	if existing, exists := q.pending[key]; exists {
		job.window = job.window.merge(existing.window)
		job.RetryAttempt = max(job.RetryAttempt, existing.RetryAttempt)
	} else {
		if len(q.pending) >= q.maxPending {
			q.failed.Add(1)
			return
		}
		q.order = append([]string{key}, q.order...)
	}
	q.pending[key] = job
}

// Promote moves up to limit deferred jobs back into the channel.
func (q *CoalescingQueue) Promote(limit int) {
	if limit <= 0 {
		limit = 128
	}
	for moved := 0; moved < limit; moved++ {
		job, ok := q.Pop()
		if !ok {
			break
		}
		select {
		case q.queue <- job:
			q.enqueued.Add(1)
			q.retried.Add(1)
		default:
			q.PushFront(job)
			q.updateBacklogHighWatermark()
			return
		}
	}
	q.updateBacklogHighWatermark()
}

// PendingLen returns the number of deferred jobs.
func (q *CoalescingQueue) PendingLen() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}

// Len returns the number of jobs currently in the channel buffer.
func (q *CoalescingQueue) Len() int { return len(q.queue) }

// Read returns a receive-only view of the channel.
func (q *CoalescingQueue) Read() <-chan RecoveryJob { return q.queue }

// ReadPriority is the channel workers must drain BEFORE Read. See the priority field.
func (q *CoalescingQueue) ReadPriority() <-chan RecoveryJob { return q.priority }

// EnqueuePriority offers a job to the priority channel. It never blocks: a full priority
// channel means the caller is producing far more targeted repairs than one per key per boot,
// which is a defect in the caller, and dropping it here surfaces that instead of hiding it
// behind unbounded memory.
func (q *CoalescingQueue) EnqueuePriority(job RecoveryJob) EnqueueResult {
	select {
	case q.priority <- job:
		q.enqueued.Add(1)
		return enqueueResultQueued
	default:
		q.failed.Add(1)
		return enqueueResultFailed
	}
}

// Stats returns a snapshot of queue-level counters.
func (q *CoalescingQueue) Stats() (enqueued, coalesced, retried, failed uint64) {
	return q.enqueued.Load(), q.coalesced.Load(), q.retried.Load(), q.failed.Load()
}

// BacklogHighWatermark returns the peak value ever observed.
func (q *CoalescingQueue) BacklogHighWatermark() uint64 { return q.backlogHW.Load() }

func (q *CoalescingQueue) coalescePending(job RecoveryJob) bool {
	key := job.ExchangeID + ":" + job.MarketType + "|" + job.Symbol

	q.mu.Lock()
	defer q.mu.Unlock()

	if existing, exists := q.pending[key]; exists {
		job.window = job.window.merge(existing.window)
		job.RetryAttempt = max(job.RetryAttempt, existing.RetryAttempt)
		q.pending[key] = job
		return true
	}
	if len(q.pending) >= q.maxPending {
		return false
	}
	q.pending[key] = job
	q.order = append(q.order, key)
	return true
}

func (q *CoalescingQueue) updateBacklogHighWatermark() {
	backlog := countToUint64(q.Len()) + countToUint64(q.PendingLen())
	for {
		current := q.backlogHW.Load()
		if backlog <= current {
			return
		}
		if q.backlogHW.CompareAndSwap(current, backlog) {
			return
		}
	}
}

// sleepMs pauses between bounded channel-full retries so workers get a moment to
// drain before the job falls back to the pending map. Only reached when the (deep)
// channel is full — an overload condition — so the brief pause is beneficial, not
// hot-path latency. Was a no-op placeholder (N6).
func sleepMs(ms int) {
	if ms > 0 {
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
