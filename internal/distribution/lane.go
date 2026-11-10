package distribution

import (
	"sync"

	"github.com/marxms/go-whale/internal/candle"
)

// DefaultLaneSize is the bounded lossless lane depth per connection. It is large enough
// to absorb a boundary burst for one client and small enough that a stalled writer is
// evicted instead of pinning memory for the life of the process.
const DefaultLaneSize = 256

// Frame is one item on a connection's lossless lane: either a versioned envelope (delta
// or revision) or the initial subscription snapshot. Snapshot frames are exempt from
// the backlog cap because a client that has just subscribed needs its baseline more than
// it needs the deltas that follow it.
type Frame struct {
	Envelope Envelope
	Snapshot *Snapshot
}

// Snapshot is the actor-built initial frame for a subscription: the sequence baseline B,
// the TIME of the bar that baseline counts, and the current open candle (from the
// in-memory cache).
type Snapshot struct {
	Scope        Scope
	Baseline     uint64
	BaselineTime int64
	Open         *candle.Candle
}

// Lane is the bounded, lossless queue between the Hub actor and one connection's
// writer. The Hub is the sole sender and the sole closer; the transport is the sole
// receiver. Push never blocks: when the queue is full it refuses the NEWEST frame and
// returns false, which is the Hub's signal to evict the connection. Dropping the newest
// (rather than the oldest) keeps the send non-blocking without a drain under a lock, and
// the resulting sequence gap is exactly what the subscriber's gap detector handles.
type Lane struct {
	frames chan Frame

	// mu guards the CLOSE transition, not the contents. Senders take it shared; Close
	// takes it exclusive. That makes "a send and the close of the same channel never
	// overlap" true by construction even if a caller violates the single-owner rule,
	// at the cost of one shared lock acquisition per enqueue.
	mu     sync.RWMutex
	closed bool
}

// NewLane builds a lane of the given depth. A non-positive size uses DefaultLaneSize.
func NewLane(size int) *Lane {
	if size <= 0 {
		size = DefaultLaneSize
	}
	return &Lane{frames: make(chan Frame, size)}
}

// Push enqueues a frame without blocking. It returns false when the lane is full or
// already closed; in both cases the caller owns the eviction decision.
func (l *Lane) Push(frame Frame) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return false
	}
	select {
	case l.frames <- frame:
		return true
	default:
		return false
	}
}

// Recv returns the receive side the transport's writer ranges over.
func (l *Lane) Recv() <-chan Frame { return l.frames }

// Close closes the lane, waking the writer which then exits. It is idempotent.
func (l *Lane) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	l.closed = true
	close(l.frames)
}

// Len reports the number of frames currently queued.
func (l *Lane) Len() int { return len(l.frames) }

// Cap reports the configured depth.
func (l *Lane) Cap() int { return cap(l.frames) }
