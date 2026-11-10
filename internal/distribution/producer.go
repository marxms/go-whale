package distribution

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// Publisher is the minimal transport the producer needs. Keeping it an interface here
// keeps this package free of a hard Redis dependency and makes the producer
// unit-testable with a fake.
type Publisher interface {
	Publish(ctx context.Context, channel string, payload []byte) error
}

const (
	// producerDrainTimeout caps the shutdown drain. See Stop for why it must be bounded.
	producerDrainTimeout = 3 * time.Second

	defaultQueueSize   = 4096
	publishSendTimeout = 2 * time.Second
)

// ProducerHooks receives the producer's side-effect signals. They are optional; a nil
// hook is a no-op. The producer itself stays free of metrics and logging clients: the
// transport wiring owns both, which lets that wiring inject its registry without
// touching this package.
type ProducerHooks struct {
	// OnDrop reports an event discarded because the outbound queue was full. total is
	// the running count for the producer.
	OnDrop func(channel string, total int64)
	// OnPublishError reports a transport failure for one channel.
	OnPublishError func(channel string, err error)
	// OnMarshalError reports an envelope that could not be serialized.
	OnMarshalError func(typ EventType, err error)
	// OnDrainTimeout reports that Stop abandoned the queued tail after the timeout.
	OnDrainTimeout func(timeout time.Duration)
}

func (h ProducerHooks) drop(channel string, total int64) {
	if h.OnDrop != nil {
		h.OnDrop(channel, total)
	}
}

func (h ProducerHooks) publishError(channel string, err error) {
	if h.OnPublishError != nil {
		h.OnPublishError(channel, err)
	}
}

func (h ProducerHooks) marshalError(typ EventType, err error) {
	if h.OnMarshalError != nil {
		h.OnMarshalError(typ, err)
	}
}

func (h ProducerHooks) drainTimeout(timeout time.Duration) {
	if h.OnDrainTimeout != nil {
		h.OnDrainTimeout(timeout)
	}
}

// AsyncProducer publishes candle domain events from a SINGLE owner goroutine per channel,
// off the ingestion path.
//
// # Why a synchronous producer does not work
//
// A per-EventType mutex held ACROSS the Redis round trip runs on the goroutine that
// processes trades. At a bucket boundary every close funnels through that one mutex —
// 203 closes at a 5m boundary, 1,624 at 00:00 UTC — each holding it for a full RTT, so
// ingestion stalls for the duration.
//
// # Why the mutex could not simply be deleted
//
// realtime-event-bus:11,18-21 requires per-publisher ordering on a channel. A single
// owner goroutine per channel provides that by construction (Go channels are FIFO and
// there is exactly one sender).
//
// # Why the send is non-blocking
//
// A bounded queue with a BLOCKING send would move the stall into the WebSocket read
// loop. Enqueue therefore uses select/default and drops the NEWEST event, reporting
// every drop through ProducerHooks. Subscribers detect the resulting sequence gap and
// reconcile, which is exactly the recovery path realtime-event-bus:37-44 mandates.
type AsyncProducer struct {
	pub    Publisher
	prefix string
	clock  func() int64
	hooks  ProducerHooks

	queues sync.Map // EventType -> chan outboundEvent
	seqs   sync.Map // EventType -> *atomic.Uint64

	// mu guards the LIFETIME of the queues, not their contents. A send and the close of
	// the same channel must never overlap, so senders hold it shared and Stop holds it
	// exclusive.
	mu      sync.RWMutex
	wg      sync.WaitGroup
	queueSz int

	dropped atomic.Int64
	stopped atomic.Bool
}

type outboundEvent struct {
	typ    EventType
	scope  Scope
	candle candle.Candle
}

// AsyncProducerOption configures an AsyncProducer.
type AsyncProducerOption func(*AsyncProducer)

// WithQueueSize sets the per-channel queue depth. Larger absorbs a bigger boundary burst
// at the cost of memory and of publishing staler events after a stall; smaller drops
// sooner.
func WithQueueSize(n int) AsyncProducerOption {
	return func(p *AsyncProducer) {
		if n > 0 {
			p.queueSz = n
		}
	}
}

// WithHooks installs the side-effect observer. Nil fields are ignored.
func WithHooks(h ProducerHooks) AsyncProducerOption {
	return func(p *AsyncProducer) { p.hooks = h }
}

// NewAsyncProducer builds a producer whose channels are served by one owner goroutine
// each. Owners are started lazily on first use, so a process that never publishes a
// given event type carries no idle goroutine.
func NewAsyncProducer(pub Publisher, prefix string, clock func() int64, opts ...AsyncProducerOption) *AsyncProducer {
	p := &AsyncProducer{pub: pub, prefix: prefix, clock: clock, queueSz: defaultQueueSize}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// PublishClose enqueues a candle.close event. It NEVER blocks the caller.
func (p *AsyncProducer) PublishClose(scope Scope, candle candle.Candle) {
	p.enqueue(EventCandleClose, scope, candle)
}

// PublishRevised enqueues a candle.revised event. It NEVER blocks the caller.
func (p *AsyncProducer) PublishRevised(scope Scope, candle candle.Candle) {
	p.enqueue(EventCandleRevised, scope, candle)
}

// Dropped returns how many events were discarded by backpressure since start.
func (p *AsyncProducer) Dropped() int64 { return p.dropped.Load() }

func (p *AsyncProducer) enqueue(typ EventType, scope Scope, candle candle.Candle) {
	if p.stopped.Load() {
		return
	}
	q := p.queueFor(typ)
	if q == nil {
		return // Stop won the race for a queue that did not exist yet
	}

	// THE SEND MUST NOT OVERLAP THE CLOSE. Without a lock spanning the stopped check and the
	// send, Stop could set stopped, take the lock and close every queue in between — and a
	// send on a closed channel is an unrecoverable panic on whatever goroutine happens to be
	// publishing, which at a bucket boundary is the goroutine that processes trades. Holding
	// the lock shared for the duration of the send makes the two mutually exclusive.
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.stopped.Load() {
		return
	}

	select {
	case q <- outboundEvent{typ: typ, scope: scope, candle: candle}:
	default:
		// Drop-NEWEST. The alternative (drop-oldest) would require draining under a lock
		// and buys nothing: either way the subscriber sees a sequence gap and reconciles.
		n := p.dropped.Add(1)
		p.hooks.drop(Channel(p.prefix, typ), n)
	}
}

// queueFor returns the channel for an event type, starting its owner goroutine on first
// use. It returns nil once Stop has run, so a late publisher drops its event instead of
// stranding an owner goroutine that nothing will ever close.
func (p *AsyncProducer) queueFor(typ EventType) chan outboundEvent {
	if q, ok := p.queues.Load(typ); ok {
		return q.(chan outboundEvent)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if q, ok := p.queues.Load(typ); ok {
		return q.(chan outboundEvent)
	}
	// Re-checked INSIDE the exclusive section. Stop closes whatever Range sees; a queue
	// created after that Range would never be closed, and its owner goroutine would
	// outlive Shutdown holding the WaitGroup that Stop is waiting on.
	if p.stopped.Load() {
		return nil
	}

	q := make(chan outboundEvent, p.queueSz)
	p.queues.Store(typ, q)
	p.wg.Add(1)
	go p.own(typ, q)
	return q
}

// own is the SINGLE sender for one channel. Being the only writer is what makes the
// observed order equal the claimed order.
func (p *AsyncProducer) own(typ EventType, q chan outboundEvent) {
	defer p.wg.Done()
	channel := Channel(p.prefix, typ)

	for ev := range q {
		env := NewEnvelope(ev.typ, ev.scope, ev.candle, p.nextSeq(ev.typ), p.clock())
		data, err := env.Marshal()
		if err != nil {
			p.hooks.marshalError(ev.typ, err)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), publishSendTimeout)
		err = p.pub.Publish(ctx, channel, data)
		cancel()
		if err != nil {
			p.hooks.publishError(channel, err)
		}
	}
}

// nextSeq claims the next per-channel sequence. It is called only from the owner
// goroutine, so the claimed order is also the order that reaches the wire.
func (p *AsyncProducer) nextSeq(typ EventType) uint64 {
	counter, _ := p.seqs.LoadOrStore(typ, new(atomic.Uint64))
	return counter.(*atomic.Uint64).Add(1)
}

// Stop closes every queue, drains what is already enqueued and waits for the owners to
// finish. Draining rather than discarding keeps the tail of a boundary burst from being
// lost at shutdown. Safe to call more than once.
func (p *AsyncProducer) Stop() {
	if p.stopped.Swap(true) {
		return
	}
	p.mu.Lock()
	p.queues.Range(func(_, v any) bool {
		close(v.(chan outboundEvent))
		return true
	})
	p.mu.Unlock()

	// BOUNDED. The drain exists so the tail of a boundary burst is not discarded, but it
	// is best-effort telemetry — and it is queued behind one transport round trip per
	// event, with a queue that only fills when the transport is SLOW. So the one
	// condition that makes this wait long is the same condition that makes it worthless.
	// Unbounded, it would sit in the shutdown sequence AHEAD of the cold pipeline's own
	// drain, trading a best-effort drain for a durable one.
	done := make(chan struct{})
	go func() { p.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(producerDrainTimeout):
		p.hooks.drainTimeout(producerDrainTimeout)
	}
}
