package distribution

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// blockingPublisher blocks every Publish until released, simulating a Redis that has
// stopped draining — the condition that turns a synchronous publish into ingest backpressure.
type blockingPublisher struct {
	release chan struct{}
	mu      sync.Mutex
	got     []string
}

func (p *blockingPublisher) Publish(_ context.Context, _ string, payload []byte) error {
	<-p.release
	p.mu.Lock()
	p.got = append(p.got, string(payload))
	p.mu.Unlock()
	return nil
}

// countingPublisher records payload order without blocking.
type countingPublisher struct {
	mu   sync.Mutex
	seen [][]byte
}

func (p *countingPublisher) Publish(_ context.Context, _ string, payload []byte) error {
	p.mu.Lock()
	p.seen = append(p.seen, append([]byte(nil), payload...))
	p.mu.Unlock()
	return nil
}

func (p *countingPublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.seen)
}

// TestAsyncProducer_EnqueueNeverBlocks is the core guard for
//
// The synchronous producer holds a per-EventType mutex ACROSS the network round trip, on
// the goroutine that processes trades. At a bucket boundary every close funnels through it:
// 203 at a 5m boundary, 1.624 at 00:00 UTC.
//
// The naive async fix is worse: a bounded channel with a BLOCKING send moves the stall into
// the WebSocket read loop, and Binance drops the connection after a 10-minute pong timeout —
// so the failure would present as a silent disconnect rather than an error.
func TestAsyncProducer_EnqueueNeverBlocks(t *testing.T) {
	t.Parallel()

	pub := &blockingPublisher{release: make(chan struct{})}
	p := NewAsyncProducer(pub, "test", func() int64 { return 1 }, WithQueueSize(2))
	defer func() { close(pub.release); p.Stop() }()

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Far more events than the queue holds, with the publisher fully stalled.
		for i := 0; i < 50; i++ {
			p.PublishClose(Scope{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "5m"},
				candle.Candle{Timestamp: int64(i)})
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("enqueue blocked while the publisher was stalled: this is backpressure into the ingest path, and Binance would drop the connection on pong timeout with no error surfaced")
	}

	if p.Dropped() == 0 {
		t.Error("queue overflowed but nothing was counted as dropped: an unobservable drop is the same class of defect as a silent discard")
	}
}

// TestAsyncProducer_PreservesPerChannelOrder pins the per-publisher ordering guarantee on a
// channel.
//
// A single owner goroutine per channel gives that by construction — Go channels are FIFO and
// there is exactly one sender. Without it, the measured cost was 54 backwards jumps and 104
// forward gaps in 60 minutes, every one a reordering, each triggering a full-sweep
// rehydration downstream.
func TestAsyncProducer_PreservesPerChannelOrder(t *testing.T) {
	t.Parallel()

	pub := &countingPublisher{}
	p := NewAsyncProducer(pub, "test", func() int64 { return 1 }, WithQueueSize(4096))

	const n = 500
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := 0; j < n/8; j++ {
				p.PublishClose(Scope{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "5m"},
					candle.Candle{Timestamp: int64(base*1000 + j)})
			}
		}(i)
	}
	wg.Wait()
	p.Stop()

	if got := pub.count(); got == 0 {
		t.Fatal("no events reached the publisher")
	}

	// Sequence numbers must be strictly increasing in the order observed on the wire.
	var last uint64
	for i, payload := range pub.seen {
		env, err := Unmarshal(payload)
		if err != nil {
			t.Fatalf("payload %d is not a valid envelope: %v", i, err)
		}
		if i > 0 && env.Seq <= last {
			t.Fatalf("sequence went backwards on the wire at index %d: %d after %d — a subscriber cannot tell reordering from loss and rehydrates on every one", i, env.Seq, last)
		}
		last = env.Seq
	}
}

// TestAsyncProducer_StopDrainsAndTerminates guards against both a lost tail and a leaked
// owner goroutine: Stop must publish what is already queued and then return.
func TestAsyncProducer_StopDrainsAndTerminates(t *testing.T) {
	t.Parallel()

	pub := &countingPublisher{}
	p := NewAsyncProducer(pub, "test", func() int64 { return 1 }, WithQueueSize(256))
	for i := 0; i < 100; i++ {
		p.PublishClose(Scope{Exchange: "binance", MarketType: "future", Symbol: "ETH/USDT", Timeframe: "1h"},
			candle.Candle{Timestamp: int64(i)})
	}

	stopped := make(chan struct{})
	go func() { p.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return: the owner goroutine leaked")
	}

	if pub.count() == 0 {
		t.Error("Stop discarded the queued tail instead of draining it")
	}
}

// TestAsyncProducer_EnvelopeAndPrefix pins the envelope/prefix wire contract. Its stability
// is independent of the producer's concurrency shape.
func TestAsyncProducer_EnvelopeAndPrefix(t *testing.T) {
	t.Parallel()

	pub := &channelCapturingPublisher{}
	p := NewAsyncProducer(pub, "gowhale:realtime", func() int64 { return 1_710_000_000_000 })
	scope := Scope{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "5m"}
	p.PublishClose(scope, candle.Candle{Timestamp: 42, Close: 100})
	p.Stop()

	ch, payload := pub.first()
	if ch != "gowhale:realtime:candle.close" {
		t.Errorf("channel = %q, want the prefixed candle.close channel", ch)
	}
	env, err := Unmarshal(payload)
	if err != nil {
		t.Fatalf("payload is not a valid envelope: %v", err)
	}
	if env.Version != SchemaVersion || env.Type != EventCandleClose || env.Scope != scope {
		t.Errorf("envelope = %+v, want version %d / candle.close / scope %+v", env, SchemaVersion, scope)
	}
	if env.EmittedAt != 1_710_000_000_000 {
		t.Errorf("emittedAt = %d, want the injected clock value", env.EmittedAt)
	}
}

// TestAsyncProducer_ChannelsAreIndependent pins isolation: each event type has its own owner
// and its own sequence, so a stalled channel cannot hold up another and sequences never
// interleave across channels.
func TestAsyncProducer_ChannelsAreIndependent(t *testing.T) {
	t.Parallel()

	pub := &channelCapturingPublisher{}
	p := NewAsyncProducer(pub, "p", func() int64 { return 1 })
	scope := Scope{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "5m"}
	for i := 0; i < 5; i++ {
		p.PublishClose(scope, candle.Candle{Timestamp: int64(i)})
		p.PublishRevised(scope, candle.Candle{Timestamp: int64(i)})
	}
	p.Stop()

	seqByChannel := map[string][]uint64{}
	for _, rec := range pub.all() {
		env, err := Unmarshal(rec.payload)
		if err != nil {
			t.Fatalf("invalid envelope on %s: %v", rec.channel, err)
		}
		seqByChannel[rec.channel] = append(seqByChannel[rec.channel], env.Seq)
	}
	if len(seqByChannel) != 2 {
		t.Fatalf("expected 2 distinct channels, got %d: %v", len(seqByChannel), seqByChannel)
	}
	for ch, seqs := range seqByChannel {
		for i, s := range seqs {
			if s != uint64(i+1) {
				t.Errorf("%s sequence = %v, want 1..N independent of the other channel", ch, seqs)
				break
			}
		}
	}
}

type capturedPublish struct {
	channel string
	payload []byte
}

type channelCapturingPublisher struct {
	mu   sync.Mutex
	recs []capturedPublish
}

func (p *channelCapturingPublisher) Publish(_ context.Context, channel string, payload []byte) error {
	p.mu.Lock()
	p.recs = append(p.recs, capturedPublish{channel: channel, payload: append([]byte(nil), payload...)})
	p.mu.Unlock()
	return nil
}

func (p *channelCapturingPublisher) first() (string, []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.recs) == 0 {
		return "", nil
	}
	return p.recs[0].channel, p.recs[0].payload
}

func (p *channelCapturingPublisher) all() []capturedPublish {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]capturedPublish(nil), p.recs...)
}
