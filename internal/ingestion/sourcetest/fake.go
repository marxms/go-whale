// Package sourcetest provides the shared conformance suite for ingestion.Source
// implementations. It contains a deterministic fake, a reconnect recorder, and the
// suite itself. Adapter packages import this package from their tests to prove that a
// venue implementation honors the consumer contract; the suite never imports a
// concrete adapter, so there is no production dependency back into vendor code.
package sourcetest

import (
	"context"
	"sync"
	"time"

	"github.com/marxms/go-whale/internal/ingestion"
	"github.com/marxms/go-whale/internal/ingestion/streaming"
	"github.com/marxms/go-whale/internal/trade"
)

// DefaultFakeCapacity mirrors the small buffer the suite configures on real adapters:
// big enough to hold a delivery batch, small enough that backpressure is reachable.
const DefaultFakeCapacity = 16

// FakeSource is a deterministic ingestion.Source for the conformance suite and for
// consumer tests that need a source without a transport. Delivery is synchronous:
// Deliver appends immediately, so the suite observes it without waiting.
type FakeSource struct {
	mu         sync.Mutex
	capacity   int
	subscribed map[string]bool
	buffers    map[string][]trade.Trade
	healthy    bool
	closed     bool
	dropped    int64
	sink       ingestion.ReconnectSink
	now        func() time.Time
	disconnect time.Time
}

// FakeOption configures a FakeSource.
type FakeOption func(*FakeSource)

// WithCapacity overrides the per-symbol buffer capacity.
func WithCapacity(n int) FakeOption {
	return func(f *FakeSource) {
		if n > 0 {
			f.capacity = n
		}
	}
}

// WithReconnectSink installs the consumer-owned reconnect notification port.
func WithReconnectSink(sink ingestion.ReconnectSink) FakeOption {
	return func(f *FakeSource) { f.sink = sink }
}

// WithClock injects the clock used for reconnect notifications.
func WithClock(now func() time.Time) FakeOption {
	return func(f *FakeSource) {
		if now != nil {
			f.now = now
		}
	}
}

// NewFakeSource builds a FakeSource. It is healthy by default.
func NewFakeSource(opts ...FakeOption) *FakeSource {
	f := &FakeSource{
		capacity:   DefaultFakeCapacity,
		subscribed: make(map[string]bool),
		buffers:    make(map[string][]trade.Trade),
		healthy:    true,
		now:        time.Now,
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// Capacity returns the per-symbol buffer capacity.
func (f *FakeSource) Capacity() int { return f.capacity }

// Source contract.

// Subscribe marks symbol as subscribed.
func (f *FakeSource) Subscribe(ctx context.Context, symbol string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return streaming.ErrClosed
	}
	if f.subscribed[symbol] {
		return nil
	}
	f.subscribed[symbol] = true
	f.buffers[symbol] = make([]trade.Trade, 0, f.capacity)
	return nil
}

// Unsubscribe clears symbol's subscription.
func (f *FakeSource) Unsubscribe(symbol string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	delete(f.subscribed, symbol)
	delete(f.buffers, symbol)
	return nil
}

// GetTrades drains the queued trades of symbol.
func (f *FakeSource) GetTrades(ctx context.Context, symbol string) ([]trade.Trade, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, streaming.ErrClosed
	}
	if !f.subscribed[symbol] {
		return nil, streaming.ErrNotSubscribed
	}
	buf := f.buffers[symbol]
	if len(buf) == 0 {
		return nil, nil
	}
	out := make([]trade.Trade, len(buf))
	copy(out, buf)
	f.buffers[symbol] = buf[:0]
	return out, nil
}

// HealthCheck reports whether the fake is open and healthy.
func (f *FakeSource) HealthCheck() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.closed && f.healthy
}

// Type reports the native client type.
func (f *FakeSource) Type() streaming.ClientType { return streaming.ClientTypeNative }

// Close closes the fake and clears its subscriptions.
func (f *FakeSource) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	f.subscribed = make(map[string]bool)
	f.buffers = make(map[string][]trade.Trade)
	return nil
}

// BackpressureSource contract.

// GetBufferFillRatio reports the configured buffer fill ratio.
func (f *FakeSource) GetBufferFillRatio(symbol string) float64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || f.capacity <= 0 {
		return 0
	}
	return float64(len(f.buffers[symbol])) / float64(f.capacity)
}

// Test controls.

// Deliver appends trades to a subscribed symbol's buffer, dropping the newest event
// once the buffer is full. It is synchronous and never blocks.
func (f *FakeSource) Deliver(symbol string, trades ...trade.Trade) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || !f.subscribed[symbol] {
		return
	}
	buf := f.buffers[symbol]
	for _, t := range trades {
		if len(buf) >= f.capacity {
			f.dropped++
			continue
		}
		buf = append(buf, t)
	}
	f.buffers[symbol] = buf
}

// Dropped reports how many events the fake discarded because its buffer was full.
func (f *FakeSource) Dropped() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dropped
}

// SimulateReconnect reports a completed reconnect for symbols through the configured
// sink, with a disconnect instant one second before the reconnect instant.
func (f *FakeSource) SimulateReconnect(symbols ...string) {
	f.mu.Lock()
	now := f.now()
	disconnect := f.disconnect
	if disconnect.IsZero() || !disconnect.Before(now) {
		disconnect = now.Add(-time.Second)
	}
	f.disconnect = now
	sink := f.sink
	f.mu.Unlock()
	if sink == nil {
		return
	}
	sink.EnqueueBatch("fake", "future", symbols, disconnect, now)
}

// SetHealthy flips the health answer, for tests of the HealthCheck surface.
func (f *FakeSource) SetHealthy(healthy bool) {
	f.mu.Lock()
	f.healthy = healthy
	f.mu.Unlock()
}

var (
	_ ingestion.Source             = (*FakeSource)(nil)
	_ ingestion.BackpressureSource = (*FakeSource)(nil)
)
