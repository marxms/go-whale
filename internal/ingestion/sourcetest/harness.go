package sourcetest

import (
	"testing"

	"github.com/marxms/go-whale/internal/ingestion"
	"github.com/marxms/go-whale/internal/trade"
)

// Harness is one Source under test plus the test-side controls the suite needs. A fake
// implements every control synchronously; an adapter harness drives its in-process
// transport (captured frames, injected connection, httptest server) and leaves
// ForceReconnect returning false when that transport cannot be dropped without network.
type Harness struct {
	// Source is the contract under test.
	Source ingestion.Source

	// Deliver makes trades observable to the source for symbol, preserving order. It
	// returns once the events are buffered (or fails the test).
	Deliver func(t *testing.T, symbol string, trades ...trade.Trade)

	// Capacity is the source's per-symbol buffer capacity; the backpressure case fills
	// a fraction of it.
	Capacity int

	// ForceReconnect drops the transport so the source must reconnect. It returns false
	// when the harness cannot drive reconnect without live network; the suite then skips
	// that case instead of pretending it passed.
	ForceReconnect func(t *testing.T, symbol string) bool

	// Reconnects reports the notifications observed through the configured
	// ingestion.ReconnectSink.
	Reconnects func() []ReconnectBatch
}

// NewHarness builds one fresh Harness for one test case. Each subtest gets its own
// source, transport and recorder so cases cannot leak state into each other.
type NewHarness func(t *testing.T) Harness

// FakeHarness builds a Harness backed by FakeSource and a ReconnectRecorder. It is the
// canonical in-process driver and the reference implementation of the Harness contract.
func FakeHarness(t *testing.T, opts ...FakeOption) Harness {
	t.Helper()
	recorder := NewReconnectRecorder()
	opts = append(opts, WithReconnectSink(recorder))
	fake := NewFakeSource(opts...)
	t.Cleanup(func() { _ = fake.Close() })
	return Harness{
		Source: fake,
		Deliver: func(_ *testing.T, symbol string, trades ...trade.Trade) {
			fake.Deliver(symbol, trades...)
		},
		Capacity: fake.Capacity(),
		ForceReconnect: func(_ *testing.T, symbol string) bool {
			fake.SimulateReconnect(symbol)
			return true
		},
		Reconnects: recorder.Batches,
	}
}

// RunFake runs the whole suite against the deterministic fake. It is the proof that the
// suite itself is well-formed before any adapter is asked to satisfy it.
func RunFake(t *testing.T, opts ...FakeOption) {
	t.Helper()
	Run(t, func(t *testing.T) Harness {
		return FakeHarness(t, opts...)
	})
}
