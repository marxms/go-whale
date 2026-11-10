package ingestion

import (
	"context"
	"time"

	"github.com/marxms/go-whale/internal/ingestion/streaming"
	"github.com/marxms/go-whale/internal/trade"
)

// Source is the consumer-owned contract for one exchange feed. The ingestion side
// declares what it needs; adapters satisfy it structurally, so adding a venue requires
// only the adapter package, its tests and explicit wiring — never a change here or in a
// handler (GW-011, GW-015, GW-017). The shared conformance suite lives in
// internal/ingestion/sourcetest and every implementation is expected to pass it.
//
// # Contract
//
// Subscribe attaches a symbol. It must be idempotent for an already-attached symbol and
// must honor ctx cancellation before touching the transport. After Close it returns an
// error matching streaming.ErrClosed.
//
// GetTrades delivers the events buffered for a symbol and clears them (consume
// semantics). Delivery order per symbol is the arrival order: the implementation must
// never reorder events for one symbol, and a batch returned by one call must not contain
// events delivered after events returned by the next call. An empty slice and a nil
// slice both mean "nothing new". For a symbol without a live subscription the error
// matches streaming.ErrNotSubscribed; after Close it matches streaming.ErrClosed.
//
// Reconnect notification is part of the construction contract, not of the polling
// interface: the source reports a completed reconnect through the
// streaming.GapRecoveryQueue supplied in streaming.ClientConfig (RecoveryCallback is the
// deprecated fallback). The report carries the disconnect and reconnect instants and
// every symbol the source re-attached. A source that silently reconnects is
// non-conformant because the consumer cannot know which window to repair.
//
// Backpressure is observable through BackpressureSource: the fill ratio is the fraction
// of the per-symbol buffer currently occupied, in [0,1]. A source that cannot expose it
// simply does not implement the optional interface; the adaptive poller then uses its
// conservative cadence.
//
// Unsubscribe is idempotent and detaches the symbol. It never tears down a transport
// shared with other symbols. After Close it is a no-op.
//
// Close is idempotent and must return nil on every call. It releases the transport and
// all goroutines. It may be called after Unsubscribe or after a canceled Subscribe; the
// source must not panic or block in either case.
//
// # Errors
//
// Every method returns errors that match a canonical sentinel declared in the streaming
// package (ErrNotSubscribed, ErrNotConnected, ErrClosed, ErrBatchUnsupported) or
// context.Canceled/context.DeadlineExceeded. Vendor error strings and concrete vendor
// error types never cross this boundary.
type Source interface {
	Subscribe(ctx context.Context, symbol string) error
	Unsubscribe(symbol string) error
	GetTrades(ctx context.Context, symbol string) ([]trade.Trade, error)
	HealthCheck() bool
	Type() streaming.ClientType
	Close() error
}

// BackpressureSource is the optional capability through which a Source reports how full
// its per-symbol buffer is. The ratio is in [0,1]; 1 means the next event would be
// dropped. It is the signal the adaptive poller uses to drain fast under euphoria and
// relax in a quiet market.
type BackpressureSource interface {
	GetBufferFillRatio(symbol string) float64
}

// ReconnectSink is the consumer-owned notification port a Source calls after a completed
// reconnect. It is deliberately narrow: one batch for every symbol that was restored,
// with the disconnect window the recovery path must repair.
type ReconnectSink interface {
	EnqueueBatch(exchangeID, marketType string, symbols []string, disconnectTime, reconnectTime time.Time)
}

// SharedSource marks sources that multiplex many symbols over one transport. Their
// lifecycle is shared: unsubscribing one symbol must not close the transport for the
// others.
type SharedSource interface {
	IsSharedSubscriptionClient() bool
}

// StatsSource is the optional capability through which a Source exposes connection
// counters. It is monitoring only; no ingestion decision depends on it.
type StatsSource interface {
	ConnectionStats() streaming.ConnectionStats
}

// The port package must remain structurally assignable to the consumer contract. These
// assertions fail at compile time if either side drifts, before any adapter does.
var (
	_ Source             = streaming.StreamingClient(nil)
	_ BackpressureSource = streaming.BufferFillAware(nil)
	_ SharedSource       = streaming.SharedSubscriptionClient(nil)
	_ ReconnectSink      = streaming.GapRecoveryQueue(nil)
	_ StatsSource        = streaming.StatsProvider(nil)
)

// Canonical error values re-exported for consumers of this package. Adapters wrap the
// streaming originals, so errors.Is matches either spelling.
var (
	ErrNotSubscribed    = streaming.ErrNotSubscribed
	ErrNotConnected     = streaming.ErrNotConnected
	ErrClosed           = streaming.ErrClosed
	ErrBatchUnsupported = streaming.ErrBatchUnsupported
)
