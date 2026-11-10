// Package streaming defines the client contracts that exchange adapters implement.
package streaming

import (
	"context"
	"errors"
	"time"

	"github.com/marxms/go-whale/internal/trade"
)

// Canonical source errors. Adapters translate vendor/protocol failures onto these
// sentinels at the boundary; consumers match with errors.Is and never with vendor
// strings or concrete error types. This is the error half of the anti-corruption
// contract (GW-014): a new exchange fails the shared conformance suite until it
// speaks this vocabulary.
var (
	// ErrNotSubscribed reports that a symbol has no live subscription on the source.
	// GetTrades returns it for a symbol that was never subscribed, was unsubscribed,
	// or is no longer owned after Close.
	ErrNotSubscribed = errors.New("streaming: symbol is not subscribed")

	// ErrNotConnected reports that an operation needs a live transport and none exists.
	ErrNotConnected = errors.New("streaming: source is not connected")

	// ErrClosed reports that the source was closed and no longer accepts work.
	ErrClosed = errors.New("streaming: source is closed")

	// ErrBatchUnsupported is the routing signal for "this source cannot attach a whole
	// symbol set in one transport operation; use the per-symbol path". It is not a
	// failure: callers must fall back rather than surface it.
	ErrBatchUnsupported = errors.New("streaming: batch subscription unsupported")
)

// RecoveryCallback is invoked when a client successfully reconnects
// after a disconnection, allowing the application to trigger gap recovery.
//
// Parameters:
//   - exchangeID: Exchange identifier (e.g., "binance", "bingx")
//   - marketType: Market type (e.g., "spot", "swap")
//   - symbol: Symbol that needs gap recovery (e.g., "BTC/USDT")
//   - disconnectTime: When the disconnection occurred
//   - reconnectTime: When reconnection completed
type RecoveryCallback func(
	exchangeID string,
	marketType string,
	symbol string,
	disconnectTime time.Time,
	reconnectTime time.Time,
)

// ClientType identifies the streaming client type.
type ClientType string

const (
	// ClientTypeNative is the only value any implementation returns. Every Type() in the module
	// returns Native, so branching on any other member would be dead code suggesting alternative
	// adapter strategies that do not exist.
	ClientTypeNative ClientType = "native" // Native WebSocket client
)

// StreamingClient is the unified interface for trade streaming.
// Implemented by: binancesdk.SDKClient, binancesdk.SpotSDKClient, BingXClient,
// BingXConnectionPool.
type StreamingClient interface {
	// Subscribe starts streaming a symbol.
	// Returns an error if subscribing fails.
	Subscribe(ctx context.Context, symbol string) error

	// Unsubscribe stops streaming a symbol.
	// Returns an error if unsubscribing fails.
	Unsubscribe(symbol string) error

	// GetTrades returns buffered trades for a symbol.
	// Clears the buffer after reading (consume behavior).
	// Returns an empty slice if there are no new trades.
	GetTrades(ctx context.Context, symbol string) ([]trade.Trade, error)

	// HealthCheck returns true if the client is healthy.
	// Used to decide failover in HybridClient.
	HealthCheck() bool

	// Type returns the client type.
	Type() ClientType

	// Close closes connections and releases resources.
	// It must be idempotent (may be called multiple times).
	Close() error
}

// BufferFillAware is an optional interface for clients that support buffer fill ratio
// Used by HybridManager for adaptive polling - higher fill = more frequent polling.
type BufferFillAware interface {
	// GetBufferFillRatio returns the fill ratio of the buffer for a symbol (0.0 to 1.0)
	// 0.0 = empty, 1.0 = full
	GetBufferFillRatio(symbol string) float64
}

// SharedSubscriptionClient marks clients that multiplex multiple symbols
// over shared transport resources and should not be closed from per-symbol unsubscribe paths.
type SharedSubscriptionClient interface {
	IsSharedSubscriptionClient() bool
}

// ConnectionStats holds runtime metrics for a single WebSocket client connection.
type ConnectionStats struct {
	ExchangeID     string        `json:"exchange_id"`
	MarketType     string        `json:"market_type"`
	Connected      bool          `json:"connected"`
	Reconnecting   bool          `json:"reconnecting"`
	ReconnectCount int           `json:"reconnect_count"`
	Symbols        int           `json:"symbols"`
	MessagesIn     uint64        `json:"messages_in"`      // Total messages received
	TradesIn       uint64        `json:"trades_in"`        // Total trades parsed
	TradesDropped  uint64        `json:"trades_dropped"`   // Trades dropped due to buffer overflow
	LastPongAgo    time.Duration `json:"last_pong_ago_ms"` // Time since last heartbeat
	Uptime         time.Duration `json:"uptime_ms"`        // Current connection age
	Healthy        bool          `json:"healthy"`
	HealthState    string        `json:"health_state,omitempty"` // Granular health state

	// LastDisconnect is the wall-clock time of the most recent disconnect for this
	// client, or the zero time if it has never disconnected in this process. It is
	// populated from each client's internal disconnectTime atomic. The periodic
	// gap-recovery backstop (reconcileGapsOnce) uses it to exclude
	// disconnect-triggered enqueues from the proof metric: a backstop
	// enqueue that merely trails a known disconnect is not evidence that the
	// event-path is insufficient.
	LastDisconnect time.Time `json:"last_disconnect,omitempty"`
}

// StatsProvider is an optional interface for streaming clients that expose connection metrics.
type StatsProvider interface {
	// ConnectionStats returns runtime metrics for this client.
	ConnectionStats() ConnectionStats
}

// ClientFactory is a function that creates a StreamingClient.
type ClientFactory func(config ClientConfig) (StreamingClient, error)

// GapRecoveryQueue is the narrow recovery capability a streaming client needs.
type GapRecoveryQueue interface {
	EnqueueBatch(exchangeID, marketType string, symbols []string, disconnectTime, reconnectTime time.Time)
}

// ClientConfig holds the configuration for creating clients.
// ClientConfig holds the configuration for creating clients.
type ClientConfig struct {
	// Identification
	ExchangeID string // "binance", "bingx", "gateio", etc
	MarketType string // "spot" or "swap"

	// Balancing limits (resolved from config.yaml/exchanges[].limits with fallback)
	MaxSymbolsPerConnection int
	MaxConnectionsPerIP     int

	// For native clients
	WSURL              string // WebSocket URL (e.g. wss://stream.binance.com)
	CombinedStreamBase string // Base URL for combined stream mode (.../stream?streams=)
	PingInterval       int    // Ping interval in seconds (default: 5)

	// Gap recovery callback (optional, DEPRECATED — use GapRecovery instead).
	// Called after successful reconnection to trigger gap fill via REST API.
	RecoveryCallback RecoveryCallback

	// Bounded worker pool for gap recovery.
	// When set, triggerGapRecovery() uses EnqueueBatch instead of per-symbol goroutines.
	// Takes precedence over RecoveryCallback if both are set.
	GapRecovery GapRecoveryQueue

	// TradeBufferSize overrides per-symbol trade buffer capacity (default: BinanceTradeBufferSize)
	TradeBufferSize int
}
