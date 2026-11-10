package bingx

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	logger "github.com/marxms/go-whale/internal/platform/logging"
	"github.com/marxms/go-whale/internal/platform/netdiag"
	"github.com/marxms/go-whale/internal/trade"
)

const (
	// DefaultMaxSymbolsPerConnection is BingX's official 200-channel limit per
	// connection; the pool auto-creates another connection when it is reached.
	DefaultMaxSymbolsPerConnection = 200
	// DefaultBingXMaxConnectionsPerIP bounds the connections one IP may open.
	DefaultBingXMaxConnectionsPerIP = 60
)

// BingXConnectionPool manages multiple WebSocket connections to distribute load.
type BingXConnectionPool struct {
	config ClientConfig
	log    *slog.Logger

	maxSymbolsPerConnection int
	maxConnectionsPerIP     int

	connections []*BingXClient
	symbolMap   map[string]int // symbol -> connection index
	mu          sync.RWMutex
	netdiag     *netdiag.Correlator

	ctx    context.Context
	cancel context.CancelFunc
}

// newBingXPool builds one pool. It performs no I/O and no caching: the cache is owned by
// ClientFactory, so two factories in one process are two independent catalogs.
func newBingXPool(config ClientConfig, logs ...*slog.Logger) *BingXConnectionPool {
	ctx, cancel := context.WithCancel(context.Background())

	return &BingXConnectionPool{
		config:                  config,
		log:                     logger.OrDiscard(logs...),
		maxSymbolsPerConnection: resolvePositive(config.MaxSymbolsPerConnection, DefaultMaxSymbolsPerConnection),
		maxConnectionsPerIP:     resolvePositive(config.MaxConnectionsPerIP, DefaultBingXMaxConnectionsPerIP),
		connections:             []*BingXClient{},
		symbolMap:               make(map[string]int),
		ctx:                     ctx,
		cancel:                  cancel,
	}
}

// SetNetdiag injects the cross-venue disconnect correlator into this pool's clients.
func (p *BingXConnectionPool) SetNetdiag(correlator *netdiag.Correlator) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.netdiag = correlator
	for _, client := range p.connections {
		client.SetNetdiag(correlator)
	}
}

// Type returns the client type (native).
func (p *BingXConnectionPool) Type() ClientType {
	return ClientTypeNative
}

// Subscribe adds a symbol to the pool, creating a new connection if needed.
func (p *BingXConnectionPool) Subscribe(ctx context.Context, symbol string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check whether already subscribed
	if _, exists := p.symbolMap[symbol]; exists {
		logger.OrDiscard(p.log).Debug("bingx pool: symbol already subscribed", "symbol", symbol)
		return nil
	}

	// Find a connection with capacity, or create a new one
	connIndex := -1
	for i := range p.connections {
		if p.countSymbolsInConnection(i) < p.maxSymbolsPerConnection {
			connIndex = i
			break
		}
	}

	// Create a new connection if needed
	if connIndex == -1 {
		if len(p.connections) >= p.maxConnectionsPerIP {
			return fmt.Errorf("pool at capacity: %d/%d connections, limit %d symbols/connection", len(p.connections), p.maxConnectionsPerIP, p.maxSymbolsPerConnection)
		}

		logger.OrDiscard(p.log).Info("bingx pool: creating connection",
			"connections", len(p.connections), "total_symbols", len(p.symbolMap)+1)

		client := NewBingXClient(p.config, p.log)
		client.SetNetdiag(p.netdiag)
		if err := client.Connect(ctx); err != nil {
			return fmt.Errorf("failed to connect a new connection: %w", err)
		}

		p.connections = append(p.connections, client)
		connIndex = len(p.connections) - 1

		logger.OrDiscard(p.log).Info("bingx pool: connection created", "connection", connIndex)
	}

	// Subscribe on the client
	if err := p.connections[connIndex].Subscribe(ctx, symbol); err != nil {
		return fmt.Errorf("failed to subscribe %s: %w", symbol, err)
	}

	// Record the mapping
	p.symbolMap[symbol] = connIndex
	logger.OrDiscard(p.log).Debug("bingx pool: symbol assigned",
		"symbol", symbol,
		"connection", connIndex,
		"connection_symbols", p.countSymbolsInConnection(connIndex),
		"max_symbols", p.maxSymbolsPerConnection)

	return nil
}

// Unsubscribe removes a symbol from the pool.
func (p *BingXConnectionPool) Unsubscribe(symbol string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	connIndex, exists := p.symbolMap[symbol]
	if !exists {
		return nil
	}

	if connIndex < len(p.connections) {
		if err := p.connections[connIndex].Unsubscribe(symbol); err != nil {
			logger.OrDiscard(p.log).Warn("bingx pool: unsubscribe failed", "symbol", symbol, "error", err)
		}
	}

	delete(p.symbolMap, symbol)
	return nil
}

// GetTrades returns trades for a symbol.
func (p *BingXConnectionPool) GetTrades(ctx context.Context, symbol string) ([]trade.Trade, error) {
	p.mu.RLock()
	connIndex, exists := p.symbolMap[symbol]
	p.mu.RUnlock()

	if !exists || connIndex >= len(p.connections) {
		return nil, nil
	}

	return p.connections[connIndex].GetTrades(ctx, symbol)
}

// HealthCheck checks the health of all connections.
func (p *BingXConnectionPool) HealthCheck() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.connections) == 0 {
		return false
	}

	// All connections must be healthy
	for _, conn := range p.connections {
		if !conn.HealthCheck() {
			return false
		}
	}

	return true
}

// Close closes all connections.
func (p *BingXConnectionPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.cancel()

	var lastErr error
	for i, conn := range p.connections {
		if err := conn.Close(); err != nil {
			logger.OrDiscard(p.log).Warn("bingx pool: closing connection failed", "connection", i, "error", err)
			lastErr = err
		}
	}

	logger.OrDiscard(p.log).Info("bingx pool: closed",
		"connections", len(p.connections), "symbols", len(p.symbolMap))

	return lastErr
}

// Stats returns pool statistics.
func (p *BingXConnectionPool) Stats() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	connStats := make([]int, len(p.connections))
	for i := range p.connections {
		connStats[i] = p.countSymbolsInConnection(i)
	}

	return map[string]interface{}{
		"total_connections":      len(p.connections),
		"total_symbols":          len(p.symbolMap),
		"symbols_per_conn":       connStats,
		"max_per_conn":           p.maxSymbolsPerConnection,
		"max_connections_per_ip": p.maxConnectionsPerIP,
	}
}

// ConnectionStats implements the StatsProvider interface by aggregating stats from all
// connections, so the health endpoint works correctly.
func (p *BingXConnectionPool) ConnectionStats() ConnectionStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.connections) == 0 {
		return ConnectionStats{
			ExchangeID: p.config.ExchangeID,
			MarketType: p.config.MarketType,
			Connected:  false,
			Symbols:    0,
			Healthy:    false,
		}
	}

	// Aggregate stats from all connections in the pool
	var totalMessagesIn, totalTradesIn, totalTradesDropped uint64
	var totalReconnectCount int
	var anyConnected, anyHealthy bool
	var oldestUptime, maxLastPongAgo time.Duration

	for _, conn := range p.connections {
		stats := conn.ConnectionStats()
		totalMessagesIn += stats.MessagesIn
		totalTradesIn += stats.TradesIn
		totalTradesDropped += stats.TradesDropped
		totalReconnectCount += stats.ReconnectCount

		if stats.Connected {
			anyConnected = true
		}
		if stats.Healthy {
			anyHealthy = true
		}
		if stats.Uptime > oldestUptime {
			oldestUptime = stats.Uptime
		}
		if stats.LastPongAgo > maxLastPongAgo {
			maxLastPongAgo = stats.LastPongAgo
		}
	}

	return ConnectionStats{
		ExchangeID:     p.config.ExchangeID,
		MarketType:     p.config.MarketType,
		Connected:      anyConnected,
		Reconnecting:   false,
		ReconnectCount: totalReconnectCount,
		Symbols:        len(p.symbolMap),
		MessagesIn:     totalMessagesIn,
		TradesIn:       totalTradesIn,
		TradesDropped:  totalTradesDropped,
		LastPongAgo:    maxLastPongAgo,
		Uptime:         oldestUptime,
		Healthy:        anyHealthy,
	}
}

// countSymbolsInConnection counts symbols on a specific connection.
func (p *BingXConnectionPool) countSymbolsInConnection(connIndex int) int {
	count := 0
	for _, idx := range p.symbolMap {
		if idx == connIndex {
			count++
		}
	}
	return count
}

// Compile-time guards: the pool is what the factory returns, so every optional capability
// the inner BingXClient implements MUST also be reachable through the pool. Without these
// the loss is silent — a consumer's type assertion just returns ok=false.
var (
	_ StreamingClient = (*BingXConnectionPool)(nil)
	_ BufferFillAware = (*BingXConnectionPool)(nil)
	_ StatsProvider   = (*BingXConnectionPool)(nil)
)

// GetBufferFillRatio forwards to the connection that owns the symbol.
//
// The factory returns the POOL, not the client, so this forward is what keeps BingX symbols
// inside the adaptive-polling path. While it was missing, every BingX symbol failed the
// HybridManager's BufferFillAware assertion, fell back to maxProcessDelay, and vanished
// from the aggregate avg_buffer_fill_ratio — with no error and no log.
//
// Returns 0 for an unknown symbol, which the caller reads as "no signal from this client"
// and is the same value an empty buffer yields.
func (p *BingXConnectionPool) GetBufferFillRatio(symbol string) float64 {
	p.mu.RLock()
	connIndex, exists := p.symbolMap[symbol]
	conns := len(p.connections)
	p.mu.RUnlock()

	if !exists || connIndex >= conns {
		return 0
	}
	return p.connections[connIndex].GetBufferFillRatio(symbol)
}

// NOTE (deliberately not fixed here): BingXConnectionPool does NOT implement
// SharedSubscriptionClient, so unsubscribing the LAST symbol makes HybridManager.Remove
// call Close() on it — even though the pool is a per-exchange singleton from ClientFactory
// whose lifecycle is managed externally, which is precisely the case that interface exists
// to protect. Adding it would be a behavior change beyond capability forwarding, so it is
// recorded rather than bundled. The conformance test does not flag it because the inner
// BingXClient does not implement the interface either — nothing is being DROPPED, the
// capability was never declared on this path.
