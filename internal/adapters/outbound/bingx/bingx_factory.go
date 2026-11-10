package bingx

import (
	"fmt"
	"log/slog"
	"sync"

	logger "github.com/marxms/go-whale/internal/platform/logging"
	"github.com/marxms/go-whale/internal/platform/netdiag"
)

// ClientFactory is the BingX connection-pool catalog. It is a VALUE owned by the
// composition root, not package-level state: two factories in one process keep separate
// pools, and a test can build one without touching a global.
//
// One pool per (exchange, market type) is what keeps WebSocket connections bounded: every
// symbol on a route shares the pool, and spot and swap have distinct pools because their
// endpoints and buffers differ.
type ClientFactory struct {
	mu      sync.Mutex
	pools   map[string]*BingXConnectionPool
	netdiag *netdiag.Correlator
	log     *slog.Logger
}

// SetNetdiag injects the cross-venue disconnect correlator into every pool this factory
// creates.
func (f *ClientFactory) SetNetdiag(correlator *netdiag.Correlator) {
	f.mu.Lock()
	f.netdiag = correlator
	f.mu.Unlock()
}

// NewClientFactory returns an empty catalog. It performs no I/O and starts no goroutine.
func NewClientFactory(logs ...*slog.Logger) *ClientFactory {
	return &ClientFactory{
		pools: make(map[string]*BingXConnectionPool),
		log:   logger.OrDiscard(logs...),
	}
}

// For returns the shared pool for the requested exchange/market, creating it exactly once.
func (f *ClientFactory) For(config ClientConfig) (StreamingClient, error) {
	if config.ExchangeID != "bingx" {
		return nil, fmt.Errorf("invalid exchange ID: expected 'bingx', got '%s'", config.ExchangeID)
	}

	key := fmt.Sprintf("%s_%s", config.ExchangeID, config.MarketType)

	f.mu.Lock()
	defer f.mu.Unlock()

	if pool, ok := f.pools[key]; ok && pool != nil {
		return pool, nil
	}

	pool := newBingXPool(config, f.log)
	pool.SetNetdiag(f.netdiag)
	f.pools[key] = pool
	logger.OrDiscard(f.log).Info("bingx connection pool created",
		"key", key,
		"max_symbols_per_connection", pool.maxSymbolsPerConnection,
		"max_connections_per_ip", pool.maxConnectionsPerIP)
	return pool, nil
}

// Close closes every cached pool and empties the catalog.
func (f *ClientFactory) Close() error {
	f.mu.Lock()
	pools := f.pools
	f.pools = make(map[string]*BingXConnectionPool)
	f.mu.Unlock()

	var firstErr error
	for _, pool := range pools {
		if pool == nil {
			continue
		}
		if err := pool.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
