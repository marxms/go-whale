package binancesdk

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/marxms/go-whale/internal/ingestion/streaming"
	logger "github.com/marxms/go-whale/internal/platform/logging"
	"github.com/marxms/go-whale/internal/platform/netdiag"
)

// ClientFactory is the Binance SDK client catalog. It is a VALUE owned by the
// composition root, not a package-level registry: two processes (or two tests in one
// process) each build their own, and neither can see the other's clients.
//
// The per-market cache is the sharing mechanism the WebSocket transport needs: one
// instance per market type serves every symbol on that route, so a fleet does not dial
// one socket per symbol. Spot and swap are separate keys, so they never share buffers.
type ClientFactory struct {
	mu      sync.Mutex
	clients map[string]streaming.StreamingClient
	metrics Metrics
	netdiag *netdiag.Correlator
	log     *slog.Logger
}

// NewClientFactory returns an empty catalog. No client is created until For is called,
// so constructing a factory performs no I/O and starts no goroutine.
func NewClientFactory(logs ...*slog.Logger) *ClientFactory {
	return &ClientFactory{
		clients: make(map[string]streaming.StreamingClient),
		log:     logger.OrDiscard(logs...),
	}
}

// SetNetdiag injects the cross-venue disconnect correlator into every client this factory
// creates.
func (f *ClientFactory) SetNetdiag(correlator *netdiag.Correlator) {
	f.mu.Lock()
	f.netdiag = correlator
	f.mu.Unlock()
}

// SetMetrics injects the observability port into every client this factory creates. The
// composition root calls it before For; a zero value is a safe no-op.
func (f *ClientFactory) SetMetrics(m Metrics) {
	f.mu.Lock()
	f.metrics = m
	f.mu.Unlock()
}

// For returns the shared client for the requested market type, creating it exactly once.
//
// The lock covers creation on purpose. Concurrent calls for the same market must resolve
// to a single client — a second socket per market is the exhaustion this cache exists to
// prevent — and holding the mutex across construction is what makes that true by
// construction rather than by a check-then-act race.
func (f *ClientFactory) For(config streaming.ClientConfig) (streaming.StreamingClient, error) {
	mt := config.MarketType
	if mt != "swap" && mt != marketTypeSpot {
		return nil, fmt.Errorf("unsupported market type %q for binance SDK (expected swap or spot)", mt)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if client, ok := f.clients[mt]; ok && client != nil {
		return client, nil
	}

	logger.OrDiscard(f.log).Info("creating binance sdk client",
		"market_type", mt, "trade_buffer_size", config.TradeBufferSize)

	var client streaming.StreamingClient
	switch mt {
	case "swap":
		c := NewSDKClient(config, f.log)
		c.SetMetrics(f.metrics)
		c.SetNetdiag(f.netdiag)
		client = c
	case marketTypeSpot:
		c := NewSpotSDKClient(config, f.log)
		c.SetMetrics(f.metrics)
		c.SetNetdiag(f.netdiag)
		client = c
	}
	if client == nil {
		return nil, fmt.Errorf("binance SDK client creation failed for market=%s", mt)
	}

	f.clients[mt] = client
	return client, nil
}

// Close closes every cached client and empties the catalog. Tests use it to release the
// client goroutines they created; production lets process teardown do the same.
func (f *ClientFactory) Close() error {
	f.mu.Lock()
	clients := f.clients
	f.clients = make(map[string]streaming.StreamingClient)
	f.mu.Unlock()

	var firstErr error
	for _, client := range clients {
		if client == nil {
			continue
		}
		if err := client.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
