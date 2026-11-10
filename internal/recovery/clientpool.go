package recovery

import (
	"errors"
	"log/slog"
	"sync"

	"github.com/marxms/go-whale/internal/market"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// errNoHistorySourceFactory is returned when a pool was built without a
// factory and is asked to create a client. The composition root always wires
// one; a test that reaches here is asking the pool to do I/O it never declared.
var errNoHistorySourceFactory = errors.New("recovery: no history source factory configured")

// LazyClientPool creates HistorySource instances on first use and reuses them
// across all recovery jobs for the same exchange:marketType pair.
//
// The factory is injected because recovery must not know which concrete
// adapter speaks to the venue: the composition root supplies the venue
// adapter, tests supply a fake. A nil factory is a declared refusal to fetch,
// not a silent fallback.
type LazyClientPool struct {
	log     *slog.Logger
	factory HistorySourceFactory
	clients map[string]HistorySource
	mu      sync.Mutex
}

// NewLazyClientPool creates an empty client pool served by factory.
func NewLazyClientPool(factory HistorySourceFactory, logs ...*slog.Logger) *LazyClientPool {
	return &LazyClientPool{factory: factory, clients: make(map[string]HistorySource), log: logger.OrDiscard(logs...)}
}

// Get returns an existing client or creates a new one lazily.
func (p *LazyClientPool) Get(exchangeID, marketType string) (HistorySource, error) {
	key := exchangeID + ":" + marketType

	p.mu.Lock()
	defer p.mu.Unlock()

	if client, ok := p.clients[key]; ok {
		return client, nil
	}

	if p.factory == nil {
		return nil, errNoHistorySourceFactory
	}

	mt, err := market.FromConfig(marketType)
	if err != nil {
		return nil, err
	}

	client, err := p.factory(exchangeID, mt)
	if err != nil {
		return nil, err
	}

	p.clients[key] = client
	logger.OrDiscard(p.log).Info("gap recovery: created shared REST client", "key", key)
	return client, nil
}

// CloseAll closes every client in the pool and empties it.
func (p *LazyClientPool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for key, client := range p.clients {
		client.Close()
		delete(p.clients, key)
	}
}
