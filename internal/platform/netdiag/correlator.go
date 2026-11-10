// Package netdiag provides cross-exchange network diagnostics shared by the
// streaming adapters. Its job is to answer one SRE question automatically: when a
// connection drops, was it that exchange's server side, or the local network/ISP
// path? A drop that coincides with another exchange's drop points to the shared
// local path; an isolated drop points to that exchange.
package netdiag

import "sync"

// Scope classifications for a disconnect's blast radius.
const (
	ScopeLocalNetwork     = "local_network"     // >=2 exchanges dropped within the window
	ScopeExchangeSpecific = "exchange_specific" // isolated to one exchange
)

// concurrentWindowMs: two exchanges disconnecting within this window are treated
// as one shared event. The reference incident had Binance + BingX failing within
// ~90s of each other (BingX could not even re-dial for ~4min); 120s captures "same
// incident" without coalescing genuinely independent drops.
const concurrentWindowMs int64 = 120_000

// Correlator records per-exchange disconnect timestamps and classifies each new
// disconnect by whether any OTHER exchange dropped within the correlation window.
// Safe for concurrent use.
type Correlator struct {
	mu       sync.Mutex
	lastByEx map[string]int64
	metrics  Metrics
}

// Metrics is the netdiag observability port. The zero value is a safe no-op.
type Metrics struct {
	ReconnectScope func(exchange, scope string)
}

func (m Metrics) incReconnectScope(exchange, scope string) {
	if m.ReconnectScope != nil {
		m.ReconnectScope(exchange, scope)
	}
}

// SetMetrics injects the observability port.
func (c *Correlator) SetMetrics(m Metrics) { c.metrics = m }

// New returns an empty Correlator.
func New() *Correlator { return &Correlator{lastByEx: make(map[string]int64)} }

// Record stamps exchange's disconnect at nowMs, classifies it against any other
// exchange that dropped within the window, increments the scope metric, and
// returns the scope. nowMs is passed in (not read from the clock) so the logic is
// deterministically testable.
func (c *Correlator) Record(exchange string, nowMs int64) string {
	c.mu.Lock()
	concurrent := false
	for ex, ts := range c.lastByEx {
		if ex == exchange {
			continue
		}
		if delta := nowMs - ts; delta <= concurrentWindowMs && delta >= -concurrentWindowMs {
			concurrent = true
			break
		}
	}
	c.lastByEx[exchange] = nowMs
	c.mu.Unlock()

	scope := ScopeExchangeSpecific
	if concurrent {
		scope = ScopeLocalNetwork
	}
	c.metrics.incReconnectScope(exchange, scope)
	return scope
}
