package health

import "sync"

// PollerHealth tracks whether a poller is currently alive.
type PollerHealth struct {
	mu    sync.RWMutex
	alive bool
}

// SetAlive updates the alive state.
func (p *PollerHealth) SetAlive(v bool) {
	p.mu.Lock()
	p.alive = v
	p.mu.Unlock()
}

// IsAlive returns the current alive state.
func (p *PollerHealth) IsAlive() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.alive
}
