package binancesdk

import (
	"context"
	"sync"
	"time"
)

// subscribeMinInterval throttles outbound SUBSCRIBE frames. Binance USDⓈ-M futures
// allows <=10 inbound messages/second per connection; 200ms (5/s) stays well under
// that for both startup and reconnect resubscription.
const subscribeMinInterval = 200 * time.Millisecond

// subscribePacer enforces a minimum interval between SUBSCRIBE frames so startup
// and reconnect never trigger Binance's rate-limit close (1008) / disconnect storm.
type subscribePacer struct {
	mu       sync.Mutex
	interval time.Duration
	next     time.Time // earliest instant the next frame may be sent
}

func newSubscribePacer(interval time.Duration) *subscribePacer {
	return &subscribePacer{interval: interval}
}

// wait blocks until this caller's reserved slot. The slot is reserved under a
// brief lock (advancing next), then the sleep happens WITHOUT holding the mutex
// (N5 fix) — so the lock is never held across I/O and concurrent callers queue on
// distinct slots instead of serializing on the mutex. Context-aware so shutdown
// is never blocked.
func (p *subscribePacer) wait(ctx context.Context) error {
	p.mu.Lock()
	now := time.Now()
	slot := p.next
	if slot.Before(now) {
		slot = now
	}
	p.next = slot.Add(p.interval)
	p.mu.Unlock()

	d := time.Until(slot)
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
