package coinalyze

import (
	"sync"
	"time"
)

// fakeClock is a manually advanced clock so budget tests are deterministic
// instead of sleeping. Safe for concurrent use: advance is serialized with the
// reads, and the production clock is only ever read.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}
