package httpapi

import (
	"sync"
	"time"
)

// rfc3339CacheMaxEntries bounds the timestamp→RFC3339 cache. Candle responses
// re-deliver the same buckets every poll (~15s), so the same ms timestamps
// recur across requests; caching their formatted strings avoids re-running
// time.Format on the per-candle hot path. The cap keeps memory bounded:
// 800 candles/timeframe × a handful of timeframes fits comfortably, and a rare
// wide range can never grow the map without limit.
const rfc3339CacheMaxEntries = 4096

// rfc3339Cache is a bounded, concurrency-safe cache mapping ms-since-epoch
// timestamps to their RFC3339 UTC string. It is handler-scoped (a Handler
// field) — no global state. When it reaches the entry cap it resets (clears)
// rather than growing, trading a cold rebuild for a hard memory bound. The
// stored string is always identical to time.UnixMilli(ts).UTC().Format(RFC3339),
// so the cache is output-neutral.
type rfc3339Cache struct {
	mu sync.Mutex
	m  map[int64]string
}

func newRFC3339Cache() *rfc3339Cache {
	return &rfc3339Cache{m: make(map[int64]string)}
}

// format returns the RFC3339 UTC string for the ms timestamp, computing and
// caching it on a miss. The returned string equals the direct time.Format.
func (c *rfc3339Cache) format(tsMillis int64) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	if s, ok := c.m[tsMillis]; ok {
		return s
	}

	s := time.UnixMilli(tsMillis).UTC().Format(time.RFC3339)

	// Hard bound: reset before exceeding the cap so the map can never grow
	// without limit (e.g. a one-off very wide range request).
	if len(c.m) >= rfc3339CacheMaxEntries {
		c.m = make(map[int64]string)
	}
	c.m[tsMillis] = s
	return s
}
