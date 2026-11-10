package realtime

import (
	"time"

	"github.com/marxms/go-whale/internal/distribution"
)

// The WebSocket edge owns liveness and connection lifetime; the fan-out actor,
// per-scope sequencing, bounded lanes and open-candle conflation live in
// internal/distribution. Hub is the thin transport wrapper that connects the two.
const (
	writeWait = 10 * time.Second
	pongWait  = 60 * time.Second
	// pingPeriod is 54s, strictly < pongWait. It is ALSO the heartbeat cadence the web
	// client's silence watchdog is calibrated against: `DEFAULT_SILENCE_TIMEOUT_MS` in
	// apps/web/src/services/chartStreamClient.ts tolerates 120s without a raw arrival. Raising this
	// past half of that threshold makes every healthy wire in a quiet market get torn down every
	// 120s — the fix becoming the defect. Both relations are asserted by TestPingPeriodCadence
	// (hub_test.go); retune this and that test tells you which coupling you broke.
	pingPeriod = pongWait * 9 / 10

	defaultEventsBuf = distribution.DefaultLaneSize // lossless lane per client; full => evict
)

// HubOption configures a Hub at construction.
type HubOption func(*Hub)

// WithEventsBufSize overrides the per-client lossless buffer (default 256). Mainly for
// tests that want deterministic, fast eviction.
func WithEventsBufSize(n int) HubOption {
	return func(h *Hub) {
		if n > 0 {
			h.eventsBufSize = n
		}
	}
}

// WithConnMaxAge bounds a connection's lifetime; on expiry the server closes it and the
// client reconnects (re-authenticating with a fresh ticket). Zero disables the bound.
func WithConnMaxAge(d time.Duration) HubOption {
	return func(h *Hub) { h.connMaxAge = d }
}

// WithPingPeriod retunes the writePump's liveness tick (default `pingPeriod`, 54s). It
// changes the period of the SINGLE existing ticker — it does not add a clock — and exists
// so a test can observe the ping/heartbeat cadence without waiting 54 seconds for one tick.
// Non-positive values are ignored.
func WithPingPeriod(d time.Duration) HubOption {
	return func(h *Hub) {
		if d > 0 {
			h.pingPeriod = d
		}
	}
}

// OpenCandleSource is the in-process source of the merged open candle.
type OpenCandleSource = distribution.OpenCandleSource

// Hub fans out realtime candle events to subscribed WebSocket clients. The actor and its
// state are owned by distribution; this wrapper carries only the transport knobs.
type Hub struct {
	*distribution.Hub

	eventsBufSize int
	connMaxAge    time.Duration
	pingPeriod    time.Duration
}

// NewHub builds a hub. opens supplies the merged open candle (in-process); clock
// returns ms epoch (time.Now().UnixMilli in production).
func NewHub(opens OpenCandleSource, clock func() int64, opts ...HubOption) *Hub {
	h := &Hub{
		Hub:           distribution.NewHub(opens, clock),
		eventsBufSize: defaultEventsBuf,
		pingPeriod:    pingPeriod,
	}
	for _, o := range opts {
		o(h)
	}
	return h
}

// add registers a client (called by ServeWS after upgrade). It copies the transport
// lifetime settings onto the client before handing it to the actor.
func (h *Hub) add(c *Client) {
	c.connMaxAge = h.connMaxAge
	c.pingPeriod = h.pingPeriod
	h.Register(c)
}
