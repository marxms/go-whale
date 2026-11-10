package binancesdk

import (
	"math"
	"strings"
	"time"
)

// saturatingInt converts a monotonic counter to int without wrapping on 32-bit builds.
func saturatingInt(v uint64) int {
	if v > math.MaxInt {
		return math.MaxInt
	}
	return int(v)
}

// reconnectReasonLabel collapses a free-form reconnect reason into a bounded label
// set so ReconnectAttemptsTotal cannot grow unbounded Prometheus cardinality from
// reasons that embed a formatted duration or counter (e.g. "no messages for 32m15s
// (stalledCount=3)"). The verbose reason is preserved in logs and SDK events.
func reconnectReasonLabel(reason string) string {
	switch {
	case strings.HasPrefix(reason, "no messages for"):
		return "stall"
	case strings.Contains(reason, "liveness stall"):
		return "liveness"
	case strings.Contains(reason, "socket"):
		return "socket"
	case strings.Contains(reason, "not marked as connected"):
		return "not_connected"
	default:
		return "other"
	}
}

// Reconnect backoff tuning. These are package-level vars (not consts) so tests can
// shrink the base/cap to keep the retry loop deterministic and network-free.
// Defaults mirror the BingX FSM: base 1s, ×2 backoff, cap 30s, ±25% jitter.
const (
	reconnectBackoffBase = 1 * time.Second
	reconnectBackoffMax  = 30 * time.Second
	reconnectBackoffRate = 2.0
)

// socketCleanupDelay is how long reconnectAndResubscribe waits after closing the
// old WebSocket before re-dialing, so the OS releases the socket and SDK
// goroutines settle. Package-level var (not const) so tests can shrink it.
const socketCleanupDelay = 2 * time.Second

// Watchdog stall gate (D2). The connection monitor must never count a stall
// while a reconnect/resubscribe is in flight, and must re-arm its stall clock
// only after resubscribe completes plus a grace. Package-level vars so tests can
// shrink them.
const (
	// stallBaseThreshold is the floor for the no-message stall threshold.
	stallBaseThreshold = 30 * time.Second

	// handshakeBudget is the worst-case combined-URL handshake + resubscribe
	// budget folded into the stall threshold so a slow handshake can never be
	// mistaken for a stall.
	handshakeBudget = 10 * time.Second

	// resubscribeGrace is the settle window after resubscribe-complete before the
	// stall clock is considered live; protects the first-message gap.
	resubscribeGrace = 15 * time.Second
)

// stallThreshold is the effective no-message age that counts as a stall (D2):
// max(stallBaseThreshold, handshakeBudget+resubscribeGrace).
func stallThreshold() time.Duration {
	if budget := handshakeBudget + resubscribeGrace; budget > stallBaseThreshold {
		return budget
	}
	return stallBaseThreshold
}

// stallClockHeld reports whether the watchdog stall clock must be HELD (no stall
// counted, no reconnect requested) at instant `now` (D2). The clock is held
// while a reconnect/resubscribe is in progress, and for resubscribeGrace after
// the resubscribe completed, so the first-message gap on freshly (re)subscribed
// streams is never mistaken for a stall. Shared by the futures and spot
// connection monitors so the gate cannot drift between them.
func stallClockHeld(reconnecting bool, resubscribedAt, now time.Time) bool {
	if reconnecting {
		return true
	}
	if !resubscribedAt.IsZero() && now.Sub(resubscribedAt) < resubscribeGrace {
		return true
	}
	return false
}
