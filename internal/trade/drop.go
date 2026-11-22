package trade

import "github.com/marxms/go-whale/internal/market"

// DropReason is the CLOSED set of ways a trade can be lost locally, before it
// reaches candle aggregation. It is a named type so a new path cannot invent a
// free-form reason that no dashboard, alert or test knows about.
type DropReason string

const (
	// DropBufferOverflow: the per-symbol buffer refused the trade at capacity.
	DropBufferOverflow DropReason = "BUFFER_OVERFLOW"
	// DropMalformedFrame: the venue frame could not be decoded.
	DropMalformedFrame DropReason = "MALFORMED_FRAME"
	// DropNilTrade: the decoded frame had no usable trade (missing price/size/id).
	DropNilTrade DropReason = "NIL_TRADE"
	// DropPanic: the callback recovered from a panic.
	DropPanic DropReason = "PANIC"
	// DropCleanupDiscard: a disconnect cleanup discarded a buffered trade.
	DropCleanupDiscard DropReason = "CLEANUP_DISCARD"
)

// DropReasons is the closed taxonomy, in a stable order so pre-registration
// cannot drift from the increment sites.
func DropReasons() []DropReason {
	return []DropReason{
		DropBufferOverflow,
		DropMalformedFrame,
		DropNilTrade,
		DropPanic,
		DropCleanupDiscard,
	}
}

// Valid reports whether the reason belongs to the closed taxonomy.
func (r DropReason) Valid() bool {
	switch r {
	case DropBufferOverflow, DropMalformedFrame, DropNilTrade, DropPanic, DropCleanupDiscard:
		return true
	default:
		return false
	}
}

// DropEvent carries the full location of a local loss: reason, venue, market,
// symbol and the real connection partition. A drop without a location is
// indistinguishable from an unmeasured path, which is what this exists to
// prevent. No payload, trade id or credential belongs here.
type DropEvent struct {
	Reason     DropReason
	Exchange   string
	MarketType market.MarketType
	Symbol     string
	Partition  int
}
