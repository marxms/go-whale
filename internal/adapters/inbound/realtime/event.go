// Package realtime owns the WebSocket edge: sessions, tickets/auth, pumps, watchdog and
// connection lifetime. The versioned event contract and the fan-out/ordering machinery
// live in internal/distribution; the aliases below keep this package's historical API
// for the analyzer, the web client and the existing wire goldens.
package realtime

import (
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/distribution"
)

// SchemaVersion is the envelope contract version. Bump only on a breaking payload change.
const SchemaVersion = distribution.SchemaVersion

// EventType enumerates the domain events on the realtime bus.
type EventType = distribution.EventType

const (
	// EventCandleClose is published when a candle bucket closes for a scope.
	EventCandleClose = distribution.EventCandleClose
	// EventCandleOpenUpdate carries the in-formation open candle, served in-process to
	// the WebSocket edge and never published on the shared Redis bus.
	EventCandleOpenUpdate = distribution.EventCandleOpenUpdate
	// EventCandleRevised invalidates an already-closed bucket (gap-recovery MAX-merge).
	EventCandleRevised = distribution.EventCandleRevised
	// EventHeartbeat is the per-CONNECTION liveness frame emitted on the WebSocket edge.
	EventHeartbeat = distribution.EventHeartbeat
)

// Scope identifies the (exchange, marketType, symbol, timeframe) a candle event belongs
// to. It is a comparable map key.
type Scope = distribution.Scope

// CandleDTO is the wire shape of a candle inside every realtime frame.
type CandleDTO = distribution.CandleDTO

// Envelope is the versioned wire contract for every realtime candle event.
type Envelope = distribution.Envelope

// NewCandleDTO maps a domain candle onto the wire shape.
func NewCandleDTO(c candle.Candle) CandleDTO { return distribution.NewCandleDTO(c) }

// NewEnvelope builds a versioned envelope.
func NewEnvelope(typ EventType, scope Scope, candle candle.Candle, seq uint64, emittedAt int64) Envelope {
	return distribution.NewEnvelope(typ, scope, candle, seq, emittedAt)
}

// Unmarshal parses an envelope from JSON bytes.
func Unmarshal(data []byte) (Envelope, error) { return distribution.Unmarshal(data) }

// Channel returns the prefixed Redis channel for an event type.
func Channel(prefix string, typ EventType) string { return distribution.Channel(prefix, typ) }
