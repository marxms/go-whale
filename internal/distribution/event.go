// Package distribution owns the transport-agnostic fan-out, ordering, sequencing and
// backpressure machinery shared by the realtime edge and the Redis Pub/Sub producer.
// It depends only on the candle domain and the standard library: the WebSocket session
// lives in adapters/inbound/realtime and the Redis transport in
// adapters/outbound/redisevents. This package never imports either.
package distribution

import (
	"encoding/json"

	"github.com/marxms/go-whale/internal/candle"
)

// SchemaVersion is the envelope contract version. Bump only on a breaking payload change.
const SchemaVersion = 1

// EventType enumerates the domain events on the realtime bus.
type EventType string

const (
	// EventCandleClose is published when a candle bucket closes for a scope.
	EventCandleClose EventType = "candle.close"
	// EventCandleOpenUpdate carries the in-formation open candle. It is served
	// in-process to the WebSocket edge (read from the CandleEngine), NOT published on
	// the shared Redis bus — it would be a firehose.
	EventCandleOpenUpdate EventType = "candle.open_update"
	// EventCandleRevised is published when an already-closed bucket is corrected
	// (gap-recovery MAX-merge). Its semantics are "invalidate and resync from this
	// timestamp", never an in-place patch.
	EventCandleRevised EventType = "candle.revised"
	// EventHeartbeat is the per-CONNECTION liveness frame emitted on the WebSocket edge.
	// It is NOT published on the Redis bus and belongs to no scope: it asserts
	// that the wire is alive, not that a scope has data — which is exactly what makes it
	// immune to a quiet market, where silence is legitimate. The protocol-level ping cannot
	// serve this purpose because a browser answers it inside its network stack and never
	// surfaces the event to JS.
	EventHeartbeat EventType = "heartbeat"
)

// Scope identifies the (exchange, marketType, symbol, timeframe) a candle event
// belongs to. All fields are comparable so a Scope is usable as a map key.
type Scope struct {
	Exchange   string `json:"exchange"`
	MarketType string `json:"marketType"`
	Symbol     string `json:"symbol"`
	Timeframe  string `json:"timeframe"`
}

// CandleDTO is the wire shape of a candle inside every realtime frame. The domain
// candle.Candle carries no JSON tags; this adapter owns the contract the analyzer
// (Redis bus) and the web client (WebSocket) consume. Field names, order and omitempty
// rules reproduce the historical candle.Candle encoding byte for byte.
type CandleDTO struct {
	Timestamp  int64   `json:"timestamp"`
	Open       float64 `json:"open"`
	High       float64 `json:"high"`
	Low        float64 `json:"low"`
	Close      float64 `json:"close"`
	Volume     float64 `json:"volume"`
	CreatedAt  int64   `json:"createdAt"`
	LastUpdate int64   `json:"lastUpdate"`

	// The measured pair is ALWAYS on the wire: zero is a real market state, and an
	// erased zero is indistinguishable from a bar the producer never measured.
	TakerBuyVolume  float64 `json:"takerBuyVolume"`
	TakerSellVolume float64 `json:"takerSellVolume"`
	Source          string  `json:"source,omitempty"`
	AggregateMethod string  `json:"aggregateMethod,omitempty"`

	FetchedAt        int64 `json:"fetchedAt,omitempty"`
	RequestedAt      int64 `json:"requestedAt,omitempty"`
	LastTradeAt      int64 `json:"lastTradeAt,omitempty"`
	KlineEventAt     int64 `json:"klineEventAt,omitempty"`
	SideUnknownCount int64 `json:"sideUnknownCount,omitempty"`
	LateTradeCount   int64 `json:"lateTradeCount,omitempty"`
	HasGap           bool  `json:"hasGap,omitempty"`
}

// NewCandleDTO maps a domain candle onto the wire shape. Origin is process-local
// provenance and deliberately has no representation on the bus or the socket.
func NewCandleDTO(c candle.Candle) CandleDTO {
	return CandleDTO{
		Timestamp:        c.Timestamp,
		Open:             c.Open,
		High:             c.High,
		Low:              c.Low,
		Close:            c.Close,
		Volume:           c.Volume,
		CreatedAt:        c.CreatedAt,
		LastUpdate:       c.LastUpdate,
		TakerBuyVolume:   c.TakerBuyVolume,
		TakerSellVolume:  c.TakerSellVolume,
		Source:           c.Source,
		AggregateMethod:  c.AggregateMethod,
		FetchedAt:        c.FetchedAt,
		RequestedAt:      c.RequestedAt,
		LastTradeAt:      c.LastTradeAt,
		KlineEventAt:     c.KlineEventAt,
		SideUnknownCount: c.SideUnknownCount,
		LateTradeCount:   c.LateTradeCount,
		HasGap:           c.HasGap,
	}
}

// ToCandle maps a wire candle back to the domain value. Origin is always Unknown: it
// never crosses the boundary.
func (d CandleDTO) ToCandle() candle.Candle {
	return candle.Candle{
		Timestamp:        d.Timestamp,
		Open:             d.Open,
		High:             d.High,
		Low:              d.Low,
		Close:            d.Close,
		Volume:           d.Volume,
		CreatedAt:        d.CreatedAt,
		LastUpdate:       d.LastUpdate,
		TakerBuyVolume:   d.TakerBuyVolume,
		TakerSellVolume:  d.TakerSellVolume,
		Source:           d.Source,
		AggregateMethod:  d.AggregateMethod,
		FetchedAt:        d.FetchedAt,
		RequestedAt:      d.RequestedAt,
		LastTradeAt:      d.LastTradeAt,
		KlineEventAt:     d.KlineEventAt,
		SideUnknownCount: d.SideUnknownCount,
		LateTradeCount:   d.LateTradeCount,
		HasGap:           d.HasGap,
	}
}

// Envelope is the versioned wire contract for every realtime candle event.
type Envelope struct {
	Version   int       `json:"v"`
	Seq       uint64    `json:"seq"`
	Type      EventType `json:"type"`
	Scope     Scope     `json:"scope"`
	Candle    CandleDTO `json:"candle"`
	EmittedAt int64     `json:"emittedAt"` // producer clock, ms epoch
}

// NewEnvelope builds a versioned envelope. The caller supplies seq (per-channel
// monotonic) and emittedAt (ms epoch) so the contract stays a pure, testable value —
// no clock or global counter is read here.
func NewEnvelope(typ EventType, scope Scope, candle candle.Candle, seq uint64, emittedAt int64) Envelope {
	return Envelope{
		Version:   SchemaVersion,
		Seq:       seq,
		Type:      typ,
		Scope:     scope,
		Candle:    NewCandleDTO(candle),
		EmittedAt: emittedAt,
	}
}

// Marshal serializes the envelope to JSON bytes for the bus or the socket.
func (e Envelope) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

// Unmarshal parses an envelope from JSON bytes.
func Unmarshal(data []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(data, &e); err != nil {
		return Envelope{}, err
	}
	return e, nil
}

// Channel returns the prefixed Redis channel for an event type. The prefix isolates
// environments and producer instances, because Pub/Sub ignores the Redis db number.
func Channel(prefix string, typ EventType) string {
	return prefix + ":" + string(typ)
}
