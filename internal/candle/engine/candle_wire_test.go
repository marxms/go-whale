package engine

import (
	"encoding/json"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

// candleJSONWire is a TEST-LOCAL copy of the historical candle JSON contract. It exists
// so engine tests can exercise a real serialize/deserialize boundary without importing an
// adapter package (engine must stay free of inbound-adapter dependencies, in tests too).
//
// The production owners of this contract are the adapter DTOs:
// realtime.CandleDTO, httpapi.CandleDTO and redisstore.candleLegacyWire. Their tests pin
// the names, the taker-pair zeros, the omitempty rules and the Origin omission; this copy
// is only the transport for the engine-level regression guards below. Origin is absent by
// construction, which is the same guarantee the adapters enforce.
type candleJSONWire struct {
	Timestamp  int64   `json:"timestamp"`
	Open       float64 `json:"open"`
	High       float64 `json:"high"`
	Low        float64 `json:"low"`
	Close      float64 `json:"close"`
	Volume     float64 `json:"volume"`
	CreatedAt  int64   `json:"createdAt"`
	LastUpdate int64   `json:"lastUpdate"`

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

func candleToJSONWire(c candle.Candle) candleJSONWire {
	return candleJSONWire{
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

func jsonWireToCandle(w candleJSONWire) candle.Candle {
	return candle.Candle{
		Timestamp:        w.Timestamp,
		Open:             w.Open,
		High:             w.High,
		Low:              w.Low,
		Close:            w.Close,
		Volume:           w.Volume,
		CreatedAt:        w.CreatedAt,
		LastUpdate:       w.LastUpdate,
		TakerBuyVolume:   w.TakerBuyVolume,
		TakerSellVolume:  w.TakerSellVolume,
		Source:           w.Source,
		AggregateMethod:  w.AggregateMethod,
		FetchedAt:        w.FetchedAt,
		RequestedAt:      w.RequestedAt,
		LastTradeAt:      w.LastTradeAt,
		KlineEventAt:     w.KlineEventAt,
		SideUnknownCount: w.SideUnknownCount,
		LateTradeCount:   w.LateTradeCount,
		HasGap:           w.HasGap,
	}
}

// wireRoundTrip exercises the boundary that carries a candle between processes. It
// reproduces the field set the Redis warm path exposes, so the guard still checks real
// serialization instead of a hand-built struct.
func wireRoundTrip(t *testing.T, in []candle.Candle) []candle.Candle {
	t.Helper()
	out := make([]candle.Candle, 0, len(in))
	for i := range in {
		raw, err := json.Marshal(candleToJSONWire(in[i]))
		if err != nil {
			t.Fatalf("marshal candle %d: %v", i, err)
		}
		var decoded candleJSONWire
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("unmarshal candle %d: %v", i, err)
		}
		out = append(out, jsonWireToCandle(decoded))
	}
	return out
}
