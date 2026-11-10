package engine

import (
	"testing"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
)

// TestFlushDepthIsOne guards the D1 fast-path invariant: flushSnapshot only
// takes the zero-alloc PeekLatestCandle path because it flushes exactly one
// candle per key. If flushDepth ever changes from 1 without a matching multi-
// candle path, this test fails so the fast-path is not silently bypassed.
func TestFlushDepthIsOne(t *testing.T) {
	if snapshotFlushDepth != 1 {
		t.Fatalf("snapshotFlushDepth changed to %d: the flushSnapshot fast-path "+
			"only builds the latest candle (PeekLatestCandle). Add a multi-candle "+
			"path before raising flushDepth.", snapshotFlushDepth)
	}
}

// TestPeekLatestCandleEqualsGetCandlesDescLimit1 proves output neutrality for
// the D1 fast-path: the CandleUpdate built from PeekLatestCandle is identical
// (field-for-field, the candle.Candle is a pure value struct) to the one the
// legacy GetCandlesDesc(limit=1) path produced, across empty / partial / full /
// wraparound buffer states.
func TestPeekLatestCandleEqualsGetCandlesDescLimit1(t *testing.T) {
	const capacity = 4 // small so wraparound is easy to drive
	key := CompositeKey{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "1m"}

	mkCandle := func(ts int64) candle.Candle {
		return candle.Candle{
			Timestamp:       ts,
			Open:            float64(ts) + 1,
			High:            float64(ts) + 9,
			Low:             float64(ts) - 3,
			Close:           float64(ts) + 4,
			Volume:          float64(ts) * 2,
			TakerBuyVolume:  float64(ts),
			TakerSellVolume: float64(ts) / 2,
			Source:          "ws_delta",
		}
	}

	tests := []struct {
		name  string
		push  []int64
		empty bool
	}{
		{name: "empty", empty: true},
		{name: "single", push: []int64{1000}},
		{name: "partial", push: []int64{1000, 2000}},
		{name: "full", push: []int64{1000, 2000, 3000, 4000}},
		{name: "wraparound", push: []int64{1000, 2000, 3000, 4000, 5000, 6000}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := NewCandleEngine(map[string]int{"default": capacity})
			for _, ts := range tt.push {
				engine.UpdateCandle(key.Exchange, key.MarketType, key.Symbol, key.Timeframe, mkCandle(ts))
			}

			// Legacy path: GetCandlesDesc(limit=1) -> first element (newest).
			desc := engine.GetCandlesDesc(key.Exchange, key.MarketType, key.Symbol, key.Timeframe, 1)

			// Fast path: PeekLatestCandle (zero-alloc).
			latest, ok := engine.PeekLatestCandle(key.Exchange, key.MarketType, key.Symbol, key.Timeframe)

			if tt.empty {
				if len(desc) != 0 {
					t.Fatalf("legacy desc: want empty, got %d", len(desc))
				}
				if ok {
					t.Fatalf("PeekLatestCandle: want ok=false on empty buffer, got candle %+v", latest)
				}
				return
			}

			if len(desc) != 1 {
				t.Fatalf("legacy desc: want 1 candle, got %d", len(desc))
			}
			if !ok {
				t.Fatal("PeekLatestCandle: want ok=true, got false")
			}

			legacyUpdate := buildSnapshotUpdate(key, desc[0])
			fastUpdate := buildSnapshotUpdate(key, latest)

			if legacyUpdate != fastUpdate {
				t.Fatalf("CandleUpdate mismatch:\n legacy=%+v\n fast  =%+v", legacyUpdate, fastUpdate)
			}
		})
	}
}

// BenchmarkFlushSnapshotPerKey measures the per-key snapshot-update cost. The
// D1 fast-path (PeekLatestCandle) must allocate ~0 per key, versus the legacy
// GetCandlesDesc(limit=1) path which allocates a full SliceDesc slice of
// rb.count candles per key and discards all but index 0.
func BenchmarkFlushSnapshotPerKey(b *testing.B) {
	const capacity = 500
	key := CompositeKey{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "1m"}
	engine := NewCandleEngine(map[string]int{"default": capacity})
	for ts := int64(1); ts <= capacity; ts++ {
		engine.UpdateCandle(key.Exchange, key.MarketType, key.Symbol, key.Timeframe, candle.Candle{
			Timestamp: ts * 60000,
			Close:     float64(ts),
			Volume:    float64(ts),
		})
	}

	b.Run("legacy_GetCandlesDesc_limit1", func(b *testing.B) {
		b.ReportAllocs()
		var sink redisstore.CandleUpdate
		for i := 0; i < b.N; i++ {
			desc := engine.GetCandlesDesc(key.Exchange, key.MarketType, key.Symbol, key.Timeframe, 1)
			if len(desc) == 1 {
				sink = buildSnapshotUpdate(key, desc[0])
			}
		}
		_ = sink
	})

	b.Run("fastpath_PeekLatestCandle", func(b *testing.B) {
		b.ReportAllocs()
		var sink redisstore.CandleUpdate
		for i := 0; i < b.N; i++ {
			latest, ok := engine.PeekLatestCandle(key.Exchange, key.MarketType, key.Symbol, key.Timeframe)
			if ok {
				sink = buildSnapshotUpdate(key, latest)
			}
		}
		_ = sink
	})
}
