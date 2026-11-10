package engine

import (
	"math"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

// TestUpdateCandle_GapRecovered_TakerMaxMerge guards the taker-field merge symmetry.
//
// UpdateCandle MAX-merges Volume for a gap-recovered bucket. If a payload arriving after a REST
// snapshot re-added its order-flow on top of the REST TOTAL, takerBuy+takerSell > volume becomes
// possible — physically impossible, and measured live on 9/18 sampled symbols, up to 23x. The
// taker pair must MAX-merge symmetrically with volume under gapRecovered.
func TestUpdateCandle_GapRecovered_TakerMaxMerge(t *testing.T) {
	const eps = 1e-9
	eng := NewCandleEngine(map[string]int{"default": 16})
	ex, mt, sym, tf := "binance", "future", "BTC/USDT", "5m"
	ts := int64(1_700_000_000_000)

	// 1) Live WS builds the open bucket with partial order-flow context.
	eng.UpdateCandle(ex, mt, sym, tf, candle.Candle{
		Timestamp: ts, Open: 100, High: 110, Low: 95, Close: 105,
		Volume: 50, TakerBuyVolume: 30, TakerSellVolume: 20,
	})

	// 2) Gap recovery: a REST snapshot writes the bucket TOTAL and marks the bucket
	// gap-recovered, so subsequent UpdateCandle calls must treat the REST value as a
	// total (MAX), never as a delta (SUM).
	eng.SeedCurrentBucketCandle(ex, mt, sym, tf, candle.Candle{
		Timestamp: ts, Open: 100, High: 110, Low: 95, Close: 105,
		Volume: 100, TakerBuyVolume: 60, TakerSellVolume: 40,
	})

	// 3) Three more WS deltas land on the SAME (recovered) bucket. They must MAX-merge,
	// never re-sum taker on top of the REST total.
	for range 3 {
		eng.UpdateCandle(ex, mt, sym, tf, candle.Candle{
			Timestamp: ts, Close: 106, Volume: 5,
			TakerBuyVolume: 3, TakerSellVolume: 2,
		})
	}

	got := eng.GetCandlesDesc(ex, mt, sym, tf, 1)
	if len(got) != 1 {
		t.Fatalf("expected 1 candle, got %d", len(got))
	}
	c := got[0]

	// Volume is MAX-gated: the REST total wins over the small deltas.
	if math.Abs(c.Volume-100) > eps {
		t.Fatalf("Volume MAX-gate: want 100 got %v", c.Volume)
	}
	// Order-flow aggregates MAX-merge symmetrically — NOT summed (summing would give 60+3*3=69).
	if math.Abs(c.TakerBuyVolume-60) > eps {
		t.Fatalf("TakerBuyVolume: want 60 (MAX) got %v — summed-bug value would be 69", c.TakerBuyVolume)
	}
	if math.Abs(c.TakerSellVolume-40) > eps {
		t.Fatalf("TakerSellVolume: want 40 (MAX) got %v", c.TakerSellVolume)
	}
	// The load-bearing physical invariant violated live (taker volume can never exceed
	// total volume). This is the assertion that fails hardest against the bug.
	if c.TakerBuyVolume+c.TakerSellVolume > c.Volume+eps {
		t.Fatalf("INVARIANT taker_sum <= volume violated: %v > %v",
			c.TakerBuyVolume+c.TakerSellVolume, c.Volume)
	}
}

// TestUpdateCandle_NotGapRecovered_TakerStillSums is the control / no-regression guard
// for the dominant live path: when a bucket was NOT gap-recovered, WS deltas MUST keep
// SUMMING all order-flow fields exactly as before the fix.
func TestUpdateCandle_NotGapRecovered_TakerStillSums(t *testing.T) {
	const eps = 1e-9
	eng := NewCandleEngine(map[string]int{"default": 16})
	ex, mt, sym, tf := "binance", "future", "ETH/USDT", "5m"
	ts := int64(1_700_000_000_000)

	// First push, then 3 incremental deltas — no SeedCurrentBucketCandle, so the bucket
	// is never marked gap-recovered and the additive path must hold.
	for range 4 {
		eng.UpdateCandle(ex, mt, sym, tf, candle.Candle{
			Timestamp: ts, Close: 50, Volume: 10,
			TakerBuyVolume: 6, TakerSellVolume: 4,
		})
	}

	got := eng.GetCandlesDesc(ex, mt, sym, tf, 1)
	if len(got) != 1 {
		t.Fatalf("expected 1 candle, got %d", len(got))
	}
	c := got[0]

	// 4 additive contributions of each field (first push + 3 SUM merges).
	if math.Abs(c.Volume-40) > eps {
		t.Fatalf("Volume SUM: want 40 got %v", c.Volume)
	}
	if math.Abs(c.TakerBuyVolume-24) > eps {
		t.Fatalf("TakerBuyVolume SUM: want 24 got %v (regression if MAX=6)", c.TakerBuyVolume)
	}
	if math.Abs(c.TakerSellVolume-16) > eps {
		t.Fatalf("TakerSellVolume SUM: want 16 got %v", c.TakerSellVolume)
	}
}

// TestUpdateCandle_GapRecoveredMarker_ClearedOnBucketRoll guards that the MAX-merge
// behavior does NOT persist beyond the recovered bucket: once a new bucket rolls over,
// the marker for the old bucket is cleared and the new bucket SUMs normally.
func TestUpdateCandle_GapRecoveredMarker_ClearedOnBucketRoll(t *testing.T) {
	const eps = 1e-9
	eng := NewCandleEngine(map[string]int{"default": 16})
	ex, mt, sym, tf := "binance", "future", "SOL/USDT", "5m"
	ts := int64(1_700_000_000_000)
	tfMs := int64(300_000)

	// Recover bucket `ts`.
	eng.UpdateCandle(ex, mt, sym, tf, candle.Candle{Timestamp: ts, Close: 10, Volume: 50, TakerBuyVolume: 30, TakerSellVolume: 20})
	eng.SeedCurrentBucketCandle(ex, mt, sym, tf, candle.Candle{Timestamp: ts, Close: 10, Volume: 100, TakerBuyVolume: 60, TakerSellVolume: 40})

	// Roll to the next bucket (clears the gap-recovered marker for `ts`), then a delta.
	eng.UpdateCandle(ex, mt, sym, tf, candle.Candle{Timestamp: ts + tfMs, Close: 11, Volume: 10, TakerBuyVolume: 6, TakerSellVolume: 4})
	eng.UpdateCandle(ex, mt, sym, tf, candle.Candle{Timestamp: ts + tfMs, Close: 11, Volume: 10, TakerBuyVolume: 6, TakerSellVolume: 4})

	got := eng.GetCandlesDesc(ex, mt, sym, tf, 1)
	if len(got) != 1 {
		t.Fatalf("expected 1 (latest) candle, got %d", len(got))
	}
	c := got[0]
	if c.Timestamp != ts+tfMs {
		t.Fatalf("latest bucket: want %d got %d", ts+tfMs, c.Timestamp)
	}
	// New bucket was never recovered → additive merge (NOT MAX).
	if math.Abs(c.Volume-20) > eps {
		t.Fatalf("new-bucket Volume SUM: want 20 got %v", c.Volume)
	}
	if math.Abs(c.TakerBuyVolume-12) > eps {
		t.Fatalf("new-bucket TakerBuyVolume SUM: want 12 got %v (marker leaked if 6)", c.TakerBuyVolume)
	}
}
