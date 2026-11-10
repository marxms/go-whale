package engine

import (
	"math"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

// TestSeedCandles_RedisWarmBootLoadedVerbatim is the NEW-HIGH regression guard
// (deploy-blocker). On warm boot, bootstrap.go seeds CandleEngine buffers directly via
// eng.SeedCandles using candles read back from Redis (decodeCandlesV2 → decodeCandleMember).
// that the prior round added INSIDE SeedCandles re-anchored such a batch from zero,
// OVERWRITING the persisted cumulative → a CVD discontinuity at every restart on the Redis
// warm path. SeedCandles MUST load Redis/cold candles VERBATIM (never re-anchor); the
// raw-backfill reconstruction belongs ONLY on the gap-recovery/seeder path
// (ReconstructBackfillCumulative), not here.
func TestSeedCandles_RedisWarmBootLoadedVerbatim(t *testing.T) {
	const eps = 1e-9
	eng := NewCandleEngine(map[string]int{"default": 16})
	ex, mt, sym, tf := "binance", "future", "BTC/USDT", "1h"
	tfMs := int64(3_600_000)
	base := int64(1_700_000_000_000) / tfMs * tfMs

	// A real session of live closes as they were persisted to Redis, each carrying the
	// measured taker pair. Warm boot must load them VERBATIM: the seed is not allowed to
	// recompute or normalise what the live path already measured.
	persisted := []candle.Candle{
		{Timestamp: base, Close: 100, Volume: 10, TakerBuyVolume: 7, TakerSellVolume: 3},
		{Timestamp: base + tfMs, Close: 101, Volume: 12, TakerBuyVolume: 8, TakerSellVolume: 4},
		{Timestamp: base + 2*tfMs, Close: 102, Volume: 8, TakerBuyVolume: 5, TakerSellVolume: 3},
	}

	// Warm boot: this is what bootstrap.go actually hands to SeedCandles, after a round-trip
	// through the candle wire DTO.
	warm := wireRoundTrip(t, persisted)

	eng.SeedCandles(ex, mt, sym, tf, warm)

	got := eng.GetCandlesAsc(ex, mt, sym, tf, 0)
	if len(got) != len(persisted) {
		t.Fatalf("expected %d candles, got %d", len(persisted), len(got))
	}
	// The measured pair MUST survive verbatim, zeros included.
	for i := range got {
		if math.Abs(got[i].TakerBuyVolume-persisted[i].TakerBuyVolume) > eps ||
			math.Abs(got[i].TakerSellVolume-persisted[i].TakerSellVolume) > eps {
			t.Errorf("bar %d taker pair = %v/%v, want %v/%v — warm boot altered a measurement",
				i, got[i].TakerBuyVolume, got[i].TakerSellVolume,
				persisted[i].TakerBuyVolume, persisted[i].TakerSellVolume)
		}
	}
}

// TestSeedCurrentBucketCandle_PreservesTakerPair guards the taker pair through the snapshot merge.
//
// When a REST snapshot is MAX-merged into the SAME open bucket, the merge must not drop the
// taker pair the live WS path accumulated. The pair is now the only source of order flow on the
// wire: losing it here loses the measurement outright.
func TestSeedCurrentBucketCandle_PreservesTakerPair(t *testing.T) {
	const eps = 1e-9
	eng := NewCandleEngine(map[string]int{"default": 16})
	ex, mt, sym, tf := "binance", "future", "BTC/USDT", "1h"
	ts := int64(1_700_000_000_000)

	// Live WS has already built the open bucket with full order-flow context.
	live := candle.Candle{
		Timestamp:       ts,
		Open:            100,
		High:            110,
		Low:             95,
		Close:           105,
		Volume:          50,
		TakerBuyVolume:  30,
		TakerSellVolume: 20,
	}
	eng.UpdateCandle(ex, mt, sym, tf, live)

	// REST snapshot for the SAME bucket arrives (e.g. post-reconnect). It carries the
	// TOTAL bucket volume but no richer order-flow than the live path already had.
	rest := candle.Candle{
		Timestamp: ts,
		Open:      100,
		High:      112, // slightly higher high seen by REST
		Low:       95,
		Close:     106,
		Volume:    48, // REST total < accumulated; MAX merge keeps 50
	}
	eng.SeedCurrentBucketCandle(ex, mt, sym, tf, rest)

	got := eng.GetCandlesDesc(ex, mt, sym, tf, 1)
	if len(got) != 1 {
		t.Fatalf("expected 1 candle, got %d", len(got))
	}
	c := got[0]

	// OHLCV merge semantics unchanged: MAX high, MAX volume, existing close preferred.
	if math.Abs(c.High-112) > eps {
		t.Fatalf("high: want 112 got %v", c.High)
	}
	if math.Abs(c.Volume-50) > eps {
		t.Fatalf("volume MAX merge: want 50 got %v", c.Volume)
	}

	// The load-bearing assertions: the measured pair must NOT be dropped to zero.
	if math.Abs(c.TakerBuyVolume-30) > eps {
		t.Fatalf("TakerBuyVolume dropped by REST merge: want 30 got %v", c.TakerBuyVolume)
	}
	if math.Abs(c.TakerSellVolume-20) > eps {
		t.Fatalf("TakerSellVolume dropped by REST merge: want 20 got %v", c.TakerSellVolume)
	}
}

// TestSeedCurrentBucketCandle_RESTRicherOrderFlow proves that when the REST snapshot
// itself carries richer order-flow (more taker volume / trades than the accumulated
// live deltas — e.g. WS missed trades during the disconnect), the MAX-style merge keeps
// the larger order-flow figures rather than discarding the REST contribution.
func TestSeedCurrentBucketCandle_RESTRicherOrderFlow(t *testing.T) {
	const eps = 1e-9
	eng := NewCandleEngine(map[string]int{"default": 16})
	ex, mt, sym, tf := "binance", "future", "ETH/USDT", "1h"
	ts := int64(1_700_000_000_000)

	// Sparse live bucket (WS dropped trades during a gap).
	eng.UpdateCandle(ex, mt, sym, tf, candle.Candle{
		Timestamp: ts, Open: 100, High: 101, Low: 99, Close: 100, Volume: 5,
		TakerBuyVolume: 3, TakerSellVolume: 2,
	})

	// REST snapshot has the authoritative fuller bucket.
	eng.SeedCurrentBucketCandle(ex, mt, sym, tf, candle.Candle{
		Timestamp: ts, Open: 100, High: 103, Low: 98, Close: 102, Volume: 40,
		TakerBuyVolume: 25, TakerSellVolume: 15,
	})

	got := eng.GetCandlesDesc(ex, mt, sym, tf, 1)
	if len(got) != 1 {
		t.Fatalf("expected 1 candle, got %d", len(got))
	}
	c := got[0]
	if math.Abs(c.TakerBuyVolume-25) > eps {
		t.Fatalf("TakerBuyVolume: want richer REST 25 got %v", c.TakerBuyVolume)
	}
	if math.Abs(c.TakerSellVolume-15) > eps {
		t.Fatalf("TakerSellVolume: want richer REST 15 got %v", c.TakerSellVolume)
	}
	// Aggression must be recomputed from the merged taker volumes via the single source.
}
