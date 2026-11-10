package engine

import (
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

// bars builds an ascending run of candles ending at endTs, spaced by stepMs.
func bars(n int, endTs, stepMs int64) []candle.Candle {
	out := make([]candle.Candle, n)
	for i := 0; i < n; i++ {
		out[i] = candle.Candle{
			Timestamp: endTs - int64(n-1-i)*stepMs,
			Close:     float64(100 + i),
			Volume:    1,
		}
	}
	return out
}

// TestSeedCandles_FreshnessBeatsDepth is the guard for the SILENT STALE HOT TIER.
//
// # THE DEFECT
//
// The keep-existing guard was `existing.Len() >= len(seedData)` — depth, which says nothing
// about whether the existing buffer is CURRENT.
//
// lifecycle.go runs WarmCandleEngine BEFORE the seed, so after a restart the buffer is first
// filled with bars persisted BEFORE the event. When the seed then returns fresh bars from the
// exchange, `len(seedData)` is at most the buffer size, so the depth comparison held and the
// FRESH data was silently discarded. The hot tier went on serving pre-outage candles with the
// hole intact, and nothing logged that anything had been dropped.
//
// This is the mechanism behind "the candle capping stopped working" after the power outage:
// the buffer was not capped wrongly, it was frozen at pre-outage content.
func TestSeedCandles_FreshnessBeatsDepth(t *testing.T) {
	t.Parallel()

	const step = 300_000 // 5m

	ce := NewCandleEngine(map[string]int{"default": 800})

	// Warm boot: a DEEP buffer of stale, pre-outage bars.
	stale := bars(800, 1_785_000_000_000, step)
	ce.SeedCandles("binance", "future", "BTC/USDT", "5m", stale)

	// Seed after the outage: FEWER bars, but current.
	freshEnd := int64(1_785_050_000_000) // ~13,9h newer
	fresh := bars(200, freshEnd, step)
	ce.SeedCandles("binance", "future", "BTC/USDT", "5m", fresh)

	got := ce.GetCandlesDesc("binance", "future", "BTC/USDT", "5m", 1)
	if len(got) == 0 {
		t.Fatal("buffer is empty after seeding")
	}
	if got[0].Timestamp != freshEnd {
		t.Fatalf("newest bar = %d, want %d — the deeper STALE buffer won over fresher data, so the "+
			"hot tier keeps serving pre-outage candles with the hole intact and nothing logs the drop",
			got[0].Timestamp, freshEnd)
	}
}

// TestSeedCandles_KeepsDeepBufferAgainstShallowStaleResponse pins the ORIGINAL protection,
// which the freshness fix must not throw away: a response that is both shallower AND not
// newer has nothing to offer, so a deep warm buffer must survive it.
func TestSeedCandles_KeepsDeepBufferAgainstShallowStaleResponse(t *testing.T) {
	t.Parallel()

	const step = 300_000
	end := int64(1_785_000_000_000)

	ce := NewCandleEngine(map[string]int{"default": 800})

	deep := bars(800, end, step)
	ce.SeedCandles("binance", "future", "ETH/USDT", "5m", deep)

	// Same newest timestamp, far fewer bars — the classic "49 bars from the exchange API".
	shallow := bars(49, end, step)
	ce.SeedCandles("binance", "future", "ETH/USDT", "5m", shallow)

	if n := len(ce.GetCandlesDesc("binance", "future", "ETH/USDT", "5m", 2000)); n != 800 {
		t.Errorf("buffer has %d bars, want 800 — a shallow, no-fresher response replaced the deep "+
			"warm buffer and threw away history the cold tier had already paid for", n)
	}
}

// TestSeedCandles_FillsAnEmptyBuffer keeps the cold-start path working.
func TestSeedCandles_FillsAnEmptyBuffer(t *testing.T) {
	t.Parallel()

	ce := NewCandleEngine(map[string]int{"default": 800})
	ce.SeedCandles("binance", "future", "SOL/USDT", "5m", bars(120, 1_785_000_000_000, 300_000))

	if n := len(ce.GetCandlesDesc("binance", "future", "SOL/USDT", "5m", 2000)); n != 120 {
		t.Errorf("buffer has %d bars, want 120 on a cold start", n)
	}
}
