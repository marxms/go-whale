package engine

import (
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// TestSeedCandles_RepairsInteriorHole is the guard for a SYSTEMIC GAP: every sampled symbol
// served a series that skipped a bucket.
//
// # THE SHAPE OF THE INCIDENT
//
// A 5m series jumped from 00:55 straight to 01:05. The 01:00 bucket was NOT lost — it sat in
// QuestDB — but the API never returned it, because the hot buffer lacked it and the read path
// cannot consult cold while hot holds `limit` bars.
//
// The chain, reproduced here:
//
//  1. The process was killed at 01:04:39 with the 01:00 bucket still OPEN. Cold only ever
//     receives CLOSED buckets, so 01:00 died with the previous process's RAM.
//  2. lifecycle.go runs WarmCandleEngine BEFORE the seed and BEFORE subscribing, so the ring
//     comes back holding 800 bars whose newest is 00:55.
//  3. The whole fleet is attached in one batch, already inside the 01:05 bucket, so the live
//     stream never sees 01:00. The first live push is 01:05 — an INTERIOR hole is born.
//  4. The repair (seed / gap-recovery / open-time continuity) fetches 01:00 over REST and
//     hands it to SeedCandles.
//
// # WHY A DEPTH/FRESHNESS GUARD DISCARDS INTERIOR REPAIRS
//
// Both comparisons hold BY CONSTRUCTION for any interior repair:
//   - a repair batch is always OLDER than the live buffer, because the stream has moved on;
//   - it is never DEEPER than the ring the warm boot already filled to capacity.
//
// A guard on those two facts discards the repair deterministically, not probabilistically — and
// silently: no log, no metric. The same slice then flows on to cold and Redis, which is why the
// two tiers disagree. The union has neither failure mode.
func TestSeedCandles_RepairsInteriorHole(t *testing.T) {
	t.Parallel()

	const (
		tf       = "5m"
		exchange = "binance"
		market   = "future"
		symbol   = "ARC/USDT"
		step     = int64(5 * 60 * 1000)
	)

	base := time.Date(2026, 7, 30, 22, 0, 0, 0, time.UTC).UnixMilli()
	ce := NewCandleEngine(map[string]int{"default": 800})

	// (1) Warm boot: the ring is filled to capacity, newest = the bar just before the hole.
	warm := make([]candle.Candle, 0, 800)
	for i := range 800 {
		warm = append(warm, candle.Candle{Timestamp: base + int64(i)*step, Close: 1, Volume: 10})
	}
	ce.SeedCandles(exchange, market, symbol, tf, warm)

	holeTS := warm[len(warm)-1].Timestamp + step // the bucket that was OPEN at the kill
	liveTS := holeTS + step                      // the first bucket the live stream sees

	// (2) The live stream resumes one bucket AFTER the hole.
	ce.UpdateCandle(exchange, market, symbol, tf, candle.Candle{
		Timestamp: liveTS, Close: 2, Volume: 63559,
	})

	if got := candleAt(ce, exchange, market, symbol, tf, holeTS); got != nil {
		t.Fatalf("precondition broken: the hole should not exist yet, found volume %v", got.Volume)
	}

	// (3) The repair arrives carrying ONLY the missing bucket — the shape every repair
	// producer emits, since the rest of the window is already present.
	ce.SeedCandles(exchange, market, symbol, tf, []candle.Candle{
		{Timestamp: holeTS, Close: 3, Volume: 121807, Source: string(sourceRESTTotal)},
	})

	repaired := candleAt(ce, exchange, market, symbol, tf, holeTS)
	if repaired == nil {
		t.Fatalf("the interior bucket at %d was DISCARDED — the hot tier keeps serving a hole "+
			"that cold can already fill, and the read path can never reach cold while hot holds "+
			"the requested limit", holeTS)
	}
	if repaired.Volume != 121807 {
		t.Errorf("repaired bucket volume = %v, want 121807 (the authoritative REST total)", repaired.Volume)
	}

	// (4) The repair must not cost the live tail. Losing it would trade a hole for a
	// truncation, which is the same class of defect one bucket further on.
	if live := candleAt(ce, exchange, market, symbol, tf, liveTS); live == nil {
		t.Error("the live bucket was dropped by the repair — a repair may never shorten the series")
	}

	// (5) And the window must be genuinely contiguous afterwards, not merely non-empty.
	if missing := missingBuckets(ce, exchange, market, symbol, tf, step); missing != 0 {
		t.Errorf("%d bucket(s) still missing after the repair — the series is not contiguous", missing)
	}
}

// TestSeedCandles_KeepsFreshBufferAgainstStaleReplay pins the protection the guard was
// originally written for: a genuinely older, shallower response must not clobber a current
// buffer. Fixing the discard must not swing the pendulum into "last writer wins".
func TestSeedCandles_KeepsFreshBufferAgainstStaleReplay(t *testing.T) {
	t.Parallel()

	const (
		tf   = "5m"
		step = int64(5 * 60 * 1000)
	)
	base := time.Date(2026, 7, 30, 22, 0, 0, 0, time.UTC).UnixMilli()
	ce := NewCandleEngine(map[string]int{"default": 800})

	fresh := make([]candle.Candle, 0, 100)
	for i := range 100 {
		fresh = append(fresh, candle.Candle{Timestamp: base + int64(i)*step, Close: 1, Volume: 10})
	}
	ce.SeedCandles("binance", "future", "BTC/USDT", tf, fresh)
	newest := fresh[len(fresh)-1].Timestamp

	// A stale replay of bars the buffer already holds, with WRONG values.
	stale := []candle.Candle{
		{Timestamp: base, Close: 999, Volume: 999},
		{Timestamp: base + step, Close: 999, Volume: 999},
	}
	ce.SeedCandles("binance", "future", "BTC/USDT", tf, stale)

	if got := candleAt(ce, "binance", "future", "BTC/USDT", tf, newest); got == nil {
		t.Fatal("the fresh tail was lost to a stale replay — the buffer must not be truncated")
	}
	if n := bufferLen(ce, "binance", "future", "BTC/USDT", tf); n != 100 {
		t.Errorf("buffer length = %d, want 100 — a stale replay must not shrink the series", n)
	}
}

// --- helpers -------------------------------------------------------------------------

func candleAt(ce *CandleEngine, exchange, market, symbol, tf string, ts int64) *candle.Candle {
	for _, c := range ce.GetCandlesDesc(exchange, market, symbol, tf, 10000) {
		if c.Timestamp == ts {
			cc := c
			return &cc
		}
	}
	return nil
}

func bufferLen(ce *CandleEngine, exchange, market, symbol, tf string) int {
	return len(ce.GetCandlesDesc(exchange, market, symbol, tf, 10000))
}

// missingBuckets counts absent slots on the timestamp grid BETWEEN the oldest and newest
// bar held. It deliberately measures only the interior: a short buffer is not a hole.
func missingBuckets(ce *CandleEngine, exchange, market, symbol, tf string, step int64) int {
	got := ce.GetCandlesDesc(exchange, market, symbol, tf, 10000)
	if len(got) < 2 {
		return 0
	}
	seen := make(map[int64]struct{}, len(got))
	oldest, newest := got[0].Timestamp, got[0].Timestamp
	for _, c := range got {
		seen[c.Timestamp] = struct{}{}
		if c.Timestamp < oldest {
			oldest = c.Timestamp
		}
		if c.Timestamp > newest {
			newest = c.Timestamp
		}
	}
	missing := 0
	for ts := oldest; ts <= newest; ts += step {
		if _, ok := seen[ts]; !ok {
			missing++
		}
	}
	return missing
}
