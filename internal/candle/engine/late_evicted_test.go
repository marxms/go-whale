package engine

import (
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

// TestLateUpdate_EvictedIsNotCountedAsInserted is the regression for a counter that
// asserted a repair had landed precisely when it had not.
//
// reason="inserted" is a CONTRACT: the bucket is in the ring, so the close-stream continuity
// check can find the hole and carry it to durability. If the Inc fires before the trim, a late
// update older than a FULL ring is inserted at index 0, sliced straight back off, and still
// reported as inserted. Nothing downstream would ever look for it again.
func TestLateUpdate_EvictedIsNotCountedAsInserted(t *testing.T) {
	const tfMs = 60_000

	t.Run("older than a FULL ring is evicted, not inserted", func(t *testing.T) {
		ce := NewCandleEngine(map[string]int{"default": 3})
		rec := newEngineTestMetrics()
		ce.SetMetrics(rec.port())
		// Fill the ring to capacity with closed buckets 3,4,5.
		for _, ts := range []int64{3 * tfMs, 4 * tfMs, 5 * tfMs} {
			ce.UpdateCandle("binance", "future", "BTC/USDT", "1m", candle.Candle{Timestamp: ts, Close: 1, Volume: 1})
		}

		beforeIns := rec.lateReason(lateReasonInserted)
		beforeEvi := rec.lateReason(lateReasonEvicted)

		// A late update for a bucket older than everything in the ring.
		ce.UpdateCandle("binance", "future", "BTC/USDT", "1m", candle.Candle{Timestamp: 1 * tfMs, Close: 9, Volume: 9})

		if got := rec.lateReason(lateReasonInserted) - beforeIns; got != 0 {
			t.Errorf(`reason="inserted" moved by %v for a bucket the trim discarded — that is the lie this fix removes`, got)
		}
		if got := rec.lateReason(lateReasonEvicted) - beforeEvi; got != 1 {
			t.Errorf(`reason="evicted" moved by %v, want 1`, got)
		}

		// And the bucket really is gone: the metric matches memory.
		if c := ce.GetCandleByTimestamp("binance", "future", "BTC/USDT", "1m", 1*tfMs); c != nil {
			t.Fatalf("bucket survived the trim; the test no longer exercises eviction")
		}
	})

	t.Run("a bucket that fits IS inserted", func(t *testing.T) {
		ce := NewCandleEngine(map[string]int{"default": 8})
		rec := newEngineTestMetrics()
		ce.SetMetrics(rec.port())
		for _, ts := range []int64{3 * tfMs, 5 * tfMs} {
			ce.UpdateCandle("binance", "future", "ETH/USDT", "1m", candle.Candle{Timestamp: ts, Close: 1, Volume: 1})
		}

		beforeIns := rec.lateReason(lateReasonInserted)
		beforeEvi := rec.lateReason(lateReasonEvicted)

		ce.UpdateCandle("binance", "future", "ETH/USDT", "1m", candle.Candle{Timestamp: 4 * tfMs, Close: 9, Volume: 9})

		if got := rec.lateReason(lateReasonInserted) - beforeIns; got != 1 {
			t.Errorf(`reason="inserted" moved by %v, want 1`, got)
		}
		if got := rec.lateReason(lateReasonEvicted) - beforeEvi; got != 0 {
			t.Errorf(`reason="evicted" moved by %v, want 0`, got)
		}
		if c := ce.GetCandleByTimestamp("binance", "future", "ETH/USDT", "1m", 4*tfMs); c == nil {
			t.Fatal(`reason="inserted" must mean the bucket IS in the ring`)
		}
	})
}
