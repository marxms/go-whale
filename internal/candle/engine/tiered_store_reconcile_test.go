package engine

import (
	"context"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// TestTieredStore_ReadTierReconcile proves D4 / B2: when the hot tier
// is frozen (stream death) but the cold tier was advanced by gap-recovery backfill,
// an enabled read returns the freshest-across-tiers data instead of the stale hot
// tier — the exact bug the dev team saw via /api/timeseries (BTC@20:35 hot while
// QuestDB held 02:40 cold). With the flag off, the legacy hot-first path is kept.
func TestTieredStore_ReadTierReconcile(t *testing.T) {
	ctx := context.Background()
	const exchange, mt, sym, tf = "binance", "future", "BTC/USDT", "5m"
	const fiveMin = int64(300_000)
	now := time.Now()

	// Hot tier frozen ~6h ago; cold tier advanced to ~5m ago (strictly fresher).
	hotBase := now.Add(-6*time.Hour).UnixMilli() / fiveMin * fiveMin
	coldNewest := now.Add(-5*time.Minute).UnixMilli() / fiveMin * fiveMin

	build := func(t *testing.T) *TieredStore {
		ts := newTestTieredStore()
		for i := 0; i < 5; i++ { // 5 stale hot candles (oldest-first)
			c := candle.Candle{Timestamp: hotBase + int64(i)*fiveMin, Close: 100 + float64(i)}
			if err := ts.AddStreamingCandle(ctx, exchange, mt, sym, tf, c); err != nil {
				t.Fatalf("AddStreamingCandle[%d]: %v", i, err)
			}
		}
		cold := &mockColdStore{} // 5 fresh candles, DESC (mirrors QuestDB ORDER BY DESC)
		for i := 0; i < 5; i++ {
			cold.candles = append(cold.candles, candle.Candle{Timestamp: coldNewest - int64(i)*fiveMin, Close: 200 + float64(i)})
		}
		ts.SetColdStore(cold)
		return ts
	}

	t.Run("flag on: fresher cold shadows frozen hot", func(t *testing.T) {
		ts := build(t)
		defer ts.cancel()

		got, err := ts.GetCandlesDesc(ctx, exchange, mt, sym, tf, 5)
		if err != nil {
			t.Fatalf("GetCandlesDesc: %v", err)
		}
		if len(got) == 0 || got[0].Timestamp != coldNewest {
			t.Fatalf("newest should be cold %d, got %+v", coldNewest, got)
		}
		seen := map[int64]bool{}
		for i, c := range got {
			if seen[c.Timestamp] {
				t.Fatalf("duplicate bucket %d", c.Timestamp)
			}
			seen[c.Timestamp] = true
			if i > 0 && got[i-1].Timestamp <= c.Timestamp {
				t.Fatalf("not strictly DESC at %d: %d <= %d", i, got[i-1].Timestamp, c.Timestamp)
			}
		}
	})
}
