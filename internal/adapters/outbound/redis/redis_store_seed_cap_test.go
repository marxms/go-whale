package redisstore

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"
)

// THE SEED LEG UNDOES THE CAP EVERY NIGHT.
//
// The live cap only bites when a bucket closes. The boot seed puts `buffer_sizes[tf]` members per
// key back at once, so while it wrote without a cap the family returned to its old size on every
// restart and stayed there until the next close of each timeframe — seven days on 1w. A cap only
// one of the two paths applies is not retention.
//
// What protects history is not the absence of the cap, it is the ORDER (proved in
// internal/candle/engine/seed_cap_ordering_test.go) and the `reconnection_sync` exception,
// proved here.

// newSeedCapStore builds a RedisStore whose candle-engine block declares the dedicated cap.
func newSeedCapStore(t *testing.T, capByTF map[string]int, chartDepth int) (*RedisStore, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	store, err := NewRedisStore(t.Context(), config.Config{
		RedisAddr:          mr.Addr(),
		RedisPoolSize:      10,
		RedisDialTimeoutS:  1,
		RedisReadTimeoutS:  1,
		RedisWriteTimeoutS: 1,
		RedisPoolTimeoutS:  1,
		RedisMaxRetries:    3,
		CandleEngine: config.CandleEngineConfig{
			BufferSizes:         map[string]int{"default": chartDepth},
			RedisCapByTimeframe: capByTF,
		},
	}, chartDepth, candle.NewTimeframeRegistry([]string{"5m", "1d"}))
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	return store, store.Client()
}

// seedBatch builds n closed buckets of `timeframe` ending before endMs, oldest first.
func seedBatch(timeframe string, n int, endMs int64) []candle.Candle {
	stepMs := candle.ParseTimeframeToSeconds(timeframe) * 1000
	out := make([]candle.Candle, 0, n)
	for i := n; i >= 1; i-- {
		ts := endMs - int64(i)*stepMs
		out = append(out, candle.Candle{
			Timestamp: ts,
			Open:      100,
			High:      110,
			Low:       90,
			Close:     105,
			Volume:    1000 + float64(i),
		})
	}
	return out
}

// TestSeedCandlesBatch_AppliesTheResidentCap.
//
// THE MUTANT IT KILLS: the seed leg without a cap, which restores the old depth on every boot.
func TestSeedCandlesBatch_AppliesTheResidentCap(t *testing.T) {
	store, client := newSeedCapStore(t, map[string]int{"5m": 521}, 800)
	ctx := context.Background()

	if err := store.SeedCandlesBatch(ctx, "binance", "future", "BTC/USDT", "5m",
		seedBatch("5m", 800, 1_760_000_000_000)); err != nil {
		t.Fatalf("SeedCandlesBatch: %v", err)
	}

	card, err := client.ZCard(ctx, "candles:binance:future:BTC/USDT:5m").Result()
	if err != nil {
		t.Fatalf("ZCard: %v", err)
	}
	if card != 521 {
		t.Fatalf("ZCARD after seeding 800 = %d, want 521", card)
	}
}

// TestSeedCandlesBatch_TimeframeWithoutAnEntryKeepsEveryCandle: 1d has no dedicated entry, and
// the seed leg cannot invent one.
func TestSeedCandlesBatch_TimeframeWithoutAnEntryKeepsEveryCandle(t *testing.T) {
	store, client := newSeedCapStore(t, map[string]int{"5m": 521}, 800)
	ctx := context.Background()

	if err := store.SeedCandlesBatch(ctx, "binance", "future", "BTC/USDT", "1d",
		seedBatch("1d", 800, 1_760_000_000_000)); err != nil {
		t.Fatalf("SeedCandlesBatch: %v", err)
	}

	card, err := client.ZCard(ctx, "candles:binance:future:BTC/USDT:1d").Result()
	if err != nil {
		t.Fatalf("ZCard: %v", err)
	}
	if card != 800 {
		t.Fatalf("1d ZCARD after seeding 800 = %d, want 800 — 1d receives no cap", card)
	}
}

// TestReplaceCandle_GapBackfillIsNotCapped.
//
// THE MUTANT IT KILLS: the one that caps the backfill. `ReplaceCandle` is the
// `reconnection_sync` path: a single bucket, with no cold counterpart written in the same call.
// Truncating by rank there erases precisely the oldest candle that was just restored.
func TestReplaceCandle_GapBackfillIsNotCapped(t *testing.T) {
	store, client := newSeedCapStore(t, map[string]int{"5m": 521}, 800)
	ctx := context.Background()
	key := "candles:binance:future:BTC/USDT:5m"

	const endMs int64 = 1_760_000_000_000
	if err := store.SeedCandlesBatch(ctx, "binance", "future", "BTC/USDT", "5m",
		seedBatch("5m", 800, endMs)); err != nil {
		t.Fatalf("SeedCandlesBatch: %v", err)
	}

	// A gap candle EARLIER than everything that survived: it is what a rank cap would eat first.
	stepMs := candle.ParseTimeframeToSeconds("5m") * 1000
	gapTs := endMs - 900*stepMs
	gap := candle.Candle{Timestamp: gapTs, Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 7}

	if err := store.ReplaceCandle(ctx, "binance", "future", "BTC/USDT", "5m", gap); err != nil {
		t.Fatalf("ReplaceCandle: %v", err)
	}

	card, err := client.ZCard(ctx, key).Result()
	if err != nil {
		t.Fatalf("ZCard: %v", err)
	}
	if card != 522 {
		t.Fatalf("ZCARD after ReplaceCandle of a gap = %d, want 522 — the backfill cannot be capped", card)
	}

	oldest := storedCandleAt(t, client, key, &redis.ZRangeBy{
		Min: "-inf", Max: "+inf", Offset: 0, Count: 1,
	})
	if oldest.Timestamp != gapTs {
		t.Fatalf("oldest member ts=%d, want %d — the gap candle was truncated", oldest.Timestamp, gapTs)
	}
}
