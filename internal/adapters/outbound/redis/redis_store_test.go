package redisstore

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"
)

// newTestRedisStore creates a RedisStore backed by miniredis.
func newTestRedisStore(t *testing.T) (*RedisStore, *redis.Client) {
	mr := miniredis.RunT(t)
	cfg := config.Config{
		RedisAddr:          mr.Addr(),
		RedisPoolSize:      10,
		RedisDB:            0,
		RedisDialTimeoutS:  1,
		RedisReadTimeoutS:  1,
		RedisWriteTimeoutS: 1,
		RedisPoolTimeoutS:  1,
		RedisMaxRetries:    3,
	}
	store, err := NewRedisStore(t.Context(), cfg, 100, candle.NewTimeframeRegistry([]string{"1m", "5m", "15m", "30m", "1h", "4h", "1d"}))
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	return store, store.Client()
}

// TestSeedCandlesBatch_ZCardPreserved verifies that repeated backfills do NOT
// reduce the ZSET cardinality.
func TestSeedCandlesBatch_ZCardPreserved(t *testing.T) {
	store, client := newTestRedisStore(t)
	ctx := context.Background()

	ts := time.Now().Add(-2 * time.Hour).Truncate(time.Hour).UnixMilli()
	bar := candle.Candle{
		Timestamp:       ts,
		Open:            100,
		High:            110,
		Low:             90,
		Close:           105,
		Volume:          1000,
		TakerBuyVolume:  500,
		TakerSellVolume: 300,
	}

	key := "candles:binance:spot:BTC/USDT:1h"

	if err := store.SeedCandlesBatch(ctx, "binance", "spot", "BTC/USDT", "1h", []candle.Candle{bar}); err != nil {
		t.Fatalf("first seed failed: %v", err)
	}

	firstCard, err := client.ZCard(ctx, key).Result()
	if err != nil {
		t.Fatalf("zcard failed: %v", err)
	}
	if firstCard != 1 {
		t.Fatalf("expected ZCard=1 after first seed, got %d", firstCard)
	}

	// Seed identical candle again; ZCard must stay 1.
	if err := store.SeedCandlesBatch(ctx, "binance", "spot", "BTC/USDT", "1h", []candle.Candle{bar}); err != nil {
		t.Fatalf("second seed failed: %v", err)
	}

	secondCard, err := client.ZCard(ctx, key).Result()
	if err != nil {
		t.Fatalf("zcard failed: %v", err)
	}
	if secondCard != 1 {
		t.Fatalf("expected ZCard=1 after second seed, got %d", secondCard)
	}

	// Verify source is set for new candle.
	stored := storedCandleAt(t, client, key, &redis.ZRangeBy{Min: "-inf", Max: "+inf"})
	if stored.Source != "gap-recovery" {
		t.Fatalf("expected source='gap-recovery', got %q", stored.Source)
	}
}

// TestSeedCandlesBatch_StructuralMerge verifies that additive aggregates
// (TakerBuyVolume, TakerSellVolume) are preserved during backfill even when
// the backfill payload carries zero values.
func TestSeedCandlesBatch_StructuralMerge(t *testing.T) {
	store, client := newTestRedisStore(t)
	ctx := context.Background()

	ts := time.Now().Add(-2 * time.Hour).Truncate(time.Hour).UnixMilli()

	// Existing candle with rich additive metadata
	existing := candle.Candle{
		Timestamp:       ts,
		Open:            100,
		High:            110,
		Low:             90,
		Close:           105,
		Volume:          1000,
		TakerBuyVolume:  400, // must survive merge
		TakerSellVolume: 300,
	}

	// First seed to establish the existing record
	if err := store.SeedCandlesBatch(ctx, "binance", "swap", "ETH/USDT", "1h", []candle.Candle{existing}); err != nil {
		t.Fatalf("seed existing failed: %v", err)
	}

	// Backfill with a higher high, lower low, but additive fields zeroed
	backfill := candle.Candle{
		Timestamp:       ts,
		Open:            99,  // different
		High:            115, // higher
		Low:             85,  // lower
		Close:           107, // updated
		Volume:          500, // lower
		TakerBuyVolume:  0,   // zero from upstream
		TakerSellVolume: 0,   // zero from upstream
	}

	if err := store.SeedCandlesBatch(ctx, "binance", "swap", "ETH/USDT", "1h", []candle.Candle{backfill}); err != nil {
		t.Fatalf("backfill seed failed: %v", err)
	}

	key := "candles:binance:swap:ETH/USDT:1h"
	merged := storedCandleAt(t, client, key, &redis.ZRangeBy{Min: "-inf", Max: "+inf"})

	if merged.TakerBuyVolume != 400 {
		t.Errorf("TakerBuyVolume: expected 400 (preserved), got %.2f", merged.TakerBuyVolume)
	}
	if merged.TakerSellVolume != 300 {
		t.Errorf("TakerSellVolume: expected 300 (preserved), got %.2f", merged.TakerSellVolume)
	}
	if merged.Open != 99 {
		t.Errorf("Open: expected 99 (from backfill), got %.2f", merged.Open)
	}
	if merged.High != 115 {
		t.Errorf("High: expected 115 (higher), got %.2f", merged.High)
	}
	if merged.Low != 85 {
		t.Errorf("Low: expected 85 (lower), got %.2f", merged.Low)
	}
	if merged.Close != 107 {
		t.Errorf("Close: expected 107 (from backfill), got %.2f", merged.Close)
	}
	if merged.Volume != 1000 {
		t.Errorf("Volume: expected 1000 (max preserved), got %.2f", merged.Volume)
	}
}
