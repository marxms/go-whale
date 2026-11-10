package recovery

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"
)

func newValidatorTestStore(t *testing.T) (*redisstore.RedisStore, *redis.Client) {
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
	store, err := redisstore.NewRedisStore(t.Context(), cfg, 100, candle.NewTimeframeRegistry([]string{"1m", "5m", "15m", "30m", "1h", "4h", "1d"}))
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	return store, store.Client()
}

// TestRedisValidator_Range_WithGap verifies that inserting candles with an
// intentional gap is detected, and filling the gap resolves it. Task 5.4.
func TestRedisValidator_Range_WithGap(t *testing.T) {
	store, _ := newValidatorTestStore(t)
	ctx := context.Background()

	tf := "1h"
	tfMs := int64(3600 * 1000)
	// The series must reach the previous closed bucket for validation to pass, so the
	// fixture ends there once the gap is filled.
	current := candle.BucketTimestamp(time.Now().UnixMilli(), tf)
	baseTs := current - 4*tfMs

	// Insert candles at positions 0, 1, 3 (skip position 2 — gap)
	gapped := []candle.Candle{
		{Timestamp: baseTs, Open: 1, High: 1, Low: 1, Close: 1, Volume: 1},
		{Timestamp: baseTs + tfMs, Open: 2, High: 2, Low: 2, Close: 2, Volume: 2},
		{Timestamp: baseTs + 3*tfMs, Open: 4, High: 4, Low: 4, Close: 4, Volume: 4},
	}
	if err := store.SeedCandlesBatch(ctx, "binance", "spot", "BTC/USDT", tf, gapped); err != nil {
		t.Fatalf("seed gapped failed: %v", err)
	}

	v := NewRedisValidator(DefaultStorageWriter{Store: store})

	// Validation should FAIL because there is a gap
	ok, missing := v.Validate(ctx, "binance", "spot", "BTC/USDT", tf, baseTs, current)
	if ok {
		t.Fatal("expected validation to fail with gap, but it passed")
	}
	if missing != 1 {
		t.Fatalf("expected missing=1, got %d", missing)
	}

	// Insert the missing candle to close the gap
	missingCandle := candle.Candle{Timestamp: baseTs + 2*tfMs, Open: 3, High: 3, Low: 3, Close: 3, Volume: 3}
	if err := store.SeedCandlesBatch(ctx, "binance", "spot", "BTC/USDT", tf, []candle.Candle{missingCandle}); err != nil {
		t.Fatalf("seed missing candle failed: %v", err)
	}

	// Validation should now PASS
	ok, missing = v.Validate(ctx, "binance", "spot", "BTC/USDT", tf, baseTs, current)
	if !ok {
		t.Fatalf("expected validation to pass after gap fill, but failed with missing=%d", missing)
	}
	if missing != 0 {
		t.Fatalf("expected missing=0 after fill, got %d", missing)
	}
}

// TestRedisValidator_Range_Continuous verifies gap-free data passes immediately.
func TestRedisValidator_Range_Continuous(t *testing.T) {
	store, _ := newValidatorTestStore(t)
	ctx := context.Background()

	tf := "1h"
	tfMs := int64(3600 * 1000)
	current := candle.BucketTimestamp(time.Now().UnixMilli(), tf)
	baseTs := current - 3*tfMs

	continuous := []candle.Candle{
		{Timestamp: baseTs, Open: 1, High: 1, Low: 1, Close: 1, Volume: 1},
		{Timestamp: baseTs + tfMs, Open: 2, High: 2, Low: 2, Close: 2, Volume: 2},
		{Timestamp: baseTs + 2*tfMs, Open: 3, High: 3, Low: 3, Close: 3, Volume: 3},
		{Timestamp: baseTs + 3*tfMs, Open: 4, High: 4, Low: 4, Close: 4, Volume: 4},
	}
	if err := store.SeedCandlesBatch(ctx, "binance", "spot", "ETH/USDT", tf, continuous); err != nil {
		t.Fatalf("seed continuous failed: %v", err)
	}

	v := NewRedisValidator(DefaultStorageWriter{Store: store})
	ok, missing := v.Validate(ctx, "binance", "spot", "ETH/USDT", tf, baseTs, current)
	if !ok {
		t.Fatalf("expected validation to pass for continuous data, but failed with missing=%d", missing)
	}
	if missing != 0 {
		t.Fatalf("expected missing=0, got %d", missing)
	}
}
