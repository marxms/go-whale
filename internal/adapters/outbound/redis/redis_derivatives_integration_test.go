package redisstore

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// TestDerivativesRedisStore_UnlinkStaleDerivatives_RemovesOrphans verifies that
// UnlinkStaleDerivatives removes keys for symbols NOT in the active set while
// preserving keys for active symbols.  Uses a real miniredis server.
func TestDerivativesRedisStore_UnlinkStaleDerivatives_RemovesOrphans(t *testing.T) {
	ctx := context.Background()

	// Start miniredis.
	m := miniredis.RunT(t)
	defer m.Close()

	client := redis.NewClient(&redis.Options{Addr: m.Addr(), DB: 0})
	defer func() { _ = client.Close() }()

	store := &DerivativesRedisStore{
		client:        client,
		maxSeriesSize: 1000,
	}

	// Seed keys for two symbols.
	snapKey1 := "derivatives:snapshot:latest:binance:future:BTCUSDT"
	snapKey2 := "derivatives:snapshot:latest:binance:future:ETHUSDT"
	seriesKey1 := "derivatives:series:binance:future:BTCUSDT:1h"
	seriesKey2 := "derivatives:series:binance:future:ETHUSDT:1h"

	if err := client.Set(ctx, snapKey1, "data1", 0).Err(); err != nil {
		t.Fatalf("seed snapKey1: %v", err)
	}
	if err := client.Set(ctx, snapKey2, "data2", 0).Err(); err != nil {
		t.Fatalf("seed snapKey2: %v", err)
	}
	if err := client.ZAdd(ctx, seriesKey1, redis.Z{Score: 1, Member: "m1"}).Err(); err != nil {
		t.Fatalf("seed seriesKey1: %v", err)
	}
	if err := client.ZAdd(ctx, seriesKey2, redis.Z{Score: 1, Member: "m2"}).Err(); err != nil {
		t.Fatalf("seed seriesKey2: %v", err)
	}

	// Call with only BTCUSDT active — ETHUSDT should be UNLINKed.
	active := map[string]struct{}{"BTCUSDT": {}}
	unlinked := store.UnlinkStaleDerivatives(ctx, "binance", "future", active)
	if unlinked == 0 {
		t.Fatal("expected >0 keys unlinked")
	}

	// BTCUSDT keys must survive.
	if client.Exists(ctx, snapKey1).Val() == 0 {
		t.Fatalf("snapKey1 should still exist")
	}
	if client.Exists(ctx, seriesKey1).Val() == 0 {
		t.Fatalf("seriesKey1 should still exist")
	}

	// ETHUSDT keys must be gone.
	if client.Exists(ctx, snapKey2).Val() > 0 {
		t.Fatalf("snapKey2 should be removed")
	}
	if client.Exists(ctx, seriesKey2).Val() > 0 {
		t.Fatalf("seriesKey2 should be removed")
	}
}

// TestDerivativesRedisStore_UnlinkStaleDerivatives_NoStaleSymbols verifies that
// when all symbols are active, nothing is removed and UNLINK returns 0.
func TestDerivativesRedisStore_UnlinkStaleDerivatives_NoStaleSymbols(t *testing.T) {
	ctx := context.Background()

	m := miniredis.RunT(t)
	defer m.Close()

	client := redis.NewClient(&redis.Options{Addr: m.Addr(), DB: 0})
	defer func() { _ = client.Close() }()

	store := &DerivativesRedisStore{
		client:        client,
		maxSeriesSize: 1000,
	}

	snapKey1 := "derivatives:snapshot:latest:binance:future:BTCUSDT"
	if err := client.Set(ctx, snapKey1, "data1", 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// BTCUSDT is active — nothing stale.
	active := map[string]struct{}{"BTCUSDT": {}}
	unlinked := store.UnlinkStaleDerivatives(ctx, "binance", "future", active)
	if unlinked != 0 {
		t.Fatalf("expected 0 unlinked when all symbols active, got %d", unlinked)
	}

	if client.Exists(ctx, snapKey1).Val() == 0 {
		t.Fatal("snapKey1 should still exist")
	}
}
