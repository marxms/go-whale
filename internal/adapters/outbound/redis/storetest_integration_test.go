//go:build integration

package redisstore

import (
	"context"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/candle/storetest"
	derivstoretest "github.com/marxms/go-whale/internal/derivatives/storetest"
	"github.com/marxms/go-whale/internal/platform/config"
)

// The REAL Redis half. miniredis proves the protocol shape; only a real
// server proves encoding, ZSET rank semantics and CAS under the actual driver.
//
// The disposable guard is not ceremony: these tests FLUSH the selected DB, and
// the LAN Redis is a shared production database. No variable, no run.
func liveRedisAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("REDIS_LIVE_ADDR")
	if addr == "" {
		t.Skip("REDIS_LIVE_ADDR not set")
	}
	if os.Getenv("REDIS_LIVE_DISPOSABLE") != "1" {
		t.Skip("REDIS_LIVE_DISPOSABLE != 1 — the suite flushes the DB; only against a disposable Redis")
	}
	return addr
}

func liveRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: liveRedisAddr(t)})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("ping em %s: %v", liveRedisAddr(t), err)
	}
	if err := client.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("FlushDB: %v", err)
	}
	return client
}

func TestLiveRedis_CandleContractSuite(t *testing.T) {
	addr := liveRedisAddr(t)
	storetest.RunHot(t, func(t *testing.T) storetest.HotStore {
		store, err := NewRedisStore(t.Context(), config.Config{
			RedisAddr:          addr,
			RedisPoolSize:      4,
			RedisDialTimeoutS:  2,
			RedisReadTimeoutS:  2,
			RedisWriteTimeoutS: 2,
			RedisPoolTimeoutS:  2,
			RedisMaxRetries:    1,
		}, candleStoretestCap, candle.NewTimeframeRegistry([]string{"1h"}))
		if err != nil {
			t.Fatalf("NewRedisStore: %v", err)
		}
		t.Cleanup(func() { _ = store.Close() })
		if err := store.Client().FlushDB(context.Background()).Err(); err != nil {
			t.Fatalf("FlushDB: %v", err)
		}
		return store
	}, storetest.HotOptions{Cap: candleStoretestCap}) // candle suite
}

func TestLiveRedis_DerivativesContractSuite(t *testing.T) {
	derivstoretest.RunHot(t, func(t *testing.T) derivstoretest.HotStore {
		client := liveRedisClient(t)
		return NewDerivativesRedisStore(client,
			config.DerivativesConfig{MaxSeriesSize: 400},
			candle.NewTimeframeRegistry([]string{"1h"}))
	})
	derivstoretest.RunBaseline(t, func(t *testing.T) derivstoretest.BaselineStore {
		client := liveRedisClient(t)
		return NewDerivativesRedisStore(client,
			config.DerivativesConfig{MaxSeriesSize: 400},
			candle.NewTimeframeRegistry([]string{"1h"}))
	})
}
