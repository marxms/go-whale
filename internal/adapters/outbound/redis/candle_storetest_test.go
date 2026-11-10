package redisstore

import (
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/candle/storetest"
	"github.com/marxms/go-whale/internal/platform/config"
)

const candleStoretestCap = 5

// TestRedisStore_SatisfiesTheCandleContractSuite runs the shared suite against
// the real adapter on miniredis. The suite is driver-agnostic; this file is the
// only place that knows a Redis is behind the port.
func TestRedisStore_SatisfiesTheCandleContractSuite(t *testing.T) {
	storetest.RunHot(t, func(t *testing.T) storetest.HotStore {
		t.Helper()
		mr := miniredis.RunT(t)
		cfg := config.Config{
			RedisAddr:          mr.Addr(),
			RedisPoolSize:      4,
			RedisDialTimeoutS:  1,
			RedisReadTimeoutS:  1,
			RedisWriteTimeoutS: 1,
			RedisPoolTimeoutS:  1,
			RedisMaxRetries:    1,
		}
		store, err := NewRedisStore(t.Context(), cfg, candleStoretestCap, candle.NewTimeframeRegistry([]string{"1h"}))
		if err != nil {
			t.Fatalf("NewRedisStore: %v", err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store
	}, storetest.HotOptions{Cap: candleStoretestCap})
}
