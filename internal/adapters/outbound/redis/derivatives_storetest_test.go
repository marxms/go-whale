package redisstore

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives/storetest"
	"github.com/marxms/go-whale/internal/platform/config"
)

func newSuiteDerivativesStore(t *testing.T) *DerivativesRedisStore {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewDerivativesRedisStore(client,
		config.DerivativesConfig{MaxSeriesSize: 400},
		candle.NewTimeframeRegistry([]string{"1h"}))
}

// TestDerivativesRedisStore_SatisfiesTheContractSuite runs the shared hot and
// ruler suites against the real adapter on miniredis.
func TestDerivativesRedisStore_SatisfiesTheContractSuite(t *testing.T) {
	storetest.RunHot(t, func(t *testing.T) storetest.HotStore {
		return newSuiteDerivativesStore(t)
	})
	storetest.RunBaseline(t, func(t *testing.T) storetest.BaselineStore {
		return newSuiteDerivativesStore(t)
	})
}
