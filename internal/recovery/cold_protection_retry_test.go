package recovery

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/exchange"
	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/candle/engine"
	"github.com/marxms/go-whale/internal/market"
	"github.com/marxms/go-whale/internal/platform/config"
	"github.com/marxms/go-whale/internal/retention"

	"github.com/alicebob/miniredis/v2"
)

type coldProtectionClient struct{}

func (coldProtectionClient) FetchOHLCV(context.Context, string, string, int64, int64) ([]candle.Candle, error) {
	return []candle.Candle{{Timestamp: time.Now().Add(-31 * 24 * time.Hour).UnixMilli(), Open: 100, High: 110, Low: 90, Close: 105, Volume: 10}}, nil
}

func (coldProtectionClient) FetchKlinesWithCVD(context.Context, string, string, int) ([]candle.Candle, bool, error) {
	return nil, false, nil
}

func (coldProtectionClient) Close() []error { return nil }

type retryProtectionCold struct {
	candle.ColdStore
	err error
}

func (c *retryProtectionCold) QueryRange(context.Context, string, string, string, string, time.Time, time.Time, int) ([]candle.Candle, error) {
	return nil, c.err
}
func (*retryProtectionCold) Close() error { return nil }

type retryProtectionAppender struct{ writes int }

func (a *retryProtectionAppender) EnqueueClosedBlocking(context.Context, string, string, string, string, candle.Candle) error {
	a.writes++
	return nil
}

// WARM-03: run the production splitter and seeder together. A cold outage must keep
// the subscription eligible for the existing reseed branch, then recover on retry.
func TestSeedAllTimeframes_ColdProtectionFailureIsTransientAndRecovers(t *testing.T) {
	mr := miniredis.RunT(t)
	reg := candle.NewTimeframeRegistry([]string{"5m", "1h"})
	r, err := redisstore.NewRedisStore(t.Context(), config.Config{RedisAddr: mr.Addr(), RedisPoolSize: 4, RedisDialTimeoutS: 1, RedisReadTimeoutS: 1, RedisWriteTimeoutS: 1, RedisPoolTimeoutS: 1}, 20, reg)
	if err != nil {
		t.Fatal(err)
	}
	ce := engine.NewCandleEngine(map[string]int{"default": 20})
	tiered := engine.NewTieredStore(ce, r, config.CandleEngineConfig{Enabled: true})
	broken := errors.New("QuestDB temporarily unavailable")
	cold := &retryProtectionCold{err: broken}
	appender := &retryProtectionAppender{}
	tiered.SetColdStore(cold)
	tiered.SetColdAppender(appender, retention.Policy{})
	t.Cleanup(func() { _ = tiered.Close() })
	store := &redisstore.CombinedStore{Writer: tiered, SeedWriter: tiered}
	valid := map[string]struct{}{"BTC/USDT": {}}
	budget := exchange.NewRestBudget()
	seed := func() *SeedResult {
		return SeedAllTimeframesMultiWithResult(context.Background(), Metrics{}, store, coldProtectionClient{}, budget, "binance", market.Future, valid, "BTC/USDT", map[string]int{"default": 2}, reg)
	}
	first := seed()
	if !first.AllFailed || !first.Transient || first.PartialError == nil || len(first.Failed) != 2 {
		t.Fatalf("outage must choose KEEP/reseed, not rollback: %+v", first)
	}
	if appender.writes != 0 {
		t.Fatal("failed protective read wrote degraded cold history")
	}
	if budget.UnderPenalty() {
		t.Fatal("storage outage penalized the exchange REST budget")
	}
	for _, tf := range reg.All() {
		hot, e := r.GetCandlesDesc(context.Background(), "binance", "future", "BTC/USDT", tf, 20)
		if e != nil || len(hot) != 0 {
			t.Fatalf("hot written on failure: %+v %v", hot, e)
		}
	}
	cold.err = nil
	second := seed()
	if second.AllFailed || second.Transient || second.PartialError != nil || len(second.Succeeded) != 2 || appender.writes != 2 {
		t.Fatalf("existing retry did not recover: %+v writes=%d", second, appender.writes)
	}
}

type permanentSeedStore struct{ splittingStore }

func (*permanentSeedStore) PersistSeedBatch(context.Context, string, string, string, string, []candle.Candle, []candle.Candle) error {
	return errors.New("invalid seed record")
}

func TestSeedAllTimeframes_UnrelatedPersistenceFailureIsNotTransient(t *testing.T) {
	result := SeedAllTimeframesMultiWithResult(context.Background(), Metrics{}, &permanentSeedStore{}, coldProtectionClient{}, nil,
		"binance", market.Future, map[string]struct{}{"BTC/USDT": {}}, "BTC/USDT",
		map[string]int{"default": 2}, candle.NewTimeframeRegistry([]string{"5m"}))
	if !result.AllFailed || result.Transient || result.PartialError == nil {
		t.Fatalf("only protective history availability is transient: %+v", result)
	}
}

func TestSeedAllTimeframes_CancelledCallerDoesNotFetchOrPersist(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := SeedAllTimeframesMultiWithResult(ctx, Metrics{}, nil, nil, nil, "binance", market.Future,
		map[string]struct{}{"BTC/USDT": {}}, "BTC/USDT", map[string]int{"default": 2},
		candle.NewTimeframeRegistry([]string{"5m", "1h"}))
	if !result.AllFailed || len(result.Failed) != 2 || result.PartialError == nil {
		t.Fatalf("canceled seed was not reported incomplete: %+v", result)
	}
}

type unavailableAdmissionStore struct {
	splittingStore
	cause error
}

func (s *unavailableAdmissionStore) PersistSeedBatch(context.Context, string, string, string, string, []candle.Candle, []candle.Candle) error {
	return fmt.Errorf("%w: %w", redisstore.ErrSeedColdAdmissionUnavailable, s.cause)
}

func TestSeedAllTimeframes_ColdAdmissionFailureKeepsSymbol(t *testing.T) {
	for _, cause := range []error{context.DeadlineExceeded, errors.New("cold pipeline fenced: UNCERTAIN")} {
		result := SeedAllTimeframesMultiWithResult(context.Background(), Metrics{}, &unavailableAdmissionStore{cause: cause}, coldProtectionClient{}, nil,
			"binance", market.Future, map[string]struct{}{"BTC/USDT": {}}, "BTC/USDT",
			map[string]int{"default": 2}, candle.NewTimeframeRegistry([]string{"5m", "15m", "30m", "1h", "2h", "4h", "1d", "1w"}))
		if !result.AllFailed || !result.Transient || len(result.Failed) != 8 {
			t.Fatalf("cold unavailability must keep a valid symbol: %+v", result)
		}
	}
}
