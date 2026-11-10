package collector

import (
	"context"
	"errors"
	"testing"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/platform/config"

	"github.com/alicebob/miniredis/v2"
)

func TestFailedOpenInterestPollPreservesPersistedObservationAndRecoveryWindow(t *testing.T) {
	mr := miniredis.RunT(t)
	reg := candle.NewTimeframeRegistry([]string{"30m"})
	redisStore, err := redisstore.NewRedisStore(t.Context(), config.Config{
		RedisAddr: mr.Addr(), RedisPoolSize: 2,
		RedisDialTimeoutS: 1, RedisReadTimeoutS: 1, RedisWriteTimeoutS: 1, RedisPoolTimeoutS: 1,
	}, 200, reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := redisStore.Client().Close(); err != nil {
			t.Errorf("close Redis client: %v", err)
		}
	})
	store := redisstore.NewDerivativesRedisStore(redisStore.Client(), config.DerivativesConfig{MaxSeriesSize: 200}, reg)
	scope := SymbolScope{ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT"}
	oldAt := time.Now().Add(-10 * time.Minute).UnixMilli()
	if err := store.SaveSnapshot(context.Background(), &derivatives.DerivativesSnapshot{
		Exchange:   scope.ExchangeID,
		MarketType: scope.MarketType, Symbol: scope.Symbol, Timestamp: oldAt,
		OpenInterestBase: fptr(100), OpenInterestObservedAtMs: &oldAt,
	}); err != nil {
		t.Fatal(err)
	}
	p := fetchTestPoller()
	p.store = store
	client := &fakeRestClient{funding: derivatives.FundingRate{Rate: fptr(0.0001), MarkPrice: fptr(10)}, openInterestErr: errors.New("fixture OI unavailable")}
	partial, err := p.fetchDerivatives(context.Background(), client, scope)
	if err != nil {
		t.Fatal(err)
	}
	if partial.OpenInterestObservedAtMs != nil || partial.OpenInterestBase != nil {
		t.Fatalf("failed fetch invented a new observation: %+v", partial)
	}
	if escrito, err := p.pollSymbol(context.Background(), client, scope); err != nil {
		t.Fatal(err)
	} else if !escrito {
		t.Fatal("the snapshot was not written — the cycle would have counted this symbol as ok")
	}
	stored, err := store.GetLatestSnapshot(context.Background(), scope.ExchangeID, scope.MarketType, scope.Symbol)
	if err != nil {
		t.Fatal(err)
	}
	if stored == nil || stored.OpenInterestObservedAtMs == nil || *stored.OpenInterestObservedAtMs != oldAt ||
		stored.OpenInterestBase == nil || *stored.OpenInterestBase != 100 || stored.Timestamp <= oldAt || stored.FundingRate == nil || *stored.FundingRate != 0.0001 {
		t.Fatalf("partial poll made old OI fresh or lost funding: %+v", stored)
	}
	client.openInterestErr = nil
	client.openInterest = derivatives.OpenInterest{Amount: fptr(120)}
	if escrito, err := p.pollSymbol(context.Background(), client, scope); err != nil {
		t.Fatal(err)
	} else if !escrito {
		t.Fatal("the snapshot was not written — the cycle would have counted this symbol as ok")
	}
	recovered, err := store.GetLatestSnapshot(context.Background(), scope.ExchangeID, scope.MarketType, scope.Symbol)
	if err != nil {
		t.Fatal(err)
	}
	if recovered == nil || recovered.OpenInterestObservedAtMs == nil || *recovered.OpenInterestObservedAtMs != recovered.Timestamp ||
		recovered.OpenInterestBase == nil || *recovered.OpenInterestBase != 120 || recovered.OIChangePct == nil || *recovered.OIChangePct != 20 ||
		recovered.OIChangePctWindowMs == nil || *recovered.OIChangePctWindowMs != recovered.Timestamp-oldAt {
		t.Fatalf("recovery window shortened by failed poll: %+v", recovered)
	}
}
