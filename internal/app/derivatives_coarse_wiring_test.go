package app

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/marxms/go-whale/internal/adapters/outbound/questdb"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"
)

func TestNewAppWiresCanonicalCoarseWithoutDurableTier(t *testing.T) {
	app, err := NewApp(configParaNewApp(t), candle.NewTimeframeRegistry([]string{"1d", "1w"}), nil, withColdTierConstructor(func(config.QuestDBConfig) (*questdb.Store, error) { return nil, errors.New("cold unavailable") }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Shutdown(context.Background()) })
	if !app.derivStore.HasCanonicalCoarse("1d") || !app.derivStore.HasCanonicalCoarse("1w") || app.derivStore.HasCanonicalCoarse("4h") {
		t.Fatal("constructor did not install canonical coarse reader")
	}
	_, err = app.derivStore.RecomputeBaseline(context.Background(), "binance", "future", "BTC/USDT", "1w")
	if err == nil || !strings.Contains(err.Error(), "durable Coinalyze history unavailable") {
		t.Fatalf("typed nil or native fallback: %v", err)
	}
}
