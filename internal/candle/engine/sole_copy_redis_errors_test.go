package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"
)

func TestSoleCopyGuardRealRedisRejectsUnreadableFrontier(t *testing.T) {
	mr := miniredis.RunT(t)
	hot, err := redisstore.NewRedisStore(t.Context(), config.Config{RedisAddr: mr.Addr()}, 0, candle.NewTimeframeRegistry([]string{"1d"}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hot.Client().Close() }()
	cold := &fakeGuardCold{}
	guard := NewSoleCopyGuard(hot, cold)
	// A confirmed absence differs from a key whose frontier could not be read.
	clean := guard.Probe(context.Background(), []string{"1d"})
	if !clean.Evaluated || clean.KeysWithSoleCopy() != 0 {
		t.Fatalf("confirmed empty: %+v", clean)
	}
	if err := mr.Set("candles:binance:future:BTC/USDT:1d", "wrong type"); err != nil {
		t.Fatal(err)
	}
	risk := guard.Probe(context.Background(), []string{"1d"})
	if risk.Evaluated || !strings.Contains(risk.Reason, "WRONGTYPE") || len(risk.Cells) != 0 || cold.calls != 0 {
		t.Fatalf("unreadable frontier certified: %+v cold calls=%d", risk, cold.calls)
	}
}
