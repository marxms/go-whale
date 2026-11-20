package app

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/marxms/go-whale/internal/adapters/outbound/questdb"
	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/candle/engine"
	"github.com/marxms/go-whale/internal/platform/config"
	logger "github.com/marxms/go-whale/internal/platform/logging"
	"github.com/marxms/go-whale/internal/recovery"
)

func TestColdDropMarksPendingAndSchedulesRepair(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := config.Config{
		RedisAddr:          mr.Addr(),
		RedisPoolSize:      2,
		RedisDialTimeoutS:  1,
		RedisReadTimeoutS:  1,
		RedisWriteTimeoutS: 1,
		RedisPoolTimeoutS:  1,
	}
	tfReg := candle.NewTimeframeRegistry([]string{"5m"})
	rs, err := redisstore.NewRedisStore(context.Background(), cfg, 10, tfReg)
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	t.Cleanup(func() { _ = rs.Close() })

	pipe := questdb.NewPipeline(nil, 1, time.Hour)
	if admitted, _ := pipe.Enqueue("binance", "future", "FILL/USDT", "5m", candle.Candle{Timestamp: 1}); !admitted {
		t.Fatal("the fill enqueue was refused")
	}
	log, sink := logger.NewCapture(logger.LevelDebug)
	manager := recovery.NewManager(nil, 0, tfReg, log)

	app := &App{redisStore: rs, qdbPipeline: pipe, gapRecovery: manager}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		app.stopColdMarkers(stopCtx)
	})
	key := engine.CompositeKey{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "5m"}
	closed := candle.Candle{
		Timestamp: 1_700_000_000_000, Close: 100, Volume: 10,
		TakerBuyVolume: 6, TakerSellVolume: 4,
	}

	app.onCandleClose(key, closed)

	pendingKey := "cold:pending:binance:future:BTC/USDT:5m:1700000000000"
	// A marca é escrita pelo dono, fora do reader: o teste espera o dono, não o chamador.
	deadline := time.Now().Add(2 * time.Second)
	for !mr.Exists(pendingKey) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !mr.Exists(pendingKey) {
		t.Fatal("the refused candle was not marked cold_pending")
	}
	if ttl := mr.TTL(pendingKey); ttl <= 0 || ttl > coldPendingTTL {
		t.Fatalf("cold_pending TTL = %v, want (0, %v]", ttl, coldPendingTTL)
	}
	if got := manager.Stats().Enqueued; got != 1 {
		t.Fatalf("targeted repairs enqueued = %d, want 1", got)
	}
	var repairLine string
	for _, record := range sink.Records() {
		if strings.Contains(record.Text(), "gap recovery targeted enqueue") {
			repairLine = record.Text()
		}
	}
	if !strings.Contains(repairLine, "timeframe=5m") || !strings.Contains(repairLine, "bucket=1700000000000") {
		t.Fatalf("targeted repair was not logged for the refused bucket: %q", repairLine)
	}
}
