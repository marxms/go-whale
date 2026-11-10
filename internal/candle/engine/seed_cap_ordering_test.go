package engine

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"
	"github.com/marxms/go-whale/internal/retention"
)

// The ordering half of the seed-leg cap.
//
// WHAT MAKES THE CAP ON THE SEED LEG SAFE IS NOT THE CAP, IT IS THE ORDER.
//
// `PersistSeedBatch` writes the DURABLE tier first and only then the hot tier, and propagates the
// durable error before the other. As long as that holds, whatever `ZREMRANGEBYRANK` removes from
// the hot window is already in the cold tier. Inverting the two writes — or, worse, truncating the
// BATCH before delivering it to the cold tier — opens a permanent hole that no re-read fills.

// recordingColdAppender stores everything that reached the durable tier and, on the FIRST candle,
// snapshots the hot ZSET cardinality. If the hot tier has already been written and capped when the
// cold tier is called, the snapshot gives it away.
type recordingColdAppender struct {
	mr           *miniredis.Miniredis
	hotKey       string
	received     []candle.Candle
	hotCardFirst int
	sawFirst     bool
}

func (r *recordingColdAppender) EnqueueClosedBlocking(_ context.Context, _, _, _, _ string, c candle.Candle) error {
	if !r.sawFirst {
		r.sawFirst = true
		members, err := r.mr.ZMembers(r.hotKey)
		if err == nil {
			r.hotCardFirst = len(members)
		}
	}
	r.received = append(r.received, c)
	return nil
}

// TestPersistSeedBatch_ColdIsWrittenWholeAndBeforeTheHotTierIsTrimmed.
//
// It kills both failure modes at once:
//   - capping BEFORE the cold write (or capping the batch itself): the cold tier would receive
//     521 instead of 800, and the hole would be permanent;
//   - inverting the order: the hot tier would already be written when the cold tier is called.
func TestPersistSeedBatch_ColdIsWrittenWholeAndBeforeTheHotTierIsTrimmed(t *testing.T) {
	mr := miniredis.RunT(t)

	candleEngineCfg := config.CandleEngineConfig{
		BufferSizes:         map[string]int{"default": 800},
		RedisCapByTimeframe: map[string]int{"5m": 521},
		SnapshotIntervalMS:  60_000,
	}

	redisStore, err := redisstore.NewRedisStore(t.Context(), config.Config{
		RedisAddr:          mr.Addr(),
		RedisPoolSize:      10,
		RedisDialTimeoutS:  1,
		RedisReadTimeoutS:  1,
		RedisWriteTimeoutS: 1,
		RedisPoolTimeoutS:  1,
		RedisMaxRetries:    3,
		CandleEngine:       candleEngineCfg,
	}, 800, candle.NewTimeframeRegistry([]string{"5m"}))
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}

	ts := NewTieredStore(NewCandleEngine(candleEngineCfg.BufferSizes), redisStore, candleEngineCfg)
	defer func() { _ = ts.Close() }()

	const hotKey = "candles:binance:future:BTC/USDT:5m"
	cold := &recordingColdAppender{mr: mr, hotKey: hotKey}
	ts.SetColdAppender(cold, retention.Policy{})

	// 800 closed 5m buckets, which is what a boot seed leg delivers today.
	const endMs int64 = 1_760_000_000_000
	stepMs := candle.ParseTimeframeToSeconds("5m") * 1000
	batch := make([]candle.Candle, 0, 800)
	for i := 800; i >= 1; i-- {
		batch = append(batch, candle.Candle{
			Timestamp: endMs - int64(i)*stepMs,
			Open:      100, High: 110, Low: 90, Close: 105, Volume: 1000 + float64(i),
		})
	}

	if err := ts.PersistSeedBatch(context.Background(), "binance", "future", "BTC/USDT", "5m", batch, batch); err != nil {
		t.Fatalf("PersistSeedBatch: %v", err)
	}

	if len(cold.received) != 800 {
		t.Fatalf("the durable tier received %d candles, want 800 — the cap must not reach cold", len(cold.received))
	}
	if cold.hotCardFirst != 0 {
		t.Fatalf("on the first cold candle the hot ZSET already had %d members, want 0 — "+
			"the order inverted and the cap ran before the durable write", cold.hotCardFirst)
	}

	members, err := mr.ZMembers(hotKey)
	if err != nil {
		t.Fatalf("ZMembers: %v", err)
	}
	if len(members) != 521 {
		t.Fatalf("ZCARD of the hot tier = %d, want 521", len(members))
	}
}
