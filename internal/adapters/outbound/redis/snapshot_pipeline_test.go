package redisstore

import (
	"context"
	"testing"

	"github.com/marxms/go-whale/internal/candle"

	"github.com/redis/go-redis/v9"
)

// TestSnapshotWrite_IsPipelinedWithoutReadBack is the guard for
//
// flushSnapshot fed every buffer key into a loop that ran one WATCH / ZRANGEBYSCORE /
// MULTI / EXEC cycle PER CANDLE — about four round trips each, serially, across 1.624 keys
// every 5 seconds (~6.500 round trips per sweep). A 30s CPU profile in production put 56%
// of the process in that path and 51% in raw network syscalls.
//
// Pipelining a read-modify-write is not a fix: the client must have the read reply before it
// can issue the write, so the round trips cannot overlap. The remedy is to remove the READ.
// The CandleEngine is the sole authority for candle content — CombinedStore routes only
// last-trade and bulk-price reads around it — and the snapshot merge consulted Redis solely
// for post-restart defense on open/high/low/createdAt, which belongs at seed time, not on
// every sweep.
func TestSnapshotWrite_IsPipelinedWithoutReadBack(t *testing.T) {
	t.Parallel()

	s := &RedisStore{}
	cmds := recordSnapshotCommands(t, s, []CandleUpdate{
		{
			Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "5m",
			Candle: candle.Candle{Timestamp: 1_700_000_000_000, Open: 1, High: 2, Low: 1, Close: 2, Volume: 10},
		},
		{
			Exchange: "binance", MarketType: "future", Symbol: "ETH/USDT", Timeframe: "5m",
			Candle: candle.Candle{Timestamp: 1_700_000_000_000, Open: 3, High: 4, Low: 3, Close: 4, Volume: 20},
		},
	})

	for _, c := range cmds {
		switch c {
		case "watch", "zrangebyscore", "multi", "exec", "unwatch":
			t.Errorf("snapshot issued %q: the read-modify-write path is still in place, and pipelining cannot help it because the write depends on the read reply", c)
		}
	}
	if len(cmds) == 0 {
		t.Fatal("snapshot issued no commands at all")
	}
}

// recordSnapshotCommands runs one snapshot write against a client whose hook records every
// command name. The server is unreachable on purpose: the hook fires before the transport,
// so the command SHAPE is observable without needing a live Redis.
func recordSnapshotCommands(t *testing.T, s *RedisStore, updates []CandleUpdate) []string {
	t.Helper()

	rec := &commandRecorder{}
	client := redis.NewClient(&redis.Options{
		Addr:                  "127.0.0.1:1", // guaranteed refused; we only need the hook to fire
		MaxRetries:            -1,
		DialTimeout:           50_000_000, // 50ms
		ContextTimeoutEnabled: true,
	})
	client.AddHook(rec)
	s.client = client
	s.candleCap = 200
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2_000_000_000) // 2s
	defer cancel()
	_ = s.WriteSnapshotBatch(ctx, updates)

	return rec.names()
}

type commandRecorder struct {
	seen []string
}

func (r *commandRecorder) DialHook(next redis.DialHook) redis.DialHook { return next }

func (r *commandRecorder) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		r.seen = append(r.seen, cmd.Name())
		return next(ctx, cmd)
	}
}

func (r *commandRecorder) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, c := range cmds {
			r.seen = append(r.seen, c.Name())
		}
		return next(ctx, cmds)
	}
}

func (r *commandRecorder) names() []string { return r.seen }
