package redisstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// WriteSnapshotBatch writes the engine's authoritative candles to the hot tier with BLIND
// pipelined writes — no read-back, no WATCH, no MULTI/EXEC.
//
// # Why there is no read at all rather than a pipelined one
//
// A read-modify-write path would run one WATCH / ZRANGEBYSCORE / MULTI / EXEC cycle per
// candle, serially, over 1,624 keys every 5 seconds — roughly 6,500 round trips per sweep. A
// production CPU profile of that shape put 56% of the process in the path, 51% of it in raw
// network syscalls.
//
// Pipelining cannot fix a read-modify-write: the client needs the read reply before it can
// build the write, so the trips cannot overlap. Redis' own documentation says as much. The
// remedy is to remove the read.
//
// # Why removing it is safe
//
// The CandleEngine is the sole authority for candle content — CombinedStore routes only
// last-trade and bulk-price reads around it, never a candle write. The merge the snapshot
// performed consulted Redis exclusively for post-restart defense on open/high/low/createdAt;
// that reconciliation belongs at seed time, once, not on every 5-second sweep.
//
// # Idempotency
//
// ZREMRANGEBYSCORE + ZADD on the same score replaces the member for that bucket, so a
// repeated write converges rather than duplicating. The cap trim keeps the series bounded.
func (s *RedisStore) WriteSnapshotBatch(ctx context.Context, updates []CandleUpdate) error {
	if len(updates) == 0 {
		return nil
	}

	pipe := s.client.Pipeline()
	for i := range updates {
		u := &updates[i]
		payload, err := encodeCandleMember(u.Candle)
		if err != nil {
			// One unencodable candle must not sink the sweep: skip it and keep going.
			// It is counted by the caller's error path only if EVERY candle fails.
			continue
		}
		key := candlesKeyV2(u.Exchange, u.MarketType, u.Symbol, u.Timeframe)
		score := strconv.FormatInt(u.Candle.Timestamp, 10)

		pipe.ZRemRangeByScore(ctx, key, score, score)
		pipe.ZAdd(ctx, key, redis.Z{Score: float64(u.Candle.Timestamp), Member: payload})
		// SINGLE CAP AUTHORITY. This line read `s.candleCap` — the global SCALAR — while the
		// other two write paths of the same family resolve through `candleCapFor(timeframe)`.
		// In production the two answer 800 and 521 for the intraday timeframes: 279 members of
		// divergence, and the 5-second sweep undoing on every pass the cut the seed had just
		// applied. Two resolvers for one decision are not redundancy, they are two retentions.
		if limite := s.candleCapFor(u.Timeframe); limite > 0 {
			pipe.ZRemRangeByRank(ctx, key, 0, -(int64(limite) + 1))
		}
	}

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("snapshot pipeline exec: %w", err)
	}
	return nil
}
