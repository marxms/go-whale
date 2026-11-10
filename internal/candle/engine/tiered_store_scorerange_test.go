package engine

import (
	"context"
	"testing"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"
	"github.com/marxms/go-whale/internal/recovery"

	"github.com/alicebob/miniredis/v2"
)

// `TieredStore.GetCandlesByScoreRange` must merge the durable tier, not read Redis alone.
//
// The gap validator requires the FIRST member of the window to sit exactly on `gapStart`, and
// the recovery job can reach 1,000 buckets — so any gap deeper than the hot cap would be
// reported as "First candle mismatch" even when the durable tier holds every bucket.

// scoreRangeColdStore is an in-memory candle.ColdStore with QuestDB's read semantics, which the
// merge depends on: the window is INCLUSIVE on both bounds, rows come back ORDER BY
// timestamp DESC, and LIMIT therefore keeps the NEWEST n. A fake that returned the OLDEST n
// would hide a limit that is too small to reach the head of the window.
type scoreRangeColdStore struct {
	rows []candle.Candle // any order; the fake sorts
}

func (c *scoreRangeColdStore) WriteBatches(context.Context, map[candle.SeriesKey][]candle.Candle) error {
	return nil
}

func (c *scoreRangeColdStore) QueryRange(_ context.Context, _, _, _, _ string, from, to time.Time, limit int) ([]candle.Candle, error) {
	var out []candle.Candle
	for _, cd := range c.rows {
		ts := time.UnixMilli(cd.Timestamp)
		if ts.Before(from) || ts.After(to) {
			continue
		}
		out = append(out, cd)
	}
	// ORDER BY timestamp DESC, then LIMIT — newest-first truncation, like the real query.
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j].Timestamp > out[i].Timestamp {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (c *scoreRangeColdStore) GetOldestTimestamp(context.Context, string, string, string, string) (time.Time, error) {
	return time.Time{}, nil
}
func (c *scoreRangeColdStore) Close() error                 { return nil }
func (c *scoreRangeColdStore) Stats() candle.ColdStoreStats { return candle.ColdStoreStats{} }

// scoreRangeRig wires the production TieredStore over miniredis (hot) and an in-memory cold
// tier, so the read path under test is the real one.
type scoreRangeRig struct {
	tiered *TieredStore
	redis  *redisstore.RedisStore
	cold   *scoreRangeColdStore
}

func newScoreRangeRig(ctx context.Context, t *testing.T, hotCap int) *scoreRangeRig {
	t.Helper()

	mr := miniredis.RunT(t)
	cfg := config.Config{
		RedisAddr:          mr.Addr(),
		RedisPoolSize:      10,
		RedisDialTimeoutS:  1,
		RedisReadTimeoutS:  1,
		RedisWriteTimeoutS: 1,
		RedisPoolTimeoutS:  1,
		RedisMaxRetries:    3,
	}
	tfReg := candle.NewTimeframeRegistry([]string{"1m", "5m", "15m", "1h"})
	redisStore, err := redisstore.NewRedisStore(ctx, cfg, hotCap, tfReg)
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}

	ce := NewCandleEngine(map[string]int{"default": hotCap})
	// RedisWriteThrough=false: no snapshot goroutine, so the hot ZSET holds exactly what the
	// test seeds into it.
	tiered := NewTieredStore(ce, redisStore, config.CandleEngineConfig{Enabled: true})
	t.Cleanup(func() { tiered.cancel() })

	return &scoreRangeRig{tiered: tiered, redis: redisStore}
}

func (r *scoreRangeRig) withCold(rows []candle.Candle) *scoreRangeRig {
	r.cold = &scoreRangeColdStore{rows: rows}
	r.tiered.SetColdStore(r.cold)
	return r
}

const (
	srExchange   = "binance"
	srMarketType = "future"
	srSymbol     = "BTC/USDT"
	srTimeframe  = "5m"
)

// srSeries builds `count` contiguous buckets of `srTimeframe` ending at `newest`.
func srSeries(newest int64, count int, tfMs int64) []candle.Candle {
	out := make([]candle.Candle, count)
	for i := range count {
		ts := newest - int64(count-1-i)*tfMs
		out[i] = candle.Candle{
			Timestamp: ts,
			Open:      100, High: 110, Low: 90, Close: 105, Volume: 1,
		}
	}
	return out
}

// TestTieredStore_GetCandlesByScoreRange_ValidatesGapDeeperThanHotCap: the hot tier holds 521
// buckets, the durable tier holds 1,000, and the validator is asked about a 900-bucket gap.
// Reading only the hot tier answers false with a head mismatch.
func TestTieredStore_GetCandlesByScoreRange_ValidatesGapDeeperThanHotCap(t *testing.T) {
	ctx := context.Background()
	tfMs, ok := candle.ParseTimeframeMs(srTimeframe)
	if !ok {
		t.Fatalf("ParseTimeframeMs(%s) failed", srTimeframe)
	}

	newest := candle.BucketTimestamp(time.Now().UnixMilli(), srTimeframe) - tfMs // last CLOSED bucket
	cold := srSeries(newest, 1000, tfMs)
	hot := cold[len(cold)-521:] // the newest 521 buckets survive the hot cap

	rig := newScoreRangeRig(t.Context(), t, 521).withCold(cold)
	if err := rig.redis.SeedCandlesBatch(ctx, srExchange, srMarketType, srSymbol, srTimeframe, hot); err != nil {
		t.Fatalf("seeding the hot tier: %v", err)
	}

	gapStart := newest - 900*tfMs

	validator := recovery.NewRedisValidator(recovery.DefaultStorageWriter{Store: rig.tiered})
	valid, missing := validator.Validate(ctx, srExchange, srMarketType, srSymbol, srTimeframe, gapStart, newest+tfMs)

	if !valid || missing != 0 {
		t.Fatalf("gap covered by the durable tier must validate: got valid=%v missing=%d (want true/0)", valid, missing)
	}
}

// TestTieredStore_GetCandlesByScoreRange_MergeContract pins what the validator depends on:
// the window is complete and ASCENDING by timestamp. ZRANGEBYSCORE is ascending and
// `mergeCandles` sorts DESC, so a merge that forgets to restore the order breaks every gap
// validation at the head.
func TestTieredStore_GetCandlesByScoreRange_MergeContract(t *testing.T) {
	ctx := context.Background()
	tfMs, _ := candle.ParseTimeframeMs(srTimeframe)
	newest := candle.BucketTimestamp(time.Now().UnixMilli(), srTimeframe) - tfMs
	cold := srSeries(newest, 1000, tfMs)
	hot := cold[len(cold)-521:]

	rig := newScoreRangeRig(t.Context(), t, 521).withCold(cold)
	if err := rig.redis.SeedCandlesBatch(ctx, srExchange, srMarketType, srSymbol, srTimeframe, hot); err != nil {
		t.Fatalf("seeding the hot tier: %v", err)
	}

	minScore := newest - 900*tfMs
	got, err := rig.tiered.GetCandlesByScoreRange(ctx, srExchange, srMarketType, srSymbol, srTimeframe, minScore, newest)
	if err != nil {
		t.Fatalf("GetCandlesByScoreRange: %v", err)
	}

	if len(got) != 901 {
		t.Fatalf("window [now-900, now] holds 901 buckets, got %d", len(got))
	}
	if got[0].Timestamp != minScore {
		t.Fatalf("first member must be the OLDEST bucket %d, got %d", minScore, got[0].Timestamp)
	}
	if got[len(got)-1].Timestamp != newest {
		t.Fatalf("last member must be the NEWEST bucket %d, got %d", newest, got[len(got)-1].Timestamp)
	}
	for i := 1; i < len(got); i++ {
		if got[i].Timestamp != got[i-1].Timestamp+tfMs {
			t.Fatalf("series must be contiguous and ascending: got[%d]=%d after %d", i, got[i].Timestamp, got[i-1].Timestamp)
		}
	}
}

// TestTieredStore_GetCandlesByScoreRange_NoColdTier keeps the deployment without a durable
// tier on the hot-only answer instead of erroring or returning nothing.
func TestTieredStore_GetCandlesByScoreRange_NoColdTier(t *testing.T) {
	ctx := context.Background()
	tfMs, _ := candle.ParseTimeframeMs(srTimeframe)
	newest := candle.BucketTimestamp(time.Now().UnixMilli(), srTimeframe) - tfMs
	hot := srSeries(newest, 50, tfMs)

	rig := newScoreRangeRig(t.Context(), t, 521) // no SetColdStore
	if err := rig.redis.SeedCandlesBatch(ctx, srExchange, srMarketType, srSymbol, srTimeframe, hot); err != nil {
		t.Fatalf("seeding the hot tier: %v", err)
	}

	got, err := rig.tiered.GetCandlesByScoreRange(ctx, srExchange, srMarketType, srSymbol, srTimeframe, hot[0].Timestamp, newest)
	if err != nil {
		t.Fatalf("GetCandlesByScoreRange: %v", err)
	}
	if len(got) != 50 || got[0].Timestamp != hot[0].Timestamp {
		t.Fatalf("hot-only read must return the 50 seeded buckets ascending, got %d members (first=%d)", len(got), got[0].Timestamp)
	}
}
