//go:build integration

package questdb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	questdb "github.com/questdb/go-questdb-client/v3"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"
)

// The sole-copy migration exercised against REAL Redis and QuestDB, on a disposable copy.
//
// # WHY THIS TEST EXISTS
//
// The migration is already proven by doubles as far as the decision goes (which boundary,
// which cell, which partial). What no double proves is the half that decides whether the data
// survives: ILP writes, WAL applies, PGX reads, DEDUP consolidates, and the re-read boundary
// returns the rest. This case measures COUNTS and HASHES before and after, and resumption
// after a ceiling — the same shape as the production incident.
//
// # NOTHING HERE MAY TOUCH THE SHARED DATABASE
//
// It requires REDIS_LIVE_DISPOSABLE=1 and QDB_LIVE_DISPOSABLE=1, flushes the Redis DB and drops
// the probe table at the end. Without both variables, SKIP.

const (
	migrationProbeTable = "candles_migration_t31"
	migrationSymbolA    = "T31/ONE"
	migrationSymbolB    = "T31/TWO"
)

func liveRedisForMigration(t *testing.T) *redisstore.RedisStore {
	t.Helper()
	addr := os.Getenv("REDIS_LIVE_ADDR")
	if addr == "" {
		t.Skip("REDIS_LIVE_ADDR not declared")
	}
	if os.Getenv("REDIS_LIVE_DISPOSABLE") != "1" {
		t.Skip("REDIS_LIVE_DISPOSABLE != 1 — the test flushes the DB")
	}
	store, err := redisstore.NewRedisStore(t.Context(), config.Config{
		RedisAddr:          addr,
		RedisPoolSize:      4,
		RedisDialTimeoutS:  2,
		RedisReadTimeoutS:  2,
		RedisWriteTimeoutS: 2,
		RedisPoolTimeoutS:  2,
		RedisMaxRetries:    1,
	}, 1000, candle.NewTimeframeRegistry([]string{"1d"}))
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Client().FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("FlushDB: %v", err)
	}
	return store
}

func migrationDay(day int) candle.Candle {
	ts := time.Date(2020, 1, day, 0, 0, 0, 0, time.UTC)
	return candle.Candle{
		Timestamp:       ts.UnixMilli(),
		Open:            100 + float64(day),
		High:            101 + float64(day),
		Low:             99 + float64(day),
		Close:           100.5 + float64(day),
		Volume:          1000 + float64(day),
		TakerBuyVolume:  600 + float64(day),
		TakerSellVolume: 400,
	}
}

func canonicalCandle(c candle.Candle) string {
	return fmt.Sprintf("%d|%.6f|%.6f|%.6f|%.6f|%.6f|%.6f|%.6f",
		c.Timestamp, c.Open, c.High, c.Low, c.Close, c.Volume, c.TakerBuyVolume, c.TakerSellVolume)
}

func candleDigest(rows []candle.Candle) string {
	sorted := append([]candle.Candle(nil), rows...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Timestamp < sorted[j].Timestamp })
	var b strings.Builder
	for _, c := range sorted {
		b.WriteString(canonicalCandle(c))
		b.WriteByte('\n')
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

func redisSeries(t *testing.T, store *redisstore.RedisStore, symbol string) []candle.Candle {
	t.Helper()
	rows, err := store.GetCandlesByScoreRange(context.Background(), "binance", "future", symbol, "1d",
		math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("GetCandlesByScoreRange %s: %v", symbol, err)
	}
	return rows
}

func coldSeries(t *testing.T, ctx context.Context, store *Store, symbol string) []candle.Candle {
	t.Helper()
	rows, err := store.QueryRange(ctx, "binance", "future", symbol, "1d",
		time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), 1000)
	if err != nil {
		t.Fatalf("QueryRange %s: %v", symbol, err)
	}
	return rows
}

func TestSoleCopyMigrationLive_DisposableCopyCountsHashesAndRecovery(t *testing.T) {
	if os.Getenv("QDB_LIVE_DISPOSABLE") != "1" {
		t.Skip("QDB_LIVE_DISPOSABLE != 1 — the proof creates and drops a probe table")
	}
	pool := liveQuestDB(t)
	redis := liveRedisForMigration(t)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)

	if _, err := pool.Exec(ctx, "DROP TABLE IF EXISTS "+migrationProbeTable); err != nil {
		t.Fatalf("DROP TABLE inicial: %v", err)
	}
	if _, err := pool.Exec(ctx, candlesTableContract(migrationProbeTable).ddl); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	t.Cleanup(func() {
		limpeza, cancelar := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelar()
		if _, err := pool.Exec(limpeza, "DROP TABLE IF EXISTS "+migrationProbeTable); err != nil {
			t.Errorf("the probe left residue: %v", err)
		}
	})

	sender, err := questdb.LineSenderFromConf(ctx, candleILPConfig(liveILPAddr(t)))
	if err != nil {
		t.Fatalf("LineSenderFromConf: %v", err)
	}
	t.Cleanup(func() { _ = sender.Close(context.Background()) })
	store := &Store{
		ilpSender:  sender,
		ilpConfStr: candleILPConfig(liveILPAddr(t)),
		pgPool:     pool,
		tableName:  migrationProbeTable,
		ctx:        ctx,
		cancel:     cancel,
		lastFlush:  time.Now(),
		priors:     emptyPriorSource{},
	}

	keyA, err := candle.SeriesKeyOf("binance", "future", migrationSymbolA, "1d")
	if err != nil {
		t.Fatalf("SeriesKeyOf A: %v", err)
	}
	keyB, err := candle.SeriesKeyOf("binance", "future", migrationSymbolB, "1d")
	if err != nil {
		t.Fatalf("SeriesKeyOf B: %v", err)
	}

	// A already has durable coverage on days 3..5; its sole copy is days 1..2. B has NO durable
	// row at all: the entire series is a sole copy.
	coldSeed := map[candle.SeriesKey][]candle.Candle{
		keyA: {migrationDay(3), migrationDay(4), migrationDay(5)},
	}
	if err := store.WriteBatches(ctx, coldSeed); err != nil {
		t.Fatalf("durable seed WriteBatches: %v", err)
	}
	target, err := store.CaptureApplyTarget(ctx)
	if err != nil {
		t.Fatalf("seed CaptureApplyTarget: %v", err)
	}
	if err := store.WaitApplied(ctx, target); err != nil {
		t.Fatalf("seed WaitApplied: %v", err)
	}

	redisA := []candle.Candle{migrationDay(1), migrationDay(2), migrationDay(3), migrationDay(4), migrationDay(5)}
	redisB := []candle.Candle{migrationDay(1), migrationDay(2), migrationDay(3)}
	if err := redis.SeedCandlesBatch(ctx, "binance", "future", migrationSymbolA, "1d", redisA); err != nil {
		t.Fatalf("seed Redis A: %v", err)
	}
	if err := redis.SeedCandlesBatch(ctx, "binance", "future", migrationSymbolB, "1d", redisB); err != nil {
		t.Fatalf("seed Redis B: %v", err)
	}

	countA, err := redis.GetCandlesCount(ctx, "binance", "future", migrationSymbolA, "1d")
	if err != nil {
		t.Fatalf("count Redis A: %v", err)
	}
	countB, err := redis.GetCandlesCount(ctx, "binance", "future", migrationSymbolB, "1d")
	if err != nil {
		t.Fatalf("count Redis B: %v", err)
	}
	redisHashA := candleDigest(redisSeries(t, redis, migrationSymbolA))
	redisHashB := candleDigest(redisSeries(t, redis, migrationSymbolB))
	t.Logf("BEFORE redis A: count=%d hash=%s", countA, redisHashA)
	t.Logf("BEFORE redis B: count=%d hash=%s", countB, redisHashB)
	t.Logf("BEFORE questdb: A=%v B=%v", timestampsOf(coldSeries(t, ctx, store, migrationSymbolA)),
		timestampsOf(coldSeries(t, ctx, store, migrationSymbolB)))

	migrator := NewSoleCopyMigrator(redis, store)

	// 1. DRY RUN. Counts what would migrate and writes no row.
	dry, err := migrator.Run(ctx, SoleCopyMigrationOptions{Timeframes: []string{"1d"}, Apply: false})
	if err != nil {
		t.Fatalf("dry-run: %v", err)
	}
	if dry.Candles() != 5 {
		t.Fatalf("dry-run candles=%d, wanted 5 (A:2 + B:3)", dry.Candles())
	}
	if len(coldSeries(t, ctx, store, migrationSymbolB)) != 0 {
		t.Fatal("the dry run wrote to the durable tier")
	}
	t.Logf("DRY-RUN: applied=%v cells=%+v commits=%d truncated=%v candles=%d",
		dry.Applied, dry.Cells, dry.Commits, dry.Truncated, dry.Candles())

	// 2. APPLY WITH A CEILING: the pass stops in A and leaves B for resumption.
	first, err := migrator.Run(ctx, SoleCopyMigrationOptions{
		Timeframes: []string{"1d"}, Apply: true, MaxCandles: 2, FlushEvery: 1,
	})
	if err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if !first.Truncated {
		t.Fatalf("the first pass did not stop at the ceiling: %+v", first)
	}
	if first.Candles() != 2 {
		t.Fatalf("first pass candles=%d, wanted 2 (only A)", first.Candles())
	}
	t.Logf("PASS 1 (ceiling=2): cells=%+v candles=%d truncated=%v", first.Cells, first.Candles(), first.Truncated)

	// 3. RESUMPTION through the RE-READ boundary: A is already covered through day 1; all of B
	// is missing.
	second, err := migrator.Run(ctx, SoleCopyMigrationOptions{
		Timeframes: []string{"1d"}, Apply: true, FlushEvery: 1,
	})
	if err != nil {
		t.Fatalf("resumption: %v", err)
	}
	if second.Candles() != 3 {
		t.Fatalf("resumption candles=%d, wanted 3 (all of B)", second.Candles())
	}
	t.Logf("PASS 2 (resumption): cells=%+v candles=%d truncated=%v", second.Cells, second.Candles(), second.Truncated)

	// 4. THIRD PASS: the boundary has nothing more below it.
	third, err := migrator.Run(ctx, SoleCopyMigrationOptions{Timeframes: []string{"1d"}, Apply: true})
	if err != nil {
		t.Fatalf("convergence pass: %v", err)
	}
	if third.Candles() != 0 || third.Truncated {
		t.Fatalf("the migration did not converge: candles=%d truncated=%v", third.Candles(), third.Truncated)
	}
	t.Logf("PASS 3 (convergence): cells=%+v candles=%d truncated=%v", third.Cells, third.Candles(), third.Truncated)

	// 5. REDIS WAS NOT TOUCHED: same counts, same hashes, same samples.
	afterA, err := redis.GetCandlesCount(ctx, "binance", "future", migrationSymbolA, "1d")
	if err != nil {
		t.Fatalf("count Redis A after: %v", err)
	}
	afterB, err := redis.GetCandlesCount(ctx, "binance", "future", migrationSymbolB, "1d")
	if err != nil {
		t.Fatalf("count Redis B after: %v", err)
	}
	if afterA != countA || afterB != countB {
		t.Fatalf("Redis changed: A %d→%d, B %d→%d — the migration does not delete", countA, afterA, countB, afterB)
	}
	if got := candleDigest(redisSeries(t, redis, migrationSymbolA)); got != redisHashA {
		t.Fatalf("Redis hash A changed: %s → %s", redisHashA, got)
	}
	if got := candleDigest(redisSeries(t, redis, migrationSymbolB)); got != redisHashB {
		t.Fatalf("Redis hash B changed: %s → %s", redisHashB, got)
	}
	t.Logf("AFTER redis A: count=%d hash=%s", afterA, redisHashA)
	t.Logf("AFTER redis B: count=%d hash=%s", afterB, redisHashB)

	// 6. THE DURABLE TIER IS THE UNION: seed + what existed only in Redis, without duplicates.
	coldA := coldSeries(t, ctx, store, migrationSymbolA)
	coldB := coldSeries(t, ctx, store, migrationSymbolB)
	if len(coldA) != 5 || len(coldB) != 3 {
		t.Fatalf("durable A=%d B=%d, wanted 5/3", len(coldA), len(coldB))
	}
	wantA := candleDigest(redisA)
	wantB := candleDigest(redisB)
	if got := candleDigest(coldA); got != wantA {
		t.Fatalf("durable hash A = %s, wanted %s (seed + sole-copy union)", got, wantA)
	}
	if got := candleDigest(coldB); got != wantB {
		t.Fatalf("durable hash B = %s, wanted %s", got, wantB)
	}
	t.Logf("AFTER questdb A: count=%d hash=%s sample=%s", len(coldA), candleDigest(coldA), sampleOf(coldA))
	t.Logf("AFTER questdb B: count=%d hash=%s sample=%s", len(coldB), candleDigest(coldB), sampleOf(coldB))

	// 7. RECOVERY: a concurrent write of the same day does not duplicate (DEDUP).
	if err := store.WriteBatches(ctx, map[candle.SeriesKey][]candle.Candle{keyB: {migrationDay(1)}}); err != nil {
		t.Fatalf("DEDUP rewrite: %v", err)
	}
	target, err = store.CaptureApplyTarget(ctx)
	if err != nil {
		t.Fatalf("CaptureApplyTarget after: %v", err)
	}
	if err := store.WaitApplied(ctx, target); err != nil {
		t.Fatalf("WaitApplied after: %v", err)
	}
	if got := len(coldSeries(t, ctx, store, migrationSymbolB)); got != 3 {
		t.Fatalf("DEDUP after the rewrite left %d row(s), wanted 3", got)
	}

	// 8. NO DATA REFUSED: the migrated sum is exactly the dry-run's sole copy.
	migrated := first.Candles() + second.Candles() + third.Candles()
	if migrated != dry.Candles() {
		t.Fatalf("migrated=%d, dry-run=%d — data was left unmigrated or counted twice", migrated, dry.Candles())
	}
	t.Logf("SUMMARY: dry-run=%d migrated=%d (passes %d+%d+%d) refused=0",
		dry.Candles(), migrated, first.Candles(), second.Candles(), third.Candles())
}

func timestampsOf(rows []candle.Candle) []int64 {
	out := make([]int64, len(rows))
	for i, c := range rows {
		out[i] = c.Timestamp
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func sampleOf(rows []candle.Candle) string {
	if len(rows) == 0 {
		return "(empty)"
	}
	sorted := append([]candle.Candle(nil), rows...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Timestamp < sorted[j].Timestamp })
	first, last := sorted[0], sorted[len(sorted)-1]
	return fmt.Sprintf("%s %s … %s %s",
		time.UnixMilli(first.Timestamp).UTC().Format("2006-01-02"), canonicalCandle(first),
		time.UnixMilli(last.Timestamp).UTC().Format("2006-01-02"), canonicalCandle(last))
}
