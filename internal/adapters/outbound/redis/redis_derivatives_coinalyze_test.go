package redisstore

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/derivatives"

	"github.com/redis/go-redis/v9"
)

// dayMs is the width of the daily bucket, written once for the proofs below.
const dayMs int64 = 86_400_000

// THE `derivatives:coinalyze:series` FAMILY IS NO LONGER WRITTEN.
//
// These cases started out exercising `SaveCoinalyzeSeries`. The writer is gone (write-only,
// resident keys with no TTL), and what remains is the proof that the removal does not change
// the read: that is what the spec requires, and what survives the removal of the writer.

// TestGetSnapshotSeries_IsIdenticalWithAndWithoutTheCoinalyzeZset checks the series read is
// BYTE FOR BYTE the same with the Coinalyze key present and absent.
//
// The member is seeded directly by the client, not by a writer: there is no writer anymore,
// and seeding by the client is the only way to reproduce the production state — resident keys
// that no read touches — without reviving the write path.
//
// KILLS the mutant that makes `GetSnapshotSeries` fall into the second-provenance key (by
// merge, by fallback or by glob), which is what would turn removing the writer into a loss.
func TestGetSnapshotSeries_IsIdenticalWithAndWithoutTheCoinalyzeZset(t *testing.T) {
	ctx := context.Background()
	store, client := newTestDerivStore(t, "1d")

	bucket := time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC).UnixMilli()
	venue := &derivatives.DerivativesSnapshot{
		Exchange: "binance", MarketType: "future", Symbol: "BTCUSDT",
		Timestamp: bucket, OpenInterestBase: fptr(107025.033),
	}
	if err := store.SaveSeriesOnly(ctx, venue, "1d"); err != nil {
		t.Fatalf("SaveSeriesOnly: %v", err)
	}

	semZset, err := store.GetSnapshotSeries(ctx, "binance", "future", "BTCUSDT", "1d", 10)
	if err != nil {
		t.Fatalf("GetSnapshotSeries without the coinalyze key: %v", err)
	}
	if len(semZset) != 1 {
		t.Fatalf("%d point(s) in the venue series, want 1", len(semZset))
	}

	// The second-provenance key, with DIFFERENT values in two buckets — including one the
	// venue series does not have. If the read touched it, the result would change.
	coinalyzeKey := derivCoinalyzeSeriesZsetKey("binance", "future", "BTCUSDT", "1d")
	seedRawSeriesMember(t, client, coinalyzeKey, bucket, &derivatives.DerivativesSnapshot{
		Exchange: "binance", MarketType: "future", Symbol: "BTCUSDT",
		Timestamp: bucket, OpenInterestBase: fptr(106117.954),
	})
	seedRawSeriesMember(t, client, coinalyzeKey, bucket-dayMs, &derivatives.DerivativesSnapshot{
		Exchange: "binance", MarketType: "future", Symbol: "BTCUSDT",
		Timestamp: bucket - dayMs, OpenInterestBase: fptr(105000.001),
	})
	if n := client.ZCard(ctx, coinalyzeKey).Val(); n != 2 {
		t.Fatalf("the seed put %d member(s) in the coinalyze key, want 2", n)
	}

	comZset, err := store.GetSnapshotSeries(ctx, "binance", "future", "BTCUSDT", "1d", 10)
	if err != nil {
		t.Fatalf("GetSnapshotSeries with the coinalyze key: %v", err)
	}

	antes, depois := mustJSON(t, semZset), mustJSON(t, comZset)
	if antes != depois {
		t.Fatalf("the read changed with the coinalyze key present:\n without: %s\n with: %s", antes, depois)
	}
	if len(comZset) != 1 || comZset[0].OpenInterestBase == nil || *comZset[0].OpenInterestBase != 107025.033 {
		t.Fatalf("the read returned %+v — it fell into the second-provenance key", comZset)
	}
}

// TestSaveSeriesOnly_NeverCreatesTheCoinalyzeKey: the venue write path does not revive the
// removed family through a glob, a shared prefix or a mirror.
//
// KILLS the mutant that reintroduces the venue-side write — the cheapest way for the family to
// come back without anything reporting it, because the read stays correct.
func TestSaveSeriesOnly_NeverCreatesTheCoinalyzeKey(t *testing.T) {
	ctx := context.Background()
	store, client := newTestDerivStore(t, "1d")

	bucket := time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC).UnixMilli()
	for i := 0; i < 3; i++ {
		snap := &derivatives.DerivativesSnapshot{
			Exchange: "binance", MarketType: "future", Symbol: "BTCUSDT",
			Timestamp: bucket + int64(i)*dayMs, OpenInterestBase: fptr(107025.033 + float64(i)),
		}
		if err := store.SaveSeriesOnly(ctx, snap, "1d"); err != nil {
			t.Fatalf("SaveSeriesOnly: %v", err)
		}
	}

	sobreviventes, err := client.Keys(ctx, derivCoinalyzeSeriesPrefix+":*").Result()
	if err != nil {
		t.Fatalf("KEYS: %v", err)
	}
	if len(sobreviventes) != 0 {
		t.Fatalf("the venue write created %d coinalyze key(s): %v", len(sobreviventes), sobreviventes)
	}
}

// TestSeriesBucketAlignment_FreezesTheIntervalStart: alignment of the existing source is the
// FLOOR of the interval, and a feed that labeled the CLOSE of the same interval must step back
// to the same bucket.
//
// The existing feeds are START-labeled: `/futures/data/openInterestHist?period=1d` and
// `globalLongShortAccountRatio?period=1d` return `t` = today 00:00Z.
func TestSeriesBucketAlignment_FreezesTheIntervalStart(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestDerivStore(t, "1d")

	intervalStart := time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC).UnixMilli()
	intervalEnd := intervalStart + dayMs

	existing := &derivatives.DerivativesSnapshot{
		Exchange: "binance", MarketType: "future", Symbol: "BTCUSDT",
		Timestamp: intervalStart, OpenInterestBase: fptr(107025.033),
	}
	if err := store.SaveSeriesOnly(ctx, existing, "1d"); err != nil {
		t.Fatalf("SaveSeriesOnly: %v", err)
	}

	series, err := store.GetSnapshotSeries(ctx, "binance", "future", "BTCUSDT", "1d", 10)
	if err != nil {
		t.Fatalf("GetSnapshotSeries: %v", err)
	}
	if len(series) != 1 {
		t.Fatalf("%d point(s), want 1", len(series))
	}
	if series[0].Timestamp != intervalStart {
		t.Fatalf("bucket=%d, want %d (the alignment frozen by the contract)", series[0].Timestamp, intervalStart)
	}
	if got := bucketAlignMsWithConvention(intervalEnd, "1d", derivatives.LabelIntervalEnd); got != intervalStart {
		t.Fatalf("the close label should step back to %d, went to %d", intervalStart, got)
	}
}

// seedRawSeriesMember puts ONE member into a series key without going through any writer.
func seedRawSeriesMember(t *testing.T, client *redis.Client, key string, score int64, snap *derivatives.DerivativesSnapshot) {
	t.Helper()
	payload, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	member := redis.Z{Score: float64(score), Member: string(payload)}
	if err := client.ZAdd(context.Background(), key, member).Err(); err != nil {
		t.Fatalf("ZAdd %s: %v", key, err)
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(raw)
}
