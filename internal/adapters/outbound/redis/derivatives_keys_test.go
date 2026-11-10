package redisstore

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

// derivCoinalyzeBaselineKey lives HERE, not in production.
//
// The per-source ruler is enforced on the READ, filtering the series by the newest point's
// provenance (`singleProvenanceSeries`, in `internal/adapters/inbound/httpapi`), and not by a
// second persisted ruler. Production therefore has no caller for this constructor, but the
// NAMESPACE remains production: the cleanup globs walk `derivCoinalyzeBaselinePrefix`, because
// a key written by a future version must not survive a per-symbol cleanup. What these cases
// assert is precisely that coverage, which is why the constructor survives as a test helper.
func derivCoinalyzeBaselineKey(exchange, marketType, symbol, timeframe string) string {
	return derivCoinalyzeBaselinePrefix + ":" + exchange + ":" + marketType + ":" + symbol + ":" + timeframe + ":" + derivBaselineKeyVersion
}

// derivCoinalyzeSeriesZsetKey lives here for the same reason. The Coinalyze series family is
// WRITE-ONLY: no production path writes `derivatives:coinalyze:series:*`, but the resident keys
// only disappear through an explicit `UNLINK` — they have no TTL.
//
// The NAMESPACE remains production, and more so than the baseline case: the cases below assert
// that all three sweeps cover the family; without the constructor, that assertion would become
// a string literal repeated in five places.
func derivCoinalyzeSeriesZsetKey(exchange, marketType, symbol, timeframe string) string {
	return derivCoinalyzeSeriesPrefix + ":" + exchange + ":" + marketType + ":" + symbol + ":" + timeframe
}

// TestSymbolFromDerivativesKey pins the POSITIONAL parse that all three sweeps share.
// The version segment is a suffix precisely so that the symbol keeps its index: an
// infix version would shift it, and every sweep would keep running, keep reporting
// success, and delete nothing.
func TestSymbolFromDerivativesKey(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want string
	}{
		{"latest snapshot", "derivatives:snapshot:latest:binance:future:BTCUSDT", "BTCUSDT"},
		{"series", "derivatives:series:binance:future:BTCUSDT:1h", "BTCUSDT"},
		{"baseline carries the version as a suffix", "derivatives:baseline:binance:future:BTCUSDT:1h:v1", "BTCUSDT"},
		{"unknown family is skipped, never guessed", "derivatives:whatever:binance:future:BTCUSDT:1h", ""},
		{"foreign key is skipped", "last_trade:binance:future:BTCUSDT", ""},
		{"truncated key is skipped", "derivatives:series:binance", ""},
		{"snapshot without the latest marker is skipped", "derivatives:snapshot:binance:future:BTCUSDT", ""},

		// The second provenance pushes every field one position, and the parse must follow.
		// Reading these keys with the legacy family's index would return "future" as the symbol,
		// and the sweep would delete whatever matched that name.
		{"coinalyze latest snapshot", "derivatives:coinalyze:snapshot:latest:binance:future:BTCUSDT", "BTCUSDT"},
		{"coinalyze series", "derivatives:coinalyze:series:binance:future:BTCUSDT:1h", "BTCUSDT"},
		{"coinalyze baseline carries the version as a suffix", "derivatives:coinalyze:baseline:binance:future:BTCUSDT:1h:v1", "BTCUSDT"},
		{"coinalyze family that nobody declared is skipped", "derivatives:coinalyze:whatever:binance:future:BTCUSDT:1h", ""},
		{"truncated coinalyze key is skipped", "derivatives:coinalyze:series:binance:future", ""},
		{"coinalyze snapshot without the latest marker is skipped", "derivatives:coinalyze:snapshot:binance:future:BTCUSDT", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := SymbolFromDerivativesKey(tc.key); got != tc.want {
				t.Fatalf("SymbolFromDerivativesKey(%q) = %q, want %q", tc.key, got, tc.want)
			}
		})
	}
}

// TestDerivativesKeyPatterns_MatchEveryFamily asserts the globs handed to the
// keyspace-wide sweep actually match every key family this package writes. A family
// missing from this list leaks: nothing reads it, nothing deletes it, and no metric
// reports it.
func TestDerivativesKeyPatterns_MatchEveryFamily(t *testing.T) {
	ctx := context.Background()
	store, client := newTestDerivStore(t, "1h")

	written := []string{
		derivLatestScopedKey("binance", "future", "BTCUSDT"),
		derivSeriesZsetKey("binance", "future", "BTCUSDT", "1h"),
		derivBaselineKey("binance", "future", "BTCUSDT", "1h"),
		derivCoinalyzeLatestKey("binance", "future", "BTCUSDT"),
		derivCoinalyzeSeriesZsetKey("binance", "future", "BTCUSDT", "1h"),
		derivCoinalyzeBaselineKey("binance", "future", "BTCUSDT", "1h"),
	}
	for _, key := range written {
		if err := client.Set(ctx, key, "x", 0).Err(); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}

	matched := make(map[string]bool, len(written))
	for _, pattern := range DerivativesKeyPatterns() {
		store.scanDerivativesKeys(ctx, pattern, 100, func(keys []string) {
			for _, k := range keys {
				matched[k] = true
			}
		})
	}

	for _, key := range written {
		if !matched[key] {
			t.Fatalf("key %q is written but matched by no sweep pattern", key)
		}
	}
}

// TestSweeps_CoverTheBaselineKey walks the two in-package sweeps over the CURRENT
// schema — including the baseline key, which did not exist when they were written.
func TestSweeps_CoverTheBaselineKey(t *testing.T) {
	tests := []struct {
		name  string
		sweep func(s *DerivativesRedisStore, ctx context.Context, active map[string]struct{}) int64
	}{
		{
			name: "UnlinkStaleDerivatives",
			sweep: func(s *DerivativesRedisStore, ctx context.Context, active map[string]struct{}) int64 {
				return s.UnlinkStaleDerivatives(ctx, "binance", "future", active)
			},
		},
		{
			name: "PurgeStaleDerivatives",
			sweep: func(s *DerivativesRedisStore, ctx context.Context, active map[string]struct{}) int64 {
				return s.PurgeStaleDerivatives(ctx, "binance", "future", active)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store, client := newTestDerivStore(t, "1h")

			seed := func(symbol string) []string {
				keys := []string{
					derivLatestScopedKey("binance", "future", symbol),
					derivBaselineKey("binance", "future", symbol, "1h"),
					// The new family enters the same seed because a key nobody deletes still
					// answers every read correctly — the leak has no symptom.
					derivCoinalyzeLatestKey("binance", "future", symbol),
					derivCoinalyzeBaselineKey("binance", "future", symbol, "1h"),
				}
				for _, k := range keys {
					if err := client.Set(ctx, k, "x", 0).Err(); err != nil {
						t.Fatalf("seed %s: %v", k, err)
					}
				}
				for _, zkey := range []string{
					derivSeriesZsetKey("binance", "future", symbol, "1h"),
					derivCoinalyzeSeriesZsetKey("binance", "future", symbol, "1h"),
				} {
					if err := client.ZAdd(ctx, zkey, redis.Z{Score: 1, Member: "m"}).Err(); err != nil {
						t.Fatalf("seed %s: %v", zkey, err)
					}
					keys = append(keys, zkey)
				}
				return keys
			}

			activeKeys := seed("BTCUSDT")
			staleKeys := seed("DOGEUSDT")

			// A symbol whose ONLY surviving key is a baseline: discovery has to find it
			// there too, otherwise a ruler outlives the series it was built from. The
			// second orphan says the same about the Coinalyze namespace — a symbol that
			// exists only in the new provenance must be DISCOVERED by it.
			orphans := []string{
				derivBaselineKey("binance", "future", "SHIBUSDT", "4h"),
				derivCoinalyzeBaselineKey("binance", "future", "PEPEUSDT", "4h"),
			}
			for _, k := range orphans {
				if err := client.Set(ctx, k, "x", 0).Err(); err != nil {
					t.Fatalf("seed orphan %s: %v", k, err)
				}
			}

			removed := tc.sweep(store, ctx, map[string]struct{}{"BTCUSDT": {}})
			if want := int64(len(staleKeys) + len(orphans)); removed != want {
				t.Fatalf("removed %d keys, want %d", removed, want)
			}

			for _, k := range append(staleKeys, orphans...) {
				if client.Exists(ctx, k).Val() != 0 {
					t.Fatalf("stale key %q survived the sweep", k)
				}
			}
			for _, k := range activeKeys {
				if client.Exists(ctx, k).Val() == 0 {
					t.Fatalf("active key %q was removed by the sweep", k)
				}
			}
		})
	}
}

// TestCleanupSymbolDerivatives_RemovesEveryFamily covers the per-symbol sweep, which
// the derank path calls directly.
func TestCleanupSymbolDerivatives_RemovesEveryFamily(t *testing.T) {
	ctx := context.Background()
	store, client := newTestDerivStore(t, "1h")

	keys := []string{
		derivLatestScopedKey("binance", "future", "DOGEUSDT"),
		derivSeriesZsetKey("binance", "future", "DOGEUSDT", "1h"),
		derivSeriesZsetKey("binance", "future", "DOGEUSDT", "4h"),
		derivBaselineKey("binance", "future", "DOGEUSDT", "1h"),
		derivBaselineKey("binance", "future", "DOGEUSDT", "4h"),
		derivCoinalyzeLatestKey("binance", "future", "DOGEUSDT"),
		derivCoinalyzeSeriesZsetKey("binance", "future", "DOGEUSDT", "1h"),
		derivCoinalyzeSeriesZsetKey("binance", "future", "DOGEUSDT", "4h"),
		derivCoinalyzeBaselineKey("binance", "future", "DOGEUSDT", "1h"),
		derivCoinalyzeBaselineKey("binance", "future", "DOGEUSDT", "4h"),
	}
	for _, k := range keys {
		if err := client.Set(ctx, k, "x", 0).Err(); err != nil {
			t.Fatalf("seed %s: %v", k, err)
		}
	}
	survivors := []string{
		derivBaselineKey("binance", "future", "BTCUSDT", "1h"),
		derivCoinalyzeBaselineKey("binance", "future", "BTCUSDT", "1h"),
	}
	for _, k := range survivors {
		if err := client.Set(ctx, k, "x", 0).Err(); err != nil {
			t.Fatalf("seed survivor %s: %v", k, err)
		}
	}

	if deleted := store.CleanupSymbolDerivatives(ctx, "binance", "future", "DOGEUSDT"); deleted != int64(len(keys)) {
		t.Fatalf("deleted %d keys, want %d", deleted, len(keys))
	}
	for _, k := range keys {
		if client.Exists(ctx, k).Val() != 0 {
			t.Fatalf("key %q survived cleanup", k)
		}
	}
	for _, k := range survivors {
		if client.Exists(ctx, k).Val() == 0 {
			t.Fatalf("cleanup removed another symbol's key %q", k)
		}
	}
}

// TestCoinalyzeKeys_AreADistinctNamespace: the same
// (exchange, marketType, symbol, timeframe) addressed by the two provenances must land
// on DIFFERENT keys, so neither source overwrites the other.
//
// The failure this pins is not an exception — it is a series that oscillates between two
// providers every cycle while every read keeps answering, and a MAD baseline that reads
// the distance BETWEEN the sources as market variance.
func TestCoinalyzeKeys_AreADistinctNamespace(t *testing.T) {
	ctx := context.Background()
	_, client := newTestDerivStore(t, "1h")

	const ex, mt, sym, tf = "binance", "future", "BTCUSDT", "1h"

	pairs := []struct {
		name     string
		existing string
		second   string
	}{
		{"latest", derivLatestScopedKey(ex, mt, sym), derivCoinalyzeLatestKey(ex, mt, sym)},
		{"series", derivSeriesZsetKey(ex, mt, sym, tf), derivCoinalyzeSeriesZsetKey(ex, mt, sym, tf)},
		{"baseline", derivBaselineKey(ex, mt, sym, tf), derivCoinalyzeBaselineKey(ex, mt, sym, tf)},
	}

	for _, pair := range pairs {
		t.Run(pair.name, func(t *testing.T) {
			if pair.existing == pair.second {
				t.Fatalf("both provenances address %q — the second source overwrites the first", pair.existing)
			}
			if err := client.Set(ctx, pair.existing, "binance-value", 0).Err(); err != nil {
				t.Fatalf("seed %s: %v", pair.existing, err)
			}
			if err := client.Set(ctx, pair.second, "coinalyze-value", 0).Err(); err != nil {
				t.Fatalf("seed %s: %v", pair.second, err)
			}
			if got := client.Get(ctx, pair.existing).Val(); got != "binance-value" {
				t.Fatalf("%s holds %q after the second provenance wrote, want %q", pair.existing, got, "binance-value")
			}
			if got := client.Get(ctx, pair.second).Val(); got != "coinalyze-value" {
				t.Fatalf("%s holds %q, want %q", pair.second, got, "coinalyze-value")
			}
		})
	}
}
