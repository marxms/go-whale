package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
)

// The guard stops the `allkeys-lru` flip from arriving before the migration. The compose file can
// be deployed by someone who never read the design; the guard measures at startup and the gate
// fails. These cases pin down what it measures and, above all, what it does NOT call clean. See
// docs/decisions/0012-sole-copy-guard-before-lru.md.

type fakeGuardHot struct {
	byTf map[string]map[redisstore.CandleKeyRef]int64
	err  error
}

func (f *fakeGuardHot) OldestCandleTimestamps(_ context.Context, timeframe string) (map[redisstore.CandleKeyRef]int64, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.byTf[timeframe], nil
}

type fakeGuardCold struct {
	byScope map[string]map[string]time.Time
	err     error
	// calls counts the queries: the durable boundary is ONE grouped query per scope, not
	// one per symbol. A startup guard that issues 500 queries is a guard someone
	// disables in the second week.
	calls int
}

func (f *fakeGuardCold) OldestTimestampsByScope(_ context.Context, exchange, marketType, timeframe string) (map[string]time.Time, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.byScope[exchange+":"+marketType+":"+timeframe], nil
}

func ref(symbol, tf string) redisstore.CandleKeyRef {
	return redisstore.CandleKeyRef{Exchange: "binance", MarketType: "future", Symbol: symbol, Timeframe: tf}
}

// TestSoleCopyGuard_FlagsWhatIsOlderThanTheDurableBoundary is the central measurement: a hot
// candle older than the durable coverage is a sole copy.
func TestSoleCopyGuard_FlagsWhatIsOlderThanTheDurableBoundary(t *testing.T) {
	boundary := time.Date(2025, 8, 23, 0, 0, 0, 0, time.UTC)

	hot := &fakeGuardHot{byTf: map[string]map[redisstore.CandleKeyRef]int64{
		"1d": {
			// Older than the boundary: 95,811 1d members are in this situation.
			ref("BTCUSDT", "1d"): boundary.Add(-30 * 24 * time.Hour).UnixMilli(),
			// Inside the coverage: nothing to migrate.
			ref("ETHUSDT", "1d"): boundary.Add(10 * 24 * time.Hour).UnixMilli(),
			// Exactly on the boundary: covered, and it is the edge that a wrong `<=` would invert.
			ref("SOLUSDT", "1d"): boundary.UnixMilli(),
		},
	}}
	cold := &fakeGuardCold{byScope: map[string]map[string]time.Time{
		"binance:future:1d": {"BTCUSDT": boundary, "ETHUSDT": boundary, "SOLUSDT": boundary},
	}}

	risk := NewSoleCopyGuard(hot, cold).Probe(context.Background(), []string{"1d"})

	if !risk.Evaluated {
		t.Fatalf("not evaluated: %s", risk.Reason)
	}
	if risk.KeysWithSoleCopy() != 1 {
		t.Fatalf("keys at risk = %d, want 1 (%s)", risk.KeysWithSoleCopy(), risk.Detail())
	}
	if len(risk.Cells) != 1 || risk.Cells[0].Keys != 3 {
		t.Fatalf("cell = %+v", risk.Cells)
	}
	if len(risk.Cells[0].Sample) != 1 || risk.Cells[0].Sample[0] != "BTCUSDT" {
		t.Fatalf("the sample does not name the symbol at risk: %v", risk.Cells[0].Sample)
	}
}

// TestSoleCopyGuard_NoDurableRowIsTheWorstCaseNotTheCleanOne is the most likely mutant: a
// symbol with NO durable row does not appear in the `GROUP BY`, and a map that returned the zero
// `time.Time` for it would make it look covered since 1970. Its ENTIRE series is a sole
// copy.
func TestSoleCopyGuard_NoDurableRowIsTheWorstCaseNotTheCleanOne(t *testing.T) {
	boundary := time.Date(2025, 8, 23, 0, 0, 0, 0, time.UTC)

	hot := &fakeGuardHot{byTf: map[string]map[redisstore.CandleKeyRef]int64{
		"1w": {
			ref("NOVOUSDT", "1w"): boundary.Add(200 * 24 * time.Hour).UnixMilli(),
		},
	}}
	// The scope responds, but with no row at all for this symbol.
	cold := &fakeGuardCold{byScope: map[string]map[string]time.Time{
		"binance:future:1w": {"BTCUSDT": boundary},
	}}

	risk := NewSoleCopyGuard(hot, cold).Probe(context.Background(), []string{"1w"})

	if !risk.Evaluated {
		t.Fatalf("not evaluated: %s", risk.Reason)
	}
	if risk.KeysWithSoleCopy() != 1 {
		t.Fatalf("keys at risk = %d, want 1 — a symbol with no durable row has its entire series as a sole copy", risk.KeysWithSoleCopy())
	}
	if risk.Cells[0].KeysWithoutColdRows != 1 {
		t.Fatalf("KeysWithoutColdRows = %d, want 1 — the case must be counted separately to be readable",
			risk.Cells[0].KeysWithoutColdRows)
	}
}

// TestSoleCopyGuard_QueriesTheDurableBoundaryOncePerScope pins the cost. The guard runs at
// startup, before the process serves its first request.
func TestSoleCopyGuard_QueriesTheDurableBoundaryOncePerScope(t *testing.T) {
	boundary := time.Date(2025, 8, 23, 0, 0, 0, 0, time.UTC)

	hotKeys := map[redisstore.CandleKeyRef]int64{}
	coverage := map[string]time.Time{}
	for _, sym := range []string{"A", "B", "C", "D", "E", "F", "G", "H"} {
		hotKeys[ref(sym, "1d")] = boundary.Add(24 * time.Hour).UnixMilli()
		coverage[sym] = boundary
	}

	hot := &fakeGuardHot{byTf: map[string]map[redisstore.CandleKeyRef]int64{"1d": hotKeys}}
	cold := &fakeGuardCold{byScope: map[string]map[string]time.Time{"binance:future:1d": coverage}}

	risk := NewSoleCopyGuard(hot, cold).Probe(context.Background(), []string{"1d"})

	if risk.KeysWithSoleCopy() != 0 {
		t.Fatalf("keys at risk = %d, want 0", risk.KeysWithSoleCopy())
	}
	if cold.calls != 1 {
		t.Fatalf("durable-boundary queries = %d for 8 symbols of ONE scope, want 1", cold.calls)
	}
}

// TestSoleCopyGuard_UnreadableBoundaryIsNotClean — not measuring is not being clean. The gate treats
// this verdict as a loud alarm; what it must NOT do is pass as "zero at risk".
func TestSoleCopyGuard_UnreadableBoundaryIsNotClean(t *testing.T) {
	boundary := time.Date(2025, 8, 23, 0, 0, 0, 0, time.UTC)
	hot := &fakeGuardHot{byTf: map[string]map[redisstore.CandleKeyRef]int64{
		"1d": {ref("BTCUSDT", "1d"): boundary.UnixMilli()},
	}}

	cases := []struct {
		name string
		hot  *fakeGuardHot
		cold *fakeGuardCold
	}{
		{"durable boundary unreadable", hot, &fakeGuardCold{err: errors.New("questdb is down")}},
		{"hot boundary unreadable", &fakeGuardHot{err: errors.New("redis is down")}, &fakeGuardCold{}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			risk := NewSoleCopyGuard(c.hot, c.cold).Probe(context.Background(), []string{"1d"})
			if risk.Evaluated {
				t.Fatal("the guard declared itself evaluated without reading one of the boundaries")
			}
			if risk.Reason == "" {
				t.Fatal("not evaluated and without saying why")
			}
			if risk.KeysWithSoleCopy() != 0 {
				t.Fatalf("an unevaluated verdict must come without a count, got %d", risk.KeysWithSoleCopy())
			}
		})
	}
}

// TestSoleCopyGuard_NoDurableTierIsNeverClean — a deployment without QuestDB is not a safe deployment
// for `allkeys-lru`: without a durable tier EVERY hot candle is a sole copy. The `if cold == nil {
// return ok }` is the mutant.
func TestSoleCopyGuard_NoDurableTierIsNeverClean(t *testing.T) {
	hot := &fakeGuardHot{byTf: map[string]map[redisstore.CandleKeyRef]int64{"1d": {ref("BTCUSDT", "1d"): 1}}}

	risk := NewSoleCopyGuard(hot, nil).Probe(context.Background(), []string{"1d"})
	if risk.Evaluated {
		t.Fatal("the guard declared clean a deployment with no durable tier")
	}

	risk = NewSoleCopyGuard(nil, &fakeGuardCold{}).Probe(context.Background(), []string{"1d"})
	if risk.Evaluated {
		t.Fatal("the guard declared clean a deployment with no hot tier")
	}

	risk = NewSoleCopyGuard(hot, &fakeGuardCold{}).Probe(context.Background(), nil)
	if risk.Evaluated {
		t.Fatal("the guard declared clean with no declared cell")
	}
}
