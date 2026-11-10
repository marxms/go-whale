package redisstore

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"

	"github.com/redis/go-redis/v9"
)

// fptr returns a pointer to v. Optional fields are pointers precisely so that
// absence is representable, so tests have to build them the same way production does.
func fptr(v float64) *float64 { return &v }
func i64ptr(v int64) *int64   { return &v }

// oiSeries builds n bucket-aligned points whose open interest in contracts follows
// values, one bar apart on timeframe tfMs.
func oiSeries(startTs, tfMs int64, values []float64) []*derivatives.DerivativesSnapshot {
	points := make([]*derivatives.DerivativesSnapshot, 0, len(values))
	for i, v := range values {
		points = append(points, &derivatives.DerivativesSnapshot{
			Timestamp:        startTs + int64(i)*tfMs,
			OpenInterestBase: fptr(v),
		})
	}
	return points
}

// TestBuildDerivativesBaseline_States asserts the terminal gate has TWO distinct
// conditions. Collapsing them would report "this symbol does not move" as "we have
// not looked for long enough", which are different facts and lead to different
// operator action.
func TestBuildDerivativesBaseline_States(t *testing.T) {
	const tfMs int64 = 3600 * 1000
	const startTs int64 = 1_700_000_000_000

	// A geometric ramp gives a constant log increment: MAD collapses to zero even
	// though open interest changes on every bar. Degenerate is about the SCALE, not
	// about the level being flat.
	ramp := make([]float64, 60)
	ramp[0] = 1000
	for i := 1; i < len(ramp); i++ {
		ramp[i] = ramp[i-1] * 1.01
	}

	// Alternating increments: enough dispersion for a usable MAD.
	varied := make([]float64, 60)
	varied[0] = 1000
	for i := 1; i < len(varied); i++ {
		if i%2 == 0 {
			varied[i] = varied[i-1] * 1.05
		} else {
			varied[i] = varied[i-1] * 0.97
		}
	}

	tests := []struct {
		name      string
		values    []float64
		wantState derivatives.BaselineState
		wantN     int
	}{
		{
			name: "fewer increments than n_min is immature",
			// 20 points => the two newest are dropped, leaving 18 points and 17 increments
			values:    varied[:20],
			wantState: derivatives.BaselineImmature,
			wantN:     17,
		},
		{
			name:      "constant log increment is degenerate, not immature",
			values:    ramp,
			wantState: derivatives.BaselineDegenerate,
			wantN:     57,
		},
		{
			name:      "dispersed increments are mature",
			values:    varied,
			wantState: derivatives.BaselineMature,
			wantN:     57,
		},
		{
			name:      "no points at all is immature, and carries no ruler",
			values:    nil,
			wantState: derivatives.BaselineImmature,
			wantN:     0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			points := oiSeries(startTs, tfMs, tc.values)
			baseline := buildDerivativesBaseline(points, "1h", 288, startTs)

			if baseline.OpenInterestState != tc.wantState {
				t.Fatalf("state = %q, want %q", baseline.OpenInterestState, tc.wantState)
			}
			if tc.wantN == 0 {
				if baseline.OpenInterest != nil {
					t.Fatalf("empty sample must carry no baseline, got %+v", *baseline.OpenInterest)
				}
				return
			}
			if baseline.OpenInterest == nil {
				t.Fatal("expected a baseline for a non-empty sample")
			}
			if baseline.OpenInterest.N != tc.wantN {
				t.Fatalf("n = %d, want %d", baseline.OpenInterest.N, tc.wantN)
			}

			// The terminal gate: anything but mature refuses to emit a score.
			_, state, ok := baseline.OpenInterest.Z(0.5, baseline.NMin)
			if ok != (tc.wantState == derivatives.BaselineMature) {
				t.Fatalf("Z ok = %v for state %q", ok, state)
			}
		})
	}
}

// TestBuildDerivativesBaseline_ExcludesScoredPoint pins the trailing rule: neither the
// possibly-open newest bucket nor the bucket being scored enters the ruler they will be
// scored against.
func TestBuildDerivativesBaseline_ExcludesScoredPoint(t *testing.T) {
	const tfMs int64 = 3600 * 1000
	const startTs int64 = 1_700_000_000_000

	// Five points => four adjacent pairs. Excluding the two newest leaves three points
	// and two increments.
	points := oiSeries(startTs, tfMs, []float64{100, 110, 121, 133.1, 1_000_000})
	baseline := buildDerivativesBaseline(points, "1h", 288, startTs)

	if baseline.OpenInterest == nil {
		t.Fatal("expected a baseline")
	}
	if baseline.OpenInterest.N != 2 {
		t.Fatalf("n = %d, want 2 (the two newest points are excluded)", baseline.OpenInterest.N)
	}
	// The excluded point is a 7500x jump; had it entered, the median would be far
	// above ln(1.1).
	if math.Abs(baseline.OpenInterest.Median-math.Log(1.1)) > 1e-9 {
		t.Fatalf("median = %v, want ln(1.1) = %v — the scored point leaked into its own ruler",
			baseline.OpenInterest.Median, math.Log(1.1))
	}
}

// adjacencyProbe is one point of the shared adjacency fixture: its offset in timeframes
// and its reading on the axis under test (nil means the axis has no reading at that bucket).
type adjacencyProbe struct {
	offset int64
	value  *float64
}

// adjacencyProbes pairs the shared hole/adjacency offsets with the caller's readings. The
// offsets are the invariant both pointer-valued axes must respect: two adjacent readings, a
// four-timeframe hole, a missing reading, a reading whose predecessor is missing, a further
// adjacent pair, then the scored point and the possibly-open point. Index 3 carries no reading.
func adjacencyProbes(values []*float64) []adjacencyProbe {
	offsets := []int64{0, 1, 5, 6, 7, 8, 9, 10}
	probes := make([]adjacencyProbe, len(offsets))
	for i, offset := range offsets {
		probes[i] = adjacencyProbe{offset: offset, value: values[i]}
	}
	return probes
}

// TestBuildDerivativesBaseline_SkipsNonAdjacentAndMissing asserts an increment is
// never taken across a hole. Bridging one would silently widen the window past the
// timeframe the column declares.
func TestBuildDerivativesBaseline_SkipsNonAdjacentAndMissing(t *testing.T) {
	const tfMs int64 = 3600 * 1000
	const startTs int64 = 1_700_000_000_000

	probes := adjacencyProbes([]*float64{
		fptr(100), fptr(110), fptr(120), nil, fptr(130), fptr(143), fptr(150), fptr(160),
	})
	points := make([]*derivatives.DerivativesSnapshot, 0, len(probes))
	for _, probe := range probes {
		points = append(points, &derivatives.DerivativesSnapshot{
			Timestamp:        startTs + probe.offset*tfMs,
			OpenInterestBase: probe.value,
		})
	}

	baseline := buildDerivativesBaseline(points, "1h", 288, startTs)
	if baseline.OpenInterest == nil {
		t.Fatal("expected a baseline")
	}
	if baseline.OpenInterest.N != 2 {
		t.Fatalf("n = %d, want 2 (only adjacent, fully-populated pairs)", baseline.OpenInterest.N)
	}
}

// TestBuildDerivativesBaseline_LongShortIsBuiltOnTheIncrement asserts the long/short
// ruler is built from ln LSR(t) − ln LSR(t−1) and not from the level log(LSR).
//
// THE DISCRIMINATOR. The sample below sits at a level far from 1 — every ratio is
// between 3.0 and 3.6 — and moves only slightly around it. Under the LEVEL estimator
// the median lands near ln 3 ≈ 1.0986; under the INCREMENT it lands near zero, because
// the sample rises as often as it falls. No single implementation gives both numbers,
// so a revert to the level fails this test on its first assertion.
//
// It also keeps the property the logarithm was adopted for: a rise by a factor and a
// fall by the same factor score the same distance from the center, with opposite signs.
func TestBuildDerivativesBaseline_LongShortIsBuiltOnTheIncrement(t *testing.T) {
	const tfMs int64 = 3600 * 1000
	const startTs int64 = 1_700_000_000_000

	// Up/down moves of TWO different magnitudes around a HIGH level. Two magnitudes
	// and not one because a single alternating step makes every increment equal in
	// absolute value, which collapses the MAD to zero and reports the axis as
	// degenerate — a real state, but not the one under test here.
	moves := []float64{1.20, 1.0 / 1.20, 1.05, 1.0 / 1.05}
	//
	// 62 moves, so that after the two trailing points are dropped the ruler holds 60
	// increments — a whole number of cycles, 15 of each move. That is what puts the
	// median exactly at zero and lets the mirror assertion below be exact.
	ratios := make([]float64, 0, 63)
	level := 3.0
	ratios = append(ratios, level)
	for i := 0; i < 62; i++ {
		level *= moves[i%len(moves)]
		ratios = append(ratios, level)
	}

	points := make([]*derivatives.DerivativesSnapshot, 0, len(ratios))
	for i, r := range ratios {
		points = append(points, &derivatives.DerivativesSnapshot{
			Timestamp:      startTs + int64(i)*tfMs,
			LongShortRatio: fptr(r),
		})
	}

	baseline := buildDerivativesBaseline(points, "1h", 288, startTs)
	if baseline.LongShortRatioState != derivatives.BaselineMature {
		t.Fatalf("state = %q, want mature", baseline.LongShortRatioState)
	}
	// ln 3 ≈ 1.0986 is what the LEVEL estimator would report here.
	if math.Abs(baseline.LongShortRatio.Median) > 0.2 {
		t.Fatalf("median = %v — that is the LEVEL of a ratio near 3, not an increment centered on no change",
			baseline.LongShortRatio.Median)
	}

	zUp, _, okUp := baseline.LongShortRatio.Z(math.Log(1.5), baseline.NMin)
	zDown, _, okDown := baseline.LongShortRatio.Z(math.Log(1.0/1.5), baseline.NMin)
	if !okUp || !okDown {
		t.Fatal("expected both mirrored increments to score")
	}
	if math.Abs(math.Abs(zUp)-math.Abs(zDown)) > 1e-9 {
		t.Fatalf("|z| asymmetric: %v vs %v", zUp, zDown)
	}
	if zUp <= 0 || zDown >= 0 {
		t.Fatalf("sign lost: zUp=%v zDown=%v", zUp, zDown)
	}
}

// TestBuildDerivativesBaseline_LongShortHonoursAdjacency asserts the long/short axis
// obeys the same hole rule as open interest, which the LEVEL estimator did not need and
// therefore did not have: a level is a point sample, an increment is not.
func TestBuildDerivativesBaseline_LongShortHonoursAdjacency(t *testing.T) {
	const tfMs int64 = 3600 * 1000
	const startTs int64 = 1_700_000_000_000

	probes := adjacencyProbes([]*float64{
		fptr(1.00), fptr(1.10), fptr(1.20), nil, fptr(1.30), fptr(1.43), fptr(1.50), fptr(1.60),
	})
	points := make([]*derivatives.DerivativesSnapshot, 0, len(probes))
	for _, probe := range probes {
		points = append(points, &derivatives.DerivativesSnapshot{
			Timestamp:      startTs + probe.offset*tfMs,
			LongShortRatio: probe.value,
		})
	}

	baseline := buildDerivativesBaseline(points, "1h", 288, startTs)
	if baseline.LongShortRatio == nil {
		t.Fatal("expected a long/short baseline")
	}
	// Under the level estimator this sample would yield 7 observations, one per bar.
	if baseline.LongShortRatio.N != 2 {
		t.Fatalf("n = %d, want 2 (only adjacent, fully-populated pairs)", baseline.LongShortRatio.N)
	}
}

// TestDerivativesBaselineWindowN pins the declared window, truncated by the measured
// 30-day retention of the upstream feed.
func TestDerivativesBaselineWindowN(t *testing.T) {
	tests := []struct {
		timeframe string
		want      int
	}{
		{"5m", 288},
		{"1h", 288},
		{"4h", 180},
		{"1d", 30},
		{"1w", 4},
	}
	for _, tc := range tests {
		t.Run(tc.timeframe, func(t *testing.T) {
			if got := derivativesBaselineWindowN(tc.timeframe); got != tc.want {
				t.Fatalf("windowN(%s) = %d, want %d", tc.timeframe, got, tc.want)
			}
		})
	}
}

// TestSeriesCapFor asserts the ZSET cap is per timeframe and can never sit below the
// baseline window. Under a single global cap of 200 members, the 288-point window of 5m
// and 1h would be unreachable by construction.
func TestSeriesCapFor(t *testing.T) {
	tests := []struct {
		name          string
		maxSeriesSize int64
		timeframe     string
		wantCap       int64
	}{
		{"5m is raised above the configured floor", 200, "5m", 291},
		{"1h is raised above the configured floor", 200, "1h", 291},
		{"4h keeps the larger configured floor", 200, "4h", 200},
		{"4h without configuration falls back to its window", 0, "4h", 183},
		{"1d without configuration falls back to its window", 0, "1d", 33},
		{"a larger configured value still wins", 5000, "5m", 5000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &DerivativesRedisStore{maxSeriesSize: tc.maxSeriesSize}
			if got := s.seriesCapFor(tc.timeframe); got != tc.wantCap {
				t.Fatalf("seriesCapFor(%s) = %d, want %d", tc.timeframe, got, tc.wantCap)
			}
		})
	}
}

// TestSeriesCapReachesDeclaredWindow is the test the cap exists for: a ZSET filled to
// its own cap must yield a baseline of exactly the declared N.
//
// It fails at cap = N+2, which looks like an off-by-one and is not:
// derivativesTrailingExcluded points are dropped by the trailing rule and an increment
// consumes two endpoints, so N+2 stored points can only ever produce N-1 increments and
// the declared window is unreachable by construction.
func TestSeriesCapReachesDeclaredWindow(t *testing.T) {
	const tfMs int64 = 3600 * 1000
	const startTs int64 = 1_700_000_000_000

	s := &DerivativesRedisStore{maxSeriesSize: 0}
	windowN := derivativesBaselineWindowN("1h")
	stored := int(s.seriesCapFor("1h"))

	// A ZSET holding exactly `stored` points, which is all the cap ever lets it hold.
	values := make([]float64, stored)
	level := 100.0
	for i := range values {
		values[i] = level
		level *= 1.01 + float64(i%3)/1000.0 // varied, so the MAD is not degenerate
	}

	baseline := buildDerivativesBaseline(oiSeries(startTs, tfMs, values), "1h", windowN, startTs)
	if baseline.OpenInterest == nil {
		t.Fatal("expected a baseline")
	}
	if baseline.OpenInterest.N != windowN {
		t.Fatalf("n = %d from a full ZSET, want the declared window %d — the cap cannot feed the window it declares",
			baseline.OpenInterest.N, windowN)
	}
	if baseline.OpenInterestState != derivatives.BaselineMature {
		t.Fatalf("state = %q on a full ZSET, want mature", baseline.OpenInterestState)
	}
}

// TestSeriesHorizonMs asserts the declared horizon matches the cap it comes from.
func TestSeriesHorizonMs(t *testing.T) {
	s := &DerivativesRedisStore{maxSeriesSize: 0}
	const hourMs = 3600 * 1000
	if got, want := s.SeriesHorizonMs("1h"), int64(291*hourMs); got != want {
		t.Fatalf("horizon(1h) = %d, want %d", got, want)
	}
	if got, want := s.SeriesHorizonMs("1d"), int64(33*24*hourMs); got != want {
		t.Fatalf("horizon(1d) = %d, want %d", got, want)
	}
}

// TestRecomputeBaseline_PersistsAndReads runs the whole loop against miniredis:
// series in, baseline key out, and the same values back through GetBaseline.
func TestRecomputeBaseline_PersistsAndReads(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestDerivStore(t, "1h")

	const tfMs int64 = 3600 * 1000
	startTs := bucketAlignMs(1_700_000_000_000, "1h")

	oi := 1000.0
	for i := 0; i < 60; i++ {
		if i%2 == 0 {
			oi *= 1.05
		} else {
			oi *= 0.97
		}
		snap := &derivatives.DerivativesSnapshot{
			Exchange:         "binance",
			MarketType:       "future",
			Symbol:           "BTCUSDT",
			Timestamp:        startTs + int64(i)*tfMs,
			OpenInterestBase: fptr(oi),
		}
		if err := store.SaveSeriesOnly(ctx, snap, "1h"); err != nil {
			t.Fatalf("SaveSeriesOnly: %v", err)
		}
	}

	computed, err := store.RecomputeBaseline(ctx, "binance", "future", "BTCUSDT", "1h")
	if err != nil {
		t.Fatalf("RecomputeBaseline: %v", err)
	}
	if computed.OpenInterestState != derivatives.BaselineMature {
		t.Fatalf("state = %q, want mature", computed.OpenInterestState)
	}
	if computed.WindowN != 288 || computed.NMin != derivativesBaselineNMin {
		t.Fatalf("declaration lost: windowN=%d nMin=%d", computed.WindowN, computed.NMin)
	}
	if computed.OpenInterest.SpanMs <= 0 {
		t.Fatalf("spanMs = %d, want the real span of the sample", computed.OpenInterest.SpanMs)
	}

	// The long/short axis had no data at all: it must be absent, not zeroed.
	if computed.LongShortRatio != nil {
		t.Fatalf("absent axis produced a ruler: %+v", *computed.LongShortRatio)
	}
	if computed.LongShortRatioState != derivatives.BaselineImmature {
		t.Fatalf("lsr state = %q, want immature", computed.LongShortRatioState)
	}

	read, err := store.GetBaseline(ctx, "binance", "future", "BTCUSDT", "1h")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if read == nil {
		t.Fatal("baseline key was not persisted")
	}
	if read.OpenInterest == nil || read.OpenInterest.MAD != computed.OpenInterest.MAD {
		t.Fatalf("round-trip lost the scale: %+v", read)
	}

	// A symbol nobody computed yet reads as absent, not as an error and not as zero.
	missing, err := store.GetBaseline(ctx, "binance", "future", "ETHUSDT", "1h")
	if err != nil {
		t.Fatalf("GetBaseline(missing): %v", err)
	}
	if missing != nil {
		t.Fatalf("expected nil for an uncomputed baseline, got %+v", missing)
	}
}

// TestSaveSnapshot_RefreshesBaseline asserts the recompute is actually WIRED to the
// write path — a ruler that only a test can trigger is a ruler nobody has.
func TestSaveSnapshot_RefreshesBaseline(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestDerivStore(t, "1h")

	snap := &derivatives.DerivativesSnapshot{
		Exchange:         "binance",
		MarketType:       "future",
		Symbol:           "BTCUSDT",
		Timestamp:        1_700_000_000_000,
		OpenInterestBase: fptr(1000),
	}
	if err := store.SaveSnapshot(ctx, snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	baseline, err := store.GetBaseline(ctx, "binance", "future", "BTCUSDT", "1h")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if baseline == nil {
		t.Fatal("saving a snapshot did not produce a baseline — the recompute is not connected")
	}
	// One point cannot make a ruler, and the record says so instead of inventing one.
	if baseline.OpenInterestState != derivatives.BaselineImmature {
		t.Fatalf("state = %q, want immature", baseline.OpenInterestState)
	}
}

// registryOrder is the configured timeframe set, in the order the registry hands it
// out (config.go). The ORDER is the point of the two tests below, not just the set.
func registryOrder() []string {
	return []string{"5m", "15m", "30m", "1h", "2h", "4h", "1d", "1w"}
}

// TestRefreshDueBaseline_DoesNotStarveTheTail reproduces a production state and asserts it
// cannot recur.
//
// What was measured: `derivatives:baseline:*` held exactly 2004 keys — 501 symbols
// on each of 5m, 15m, 30m and 1h, and ZERO on 2h, 4h, 1d and 1w — while the series
// existed for all eight. Those four are precisely the first four entries of the
// registry, and BTC's four rulers all carried an `updatedAt` inside the last ~33
// minutes against a 30-minute interval. So the head was being refreshed round-robin
// while the tail had never been written once.
//
// The mechanism is head-of-line starvation, and it needs no bug in the walk itself:
// the walk advances one timeframe per save, but the head falls due again after 30
// minutes, and a full sweep of eight timeframes needs eight saves — about 60 minutes
// at the observed cycle. The head therefore becomes eligible again before the walk
// ever reaches the tail, and a scan that always restarts at index 0 re-serves the
// covered prefix forever.
//
// The seed below is that exact state: a head that already has rulers and is past the
// interval, a tail that has none.
func TestRefreshDueBaseline_DoesNotStarveTheTail(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestDerivStore(t, registryOrder()...)

	head := registryOrder()[:4]
	tail := registryOrder()[4:]

	snap := &derivatives.DerivativesSnapshot{
		Exchange:         "binance",
		MarketType:       "future",
		Symbol:           "BTCUSDT",
		Timestamp:        1_700_000_000_000,
		OpenInterestBase: fptr(1000),
	}

	// Head: has a ruler AND is already past the refresh interval, so under an
	// index-0 scan it outranks the tail on every single save.
	staleMs := time.Now().UnixMilli() - derivativesBaselineRefreshInterval.Milliseconds() - 60_000
	for _, tf := range head {
		if _, err := store.RecomputeBaseline(ctx, snap.Exchange, snap.MarketType, snap.Symbol, tf); err != nil {
			t.Fatalf("seeding head %s: %v", tf, err)
		}
		store.markBaselineRefreshed(baselineTrackerKey(snap.Exchange, snap.MarketType, snap.Symbol, tf), staleMs)
	}

	// One save per starved timeframe. The one-recompute-per-save bound is deliberate,
	// so four saves is the minimum that can serve four timeframes — this asserts the
	// tail is reached, not that the bound was abandoned.
	for i := range tail {
		if err := store.SaveSnapshot(ctx, snap); err != nil {
			t.Fatalf("SaveSnapshot #%d: %v", i+1, err)
		}
	}

	for _, tf := range tail {
		baseline, err := store.GetBaseline(ctx, snap.Exchange, snap.MarketType, snap.Symbol, tf)
		if err != nil {
			t.Fatalf("GetBaseline %s: %v", tf, err)
		}
		if baseline == nil {
			t.Fatalf("%s never got a baseline: the head of the registry is starving the tail, "+
				"so every timeframe past the fourth stays permanently indeterminate", tf)
		}
	}
}

// TestRefreshDueBaseline_ServesEveryTimeframeBeforeRepeating pins the property that
// actually prevents the starvation: one full sweep before any repeat.
//
// Asserting only "the tail eventually gets a baseline" would still pass for a rule
// that serves the tail once and then goes back to favoring the head, which is the
// same defect with a slower clock.
//
// The clock is what makes this discriminating, so the clock is simulated. Real saves
// in a test happen microseconds apart, and with no time passing the head never falls
// due again mid-sweep — the defect simply does not reproduce. Aging every tracker by
// the observed per-symbol cadence after each save is what puts the production
// condition back: a 30-minute interval against a sweep that needs eight saves.
func TestRefreshDueBaseline_ServesEveryTimeframeBeforeRepeating(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestDerivStore(t, registryOrder()...)

	snap := &derivatives.DerivativesSnapshot{
		Exchange:         "binance",
		MarketType:       "future",
		Symbol:           "BTCUSDT",
		Timestamp:        1_700_000_000_000,
		OpenInterestBase: fptr(1000),
	}

	// Real cadence: a symbol is saved once per poll cycle, and the cycle is paced to
	// fit its 300s period. Eight timeframes therefore need ~40 min to sweep against a
	// 30-min interval — the head falls due again before the tail is reached, which is
	// the whole defect. The ordering rule under test does not depend on this number,
	// only on every timeframe being overdue; it is here so the fixture describes the
	// system it stands for.
	const perSaveAgeMs int64 = 5 * 60_000

	// Every timeframe already has a ruler and every one is overdue, so the choice is
	// forced to be about ordering rather than about who is eligible.
	base := time.Now().UnixMilli() - derivativesBaselineRefreshInterval.Milliseconds()
	for i, tf := range registryOrder() {
		if _, err := store.RecomputeBaseline(ctx, snap.Exchange, snap.MarketType, snap.Symbol, tf); err != nil {
			t.Fatalf("seeding %s: %v", tf, err)
		}
		age := int64(len(registryOrder())-i) * 60_000
		store.markBaselineRefreshed(baselineTrackerKey(snap.Exchange, snap.MarketType, snap.Symbol, tf), base-age)
	}

	seen := make(map[string]int, len(registryOrder()))
	for i := 0; i < len(registryOrder()); i++ {
		before := snapshotBaselineUpdatedAt(ctx, t, store, snap)
		if err := store.SaveSnapshot(ctx, snap); err != nil {
			t.Fatalf("SaveSnapshot #%d: %v", i+1, err)
		}
		after := snapshotBaselineUpdatedAt(ctx, t, store, snap)

		changed := make([]string, 0, 1)
		for _, tf := range registryOrder() {
			if after[tf] != before[tf] {
				changed = append(changed, tf)
			}
		}
		if len(changed) != 1 {
			t.Fatalf("save #%d recomputed %d timeframes (%v), want exactly 1 — the "+
				"one-per-save bound keeps the fleet-wide storm off the poll path", i+1, len(changed), changed)
		}
		seen[changed[0]]++

		ageBaselineTrackers(store, perSaveAgeMs)
	}

	for _, tf := range registryOrder() {
		if seen[tf] != 1 {
			t.Fatalf("after one sweep of %d saves, %s was served %d times (full tally: %v) — "+
				"a timeframe served twice before another is served once is the starvation",
				len(registryOrder()), tf, seen[tf], seen)
		}
	}
}

// ageBaselineTrackers pushes every tracked refresh further into the past, standing in
// for wall-clock time so a test can reach the 30-minute interval without waiting.
func ageBaselineTrackers(store *DerivativesRedisStore, byMs int64) {
	store.baselineMu.Lock()
	defer store.baselineMu.Unlock()
	for key, at := range store.baselineLastAt {
		store.baselineLastAt[key] = at - byMs
	}
}

// snapshotBaselineUpdatedAt reads the persisted updatedAt of every timeframe, so a
// test can tell which one a save actually recomputed. Absent reads as 0.
func snapshotBaselineUpdatedAt(
	ctx context.Context,
	t *testing.T,
	store *DerivativesRedisStore,
	snap *derivatives.DerivativesSnapshot,
) map[string]int64 {
	t.Helper()
	out := make(map[string]int64, len(registryOrder()))
	for _, tf := range registryOrder() {
		baseline, err := store.GetBaseline(ctx, snap.Exchange, snap.MarketType, snap.Symbol, tf)
		if err != nil {
			t.Fatalf("GetBaseline %s: %v", tf, err)
		}
		if baseline != nil {
			out[tf] = baseline.UpdatedAt
		}
	}
	return out
}

// ============================================================================
// THE PRICE AXIS
// ============================================================================

// seedCandles writes closes into the CANDLE zset for one (symbol, timeframe), which is
// where the price axis reads from. It writes the real key and the real member shape,
// not a fixture of its own — a test that seeded a private shape would pass against a
// reader that never worked in production.
func seedCandles(t *testing.T, client *redis.Client, exchange, marketType, symbol, timeframe string,
	startTs, tfMs int64, closes []float64,
) {
	t.Helper()
	ctx := context.Background()
	key := candlesKeyV2(exchange, marketType, symbol, timeframe)
	for i, close := range closes {
		ts := startTs + int64(i)*tfMs
		encoded, err := encodeCandleMember(candle.Candle{
			Timestamp: ts,
			Open:      close,
			High:      close,
			Low:       close,
			Close:     close,
		})
		if err != nil {
			t.Fatalf("encode candle: %v", err)
		}
		if err := client.ZAdd(ctx, key, redis.Z{Score: float64(ts), Member: encoded}).Err(); err != nil {
			t.Fatalf("seed candle: %v", err)
		}
	}
}

// alternatingCloses builds a close series whose log increments have real dispersion, so
// the resulting MAD is non-zero and the axis reaches `mature`.
func alternatingCloses(start float64, n int) []float64 {
	moves := []float64{1.02, 1.0 / 1.02, 1.005, 1.0 / 1.005}
	out := make([]float64, 0, n)
	level := start
	out = append(out, level)
	for i := 0; i < n-1; i++ {
		level *= moves[i%len(moves)]
		out = append(out, level)
	}
	return out
}

// TestRecomputeBaseline_BuildsThePriceAxisFromCandles is the anchor for the axis that
// did not exist: `priceZ` was literally null because no producer computed it.
//
// It asserts the ruler is built from the CANDLE close, that it is a real ruler rather
// than a zeroed placeholder, and — the part with no other guard — that the price axis
// is INDEPENDENT of the derivatives series: this fixture writes no derivatives points
// at all, so an implementation that derived price from the snapshot (from the notional,
// say) produces nothing here.
func TestRecomputeBaseline_BuildsThePriceAxisFromCandles(t *testing.T) {
	ctx := context.Background()
	store, client := newTestDerivStore(t, "1h")

	const tfMs int64 = 3600 * 1000
	startTs := bucketAlignMs(1_700_000_000_000, "1h")
	closes := alternatingCloses(60_000, 62)
	seedCandles(t, client, "binance", "future", "BTCUSDT", "1h", startTs, tfMs, closes)

	baseline, err := store.RecomputeBaseline(ctx, "binance", "future", "BTCUSDT", "1h")
	if err != nil {
		t.Fatalf("RecomputeBaseline: %v", err)
	}
	if baseline.Price == nil {
		t.Fatal("no price ruler was built — the axis is still absent")
	}
	if baseline.PriceState != derivatives.BaselineMature {
		t.Fatalf("price state = %q, want mature (n=%d)", baseline.PriceState, baseline.Price.N)
	}
	// 62 closes, the two newest excluded by the trailing rule, leaves 59 increments.
	if baseline.Price.N != 59 {
		t.Fatalf("price n = %d, want 59 (strictly trailing, one increment per adjacent pair)", baseline.Price.N)
	}
	if baseline.Price.MAD <= 0 {
		t.Fatalf("price MAD = %v, want a real scale", baseline.Price.MAD)
	}

	// STRICTLY TRAILING, proven by construction. A SECOND symbol carries the identical
	// history except for a tenfold move on each of its TWO newest bars — the bar being
	// scored and the one behind it that may still be open. Its ruler must come out
	// byte-identical to the first: a baseline that contained the observation being
	// scored would shrink every score exactly when the observation is largest, which is
	// the failure mode this rule exists to prevent.
	spiked := append(append([]float64(nil), closes[:len(closes)-2]...),
		closes[len(closes)-3]*10, closes[len(closes)-3]*100)
	seedCandles(t, client, "binance", "future", "SPIKEUSDT", "1h", startTs, tfMs, spiked)

	after, err := store.RecomputeBaseline(ctx, "binance", "future", "SPIKEUSDT", "1h")
	if err != nil {
		t.Fatalf("RecomputeBaseline (spiked): %v", err)
	}
	if after.Price == nil {
		t.Fatal("the spiked symbol got no price ruler at all")
	}
	if after.Price.N != baseline.Price.N {
		t.Fatalf("n = %d for the spiked symbol, want %d", after.Price.N, baseline.Price.N)
	}
	if after.Price.MAD != baseline.Price.MAD || after.Price.Median != baseline.Price.Median {
		t.Fatalf("the newest bar moved its own ruler: (median %v, MAD %v) vs (median %v, MAD %v)",
			after.Price.Median, after.Price.MAD, baseline.Price.Median, baseline.Price.MAD)
	}

	// The persisted key round-trips the new axis; a ruler that only exists in memory is
	// a ruler the read path never sees.
	read, err := store.GetBaseline(ctx, "binance", "future", "BTCUSDT", "1h")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if read.Price == nil || read.Price.MAD != baseline.Price.MAD {
		t.Fatalf("the price axis did not survive the round-trip: %+v", read.Price)
	}
}

// TestRecomputeBaseline_NoCandlesLeavesThePriceAxisAbsent asserts a symbol with no
// candle series gets NO price ruler rather than a zeroed one — a median of 0 with a MAD
// of 0 would be scored against and would read as a legitimate ruler for an asset whose
// price never moves.
func TestRecomputeBaseline_NoCandlesLeavesThePriceAxisAbsent(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestDerivStore(t, "1h")

	baseline, err := store.RecomputeBaseline(ctx, "binance", "future", "NOCANDLE", "1h")
	if err != nil {
		t.Fatalf("RecomputeBaseline: %v", err)
	}
	if baseline.Price != nil {
		t.Fatalf("an absent candle series produced a ruler: %+v", *baseline.Price)
	}
	if baseline.PriceState != derivatives.BaselineImmature {
		t.Fatalf("price state = %q, want immature", baseline.PriceState)
	}
}

// TestPriceIncrement asserts the scored point is taken between the named bar and the
// one exactly before it, and that every way of not having it is NAMED.
func TestPriceIncrement(t *testing.T) {
	ctx := context.Background()
	store, client := newTestDerivStore(t, "1h")

	const tfMs int64 = 3600 * 1000
	startTs := bucketAlignMs(1_700_000_000_000, "1h")
	// Three bars: startTs, +1h, and a hole at +2h, then a bar at +3h.
	seedCandles(t, client, "binance", "future", "BTCUSDT", "1h", startTs, tfMs, []float64{100, 103})
	seedCandles(t, client, "binance", "future", "BTCUSDT", "1h", startTs+3*tfMs, tfMs, []float64{107})

	got, reason, err := store.PriceIncrement(ctx, "binance", "future", "BTCUSDT", "1h", startTs+tfMs)
	if err != nil {
		t.Fatalf("PriceIncrement: %v", err)
	}
	if got == nil {
		t.Fatalf("no increment for an adjacent pair (reason %q)", reason)
	}
	if math.Abs(*got-math.Log(103.0/100.0)) > 1e-12 {
		t.Fatalf("increment = %v, want %v", *got, math.Log(103.0/100.0))
	}

	// A hole is never bridged: the bar at +3h has no neighbor at +2h, and the reader
	// must NOT reach back to +1h to manufacture one.
	holed, reason, err := store.PriceIncrement(ctx, "binance", "future", "BTCUSDT", "1h", startTs+3*tfMs)
	if err != nil {
		t.Fatalf("PriceIncrement (hole): %v", err)
	}
	if holed != nil {
		t.Fatalf("an increment was taken across a hole: %v", *holed)
	}
	if reason == "" {
		t.Fatal("the increment is absent across a hole and nothing said why")
	}

	// A bar with no candle at all is absent-with-a-reason, not zero.
	missing, reason, err := store.PriceIncrement(ctx, "binance", "future", "BTCUSDT", "1h", startTs+50*tfMs)
	if err != nil {
		t.Fatalf("PriceIncrement (missing): %v", err)
	}
	if missing != nil || reason == "" {
		t.Fatalf("a bar with no candle produced %v with reason %q", missing, reason)
	}
}

// TestBuildDerivativesBaseline_TheRulerExcludesTheOpenBucketAndTheScoredPoint pins the
// strictly-trailing invariant under the new scoring rule.
//
// The surface scores the last CLOSED bucket, which is the SECOND-newest stored point
// whenever the newest is still open. A ruler that dropped only the newest would
// therefore contain the very observation it is about to measure — the leak the
// strictly-trailing rule exists to prevent, and it bites hardest exactly when that
// observation is extreme.
//
// The fixture is built so the two rules cannot both pass: the two newest points carry
// moves of a completely different order of magnitude from the rest, and with only one
// of them dropped the MEDIAN itself moves.
func TestBuildDerivativesBaseline_TheRulerExcludesTheOpenBucketAndTheScoredPoint(t *testing.T) {
	const tfMs int64 = 3600 * 1000
	const startTs int64 = 1_700_000_000_000

	tests := []struct {
		name       string
		build      func(ts int64, values []float64) []*derivatives.DerivativesSnapshot
		axis       func(b *DerivativesBaseline) *derivatives.Baseline
		values     []float64
		wantN      int
		wantMedian float64
	}{
		{
			name:  "open interest",
			build: func(ts int64, v []float64) []*derivatives.DerivativesSnapshot { return oiSeries(ts, tfMs, v) },
			axis:  func(b *DerivativesBaseline) *derivatives.Baseline { return b.OpenInterest },
			// Two ordinary 10% steps, then the scored point and the open bucket, both
			// enormous. Dropping two leaves {ln 1.1, ln 1.1}; dropping one leaves
			// {ln 1.1, ln 1.1, ln 9090.9}, whose median is still ln 1.1 — so N is what
			// discriminates here, and the median guards the newest point on top of it.
			values:     []float64{100, 110, 121, 1_100_000, 11_000_000},
			wantN:      2,
			wantMedian: math.Log(1.1),
		},
		{
			name: "long/short ratio",
			build: func(ts int64, v []float64) []*derivatives.DerivativesSnapshot {
				points := make([]*derivatives.DerivativesSnapshot, 0, len(v))
				for i, r := range v {
					points = append(points, &derivatives.DerivativesSnapshot{
						Timestamp:      ts + int64(i)*tfMs,
						LongShortRatio: fptr(r),
					})
				}
				return points
			},
			axis: func(b *DerivativesBaseline) *derivatives.Baseline { return b.LongShortRatio },
			// Only ONE ordinary step survives here, so the leak is legible in the median
			// too: dropping two leaves {ln 1.1}, median ln 1.1; dropping one leaves
			// {ln 1.1, ln 9090.9}, whose median is the midpoint of the two — nowhere
			// near ln 1.1.
			values:     []float64{1.0, 1.1, 10_000.0, 100_000.0},
			wantN:      1,
			wantMedian: math.Log(1.1),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			baseline := buildDerivativesBaseline(tc.build(startTs, tc.values), "1h", 288, startTs)

			axis := tc.axis(baseline)
			if axis == nil {
				t.Fatal("expected a ruler")
			}
			if axis.N != tc.wantN {
				t.Fatalf("n = %d, want %d — the ruler holds more observations than the two "+
					"trailing exclusions allow", axis.N, tc.wantN)
			}
			if math.Abs(axis.Median-tc.wantMedian) > 1e-9 {
				t.Fatalf("median = %v, want %v — an excluded point leaked into the ruler it "+
					"is about to be scored against", axis.Median, tc.wantMedian)
			}
		})
	}
}

// seedDerivativesBars writes count bucket-aligned points into the DERIVATIVES series,
// the newest of them labeled newestTs, so a test can put that series at a chosen
// distance behind the candle series.
func seedDerivativesBars(t *testing.T, store *DerivativesRedisStore, symbol, timeframe string,
	newestTs, tfMs int64, count int,
) {
	t.Helper()
	ctx := context.Background()
	oi := 1000.0
	for i := count - 1; i >= 0; i-- {
		oi *= 1.01
		err := store.SaveSeriesOnly(ctx, &derivatives.DerivativesSnapshot{
			Exchange:         "binance",
			MarketType:       "future",
			Symbol:           symbol,
			Timestamp:        newestTs - int64(i)*tfMs,
			OpenInterestBase: fptr(oi),
		}, timeframe)
		if err != nil {
			t.Fatalf("SaveSeriesOnly %s: %v", symbol, err)
		}
	}
}

// waitPastBarBoundary sleeps into the next bar when the current one is within margin of
// closing, so a fixture anchored on "the bar that is open right now" cannot be
// invalidated mid-test by the bar turning under it.
func waitPastBarBoundary(t *testing.T, timeframe string, margin time.Duration) {
	t.Helper()
	tfMs := candle.ParseTimeframeToSeconds(timeframe) * 1000
	nowMs := time.Now().UnixMilli()
	remaining := bucketAlignMs(nowMs, timeframe) + tfMs - nowMs
	if remaining > margin.Milliseconds() {
		return
	}
	time.Sleep(time.Duration(remaining+1) * time.Millisecond)
}

// TestRecomputeBaseline_ThePriceRulerExcludesTheBarTHISSeriesScores pins the price
// ruler's exclusion to the IDENTITY of the scored bar rather than to a position.
//
// The two rulers live on two independent series: the scored bar is the last CLOSED
// bucket of the DERIVATIVES series, and the price ruler is built from the CANDLE
// series. Dropping a fixed count of newest bars from each only lines the two up while
// both series sit at the same bar, and even then the margin is zero — the drop reaches
// the scored bar and stops there. One bar of derivatives lag, which is what the 600 s
// gap over a 300 s interval and any poller stall produce, then leaves the scored
// increment inside the ruler it is about to be measured against.
//
// Each row seeds the derivatives series at its own distance behind the open candle,
// spikes the candle close AT THAT ROW'S SCORED BAR, and requires the resulting ruler to
// come out byte-identical to an unspiked twin. n is asserted alongside because it is
// what separates alignment from bumping the constant: the aligned rows must keep 59.
func TestRecomputeBaseline_ThePriceRulerExcludesTheBarTHISSeriesScores(t *testing.T) {
	const tfMs int64 = 3600 * 1000
	const candleCount = 62

	tests := []struct {
		name string
		// barsBehindOpen is where the NEWEST derivatives bucket sits relative to the
		// open candle, in bars.
		barsBehindOpen int64
		// scoredBehindOpen is the bar the reading is scored on, in bars behind the open
		// candle: the newest derivatives bucket once it has closed, else the one before.
		scoredBehindOpen int64
		wantN            int
	}{
		{
			name:             "aligned: the newest bucket is the open one, so the bar behind it is scored",
			barsBehindOpen:   0,
			scoredBehindOpen: 1,
			wantN:            59,
		},
		{
			name:             "the poller is one bucket behind, which has closed and is itself scored",
			barsBehindOpen:   1,
			scoredBehindOpen: 1,
			wantN:            59,
		},
		{
			name:             "the poller has fallen a bar further behind, and the price ruler has to follow",
			barsBehindOpen:   2,
			scoredBehindOpen: 2,
			wantN:            58,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store, client := newTestDerivStore(t, "1h")

			// Anchored on the real clock because closure is decided against it: the
			// newest candle has to be the bar that is genuinely open right now.
			// RecomputeBaseline reads that clock itself, so the fixture would be void if
			// the bar turned between here and there — the wait below buys a full bar of
			// margin against a body that runs in tens of milliseconds, and it only ever
			// fires in the last seconds of an hour.
			waitPastBarBoundary(t, "1h", 5*time.Second)
			openTs := bucketAlignMs(time.Now().UnixMilli(), "1h")
			startTs := openTs - int64(candleCount-1)*tfMs
			scoredTs := openTs - tc.scoredBehindOpen*tfMs
			scoredIdx := candleCount - 1 - int(tc.scoredBehindOpen)

			closes := alternatingCloses(60_000, candleCount)
			spiked := append([]float64(nil), closes...)
			spiked[scoredIdx] *= 10

			seedCandles(t, client, "binance", "future", "PLAINUSDT", "1h", startTs, tfMs, closes)
			seedCandles(t, client, "binance", "future", "SPIKEUSDT", "1h", startTs, tfMs, spiked)
			newestDerivTs := openTs - tc.barsBehindOpen*tfMs
			seedDerivativesBars(t, store, "PLAINUSDT", "1h", newestDerivTs, tfMs, 5)
			seedDerivativesBars(t, store, "SPIKEUSDT", "1h", newestDerivTs, tfMs, 5)

			// The spike is on the bar the reading is actually scored on, not on some bar
			// the test merely believes is scored: PriceIncrement resolves the same label
			// the surface asks for, and it has to come back carrying the tenfold move.
			increment, reason, err := store.PriceIncrement(ctx, "binance", "future", "SPIKEUSDT", "1h", scoredTs)
			if err != nil {
				t.Fatalf("PriceIncrement: %v", err)
			}
			if increment == nil {
				t.Fatalf("no increment on the scored bar %d (reason %q)", scoredTs, reason)
			}
			if *increment < 2 {
				t.Fatalf("the scored increment is %v — the spike is not on the scored bar", *increment)
			}

			plain, err := store.RecomputeBaseline(ctx, "binance", "future", "PLAINUSDT", "1h")
			if err != nil {
				t.Fatalf("RecomputeBaseline (plain): %v", err)
			}
			if plain.Price == nil {
				t.Fatal("no price ruler was built for the unspiked twin")
			}
			if plain.Price.N != tc.wantN {
				t.Fatalf("price n = %d, want %d — the ruler does not end one increment "+
					"before the bar this series scores (%d)", plain.Price.N, tc.wantN, scoredTs)
			}

			after, err := store.RecomputeBaseline(ctx, "binance", "future", "SPIKEUSDT", "1h")
			if err != nil {
				t.Fatalf("RecomputeBaseline (spiked): %v", err)
			}
			if after.Price == nil {
				t.Fatal("the spiked symbol got no price ruler at all")
			}
			if after.Price.N != plain.Price.N {
				t.Fatalf("n = %d for the spiked symbol, want %d", after.Price.N, plain.Price.N)
			}
			if after.Price.MAD != plain.Price.MAD || after.Price.Median != plain.Price.Median {
				t.Fatalf("the scored bar (%d) moved the ruler it is measured against: "+
					"(median %v, MAD %v) vs (median %v, MAD %v)", scoredTs,
					after.Price.Median, after.Price.MAD, plain.Price.Median, plain.Price.MAD)
			}
		})
	}
}
