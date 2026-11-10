package ops

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/derivatives/collector"
	"github.com/marxms/go-whale/internal/platform/config"
)

// seedCandleFleet writes `universe` symbols × one timeframe of candle ZSETs,
// alternating the depth so the cost fit has the ≥2 cardinalities it refuses to work
// without, and registers every symbol in active_symbols so SCARD can answer A.
func seedCandleFleet(t *testing.T, c *redis.Client, scope string, universe int, timeframe string, depths []int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < universe; i++ {
		symbol := fmt.Sprintf("SYM%04d/USDT", i)
		if err := c.SAdd(ctx, "active_symbols:"+scope, symbol).Err(); err != nil {
			t.Fatalf("SAdd: %v", err)
		}
		depth := depths[i%len(depths)]
		key := fmt.Sprintf("candles:%s:%s:%s", scope, symbol, timeframe)
		members := make([]redis.Z, 0, depth)
		for j := 0; j < depth; j++ {
			members = append(members, redis.Z{
				Score:  float64(j),
				Member: fmt.Sprintf(`{"timestamp":%d,"open":1.0,"high":1.0,"low":1.0,"close":1.0,"volume":1.0}`, j),
			})
		}
		if err := c.ZAdd(ctx, key, members...).Err(); err != nil {
			t.Fatalf("ZAdd %s: %v", key, err)
		}
	}
}

func measuredFamilyMiB(t *testing.T, c *redis.Client, pattern string) float64 {
	t.Helper()
	ctx := context.Background()
	keys, err := redisstore.SampleFamilyKeys(ctx, c, pattern, 1_000_000)
	if err != nil {
		t.Fatalf("SampleFamilyKeys: %v", err)
	}
	var total int64
	for _, k := range keys {
		used, err := c.MemoryUsage(ctx, k).Result()
		if err != nil {
			t.Fatalf("MEMORY USAGE %s: %v", k, err)
		}
		total += used
	}
	return float64(total) / float64(1<<20)
}

// buildCandleSheet runs the real sheet builder against a seeded server. The family
// row comes from CandleSizingFamily — the SAME assembly production uses.
func buildCandleSheet(t *testing.T, c *redis.Client, scope, timeframe string, capMembers int64) Sheet {
	t.Helper()
	ctx := context.Background()
	universe, source := ResolveUniverse(ctx, c, "active_symbols:"+scope)
	pattern := fmt.Sprintf("candles:%s:*", scope)
	ce := config.CandleEngineConfig{
		Timeframes:          []string{timeframe},
		BufferSizes:         map[string]int{"default": 800},
		RedisCapByTimeframe: map[string]int{timeframe: int(capMembers)},
	}
	return BuildSheetFrom(ctx, SheetInput{
		Service:        ServiceStreamer,
		Client:         c,
		UniverseCount:  universe,
		UniverseSource: source,
		SampleBudget:   CoefficientSample,
		NowMs:          1,
		WarnRatio:      0.60,
		Families: []FamilyInput{CandleSizingFamily(
			ce,
			[]string{timeframe},
			pattern,
			SampleAvailableDepth(ctx, c, pattern, []string{timeframe}, DepthSample),
		)},
	})
}

func TestSizingSheet_ProjectionTracksTheMeasuredBytesAcrossUniverses(t *testing.T) {
	const scope = "binance:future"
	const timeframe = "1d"
	depths := []int{8, 12}

	for _, universe := range []int{500, 800} {
		t.Run(fmt.Sprintf("A=%d", universe), func(t *testing.T) {
			mr := miniredis.RunT(t)
			c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			t.Cleanup(func() { _ = c.Close() })

			seedCandleFleet(t, c, scope, universe, timeframe, depths)

			sheet := buildCandleSheet(t, c, scope, timeframe, 800)

			if sheet.Universe != int64(universe) {
				t.Fatalf("Universe = %d, want %d (from SCARD)", sheet.Universe, universe)
			}
			measured := measuredFamilyMiB(t, c, fmt.Sprintf("candles:%s:*", scope))
			if measured <= 0 {
				t.Fatalf("measured family bytes = %v, nothing to compare against", measured)
			}
			drift := math.Abs(sheet.TotalMiB-measured) / measured
			if drift > 0.10 {
				t.Fatalf("projection %.4f MiB vs measured %.4f MiB — divergence %.1f%%, limit 10%%",
					sheet.TotalMiB, measured, drift*100)
			}
		})
	}
}

func TestSizingSheet_UniverseComesFromScardNotFromTheConfiguredCeiling(t *testing.T) {
	const scope = "binance:future"
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	seedCandleFleet(t, c, scope, 30, "1d", []int{8, 12})

	universe, source := ResolveUniverse(context.Background(), c, "active_symbols:"+scope)
	if universe != 30 {
		t.Fatalf("A = %d, want 30 — the published fleet, not the configured ceiling", universe)
	}
	if source != "SCARD active_symbols:binance:future" {
		t.Fatalf("origin of A = %q, want the SCARD expression", source)
	}
}

func TestSizingSheet_LinesCarryOriginAndDerivation(t *testing.T) {
	const scope = "binance:future"
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	seedCandleFleet(t, c, scope, 20, "1d", []int{8, 12})

	sheet := buildCandleSheet(t, c, scope, "1d", 800)
	body := strings.Join(sheet.Lines(), "\n")

	for _, want := range []string{
		"origin: candle_engine.redis_cap_by_timeframe",
		"derivation: ZSET cap resolved by CandleEngineConfig.RedisCapFor",
		"coefficients: b_candles=",
		"A=20 (SCARD active_symbols:binance:future)",
		"horizon=",
		"TARGET<=60%",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("the sheet does not declare %q:\n%s", want, body)
		}
	}
}

func TestSizingSheet_UndeclaredTargetIsSaidOutLoud(t *testing.T) {
	sheet := Sheet{Service: "streamer", WarnRatio: 0}
	body := strings.Join(sheet.Lines(), "\n")
	if !strings.Contains(body, "TARGET not declared (retention.budget_warn_ratio missing)") {
		t.Fatalf("the sheet invented a target instead of declaring the absence:\n%s", body)
	}
}

type fakeHorizonSource struct{ effective map[string]int64 }

func (f fakeHorizonSource) SeriesHorizonMs(tf string) int64 {
	secs := map[string]int64{"5m": 300, "15m": 900, "1d": 86400}
	return f.effective[tf] * secs[tf] * 1000
}

func TestDerivativesCapByTimeframe_ReportsTheEffectiveCapAndTheFloorThatRaisedIt(t *testing.T) {
	src := fakeHorizonSource{effective: map[string]int64{"5m": 291, "15m": 291, "1d": 800}}
	declared := func(tf string) int {
		return map[string]int{"5m": 3, "15m": 291, "1d": 800}[tf]
	}

	caps, notes := DerivativesCapByTimeframe(src, declared, []string{"5m", "15m", "1d"})

	if caps["5m"] != 291 {
		t.Fatalf("5m cap = %d, want 291 (effective, raised to the floor) — declared was 3", caps["5m"])
	}
	if caps["1d"] != 800 {
		t.Fatalf("1d cap = %d, want 800", caps["1d"])
	}
	joined := strings.Join(notes, " | ")
	if !strings.Contains(joined, "5m: declared=3 → EFFECTIVE=291") {
		t.Fatalf("the raise to the floor was not recorded: %q", joined)
	}
	if strings.Contains(joined, "15m:") || strings.Contains(joined, "1d:") {
		t.Fatalf("a timeframe without a raise must not generate noise: %q", joined)
	}
}

func TestSizingSheet_PublishesItsLineAndAggregatesTheOtherService(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	streamer := Sheet{Service: "streamer", PublishedAtMs: 1_000_000, Universe: 500, UniverseSource: "SCARD active_symbols:binance:future", TotalMiB: 1230.5}
	if err := PublishSheet(ctx, c, streamer); err != nil {
		t.Fatalf("PublishSheet: %v", err)
	}
	analyzer := Sheet{Service: "analyzer", PublishedAtMs: 700_000, Universe: 500, TotalMiB: 739.8}
	if err := PublishSheet(ctx, c, analyzer); err != nil {
		t.Fatalf("PublishSheet(analyzer): %v", err)
	}

	total, parts, err := AggregateMiB(ctx, c, 1_000_000)
	if err != nil {
		t.Fatalf("AggregateMiB: %v", err)
	}
	if math.Abs(total-(1230.5+739.8)) > 1e-6 {
		t.Fatalf("total = %v, want %v", total, 1230.5+739.8)
	}
	joined := strings.Join(parts, " · ")
	if !strings.Contains(joined, "streamer=1230.5 MiB") || !strings.Contains(joined, "analyzer=739.8 MiB") {
		t.Fatalf("the aggregation does not name the two services: %q", joined)
	}
	if !strings.Contains(joined, "analyzer=739.8 MiB (A=500, published 5 min ago)") {
		t.Fatalf("the age of the analyzer line was not declared: %q", joined)
	}
}

func TestSizingSheet_RefusesToProjectWithoutAMeasurement(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	sheet := BuildSheetFrom(ctx, SheetInput{
		Service:       ServiceStreamer,
		Client:        c,
		UniverseCount: 500,
		SampleBudget:  CoefficientSample,
		Families: []FamilyInput{{
			Family:         "candles",
			KeyPattern:     "candles:*",
			KeysPerSymbol:  map[string]float64{"1d": 1},
			CapByTimeframe: map[string]int64{"1d": 800},
			Origin:         "candle_engine.buffer_sizes",
		}},
	})

	if sheet.TotalMiB != 0 {
		t.Fatalf("TotalMiB = %v with nothing measured, want 0 — a projection was invented", sheet.TotalMiB)
	}
	body := strings.Join(sheet.Lines(), "\n")
	if !strings.Contains(body, "coefficients NOT measured") {
		t.Fatalf("the sheet did not declare that it did not measure:\n%s", body)
	}
}

func TestSampleImmortalFamilies_OnlyMinusOneIsImmortality(t *testing.T) {
	ctx := context.Background()
	const scope = "binance:future"
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	if err := c.ZAdd(ctx, "candles:"+scope+":BTC/USDT:1d", redis.Z{Score: 1, Member: "a"}).Err(); err != nil {
		t.Fatalf("seed candles: %v", err)
	}
	key := "derivatives:series:" + scope + ":BTC/USDT:1d"
	if err := c.ZAdd(ctx, key, redis.Z{Score: 1, Member: "a"}).Err(); err != nil {
		t.Fatalf("seed deriv: %v", err)
	}
	if err := c.Expire(ctx, key, time.Hour).Err(); err != nil {
		t.Fatalf("expire: %v", err)
	}

	got := SampleImmortalFamilies(ctx, c, scope)
	if len(got) != 1 || got[0] != "candles" {
		t.Fatalf("immortal families = %v, want [candles] — only TTL −1 is immortality", got)
	}
}

func TestIsImmortalTTL_MissingKeyIsNotImmortal(t *testing.T) {
	tests := []struct {
		name string
		ttl  time.Duration
		want bool
	}{
		{name: "-1: exists and does not expire", ttl: -1 * time.Nanosecond, want: true},
		{name: "-2: does not exist", ttl: -2 * time.Nanosecond, want: false},
		{name: "positive: expires", ttl: time.Hour, want: false},
		{name: "zero", ttl: 0, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsImmortalTTL(tt.ttl); got != tt.want {
				t.Fatalf("IsImmortalTTL(%v) = %v, want %v", tt.ttl, got, tt.want)
			}
		})
	}
}

func TestResidentFamilies_TheDeclarationCoversExactlyWhatTheStreamerProjects(t *testing.T) {
	const scope = "binance:future"
	ctx := context.Background()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	ce := config.CandleEngineConfig{Timeframes: []string{"1d"}, BufferSizes: map[string]int{"default": 800}}
	derivStore := fakeHorizonSource{effective: map[string]int64{"1d": 800}}
	declared := func(string) int { return 800 }

	projected := make([]string, 0, 2)
	for _, fam := range Families(ctx, c, ce, declared, derivStore, []string{"1d"}, scope) {
		projected = append(projected, fam.Family)
	}
	sort.Strings(projected)

	declaredFamilies := make([]string, 0, len(ResidentFamilies()))
	for _, fam := range ResidentFamilies() {
		if fam.Owner != ServiceStreamer {
			continue
		}
		if fam.Retention == "" {
			t.Fatalf("family %q declared without a retention mechanism", fam.Name)
		}
		if got := fam.PatternFor(scope); !strings.HasPrefix(got, fam.Name+":") {
			t.Fatalf("the pattern of %q is %q — the pattern must reach the family itself", fam.Name, got)
		}
		declaredFamilies = append(declaredFamilies, fam.Name)
	}
	sort.Strings(declaredFamilies)

	if strings.Join(projected, ",") != strings.Join(declaredFamilies, ",") {
		t.Fatalf("the sheet projects [%s] and the declaration names [%s]",
			strings.Join(projected, ","), strings.Join(declaredFamilies, ","))
	}
}

func TestSampleImmortalFamilies_ReadsTheDeclarationInsteadOfASecondLiteral(t *testing.T) {
	ctx := context.Background()
	const scope = "binance:future"
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	for _, fam := range ResidentFamilies() {
		key := strings.TrimSuffix(fam.PatternFor(scope), "*") + "BTC/USDT:1d"
		if err := c.ZAdd(ctx, key, redis.Z{Score: 1, Member: "a"}).Err(); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}

	got := SampleImmortalFamilies(ctx, c, scope)
	want := make([]string, 0, len(ResidentFamilies()))
	for _, fam := range ResidentFamilies() {
		want = append(want, fam.Name)
	}
	sort.Strings(want)

	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("immortal families = [%s], want [%s]", strings.Join(got, ","), strings.Join(want, ","))
	}
}

// THE SHEET PUBLISHES THE FUNCTION AND THE THREE INSTANCES, with the formula alongside.
func TestDerivativesDiskBudget_TheSheetPublishesTheFormulaAndTheThreeInstances(t *testing.T) {
	window := 2000 * 24 * time.Hour
	published := DerivativesBudgetLines("streamer", derivDurableTFsForOps(), window)
	lines := strings.Join(published, "\n")

	for _, needed := range []string{"A=500", "A=600", "A=800"} {
		if !strings.Contains(lines, needed) {
			t.Errorf("the sheet does not instantiate %s:\n%s", needed, lines)
		}
	}
	if !strings.Contains(lines, "bytes(A) =") {
		t.Errorf("the sheet publishes numbers without the formula that produced them:\n%s", lines)
	}
	if !strings.Contains(lines, "RETAINED window=") {
		t.Errorf("the sheet publishes the cost without saying over which window:\n%s", lines)
	}
	if !strings.Contains(lines, "OWNER'S DECISION") {
		t.Errorf("the sheet does not declare whose decision the window is:\n%s", lines)
	}

	costs := map[string]bool{}
	for _, line := range published {
		if idx := strings.Index(line, "→ "); idx >= 0 && strings.Contains(line, "rows →") {
			costs[line[strings.LastIndex(line, "→ "):]] = true
		}
	}
	if len(costs) != 3 {
		t.Errorf("the three instantiations printed %d distinct costs, want 3:\n%s", len(costs), lines)
	}
	if !strings.Contains(lines, "GiB") {
		t.Errorf("the budget came out in MiB on a scale of hundreds of GiB:\n%s", lines)
	}
	if !strings.Contains(lines, fmt.Sprintf("seed=%d", collector.DurableSeedPoints())) {
		t.Errorf("the sheet does not distinguish the seed footprint from the regime:\n%s", lines)
	}
	if !strings.Contains(lines, "NOT the regime") {
		t.Errorf("the seed was published without saying it is not the regime:\n%s", lines)
	}
}

func derivDurableTFsForOps() []string {
	return []string{"5m", "15m", "30m", "1h", "2h", "4h", "1d"}
}

func TestDerivativesDiskBudget_NoCoverageIsSaidOutLoud(t *testing.T) {
	lines := strings.Join(DerivativesBudgetLines("streamer", nil, 2000*24*time.Hour), "\n")
	if !strings.Contains(lines, "WITHOUT durable coverage") {
		t.Errorf("empty coverage came out as a normal projection:\n%s", lines)
	}
	if strings.Contains(lines, "A=500") {
		t.Errorf("a function was instantiated with no coverage at all:\n%s", lines)
	}
}

func TestDerivativesDiskBudget_NoRetentionWindowIsSaidOutLoud(t *testing.T) {
	lines := strings.Join(DerivativesBudgetLines("streamer", derivDurableTFsForOps(), 0), "\n")
	if !strings.Contains(lines, "WITHOUT declared retention") {
		t.Errorf("missing window came out as a normal projection:\n%s", lines)
	}
	if strings.Contains(lines, "A=500") {
		t.Errorf("a function was instantiated with no window at all:\n%s", lines)
	}
}

// AND THE LINE REACHES THE ASSEMBLED SHEET.
func TestSizingSheet_CarriesTheDerivativesDiskBudget(t *testing.T) {
	const window = 2000 * 24 * time.Hour
	sheet := Sheet{
		Service:                   "streamer",
		DurableTimeframes:         []string{"1h", "4h", "1d"},
		DerivativesRetainedWindow: window,
	}
	lines := strings.Join(sheet.Lines(), "\n")
	if !strings.Contains(lines, "derivatives (disk) FUNCTION of A") {
		t.Errorf("the assembled sheet does not publish the disk budget:\n%s", lines)
	}
	if !strings.Contains(lines, "RETAINED window=") {
		t.Errorf("the assembled sheet lost the window along the way:\n%s", lines)
	}
}

// TestDurableCoverageLines_AWrittenColumnTheRouteDoesNotAskIsNotCovered: a column
// that is written but not read is NOT covered.
func TestDurableCoverageLines_AWrittenColumnTheRouteDoesNotAskIsNotCovered(t *testing.T) {
	coverage := config.DurableCellCoverage{
		FieldColumn: map[string]string{
			"fundingRate":  "funding_rate",
			"openInterest": "open_interest",
			"oiChangePct":  "",
		},
		ReadableColumns: []string{"timestamp", "funding_rate"},
	}

	lines := strings.Join(DurableCoverageLines("streamer", coverage), "\n")

	if !strings.Contains(lines, "1 of 3 quantities are WRITTEN AND READABLE") {
		t.Errorf("the count counted the column the read does not ask for as covered:\n%s", lines)
	}
	if strings.Contains(lines, "openInterest→open_interest") {
		t.Errorf("openInterest appeared in the covered list:\n%s", lines)
	}
	if !strings.Contains(lines, "fundingRate→funding_rate") {
		t.Errorf("the field that is written AND readable disappeared from the covered list:\n%s", lines)
	}
	if !strings.Contains(lines, "openInterest (open_interest is written and the read does NOT ask for it)") {
		t.Errorf("the reason openInterest is not covered does not appear:\n%s", lines)
	}
	if !strings.Contains(lines, "oiChangePct (no durable column") {
		t.Errorf("the reason oiChangePct is not covered does not appear:\n%s", lines)
	}
}

func TestDurableCoverageLines_TheSheetAgreesWithTheGate(t *testing.T) {
	coverage := DurableFieldCoverage()
	lines := strings.Join(DurableCoverageLines("streamer", coverage), "\n")

	uncoveredByGate := map[string]bool{}
	for _, finding := range coverage.UncoveredMemberFields("qualquer") {
		uncoveredByGate[strings.SplitN(finding, " (", 2)[0]] = true
	}
	if len(uncoveredByGate) == 0 {
		t.Fatal("the gate found no uncovered field — the comparison would pass vacuously")
	}

	for field, column := range coverage.FieldColumn {
		onCoveredList := strings.Contains(lines, field+"→"+column)
		if uncoveredByGate[field] && onCoveredList {
			t.Errorf("the sheet counts %q as covered and the gate refuses it", field)
		}
		if !uncoveredByGate[field] && !onCoveredList {
			t.Errorf("the gate accepts %q and the sheet does not count it as covered", field)
		}
	}
}

func TestDurableCoverageLines_TheRealDeclarationIsTwelveOfThirteen(t *testing.T) {
	lines := strings.Join(DurableCoverageLines("streamer", DurableFieldCoverage()), "\n")

	if !strings.Contains(lines, "12 of 13 quantities are WRITTEN AND READABLE") {
		t.Errorf("the real shape of the coverage changed and the sheet did not follow:\n%s", lines)
	}
	if !strings.Contains(lines, "oiChangePct (no durable column") {
		t.Errorf("the only quantity without a column is not named:\n%s", lines)
	}
	for _, field := range []string{
		"lsLongPct→ls_long_pct", "lsShortPct→ls_short_pct", "openInterest→open_interest",
		"interestRate→interest_rate", "fundingAtAnchor→funding_at_anchor",
		"nextFundingRate→next_funding_rate",
	} {
		if !strings.Contains(lines, field) {
			t.Errorf("%s does not appear as written AND readable:\n%s", field, lines)
		}
	}
}

func TestSizingSheet_CarriesTheTwoHalvesOfDurableCoverage(t *testing.T) {
	lines := strings.Join(Sheet{Service: "streamer"}.Lines(), "\n")
	if !strings.Contains(lines, "WRITTEN AND READABLE") {
		t.Errorf("the assembled sheet does not publish the conjunction the gate requires:\n%s", lines)
	}
	if !strings.Contains(lines, "NOT covered") {
		t.Errorf("the assembled sheet does not publish what the cap would destroy:\n%s", lines)
	}
}

// THE ZSET CAP AND THE RING CAP ARE DIFFERENT KEYS: the sheet reads RedisCapFor.
func TestCandleCapByTimeframe_UsesTheStoreAuthority(t *testing.T) {
	ce := config.CandleEngineConfig{
		BufferSizes:         map[string]int{"default": 800},
		RedisCapByTimeframe: map[string]int{"5m": 521},
	}
	caps, notes := CandleCapByTimeframe(ce, []string{"5m", "1d"})
	if caps["5m"] != 521 {
		t.Errorf("5m cap = %d, want 521", caps["5m"])
	}
	if caps["1d"] != 800 {
		t.Errorf("1d cap = %d, want the inherited 800", caps["1d"])
	}
	if !strings.Contains(strings.Join(notes, " "), "1d WITHOUT its own entry") {
		t.Errorf("the inheritance from buffer_sizes was not declared: %v", notes)
	}
}

func TestCandleCapEntries(t *testing.T) {
	ce := config.CandleEngineConfig{RedisCapByTimeframe: map[string]int{"5m": 521}}
	got := CandleCapEntries(ce)
	if got["5m"] != 521 {
		t.Fatalf("entries = %v", got)
	}
	if _, ok := got["1d"]; ok {
		t.Fatalf("a timeframe without an entry appeared: %v", got)
	}
}

func TestFormatHorizon(t *testing.T) {
	tests := map[int64]string{0: "n/d", 3600: "1.0h", 86400: "1.0d", 86400 * 365: "1.0a"}
	for seconds, want := range tests {
		if got := FormatHorizon(seconds); got != want {
			t.Errorf("FormatHorizon(%d) = %q, want %q", seconds, got, want)
		}
	}
	if got := FormatMiB(0); got != "n/d" {
		t.Errorf("FormatMiB(0) = %q", got)
	}
	if got := FormatBudgetSize(0); got != "n/d" {
		t.Errorf("FormatBudgetSize(0) = %q", got)
	}
	if got := FormatBudgetSize(1 << 20); got != "1 MiB" {
		t.Errorf("FormatBudgetSize(1 MiB) = %q", got)
	}
	if got := FormatBudgetSize(2 << 30); got != "2.0 GiB" {
		t.Errorf("FormatBudgetSize(2 GiB) = %q", got)
	}
}

// The family line comes from the SAME assembly production uses.
func TestCandleSizingFamily_OriginIsTheStoreKey(t *testing.T) {
	fam := CandleSizingFamily(config.CandleEngineConfig{
		BufferSizes: map[string]int{"default": 800},
	}, []string{"1d"}, "candles:binance:future:*", nil)
	if fam.Origin != CandleCapConfigKey {
		t.Errorf("origin = %q, want %q", fam.Origin, CandleCapConfigKey)
	}
	if fam.Family != "candles" {
		t.Errorf("family = %q", fam.Family)
	}
}

func TestUniformKeysPerSymbol(t *testing.T) {
	got := UniformKeysPerSymbol([]string{"5m", "1h"})
	if len(got) != 2 || got["5m"] != 1 || got["1h"] != 1 {
		t.Fatalf("UniformKeysPerSymbol = %v", got)
	}
}
