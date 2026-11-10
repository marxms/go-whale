package questdb

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
)

// ============================================================================
// DOUBLES — a hot tier and a stateful durable tier
// ============================================================================

// fakeHotCandles stores series by (symbol, tf) and answers by SCORE RANGE, like the ZSET.
//
// State, not a call list: the question here is "which candles ended up in the durable tier",
// and a double that only records calls cannot distinguish migrating the right ones from
// migrating all of them.
type fakeHotCandles struct {
	series  map[redisstore.CandleKeyRef][]candle.Candle
	scanErr error
	// scanErrFor breaks the scan of ONE timeframe, so the abort lands after another one has
	// already completed.
	scanErrFor map[string]error
	reads      int
	// mutated counts any write/removal on the hot side. It must stay at zero.
	mutated int
}

func newFakeHotCandles() *fakeHotCandles {
	return &fakeHotCandles{series: map[redisstore.CandleKeyRef][]candle.Candle{}}
}

func (h *fakeHotCandles) seed(ref redisstore.CandleKeyRef, cs []candle.Candle) {
	h.series[ref] = append(h.series[ref], cs...)
}

func (h *fakeHotCandles) ScanCandleKeys(_ context.Context, timeframe string) ([]redisstore.CandleKeyRef, error) {
	if h.scanErr != nil {
		return nil, h.scanErr
	}
	if err, hasErr := h.scanErrFor[timeframe]; hasErr {
		return nil, err
	}
	var out []redisstore.CandleKeyRef
	for ref := range h.series {
		if ref.Timeframe == timeframe {
			out = append(out, ref)
		}
	}
	return out, nil
}

func (h *fakeHotCandles) GetCandlesByScoreRange(_ context.Context, exchange, marketType, symbol, timeframe string,
	minScore, maxScore int64,
) ([]candle.Candle, error) {
	h.reads++
	ref := redisstore.CandleKeyRef{Exchange: exchange, MarketType: marketType, Symbol: symbol, Timeframe: timeframe}
	var out []candle.Candle
	for _, c := range h.series[ref] {
		if c.Timestamp >= minScore && c.Timestamp <= maxScore {
			out = append(out, c)
		}
	}
	return out, nil
}

// fakeColdCandles is the durable tier: boundary per (symbol, tf) and the rows written.
type fakeColdCandles struct {
	oldest    map[redisstore.CandleKeyRef]time.Time
	written   map[candle.SeriesKey][]candle.Candle
	commits   int
	writeErr  error
	oldestErr error
	// oldestErrFor breaks the boundary of ONE symbol. See failOldestFor.
	oldestErrFor map[string]error
}

func newFakeColdCandles() *fakeColdCandles {
	return &fakeColdCandles{
		oldest:  map[redisstore.CandleKeyRef]time.Time{},
		written: map[candle.SeriesKey][]candle.Candle{},
	}
}

func (c *fakeColdCandles) coverFrom(ref redisstore.CandleKeyRef, from time.Time) {
	c.oldest[ref] = from
}

// failOldestFor breaks the boundary of ONE symbol, so the pass aborts in the middle of the
// loop with earlier series already counted.
func (c *fakeColdCandles) failOldestFor(symbol string, err error) {
	if c.oldestErrFor == nil {
		c.oldestErrFor = map[string]error{}
	}
	c.oldestErrFor[symbol] = err
}

func (c *fakeColdCandles) GetOldestTimestamp(_ context.Context, exchange, marketType, symbol, timeframe string) (time.Time, error) {
	if c.oldestErr != nil {
		return time.Time{}, c.oldestErr
	}
	if err, hasErr := c.oldestErrFor[symbol]; hasErr {
		return time.Time{}, err
	}
	return c.oldest[redisstore.CandleKeyRef{Exchange: exchange, MarketType: marketType, Symbol: symbol, Timeframe: timeframe}], nil
}

func (c *fakeColdCandles) WriteBatches(_ context.Context, groups map[candle.SeriesKey][]candle.Candle) error {
	if c.writeErr != nil {
		return c.writeErr
	}
	c.commits++
	for k, v := range groups {
		c.written[k] = append(c.written[k], v...)
	}
	return nil
}

// advanceOldestFromWrites moves each symbol's durable boundary to the SMALLEST timestamp it
// already has written. It is what QuestDB would answer on the next boot, and it is what makes
// resumption observable without any state stored by the migration.
func (c *fakeColdCandles) advanceOldestFromWrites() {
	for key, escritas := range c.written {
		if len(escritas) == 0 {
			continue
		}
		menor := escritas[0].Timestamp
		for _, candle := range escritas {
			if candle.Timestamp < menor {
				menor = candle.Timestamp
			}
		}
		ref := redisstore.CandleKeyRef{
			Exchange: key.Scope.Exchange.String(), MarketType: key.Scope.MarketType.String(),
			Symbol: key.Scope.Symbol.String(), Timeframe: key.Timeframe.String(),
		}
		if atual, existe := c.oldest[ref]; !existe || menor < atual.UnixMilli() {
			c.oldest[ref] = time.UnixMilli(menor)
		}
	}
}

func (c *fakeColdCandles) totalWritten() int {
	var n int
	for _, v := range c.written {
		n += len(v)
	}
	return n
}

// --- seeding -----------------------------------------------------------------

func dayCandles(startMs int64, n int, stepMs int64) []candle.Candle {
	out := make([]candle.Candle, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, candle.Candle{
			Timestamp: startMs + int64(i)*stepMs,
			Open:      100, High: 101, Low: 99, Close: 100.5, Volume: 12.5,
		})
	}
	return out
}

func ref1d(symbol string) redisstore.CandleKeyRef {
	return redisstore.CandleKeyRef{Exchange: "binance", MarketType: "future", Symbol: symbol, Timeframe: "1d"}
}

const oneDayMs int64 = 86_400_000

// The MEASURED coverage of the durable tier for `1d` starts on 2025-08-23; `retention_days:
// 365` is the ceiling, not the coverage. This is the number a mutant replaces with "the last
// 365 days".
func coldStart1d() time.Time {
	return time.Date(2025, 8, 23, 0, 0, 0, 0, time.UTC)
}

// ============================================================================
// CASES
// ============================================================================

// TestSoleCopyMigration_MigratesEveryMemberOlderThanTheColdBoundary is the central criterion:
// what is migrated is what lies BELOW the durable boundary, and only that.
func TestSoleCopyMigration_MigratesEveryMemberOlderThanTheColdBoundary(t *testing.T) {
	t.Parallel()

	hot, cold := newFakeHotCandles(), newFakeColdCandles()
	r := ref1d("BTC/USDT")
	fronteira := coldStart1d().UnixMilli()

	// 200 candles before the boundary and 300 after — only the 200 are sole copies.
	hot.seed(r, dayCandles(fronteira-200*oneDayMs, 200, oneDayMs))
	hot.seed(r, dayCandles(fronteira, 300, oneDayMs))
	cold.coverFrom(r, coldStart1d())

	report, err := NewSoleCopyMigrator(hot, cold).Run(context.Background(),
		SoleCopyMigrationOptions{Timeframes: []string{"1d"}, Apply: true})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if report.Candles() != 200 {
		t.Fatalf("%d candle(s) migrated, wanted 200", report.Candles())
	}
	if cold.totalWritten() != 200 {
		t.Fatalf("%d row(s) written, wanted 200", cold.totalWritten())
	}
	for _, c := range cold.written[mustSeriesKey("binance", "future", "BTC/USDT", "1d")] {
		if c.Timestamp >= fronteira {
			t.Fatalf("candle %d was already covered and was rewritten", c.Timestamp)
		}
	}
}

// TestSoleCopyMigration_ASymbolWithNoColdRowsMigratesItsWholeSeries.
//
// KILLS the mutant that migrates "only what is missing" computed from a boundary that does
// not exist: without a durable row, `MIN(timestamp)` is null, and treating null as zero (or
// as "now") migrates the whole series or no candles. The right answer is the whole series.
func TestSoleCopyMigration_ASymbolWithNoColdRowsMigratesItsWholeSeries(t *testing.T) {
	t.Parallel()

	hot, cold := newFakeHotCandles(), newFakeColdCandles()
	r := ref1d("ORPHAN/USDT")
	hot.seed(r, dayCandles(coldStart1d().UnixMilli()-100*oneDayMs, 434, oneDayMs))
	// WITHOUT cold.coverFrom: the symbol has no durable row at all.

	report, err := NewSoleCopyMigrator(hot, cold).Run(context.Background(),
		SoleCopyMigrationOptions{Timeframes: []string{"1d"}, Apply: true})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if report.Candles() != 434 {
		t.Fatalf("%d candle(s) migrated, wanted the 434 of the whole series", report.Candles())
	}
	if report.Cells[0].KeysWithoutColdRows != 1 {
		t.Fatalf("the report did not record the key without a durable row: %+v", report.Cells[0])
	}
}

// TestSoleCopyMigration_TheBoundaryIsREADFromTheColdTier asserts the boundary comes from the
// durable tier: a migration that "only migrates what was already covered" must fail.
//
// A LITERAL boundary — today's date minus `retention_days`, or 2025-08-23 written in the
// code — gives the same result in both cases below. The read boundary gives different
// results, because it is different.
func TestSoleCopyMigration_TheBoundaryIsREADFromTheColdTier(t *testing.T) {
	t.Parallel()

	migradas := func(cobreDe time.Time) int {
		hot, cold := newFakeHotCandles(), newFakeColdCandles()
		r := ref1d("BTC/USDT")
		hot.seed(r, dayCandles(coldStart1d().UnixMilli()-500*oneDayMs, 1000, oneDayMs))
		cold.coverFrom(r, cobreDe)

		report, err := NewSoleCopyMigrator(hot, cold).Run(context.Background(),
			SoleCopyMigrationOptions{Timeframes: []string{"1d"}, Apply: true})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		return report.Candles()
	}

	rasa := migradas(coldStart1d())                             // covers from 2025-08-23
	funda := migradas(coldStart1d().Add(-300 * 24 * time.Hour)) // covers 300 days earlier

	if rasa != 500 {
		t.Fatalf("with coverage from the measured boundary, migrated %d, wanted 500", rasa)
	}
	if funda != 200 {
		t.Fatalf("with coverage 300 days deeper, migrated %d, wanted 200", funda)
	}
	if rasa == funda {
		t.Fatal("the boundary did not come from the durable tier — both cases gave the same number")
	}
}

// TestSoleCopyMigration_TheFleetCohortReachesTheMeasuredTotal stages the cohort measured in
// production: 297 of 500 `1d` keys with 95,811 members older than the coverage.
//
// KILLS the migration that stops early — on a key ceiling, a read limit or a batch that
// swallows the rest.
func TestSoleCopyMigration_TheFleetCohortReachesTheMeasuredTotal(t *testing.T) {
	t.Parallel()

	hot, cold := newFakeHotCandles(), newFakeColdCandles()
	fronteira := coldStart1d().UnixMilli()

	// 95,811 = 296 keys × 323 + 1 key × 203. The measured mean is 191.6 and the maximum 434;
	// what this case fixes is the TOTAL, the number the spec cites.
	const soleCopyTotal = 95_811
	restante := soleCopyTotal
	for i := 0; i < 500; i++ {
		r := ref1d(fmt.Sprintf("SYM%03d/USDT", i))
		// All 500 have a recent series, already covered.
		hot.seed(r, dayCandles(fronteira, 30, oneDayMs))
		cold.coverFrom(r, coldStart1d())
		if i < 297 && restante > 0 {
			n := 323
			if i == 296 {
				n = restante
			}
			hot.seed(r, dayCandles(fronteira-int64(n)*oneDayMs, n, oneDayMs))
			restante -= n
		}
	}

	report, err := NewSoleCopyMigrator(hot, cold).Run(context.Background(),
		SoleCopyMigrationOptions{Timeframes: []string{"1d"}, Apply: true})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if report.Candles() != soleCopyTotal {
		t.Fatalf("%d candle(s) migrated, wanted %d", report.Candles(), soleCopyTotal)
	}
	if cold.totalWritten() != soleCopyTotal {
		t.Fatalf("%d row(s) in the durable tier, wanted %d", cold.totalWritten(), soleCopyTotal)
	}
	if report.Cells[0].KeysWithSoleCopy != 297 {
		t.Fatalf("%d key(s) with a sole copy, wanted 297", report.Cells[0].KeysWithSoleCopy)
	}
	if report.Cells[0].Keys != 500 {
		t.Fatalf("%d key(s) scanned, wanted 500", report.Cells[0].Keys)
	}
}

// TestSoleCopyMigration_CommitsInBatchesNotOnePerCandle: 129,190 candles point by point
// would reintroduce in one go the amplification that cost 3.91 TB written to store 3.6 GB.
func TestSoleCopyMigration_CommitsInBatchesNotOnePerCandle(t *testing.T) {
	t.Parallel()

	hot, cold := newFakeHotCandles(), newFakeColdCandles()
	fronteira := coldStart1d().UnixMilli()
	for i := 0; i < 50; i++ {
		r := ref1d(fmt.Sprintf("SYM%03d/USDT", i))
		hot.seed(r, dayCandles(fronteira-200*oneDayMs, 200, oneDayMs))
		cold.coverFrom(r, coldStart1d())
	}

	report, err := NewSoleCopyMigrator(hot, cold).Run(context.Background(),
		SoleCopyMigrationOptions{Timeframes: []string{"1d"}, Apply: true})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if report.Candles() != 10_000 {
		t.Fatalf("%d candle(s), wanted 10,000", report.Candles())
	}
	if cold.commits != 200 {
		t.Fatalf("%d commit(s) for 200 DAYs/50 series/10,000 candles; wanted 200 (one per DAY, never per row)",
			cold.commits)
	}
}

// TestSoleCopyMigration_TheBatchIsBoundedNotUnbounded: the other side of the case above. An
// unbounded batch accumulates all 129,190 candles in process memory before the first commit.
func TestSoleCopyMigration_TheBatchIsBoundedNotUnbounded(t *testing.T) {
	t.Parallel()

	hot, cold := newFakeHotCandles(), newFakeColdCandles()
	fronteira := coldStart1d().UnixMilli()
	for i := 0; i < 10; i++ {
		r := ref1d(fmt.Sprintf("SYM%03d/USDT", i))
		hot.seed(r, dayCandles(fronteira-100*oneDayMs, 100, oneDayMs))
		cold.coverFrom(r, coldStart1d())
	}

	report, err := NewSoleCopyMigrator(hot, cold).Run(context.Background(),
		SoleCopyMigrationOptions{Timeframes: []string{"1d"}, Apply: true, FlushEvery: 250})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.Candles() != 1000 || cold.totalWritten() != 1000 {
		t.Fatalf("report=%d written=%d, wanted 1000 in both", report.Candles(), cold.totalWritten())
	}
	if cold.commits < 4 {
		t.Fatalf("%d commit(s) for 1,000 candles with a ceiling of 250, wanted at least 4", cold.commits)
	}
}

// TestSoleCopyMigration_NeverTouchesTheHotTier is the other side of the route test the spec
// requires ("`/api/timeseries?tf=1d&limit=800` identical before and after"): the migration
// READS Redis.
//
// KILLS the migration that uses the pass to cap or delete — that is a later stage, and doing
// it here would invert the order the retention safety depends on.
func TestSoleCopyMigration_NeverTouchesTheHotTier(t *testing.T) {
	t.Parallel()

	hot, cold := newFakeHotCandles(), newFakeColdCandles()
	r := ref1d("BTC/USDT")
	fronteira := coldStart1d().UnixMilli()
	hot.seed(r, dayCandles(fronteira-300*oneDayMs, 300, oneDayMs))
	hot.seed(r, dayCandles(fronteira, 500, oneDayMs))
	cold.coverFrom(r, coldStart1d())
	before := len(hot.series[r])

	if _, err := NewSoleCopyMigrator(hot, cold).Run(context.Background(),
		SoleCopyMigrationOptions{Timeframes: []string{"1d"}, Apply: true}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if hot.mutated != 0 {
		t.Fatalf("the migration performed %d write(s) on the hot side", hot.mutated)
	}
	if len(hot.series[r]) != before {
		t.Fatalf("the hot series has %d candle(s), had %d — the migration shrank Redis",
			len(hot.series[r]), before)
	}
}

// TestSoleCopyMigration_OnlyTheDeclaredCellsAreWalked: the fast cells have ZERO members older
// than the coverage (`ZCOUNT` over 500 keys of each), and scanning them would cost the whole
// fleet to find nothing.
func TestSoleCopyMigration_OnlyTheDeclaredCellsAreWalked(t *testing.T) {
	t.Parallel()

	hot, cold := newFakeHotCandles(), newFakeColdCandles()
	fronteira := coldStart1d().UnixMilli()
	for _, tf := range []string{"1d", "1w", "1h", "5m"} {
		r := redisstore.CandleKeyRef{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: tf}
		hot.seed(r, dayCandles(fronteira-10*oneDayMs, 10, oneDayMs))
		cold.coverFrom(r, coldStart1d())
	}

	report, err := NewSoleCopyMigrator(hot, cold).Run(context.Background(),
		SoleCopyMigrationOptions{Timeframes: []string{"1d", "1w"}, Apply: true})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(report.Cells) != 2 {
		t.Fatalf("%d cell(s) in the report, wanted 2", len(report.Cells))
	}
	for k := range cold.written {
		if k.Timeframe.String() != "1d" && k.Timeframe.String() != "1w" {
			t.Fatalf("the migration wrote cell %s, which was not declared", k.Timeframe.String())
		}
	}
	if report.Candles() != 20 {
		t.Fatalf("%d candle(s), wanted 20 (10 from 1d + 10 from 1w)", report.Candles())
	}
}

// TestSoleCopyMigration_DryRunIsTheDefaultAndWritesNothing: the dry pass reads the boundary
// and counts, so the number is checked BEFORE writing to the durable tier.
func TestSoleCopyMigration_DryRunIsTheDefaultAndWritesNothing(t *testing.T) {
	t.Parallel()

	hot, cold := newFakeHotCandles(), newFakeColdCandles()
	r := ref1d("BTC/USDT")
	hot.seed(r, dayCandles(coldStart1d().UnixMilli()-200*oneDayMs, 200, oneDayMs))
	cold.coverFrom(r, coldStart1d())

	report, err := NewSoleCopyMigrator(hot, cold).Run(context.Background(),
		SoleCopyMigrationOptions{Timeframes: []string{"1d"}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.Applied {
		t.Fatal("the dry pass report declared itself applied")
	}
	if report.Candles() != 200 {
		t.Fatalf("the dry pass counted %d candle(s), wanted 200", report.Candles())
	}
	if cold.totalWritten() != 0 {
		t.Fatalf("the dry pass wrote %d row(s)", cold.totalWritten())
	}
}

// TestSoleCopyMigration_AFailedCommitAbortsWithoutClaimingSuccess: a write error cannot
// become a report that says it migrated.
func TestSoleCopyMigration_AFailedCommitAbortsWithoutClaimingSuccess(t *testing.T) {
	t.Parallel()

	hot, cold := newFakeHotCandles(), newFakeColdCandles()
	cold.writeErr = errors.New("questdb down")
	r := ref1d("BTC/USDT")
	hot.seed(r, dayCandles(coldStart1d().UnixMilli()-200*oneDayMs, 200, oneDayMs))
	cold.coverFrom(r, coldStart1d())

	report, err := NewSoleCopyMigrator(hot, cold).Run(context.Background(),
		SoleCopyMigrationOptions{Timeframes: []string{"1d"}, Apply: true})
	if err == nil {
		t.Fatal("the write failure was swallowed")
	}
	if report.Commits != 0 {
		t.Fatalf("the report counted %d commit(s) despite the failure", report.Commits)
	}
}

// TestSoleCopyMigration_AnUnreadableBoundaryAborts: without a boundary there is nothing to
// decide, and choosing one on its own is the mutant this task exists to kill.
func TestSoleCopyMigration_AnUnreadableBoundaryAborts(t *testing.T) {
	t.Parallel()

	hot, cold := newFakeHotCandles(), newFakeColdCandles()
	cold.oldestErr = errors.New("questdb down")
	r := ref1d("BTC/USDT")
	hot.seed(r, dayCandles(coldStart1d().UnixMilli()-200*oneDayMs, 200, oneDayMs))

	if _, err := NewSoleCopyMigrator(hot, cold).Run(context.Background(),
		SoleCopyMigrationOptions{Timeframes: []string{"1d"}, Apply: true}); err == nil {
		t.Fatal("the pass continued without being able to read the boundary")
	}
	if cold.totalWritten() != 0 {
		t.Fatalf("wrote %d row(s) without a boundary", cold.totalWritten())
	}
}

// TestSoleCopyMigration_NoDeclaredCellIsRefused: an empty cell list is not "all". This pass
// writes to the durable tier, and the list is the caller's declaration.
func TestSoleCopyMigration_NoDeclaredCellIsRefused(t *testing.T) {
	t.Parallel()

	hot, cold := newFakeHotCandles(), newFakeColdCandles()
	if _, err := NewSoleCopyMigrator(hot, cold).Run(context.Background(), SoleCopyMigrationOptions{}); err == nil {
		t.Fatal("the pass agreed to run without a declared cell")
	}
}

// TestSoleCopyMigration_TheCeilingBoundsTheWorkAndSaysSo asserts the engine's work ceiling.
//
// A binary in crashloop runs this pass once per restart — seven times in the LAN incident.
// Without a ceiling, every restart starts the whole job again and none of them finishes. With
// a ceiling, each boot advances one slice, and the report SAYS it stopped at the ceiling —
// the difference between "finished" and "ran out of budget".
func TestSoleCopyMigration_TheCeilingBoundsTheWorkAndSaysSo(t *testing.T) {
	t.Parallel()

	hot, cold := newFakeHotCandles(), newFakeColdCandles()
	fronteira := coldStart1d().UnixMilli()
	for i := 0; i < 10; i++ {
		r := ref1d(fmt.Sprintf("SYM%03d/USDT", i))
		hot.seed(r, dayCandles(fronteira-100*oneDayMs, 100, oneDayMs))
		cold.coverFrom(r, coldStart1d())
	}

	report, err := NewSoleCopyMigrator(hot, cold).Run(context.Background(),
		SoleCopyMigrationOptions{Timeframes: []string{"1d"}, Apply: true, MaxCandles: 250})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if !report.Truncated {
		t.Fatal("the pass stopped at the ceiling and the report does not say so — its reader takes it as complete")
	}
	// The ceiling is checked at the SYMBOL EDGE: 250 falls in the middle of the third, so the
	// pass goes on until it closes it (300) and stops. Cutting inside would leave that
	// symbol's boundary at a point the re-read does not find again.
	if report.Candles() != 300 {
		t.Fatalf("%d candle(s), wanted 300 — ceiling of 250 rounded up at the symbol edge", report.Candles())
	}
	if cold.totalWritten() != 300 {
		t.Fatalf("%d candle(s) written, wanted 300 — the ceiling's pending work was not committed", cold.totalWritten())
	}
}

// TestSoleCopyMigration_TheNextPassResumesFromTheREREADBoundary asserts the RESUMPTION
// behavior, and it stores no state at all.
//
// Pass 1 stops at the ceiling. Pass 2 re-reads `MIN(timestamp)` from the durable tier — which
// now includes what pass 1 wrote — and migrates exactly the rest. No mark, no cursor, no
// file: the boundary IS the cursor.
func TestSoleCopyMigration_TheNextPassResumesFromTheREREADBoundary(t *testing.T) {
	t.Parallel()

	hot, cold := newFakeHotCandles(), newFakeColdCandles()
	fronteira := coldStart1d().UnixMilli()
	for i := 0; i < 4; i++ {
		r := ref1d(fmt.Sprintf("SYM%03d/USDT", i))
		hot.seed(r, dayCandles(fronteira-100*oneDayMs, 100, oneDayMs))
		cold.coverFrom(r, coldStart1d())
	}

	migrator := NewSoleCopyMigrator(hot, cold)
	primeira, err := migrator.Run(context.Background(),
		SoleCopyMigrationOptions{Timeframes: []string{"1d"}, Apply: true, MaxCandles: 150})
	if err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if !primeira.Truncated || primeira.Candles() != 200 {
		t.Fatalf("first pass: truncated=%v candles=%d, wanted true/200", primeira.Truncated, primeira.Candles())
	}

	// The durable boundary MOVED: what was written becomes those symbols' `MIN(timestamp)`.
	// It is what QuestDB would answer on the next boot.
	cold.advanceOldestFromWrites()

	segunda, err := migrator.Run(context.Background(),
		SoleCopyMigrationOptions{Timeframes: []string{"1d"}, Apply: true, MaxCandles: 150})
	if err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if segunda.Candles() != 200 {
		t.Fatalf("second pass migrated %d candle(s), wanted the remaining 200 — resumption repeated or skipped work",
			segunda.Candles())
	}
	if total := cold.totalWritten(); total != 400 {
		t.Fatalf("%d candle(s) written across both boots, wanted 400 — each candle once", total)
	}
}

// TestSoleCopyMigration_AnAbortStillPublishesWhatItMigrated asserts an abort publishes the
// partial result.
//
// `report.Cells = append(…)` after the symbol loop would make every `return report, err` from
// inside it return a report WITHOUT the cell. An error at symbol 300 of 500 would make the
// 299 series already counted and migrated disappear, and a reader of `candles = 0` takes that
// as "nothing to do" — which is exactly the opposite of what happened.
//
// THE MUTANT IT KILLS: returning the empty report on the abort path.
func TestSoleCopyMigration_AnAbortStillPublishesWhatItMigrated(t *testing.T) {
	t.Parallel()

	hot, cold := newFakeHotCandles(), newFakeColdCandles()
	fronteira := coldStart1d().UnixMilli()
	for i := 0; i < 5; i++ {
		r := ref1d(fmt.Sprintf("SYM%03d/USDT", i))
		hot.seed(r, dayCandles(fronteira-100*oneDayMs, 100, oneDayMs))
		cold.coverFrom(r, coldStart1d())
	}
	// The fourth symbol (deterministic order by name) has an unreadable boundary: the pass
	// aborts with three series already counted.
	cold.failOldestFor("SYM003/USDT", errors.New("questdb: connection reset"))

	report, err := NewSoleCopyMigrator(hot, cold).Run(context.Background(),
		SoleCopyMigrationOptions{Timeframes: []string{"1d"}, Apply: true, FlushEvery: 50})

	if err == nil {
		t.Fatal("the unreadable boundary was swallowed")
	}
	if len(report.Cells) != 1 {
		t.Fatalf("%d cell(s) in the abort report, wanted 1 — the cell disappeared with the error", len(report.Cells))
	}
	if report.Cells[0].Timeframe != "1d" {
		t.Fatalf("published cell = %q, wanted `1d`", report.Cells[0].Timeframe)
	}
	if report.Candles() != 300 {
		t.Fatalf("the abort report publishes %d candle(s), wanted the 300 from the three symbols already read",
			report.Candles())
	}
	if report.Cells[0].Keys != 5 {
		t.Fatalf("the abort report publishes %d key(s) scanned, wanted 5", report.Cells[0].Keys)
	}
	if report.Cells[0].KeysWithSoleCopy != 3 {
		t.Fatalf("the abort report publishes %d key(s) with a sole copy, wanted 3",
			report.Cells[0].KeysWithSoleCopy)
	}
}

// TestSoleCopyMigration_AnAbortOnTheSecondCellKeepsTheFirst — the other form of the same
// defect: the entire earlier cell would disappear when the error landed in the second scan.
func TestSoleCopyMigration_AnAbortOnTheSecondCellKeepsTheFirst(t *testing.T) {
	t.Parallel()

	hot, cold := newFakeHotCandles(), newFakeColdCandles()
	r := ref1d("BTC/USDT")
	hot.seed(r, dayCandles(coldStart1d().UnixMilli()-200*oneDayMs, 200, oneDayMs))
	cold.coverFrom(r, coldStart1d())
	hot.scanErrFor = map[string]error{"1w": errors.New("redis: connection refused")}

	report, err := NewSoleCopyMigrator(hot, cold).Run(context.Background(),
		SoleCopyMigrationOptions{Timeframes: []string{"1d", "1w"}, Apply: true})

	if err == nil {
		t.Fatal("the unreadable `1w` scan was swallowed")
	}
	if len(report.Cells) != 2 {
		t.Fatalf("%d cell(s) in the report, wanted 2 — the completed `1d` and the empty aborted `1w`",
			len(report.Cells))
	}
	if report.Candles() != 200 {
		t.Fatalf("the report publishes %d candle(s), wanted the 200 from `1d`", report.Candles())
	}
}

// Checkpoints are the actual oldest persisted timestamp, updated after each DAY.
type checkpointMigrationCold struct {
	*fakeColdCandles
	attempts int
	failAt   int
	days     []int64
}

func (c *checkpointMigrationCold) WriteBatches(ctx context.Context, groups map[candle.SeriesKey][]candle.Candle) error {
	c.attempts++
	if c.attempts == c.failAt {
		return errors.New("before transmission")
	}
	var day int64
	first := true
	for _, cs := range groups {
		for _, candle := range cs {
			current := time.UnixMilli(candle.Timestamp).UTC().Truncate(24 * time.Hour).UnixMilli()
			if !first && current != day {
				return errors.New("migration bypassed DAY boundary")
			}
			day = current
			first = false
		}
	}
	if err := c.fakeColdCandles.WriteBatches(ctx, groups); err != nil {
		return err
	}
	c.days = append(c.days, day)
	c.advanceOldestFromWrites()
	return nil
}

func TestSoleCopyMigration_PartialDAYFailureResumesEveryRowFromMIN(t *testing.T) {
	hot := newFakeHotCandles()
	cold := &checkpointMigrationCold{fakeColdCandles: newFakeColdCandles(), failAt: 2}
	ref := ref1d("BTC/USDT")
	base := coldStart1d().Add(-5 * 24 * time.Hour).UnixMilli()
	hot.seed(ref, dayCandles(base, 5, oneDayMs))
	cold.coverFrom(ref, coldStart1d())
	migrator := NewSoleCopyMigrator(hot, cold)
	first, err := migrator.Run(context.Background(), SoleCopyMigrationOptions{Timeframes: []string{"1d"}, Apply: true})
	if err == nil || first.Commits != 1 || cold.totalWritten() != 1 || cold.days[0] != base+4*oneDayMs {
		t.Fatalf("partial prefix/checkpoint wrong: commits=%d rows=%d days=%v err=%v", first.Commits, cold.totalWritten(), cold.days, err)
	}
	cold.failAt = 0
	second, err := migrator.Run(context.Background(), SoleCopyMigrationOptions{Timeframes: []string{"1d"}, Apply: true})
	if err != nil || second.Candles() != 4 || second.Commits != 4 || cold.totalWritten() != 5 {
		t.Fatalf("MIN resume lost/replayed middle: report=%+v rows=%d err=%v", second, cold.totalWritten(), err)
	}
	key := mustSeriesKey(ref.Exchange, ref.MarketType, ref.Symbol, ref.Timeframe)
	seen := map[int64]bool{}
	for _, candle := range cold.written[key] {
		if seen[candle.Timestamp] {
			t.Fatal("confirmed prefix replayed")
		}
		seen[candle.Timestamp] = true
	}
	for i := range 5 {
		if !seen[base+int64(i)*oneDayMs] {
			t.Fatalf("missing timestamp %d", base+int64(i)*oneDayMs)
		}
	}
	if hot.mutated != 0 || len(hot.series[ref]) != 5 {
		t.Fatal("source Redis changed")
	}
}

// The wrapper keeps the production QuestDB coordinator, replacing only the boundary read so
// this transition test does not require a network database.
type coordinatedMigrationCold struct {
	*Store
	boundary time.Time
}

func (c *coordinatedMigrationCold) GetOldestTimestamp(context.Context, string, string, string, string) (time.Time, error) {
	return c.boundary, nil
}

func migrationTransitionRig(t *testing.T) (*fakeHotCandles, *coordinatedMigrationCold, *fakeLineSender) {
	t.Helper()
	hot := newFakeHotCandles()
	ref := ref1d("BTC/USDT")
	base := coldStart1d().Add(-3 * 24 * time.Hour).UnixMilli()
	hot.seed(ref, dayCandles(base, 3, oneDayMs))
	sender := newFakeLineSender()
	q := newFakeColdStore(sender)
	q.walPosition = func(context.Context) (coldWALPosition, error) {
		return coldWALPosition{writer: int64(sender.flushes), sequencer: int64(sender.flushes)}, nil
	}
	return hot, &coordinatedMigrationCold{Store: q, boundary: coldStart1d()}, sender
}

func TestSoleCopyMigration_CaptureFailureRetainsPredecessorIntoPipeline(t *testing.T) {
	hot, cold, sender := migrationTransitionRig(t)
	reads := 0
	cold.walPosition = func(context.Context) (coldWALPosition, error) {
		reads++
		if reads == 1 {
			return coldWALPosition{}, errors.New("metadata unavailable")
		}
		return coldWALPosition{writer: int64(sender.flushes), sequencer: int64(sender.flushes)}, nil
	}
	_, err := NewSoleCopyMigrator(hot, cold).Run(context.Background(), SoleCopyMigrationOptions{Timeframes: []string{"1d"}, Apply: true})
	if err == nil || sender.flushes != 1 || cold.pendingPlan.current() == nil || !cold.pendingPlan.current().acknowledged {
		t.Fatalf("migration lost ACK frontier: flushes=%d pending=%+v err=%v", sender.flushes, cold.pendingPlan.current(), err)
	}
	p := NewPipeline(cold, 16, time.Hour)
	entry := scriptedEntries()[0]
	entry.Candle.Timestamp = coldStart1d().Add(-10 * 24 * time.Hour).UnixMilli()
	p.flushBatches([]candleEntry{entry})
	if sender.flushes != 4 || p.Stats().Flushed != 1 || cold.pendingPlan.current() != nil {
		t.Fatalf("pipeline replayed or bypassed predecessor: flushes=%d stats=%+v pending=%+v", sender.flushes, p.Stats(), cold.pendingPlan.current())
	}
	if len(sender.lines) != 4 || !sender.lines[0].ts.After(sender.lines[1].ts) || !sender.lines[1].ts.After(sender.lines[2].ts) || !sender.lines[2].ts.After(sender.lines[3].ts) {
		t.Fatalf("predecessor order changed: %+v", sender.lines)
	}
}

func TestSoleCopyMigration_UncertainPredecessorBlocksPipelineWithoutSendingIt(t *testing.T) {
	hot, cold, sender := migrationTransitionRig(t)
	sender.flushRr = errors.New("ACK lost")
	_, err := NewSoleCopyMigrator(hot, cold).Run(context.Background(), SoleCopyMigrationOptions{Timeframes: []string{"1d"}, Apply: true})
	if err == nil || cold.pendingPlan.current() == nil || !cold.pendingPlan.current().uncertain {
		t.Fatalf("migration uncertainty discarded: %v", err)
	}
	sender.flushRr = nil
	p := NewPipeline(cold, 16, time.Hour)
	entry := scriptedEntries()[0]
	entry.Candle.Timestamp = coldStart1d().UnixMilli()
	p.flushBatches([]candleEntry{entry})
	p.flushBatches(nil)
	p.Stop(t.Context())
	if sender.flushes != 1 || p.Stats().Flushed != 0 || p.Stats().BlockedPhase != "PREVIOUS_UNCERTAIN" || p.pendingDays[0].acknowledged || p.pendingDays[0].uncertain {
		t.Fatalf("new DAY sent/mislabeled after predecessor uncertainty: flushes=%d stats=%+v", sender.flushes, p.Stats())
	}
}

func TestColdDayCoordinator_CancelledWaitDoesNotHoldILPForDerivatives(t *testing.T) {
	_, cold, sender := migrationTransitionRig(t)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	done := make(chan error, 1)
	cold.walPosition = func(ctx context.Context) (coldWALPosition, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
			return coldWALPosition{writer: 1, sequencer: 1}, nil
		case <-ctx.Done():
			return coldWALPosition{}, ctx.Err()
		}
	}
	first := &coldDayWrite{entries: []candleEntry{scriptedEntries()[0]}}
	go func() { _, err := applyColdDay(context.Background(), cold, first); done <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("WAL wait not entered")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	next := &coldDayWrite{entries: []candleEntry{scriptedEntries()[1]}}
	phase, err := applyColdDay(canceled, cold, next)
	if !errors.Is(err, context.Canceled) || phase != "COORDINATOR" || next.acknowledged || sender.flushes != 1 {
		close(release)
		<-done
		t.Fatalf("canceled admission advanced: phase=%s err=%v", phase, err)
	}
	derivativeDone := make(chan error, 1)
	go func() { derivativeDone <- cold.WriteDerivativesBatch(context.Background(), samplePoints(1)) }()
	select {
	case err := <-derivativeDone:
		if err != nil {
			close(release)
			<-done
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(release)
		<-done
		t.Fatal("WAL wait held ILP sender mutex")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestColdDayCoordinator_KnownPreSendFailureKeepsFallback(t *testing.T) {
	_, cold, sender := migrationTransitionRig(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	plan := &coldDayPlan{days: []*coldDayWrite{{entries: []candleEntry{scriptedEntries()[0]}}}}
	if _, err := applyColdPlan(ctx, cold, plan); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-send cancel not returned: %v", err)
	}
	if cold.pendingPlan != nil || sender.flushes != 0 {
		t.Fatal("known unsent request fenced unrelated work")
	}
	p := NewPipeline(cold, 16, time.Hour)
	p.flushBatches([]candleEntry{scriptedEntries()[1]})
	if sender.flushes != 1 || p.Stats().Flushed != 1 {
		t.Fatalf("legitimate fallback blocked: %+v", p.Stats())
	}
}

func TestColdDayCoordinator_PrefixThenPreSendCancelStillRetainsMigrationTail(t *testing.T) {
	hot, cold, sender := migrationTransitionRig(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reads := 0
	cold.walPosition = func(context.Context) (coldWALPosition, error) {
		reads++
		if reads == 2 {
			cancel()
		}
		return coldWALPosition{writer: int64(sender.flushes), sequencer: int64(sender.flushes)}, nil
	}
	report, err := NewSoleCopyMigrator(hot, cold).Run(ctx, SoleCopyMigrationOptions{Timeframes: []string{"1d"}, Apply: true})
	if !errors.Is(err, context.Canceled) || report.Commits != 1 || sender.flushes != 1 || cold.pendingPlan == nil || cold.pendingPlan.next != 1 || cold.pendingPlan.current().acknowledged {
		t.Fatalf("tail after confirmed prefix discarded: report=%+v err=%v pending=%+v", report, err, cold.pendingPlan)
	}
	p := NewPipeline(cold, 16, time.Hour)
	entry := scriptedEntries()[0]
	entry.Candle.Timestamp = coldStart1d().Add(-10 * 24 * time.Hour).UnixMilli()
	p.flushBatches([]candleEntry{entry})
	if sender.flushes != 4 || len(sender.lines) != 4 || !sender.lines[2].ts.After(sender.lines[3].ts) {
		t.Fatalf("older online DAY overtook incomplete migration tail: flushes=%d lines=%+v", sender.flushes, sender.lines)
	}
}
