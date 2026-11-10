package questdb

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/derivatives"
	logger "github.com/marxms/go-whale/internal/platform/logging"

	questdb "github.com/questdb/go-questdb-client/v3"
)

// --- ILP double -------------------------------------------------------------

// ilpLine is an assembled line, the way the server would receive it.
type ilpLine struct {
	table   string
	symbols map[string]string
	floats  map[string]float64
	// bools is the THIRD measurement column shape, and it arrived with `funding_at_anchor`.
	// While this map did not exist, the double SWALLOWED `BoolColumn` — the column was
	// emitted in production and invisible here, the same blindness the `n_step` SYMBOL
	// already cost.
	bools map[string]bool
	ts    time.Time
	// err is the PROTOCOL refusal accumulated on this line. See fakeLineSender.Symbol.
	err error
}

// fakeLineSender records the lines and counts the FLUSHES — which is the number that decides
// whether the write was batched or row by row.
type fakeLineSender struct {
	lines   []ilpLine
	flushes int

	current ilpLine
	atErr   error
	flushRr error
}

func newFakeLineSender() *fakeLineSender { return &fakeLineSender{} }

func (f *fakeLineSender) reset(table string) {
	f.current = ilpLine{table: table, symbols: map[string]string{}, floats: map[string]float64{}, bools: map[string]bool{}}
}

func (f *fakeLineSender) Table(name string) questdb.LineSender {
	f.reset(name)
	return f
}

// errIlpSymbolAfterField is the REAL client's refusal, word for word.
//
// It comes from `buffer.Symbol` in go-questdb-client v3.2.0: a `Symbol()` after any value
// column records `lastErr` and the entire line is lost at `At()`. The line protocol puts
// symbols BEFORE fields, and that is not a convention — it is the grammar.
var errIlpSymbolAfterField = errors.New("symbols have to be written before any other column")

// Symbol REPRODUCES the protocol's mandatory order.
//
// A double that accepts any order lets a conditional `.Symbol()` placed AFTER the floats
// block — the natural position for someone writing "emit n_step only when funding exists"
// right next to `if p.FundingRate != nil` — pass here and fail row by row in production, with
// one WARN per row and the table stopping its growth.
func (f *fakeLineSender) Symbol(name, val string) questdb.LineSender {
	if len(f.current.floats) > 0 || len(f.current.bools) > 0 {
		f.current.err = fmt.Errorf("%w: %s", errIlpSymbolAfterField, name)
		return f
	}
	f.current.symbols[name] = val
	return f
}

func (f *fakeLineSender) Float64Column(name string, val float64) questdb.LineSender {
	f.current.floats[name] = val
	return f
}

func (f *fakeLineSender) Int64Column(string, int64) questdb.LineSender         { return f }
func (f *fakeLineSender) Long256Column(string, *big.Int) questdb.LineSender    { return f }
func (f *fakeLineSender) TimestampColumn(string, time.Time) questdb.LineSender { return f }
func (f *fakeLineSender) StringColumn(string, string) questdb.LineSender       { return f }
func (f *fakeLineSender) BoolColumn(name string, val bool) questdb.LineSender {
	f.current.bools[name] = val
	return f
}

func (f *fakeLineSender) At(_ context.Context, ts time.Time) error {
	if f.atErr != nil {
		return f.atErr
	}
	// THE PROTOCOL REFUSAL COMES OUT HERE, as in the real client: it accumulates `lastErr`
	// and only returns it at `At()`, and the line is NOT emitted.
	if f.current.err != nil {
		return f.current.err
	}
	f.current.ts = ts
	f.lines = append(f.lines, f.current)
	return nil
}

func (f *fakeLineSender) AtNow(ctx context.Context) error { return f.At(ctx, time.Time{}) }

func (f *fakeLineSender) Flush(context.Context) error {
	f.flushes++
	return f.flushRr
}

func (f *fakeLineSender) Close(context.Context) error { return nil }

// newFakeColdStore assembles a Store whose transport is the double above. The real
// constructor opens connections; here only the write path is exercised.
func newFakeColdStore(sender *fakeLineSender) *Store {
	// `priors` DECLARED, not left nil: nil would fall back to the store itself, which here
	// has no pool. This double's table is EMPTY, the state in which nothing can be eroded. A
	// case that needs a previous row injects its own. See derivatives_erosion.go.
	return &Store{ilpSender: sender, tableName: "candles", priors: emptyPriorSource{}}
}

func fp(v float64) *float64 { return &v }

// samplePoints builds n points through the same fields the durable writer receives.
func samplePoints(n int) []derivatives.DerivativesColdPoint {
	base := time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC).UnixMilli()
	out := make([]derivatives.DerivativesColdPoint, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, derivatives.DerivativesColdPoint{
			Timestamp:        time.UnixMilli(base + int64(i)*86_400_000).UTC(),
			Source:           "coinalyze",
			Exchange:         "binance",
			MarketType:       "future",
			Symbol:           "BTC/USDT",
			Timeframe:        "1d",
			DenominatedIn:    "BASE_ASSET",
			LabelConvention:  derivatives.LabelIntervalStart.String(),
			OpenInterestBase: fp(106117.954 + float64(i)),
			FundingRate:      fp(0.0001),
			LongShortRatio:   fp(0.9554),
		})
	}
	return out
}

// --- tests ------------------------------------------------------------------

// TestWriteDerivativesBatch_OneCommitForTheWholeBatch asserts rows per commit, the number
// that cost 3.91 TB written to store 3.6 GB.
//
// One commit per row triggers a WAL apply with a per-row dedup merge, on a table
// partitioned by DAY whose timestamps are out of order by construction.
func TestWriteDerivativesBatch_OneCommitForTheWholeBatch(t *testing.T) {
	tests := []struct {
		name string
		rows int
	}{
		{name: "one day of one symbol", rows: 1},
		{name: "one daily year of one symbol", rows: 365},
		{name: "the fleet's 00:00 UTC boundary", rows: 1624},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sender := newFakeLineSender()
			store := newFakeColdStore(sender)

			if err := store.WriteDerivativesBatch(context.Background(), samplePoints(tc.rows)); err != nil {
				t.Fatalf("WriteDerivativesBatch: %v", err)
			}
			if sender.flushes != 1 {
				t.Fatalf("%d commits for %d rows, wanted 1", sender.flushes, tc.rows)
			}
			if len(sender.lines) != tc.rows {
				t.Fatalf("%d lines assembled, wanted %d", len(sender.lines), tc.rows)
			}
		})
	}
}

// TestWriteDerivativesBatch_EmptyBatchDoesNotCommit: an empty batch is not an empty commit.
func TestWriteDerivativesBatch_EmptyBatchDoesNotCommit(t *testing.T) {
	sender := newFakeLineSender()
	store := newFakeColdStore(sender)

	if err := store.WriteDerivativesBatch(context.Background(), nil); err != nil {
		t.Fatalf("WriteDerivativesBatch: %v", err)
	}
	if sender.flushes != 0 {
		t.Fatalf("%d commit(s) for an empty batch, wanted 0", sender.flushes)
	}
}

// TestWriteDerivativesBatch_RowsGoOutInAscendingTimestampOrder: ascending order keeps
// ingestion on the append path instead of the partition merge (O3).
func TestWriteDerivativesBatch_RowsGoOutInAscendingTimestampOrder(t *testing.T) {
	sender := newFakeLineSender()
	store := newFakeColdStore(sender)

	points := samplePoints(4)
	// Deliberately shuffled: ordering is the write's responsibility, not the caller's.
	points[0], points[3] = points[3], points[0]

	if err := store.WriteDerivativesBatch(context.Background(), points); err != nil {
		t.Fatalf("WriteDerivativesBatch: %v", err)
	}
	for i := 1; i < len(sender.lines); i++ {
		if !sender.lines[i-1].ts.Before(sender.lines[i].ts) {
			t.Fatalf("line %d (%s) is not later than the previous one (%s)",
				i, sender.lines[i].ts, sender.lines[i-1].ts)
		}
	}
}

// TestWriteDerivativesBatch_EveryRowDeclaresSourceUnitAndConvention asserts the written row
// carries source, denomination and label convention: they travel with the number.
func TestWriteDerivativesBatch_EveryRowDeclaresSourceUnitAndConvention(t *testing.T) {
	sender := newFakeLineSender()
	store := newFakeColdStore(sender)

	if err := store.WriteDerivativesBatch(context.Background(), samplePoints(1)); err != nil {
		t.Fatalf("WriteDerivativesBatch: %v", err)
	}
	if len(sender.lines) != 1 {
		t.Fatalf("%d line(s), wanted 1", len(sender.lines))
	}
	line := sender.lines[0]
	if line.table != "derivatives" {
		t.Fatalf("table %q, wanted \"derivatives\"", line.table)
	}
	want := map[string]string{
		"source":           "coinalyze",
		"exchange":         "binance",
		"market_type":      "future",
		"symbol":           "BTC/USDT",
		"timeframe":        "1d",
		"denominated_in":   "BASE_ASSET",
		"label_convention": "interval-start",
	}
	for name, value := range want {
		if got := line.symbols[name]; got != value {
			t.Fatalf("column %s=%q, wanted %q", name, got, value)
		}
	}
	if got := line.floats["open_interest_base"]; got != 106117.954 {
		t.Fatalf("open_interest_base=%v, wanted 106117.954", got)
	}
}

// TestWriteDerivativesBatch_UndeclaredPointIsRefusedNotSubstituted: a point that does not
// say what it measures is REFUSED. Writing it with a blank unit would make it
// indistinguishable, on read, from a measured point.
func TestWriteDerivativesBatch_UndeclaredPointIsRefusedNotSubstituted(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*derivatives.DerivativesColdPoint)
	}{
		{name: "no source", mutate: func(p *derivatives.DerivativesColdPoint) { p.Source = "" }},
		{name: "no denomination", mutate: func(p *derivatives.DerivativesColdPoint) { p.DenominatedIn = "" }},
		{name: "no label convention", mutate: func(p *derivatives.DerivativesColdPoint) { p.LabelConvention = "" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sender := newFakeLineSender()
			store := newFakeColdStore(sender)

			points := samplePoints(2)
			tc.mutate(&points[0])

			if err := store.WriteDerivativesBatch(context.Background(), points); err != nil {
				t.Fatalf("WriteDerivativesBatch: %v", err)
			}
			if len(sender.lines) != 1 {
				t.Fatalf("%d line(s) written, wanted 1 — the undeclared one had to be refused", len(sender.lines))
			}
			if sender.lines[0].floats["open_interest_base"] == 106117.954 {
				t.Fatal("the refused line was written anyway")
			}
		})
	}
}

// TestWriteDerivativesBatch_AbsentQuantityIsOmittedNotZeroed: ILP has no NULL, and writing
// 0 for a call that did not return would invent a reading — zero funding IS a reading.
func TestWriteDerivativesBatch_AbsentQuantityIsOmittedNotZeroed(t *testing.T) {
	sender := newFakeLineSender()
	store := newFakeColdStore(sender)

	points := samplePoints(1)
	points[0].FundingRate = nil

	if err := store.WriteDerivativesBatch(context.Background(), points); err != nil {
		t.Fatalf("WriteDerivativesBatch: %v", err)
	}
	if _, present := sender.lines[0].floats["funding_rate"]; present {
		t.Fatalf("funding_rate=%v written for an absent reading", sender.lines[0].floats["funding_rate"])
	}
	if _, present := sender.lines[0].floats["open_interest_base"]; !present {
		t.Fatal("open_interest_base disappeared too — omission is per field, not per row")
	}
}

// TestWriteDerivativesBatch_FlushFailurePropagates: the commit failure is RETURNED by this
// layer. Swallowing it is the caller's decision, because only the caller knows what else has
// already been written.
func TestWriteDerivativesBatch_FlushFailurePropagates(t *testing.T) {
	sender := newFakeLineSender()
	sender.flushRr = errors.New("questdb down")
	store := newFakeColdStore(sender)

	if err := store.WriteDerivativesBatch(context.Background(), samplePoints(3)); err == nil {
		t.Fatal("commit failure was swallowed at the wrong layer")
	}
}

// CRITICALITY WAS INVERTED — hot/cold capping.
//
// While this path wrote two tiers, the durable failure was swallowed because the ZSET kept
// serving the recent read. The ZSET is write-only and gone, so swallowing the durable failure
// would leave the cycle with NO FAILURE POINT AT ALL: it would spend call budget, mark the
// symbols as seeded and silently discard the batch.
//
// The four cases below are the four ways the batch can reach nowhere, and all four must
// return an error.

// TestFakeLineSender_ASymbolAfterAFloatIsRefusedLikeTheRealClient is the proof that the
// double DISCRIMINATES the order — and it is the case a permissive double cannot provide.
//
// Without it, "symbols before floats" and "symbols anywhere" give the SAME answer in every
// assertion in this file, and the mutant that puts the conditional `.Symbol()` after the
// floats block survives the entire suite while losing EVERY row in production.
func TestFakeLineSender_ASymbolAfterAFloatIsRefusedLikeTheRealClient(t *testing.T) {
	sender := newFakeLineSender()

	// The RIGHT order passes.
	err := sender.Table("derivatives").
		Symbol("symbol", "BTC/USDT").
		Float64Column("funding_rate", 0.0001).
		At(context.Background(), time.Unix(0, 0))
	if err != nil {
		t.Fatalf("symbol before float was refused: %v", err)
	}
	if len(sender.lines) != 1 {
		t.Fatalf("%d line(s) emitted in the right order, wanted 1", len(sender.lines))
	}

	// The WRONG order is refused, and the line is NOT emitted.
	err = sender.Table("derivatives").
		Float64Column("funding_rate", 0.0001).
		Symbol("n_step", "measured").
		At(context.Background(), time.Unix(0, 0))
	if !errors.Is(err, errIlpSymbolAfterField) {
		t.Fatalf("symbol after float returned %v, wanted the protocol refusal", err)
	}
	if len(sender.lines) != 1 {
		t.Fatalf("%d line(s) emitted, wanted 1 — the refused line cannot enter", len(sender.lines))
	}
}

// TestWriteDerivativesBatch_ARowWithFundingCarriesTheIntervalAndTheRung asserts a funding
// value travels with the unit that produced it.
//
// A funding without the `N` that produced it is a number without a unit: the same 0.01% costs
// 0.03%/day at N=8h and 0.24%/day at N=1h. Both columns go out on the SAME line — the rung as
// `SYMBOL`, because it is a state name, and the interval as `DOUBLE`.
func TestWriteDerivativesBatch_ARowWithFundingCarriesTheIntervalAndTheRung(t *testing.T) {
	sender := newFakeLineSender()
	store := newFakeColdStore(sender)
	writer := derivatives.NewDerivativesTieredWriter(store)

	batch := derivatives.DerivativesBatch{
		Timeframe:     "4h",
		Source:        "coinalyze",
		DenominatedIn: "BASE_ASSET",
		Label:         derivatives.LabelIntervalStart,
		Snapshots: []*derivatives.DerivativesSnapshot{{
			Exchange: "binance", MarketType: "future", Symbol: "TRUMP/USDT",
			Timestamp:            time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC).UnixMilli(),
			OpenInterestBase:     fp(1234.5),
			FundingRate:          fp(-8.547e-05),
			FundingRate8h:        fp(-1.7094e-04),
			FundingIntervalHours: fp(4),
			NStep:                derivatives.NFromMeasuredSpacing,
			LongShortRatio:       fp(0.9554),
		}},
	}
	if err := writer.WriteBatch(context.Background(), batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if len(sender.lines) != 1 {
		t.Fatalf("%d line(s) emitted, wanted 1 — a `.Symbol()` after the floats loses the whole line",
			len(sender.lines))
	}

	linha := sender.lines[0]
	if got, ok := linha.floats["funding_interval_hours"]; !ok || got != 4 {
		t.Fatalf("funding_interval_hours=%v (present=%v), wanted 4 — the N that produced the equivalent", got, ok)
	}
	if got, ok := linha.symbols["n_step"]; !ok || got != string(derivatives.NFromMeasuredSpacing) {
		t.Fatalf("n_step=%q (present=%v), wanted %q — the evidence of the interval above",
			got, ok, derivatives.NFromMeasuredSpacing)
	}
	// And the rung is SYMBOL, not float: it is a state name.
	if _, comoFloat := linha.floats["n_step"]; comoFloat {
		t.Fatal("n_step came out as a value column; it is a state name and the column is SYMBOL")
	}
}

// TestWriteDerivativesBatch_ARowWithoutFundingOmitsTheRungInsteadOfWritingItBlank asserts an
// absent rung is omitted, never written blank.
//
// The zero value of `NDerivationStep` is `""` — a FOURTH nameless value, alongside measured,
// seed, inherited and unavailable. Writing it would create a rung no consumer knows how to
// read and that the coherence gauge cannot distinguish from "not classified yet".
//
// A row with only open interest is ROUTINE, not an exception: the three quantities come from
// three separate provider responses, and a cycle in which the funding one did not return
// produces exactly this row. Refusing it would cut open-interest coverage along with it.
func TestWriteDerivativesBatch_ARowWithoutFundingOmitsTheRungInsteadOfWritingItBlank(t *testing.T) {
	sender := newFakeLineSender()
	store := newFakeColdStore(sender)
	writer := derivatives.NewDerivativesTieredWriter(store)

	batch := derivatives.DerivativesBatch{
		Timeframe:     "1d",
		Source:        "coinalyze",
		DenominatedIn: "BASE_ASSET",
		Label:         derivatives.LabelIntervalStart,
		Snapshots: []*derivatives.DerivativesSnapshot{{
			Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT",
			Timestamp:        time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC).UnixMilli(),
			OpenInterestBase: fp(106117.954),
		}},
	}
	if err := writer.WriteBatch(context.Background(), batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if len(sender.lines) != 1 {
		t.Fatalf("%d line(s) emitted, wanted 1 — a row with only open interest is routine and is not refused",
			len(sender.lines))
	}

	linha := sender.lines[0]
	if valor, presente := linha.symbols["n_step"]; presente {
		t.Fatalf("n_step=%q on a row without funding — the zero value is a nameless rung, and it must be OMITTED", valor)
	}
	if valor, presente := linha.floats["funding_interval_hours"]; presente {
		t.Fatalf("funding_interval_hours=%v on a row without funding", valor)
	}
	// And the quantity the row CARRIES keeps arriving.
	if got, ok := linha.floats["open_interest_base"]; !ok || got != 106117.954 {
		t.Fatalf("open_interest_base=%v (present=%v), wanted 106117.954", got, ok)
	}
}

// ============================================================================
// THE DEGRADATION ALARM DECLARES ITS RATE AND DISCRIMINATES
//
// THE TRIGGER WAS `preserved > 0 || holes > 0`, and `preserved` is the COMMON case by
// construction: `completeRow` requires twelve columns and Coinalyze never fills four of them,
// so almost every batch has an omitted column to preserve. The alarm fired at the
// PRESERVATION rate — and an alarm with a ~100% duty cycle is indistinguishable from silence.
//
// THE PAIR ALREADY EXISTED (`derivativesColdDegradedTotal{outcome=...}`); what these cases
// require is that the partition GOVERNS the firing: a cycle in which every write is degraded
// and ZERO holes occur SHALL be distinguishable from a cycle with real holes — by the log
// level, and not only by the counter.
// ============================================================================

// loteDegradadoSemBuraco assembles a batch in which EVERY row is degraded and NONE becomes a
// hole: reading what was already in the table answers and returns what the row omitted.
func loteDegradadoSemBuraco(n int) ([]derivatives.DerivativesColdPoint, *recordingPriorSource) {
	prior := &recordingPriorSource{}
	pontos := samplePoints(n)
	for i := range pontos {
		// The degraded cycle: one of the provider calls did not return.
		pontos[i].FundingRate = nil
		// And the bucket already exists in the table, with the funding the new row does not
		// bring.
		prior.rows = append(prior.rows, &derivatives.DerivativesSnapshot{
			Timestamp:        pontos[i].Timestamp.UnixMilli(),
			FundingRate:      fp(0.0001),
			OpenInterestBase: pontos[i].OpenInterestBase,
		})
	}
	return pontos, prior
}

// TestWriteDerivativesBatch_ADegradedCycleWithoutHolesDoesNotAlarm is the half the duty cycle
// drained: every write degraded, ZERO holes — and therefore no alarm.
func TestWriteDerivativesBatch_ADegradedCycleWithoutHolesDoesNotAlarm(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	pontos, prior := loteDegradadoSemBuraco(3)
	store := &Store{log: log, ilpSender: newFakeLineSender(), tableName: "candles", priors: prior}
	preserved := 0
	store.SetMetrics(Metrics{DerivativesColdDegraded: func(outcome string, n float64) {
		if outcome == outcomePreserved {
			preserved += int(n)
		}
	}})
	if err := store.WriteDerivativesBatch(context.Background(), pontos); err != nil {
		t.Fatalf("WriteDerivativesBatch: %v", err)
	}

	// PRESERVATION HAPPENED — without this premise the case would pass without exercising
	// degradation.
	if delta := preserved; delta != 3 {
		t.Fatalf("preserved summed %v, wanted 3 — the scenario requires the three degraded rows", delta)
	}
	// AND IT DOES NOT ALARM: no row became a hole, so no data was lost.
	if _, alarmou := sink.LevelOf("cycle with a hole"); alarmou {
		t.Fatalf("a cycle in which no row became a hole alarmed: %v", sink.Records())
	}
}

// TestWriteDerivativesBatch_AHoleIsAlarmedWithTheDeclaredRate — a cycle with REAL holes
// alarms, at WARN, and the line DECLARES the batch rate.
//
// The rate is half the requirement: "half the batch was lost" is not the same verdict as
// "one row was lost", and without the number the operator cannot distinguish the two.
func TestWriteDerivativesBatch_AHoleIsAlarmedWithTheDeclaredRate(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	// THE READ OF WHAT WAS ALREADY THERE DOES NOT ANSWER: incomplete rows are not written —
	// the choice is the hole instead of erosion —, and it is this half that must alarm.
	prior := &recordingPriorSource{err: errors.New("questdb down")}
	pontos := samplePoints(4)
	for i := 0; i < 3; i++ {
		pontos[i].FundingRate = nil
	}
	// THE FOURTH IS NOT AT RISK: complete, it cannot be punished for the scope's read failure
	// — and it is what makes the rate 3 of 4 instead of 3 of 3.
	completar(&pontos[3])

	store := &Store{log: log, ilpSender: newFakeLineSender(), tableName: "candles", priors: prior}
	if err := store.WriteDerivativesBatch(context.Background(), pontos); err != nil {
		t.Fatalf("WriteDerivativesBatch: %v", err)
	}

	linha := linhaCom(t, sink, "cycle with a hole")
	if linha.Level != logger.LevelWarn {
		t.Errorf("the hole came out at %s; below WARN it does not appear at the process default level", linha.Level)
	}
	if !strings.Contains(linha.Text(), "holes=3") {
		t.Errorf("the alarm line does not declare the batch count (3 of 4): %q", linha.Text())
	}
	if !strings.Contains(linha.Text(), "hole_rate=75") {
		t.Errorf("the alarm line does not declare the batch rate (75.0%%): %q", linha.Text())
	}
}

// NEGATIVE CONTROL: a healthy batch neither alarms nor counts — without this case, an
// unconditional alarm would pass the one above and the log would gain a line per cycle.
func TestWriteDerivativesBatch_AHealthyCycleDoesNotAlarm(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	store := newFakeColdStore(newFakeLineSender())
	store.log = log
	if err := store.WriteDerivativesBatch(context.Background(), samplePoints(3)); err != nil {
		t.Fatalf("WriteDerivativesBatch: %v", err)
	}
	if _, alarmou := sink.LevelOf("cycle with a hole"); alarmou {
		t.Errorf("a batch without an incomplete row alarmed: %v", sink.Records())
	}
}

// TestDerivativesHoleRate_IsTheShareOfTheBatchAndNotAnAbsolute — the rate is the FRACTION of
// the batch: the same number of holes in batches of different sizes is not the same loss.
func TestDerivativesHoleRate_IsTheShareOfTheBatchAndNotAnAbsolute(t *testing.T) {
	if got := derivativesHoleRate(3, 4); got != 75 {
		t.Errorf("3 of 4 = %v%%, wanted 75", got)
	}
	if got := derivativesHoleRate(3, 300); got != 1 {
		t.Errorf("3 of 300 = %v%%, wanted 1 — the rate is the fraction of the batch", got)
	}
	// An empty batch lost nothing, and there is no fraction to invent.
	if got := derivativesHoleRate(0, 0); got != 0 {
		t.Errorf("empty batch = %v%%, wanted 0", got)
	}
}
