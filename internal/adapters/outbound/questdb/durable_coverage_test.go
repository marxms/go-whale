package questdb

import (
	"context"
	"sort"
	"testing"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/derivatives"
)

// ============================================================================
// THE COVERAGE DECLARATION AGAINST WHAT THE WRITE ACTUALLY EMITS
// ============================================================================
//
// `redisstore.DerivativesDurableCoverage()` says, per field, which durable-tier column carries
// each snapshot quantity. That is the document whoever caps the derivatives family will read
// to decide what may be deleted.
//
// AN UNCHECKED DOCUMENT IS A STALE CITATION WAITING TO HAPPEN — unchecked prose citations
// have already been wrong four times here. The case below closes the loop on the behavior side: it
// assembles a snapshot with ALL quantities filled, pushes it through the real durable write
// path and compares the set of emitted columns with the set declared as covered.
//
// The equality is IN BOTH DIRECTIONS, and each direction catches a different defect:
//
//   - a declared column that is not emitted = claimed coverage the table does not have. It is
//     written permission to cap a cell believing the cold tier covers it.
//   - an emitted column that is not declared = the durable schema gained a field and the
//     published gap shrank without anyone updating it.
//
// # THE CHECKER LOOKS AT ALL THREE COLUMN TYPES
//
// Comparing only `floats` fails because a measurement column is not necessarily `DOUBLE`: the
// rung that produced the funding interval is `SYMBOL`, because it is a state name and not a
// number. With the comparison restricted to floats, declaring `nStep: "n_step"` failed the
// equality even with the column being written — the declaration was BLOCKED by a checker that
// did not see half the row.
//
// The union is `floats ∪ symbols ∪ bools` MINUS identity. The seven identity symbols name the
// ROW (who, where, when, under which convention), not a snapshot quantity, and none of them
// appears in the coverage declaration. The list is here and not in production on purpose: a new
// identity symbol makes this suite fail, and failing loudly is the right behavior — someone has
// to decide whether it is identity or measurement.

// TestDurableCoverage_TheDeclaredColumnsAreExactlyTheWrittenOnes.
func TestDurableCoverage_TheDeclaredColumnsAreExactlyTheWrittenOnes(t *testing.T) {
	sender := newFakeLineSender()
	store := newFakeColdStore(sender)
	writer := derivatives.NewDerivativesTieredWriter(store)

	batch := fullyPopulatedBatch()
	if err := writer.WriteBatch(context.Background(), batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if len(sender.lines) != 1 {
		t.Fatalf("%d line(s) emitted, wanted 1", len(sender.lines))
	}

	emitidas := colunasDeMedicaoEmitidas(sender.lines[0])

	var declaradas []string
	for campo, coluna := range redisstore.DerivativesDurableCoverage() {
		if coluna == "" {
			continue
		}
		_ = campo
		declaradas = append(declaradas, coluna)
	}
	sort.Strings(declaradas)

	if len(emitidas) != len(declaradas) {
		t.Fatalf("the durable write emits %v and the declaration covers %v", emitidas, declaradas)
	}
	for i := range declaradas {
		if emitidas[i] != declaradas[i] {
			t.Fatalf("the durable write emits %v and the declaration covers %v", emitidas, declaradas)
		}
	}
}

// TestDurableCoverage_TheUndeclaredQuantitiesNeverReachTheTable is the other side, and what the
// cap decision consumes: nine snapshot quantities do NOT reach the durable tier, under any
// name. Migrating this family would preserve 4 of 13.
//
// KILLS the optimistic reading of "the cold tier covers derivatives" — it covers four columns,
// over a source that is not the venue, and only in three of the eight timeframes.
func TestDurableCoverage_TheUndeclaredQuantitiesNeverReachTheTable(t *testing.T) {
	sender := newFakeLineSender()
	store := newFakeColdStore(sender)
	writer := derivatives.NewDerivativesTieredWriter(store)

	if err := writer.WriteBatch(context.Background(), fullyPopulatedBatch()); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	linha := sender.lines[0]
	// Plausible names a future implementation would use for each quantity. The check is
	// DECLARATION-DRIVEN: for a field declared WITHOUT a column, none of these names may appear
	// in the row, in floats, symbols or bools. For a field declared WITH a column, the set
	// equality of the case above is what checks it.
	//
	// Fixing here WHICH fields are uncovered would make the hard-coded list — not the
	// declaration — the decision maker. Covering a field would then fail a test that is not
	// about coverage, which is the inverted order: the checker dictating what may be
	// implemented.
	for campo, nomes := range map[string][]string{
		"openInterest":         {"open_interest", "open_interest_notional", "openInterest"},
		"lsLongPct":            {"ls_long_pct", "long_pct", "lsLongPct"},
		"lsShortPct":           {"ls_short_pct", "short_pct", "lsShortPct"},
		"fundingIntervalHours": {"funding_interval_hours", "fundingIntervalHours"},
		"nStep":                {"n_step", "nStep"},
		"interestRate":         {"interest_rate", "interestRate"},
		"fundingAtAnchor":      {"funding_at_anchor", "fundingAtAnchor"},
		"nextFundingRate":      {"next_funding_rate", "nextFundingRate"},
		"oiChangePct":          {"oi_change_pct", "oiChangePct"},
	} {
		if coluna := redisstore.DerivativesDurableCoverage()[campo]; coluna != "" {
			continue
		}
		for _, nome := range nomes {
			if _, presente := linha.floats[nome]; presente {
				t.Errorf("the durable write emitted %q, but %s is declared without a column", nome, campo)
			}
			if _, presente := linha.symbols[nome]; presente {
				t.Errorf("the durable write emitted symbol %q, but %s is declared without a column", nome, campo)
			}
			if _, presente := linha.bools[nome]; presente {
				t.Errorf("the durable write emitted boolean %q, but %s is declared without a column", nome, campo)
			}
		}
	}
}

// identidadeDaLinha names the symbols that identify the ROW instead of measuring a quantity.
//
// They are not in `DerivativesDurableCoverage` because they are not snapshot fields: they are
// the who, the where, the when and the convention under which the row was written.
func identidadeDaLinha() map[string]struct{} {
	return map[string]struct{}{
		"source": {}, "exchange": {}, "market_type": {}, "symbol": {},
		"timeframe": {}, "denominated_in": {}, "label_convention": {},
	}
}

// colunasDeMedicaoEmitidas returns, sorted, the MEASUREMENT columns an ILP line carries.
//
// `floats ∪ symbols ∪ bools`, minus identity. The union is what unblocks the declaration of a
// `SYMBOL` column; the subtraction is what keeps the seven identity symbols from being counted
// as coverage.
func colunasDeMedicaoEmitidas(linha ilpLine) []string {
	var out []string
	for coluna := range linha.floats {
		if _, identidade := identidadeDaLinha()[coluna]; identidade {
			continue
		}
		out = append(out, coluna)
	}
	for coluna := range linha.symbols {
		if _, identidade := identidadeDaLinha()[coluna]; identidade {
			continue
		}
		out = append(out, coluna)
	}
	// AND THE BOOLEANS. `funding_at_anchor` is a measurement in a third column type, and a
	// checker that looked only at `floats ∪ symbols` would fail its declaration even with the
	// write emitting it — exactly what happened with the SYMBOL `n_step`.
	for coluna := range linha.bools {
		if _, identidade := identidadeDaLinha()[coluna]; identidade {
			continue
		}
		out = append(out, coluna)
	}
	sort.Strings(out)
	return out
}

// TestColunasDeMedicaoEmitidas_EnxergaAColunaSymbol is the proof that the checker
// DISCRIMINATES — and it is the case a permissive checker cannot provide.
//
// Without it, "compare only floats" and "compare floats ∪ symbols" give the SAME answer while
// no measurement column is `SYMBOL`, and the mutant survives the entire suite. The line here is
// synthetic precisely for that reason: it carries the `n_step` the write emits.
func TestColunasDeMedicaoEmitidas_EnxergaAColunaSymbol(t *testing.T) {
	linha := ilpLine{
		symbols: map[string]string{
			// identity, which does not count as coverage
			"source": "coinalyze", "exchange": "binance", "market_type": "future",
			"symbol": "BTC/USDT", "timeframe": "1d", "denominated_in": "BASE_ASSET",
			"label_convention": "interval-start",
			// MEASUREMENT in a SYMBOL column — the rung is a state name, not a number
			"n_step": "measured",
		},
		floats: map[string]float64{"funding_rate": 0.0001},
		bools:  map[string]bool{},
	}

	got := colunasDeMedicaoEmitidas(linha)
	want := []string{"funding_rate", "n_step"}

	if len(got) != len(want) {
		t.Fatalf("measurement columns = %v, wanted %v — a checker that looks only at floats does not see "+
			"the SYMBOL column and fails its declaration", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("measurement columns = %v, wanted %v", got, want)
		}
	}
}

// TestColunasDeMedicaoEmitidas_ASubtracaoDaIdentidadeNaoEngoleMedicao — the subtraction must be
// by the exact identity column name, not by prefix or by type: `symbol` is identity and
// `n_step` is measurement, and both travel in the same map.
func TestColunasDeMedicaoEmitidas_ASubtracaoDaIdentidadeNaoEngoleMedicao(t *testing.T) {
	soIdentidade := ilpLine{
		symbols: map[string]string{
			"source": "coinalyze", "exchange": "binance", "market_type": "future",
			"symbol": "BTC/USDT", "timeframe": "1d", "denominated_in": "BASE_ASSET",
			"label_convention": "interval-start",
		},
		floats: map[string]float64{},
		bools:  map[string]bool{},
	}
	if got := colunasDeMedicaoEmitidas(soIdentidade); len(got) != 0 {
		t.Fatalf("an identity-only line returned %v as measurement", got)
	}
}

// fullyPopulatedBatch assembles a batch whose snapshot has ALL quantities filled.
//
// Filled on purpose: with absent fields the write omits columns (ILP has no NULL), and the set
// comparison would pass by omission instead of by coverage.
func fullyPopulatedBatch() derivatives.DerivativesBatch {
	atAnchor := true
	return derivatives.DerivativesBatch{
		Timeframe:     "1d",
		Source:        "coinalyze",
		DenominatedIn: "BASE_ASSET",
		Label:         derivatives.LabelIntervalStart,
		Snapshots: []*derivatives.DerivativesSnapshot{{
			Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT",
			Timestamp:            1_756_000_000_000,
			OpenInterestBase:     fp(106117.954),
			OpenInterest:         fp(5_432_100_000),
			OIChangePct:          fp(1.25),
			FundingRate:          fp(0.0001),
			NextFundingRate:      fp(0.00012),
			FundingRate8h:        fp(0.0003),
			FundingIntervalHours: fp(8),
			NStep:                derivatives.NFromMeasuredSpacing,
			InterestRate:         fp(0.0001),
			FundingAtAnchor:      &atAnchor,
			LongShortRatio:       fp(0.9554),
			LSLongPct:            fp(48.86),
			LSShortPct:           fp(51.14),
		}},
	}
}

// TestColunasDeMedicaoEmitidas_EnxergaAColunaBoolean is the twin of the SYMBOL column case, and
// the mutant it kills is the same: while no measurement is BOOLEAN, "look at floats ∪ symbols"
// and "look at floats ∪ symbols ∪ bools" give the SAME answer, and the difference survives the
// entire suite. `funding_at_anchor` is the first one, and it is a PREDICATE — not a 0/1.
func TestColunasDeMedicaoEmitidas_EnxergaAColunaBoolean(t *testing.T) {
	linha := ilpLine{
		symbols: map[string]string{
			"source": "coinalyze", "exchange": "binance", "market_type": "future",
			"symbol": "BTC/USDT", "timeframe": "1d", "denominated_in": "BASE_ASSET",
			"label_convention": "interval-start",
		},
		floats: map[string]float64{"funding_rate": 0.0001},
		bools:  map[string]bool{"funding_at_anchor": true},
	}

	got := colunasDeMedicaoEmitidas(linha)
	want := []string{"funding_at_anchor", "funding_rate"}

	if len(got) != len(want) {
		t.Fatalf("measurement columns = %v, wanted %v — a checker that does not look at BOOLEAN "+
			"fails funding_at_anchor's declaration with the write emitting it", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("measurement columns = %v, wanted %v", got, want)
		}
	}
}
