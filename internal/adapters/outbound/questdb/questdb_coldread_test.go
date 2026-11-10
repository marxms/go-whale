package questdb

import (
	"errors"
	"strings"
	"testing"
)

// TestColdSelectColumns guards the read SQL builder: the cold read must be able to emit a
// COMPATIBLE column list (no taker_sell_volume) so a table where the additive column is absent
// (EnsureSchema ALTER failed: slow boot / perms / syntax) still reads instead of erroring
// "column taker_sell_volume does not exist" → API 500.
func TestColdSelectColumns(t *testing.T) {
	withCum := coldSelectColumns(true)
	if !strings.Contains(withCum, "taker_sell_volume") {
		t.Fatalf("expected taker_sell_volume in column list when present, got %q", withCum)
	}

	legacy := coldSelectColumns(false)
	if strings.Contains(legacy, "taker_sell_volume") {
		t.Fatalf("compatible column list must NOT reference taker_sell_volume, got %q", legacy)
	}
	// The compatible list must still carry the other additive CVD column
	// (taker_buy_volume) so only the genuinely-absent column is dropped.
	for _, want := range []string{"timestamp", "open", "high", "low", "close", "volume", "taker_buy_volume"} {
		if !strings.Contains(legacy, want) {
			t.Fatalf("compatible column list missing %q: %q", want, legacy)
		}
	}
}

// TestColdSelectColumns_NeverSelectsNumberOfTrades guards the read side of number_of_trades.
//
// The field is absent from the DDL and from the write path because nothing consumes it. If
// either SELECT list named it, every cold read against a table created from the current DDL
// would fail with "column does not exist" — the write path would look healthy while reads
// returned 500s. Not selecting a column is valid against a table that still has it, so this
// direction is safe for deployments that have not run the manual DROP.
func TestColdSelectColumns_NeverSelectsNumberOfTrades(t *testing.T) {
	for _, withCum := range []bool{true, false} {
		if got := coldSelectColumns(withCum); strings.Contains(got, "number_of_trades") {
			t.Fatalf("column list (includeTakerSell=%v) must not select the removed column: %q", withCum, got)
		}
	}
}

// TestIsMissingColumnErr is the safety-net detector: when the optimistic SELECT hits a table
// without the column, the read flips to the compatible SELECT instead of surfacing a 500. It
// must match QuestDB's column-absent error shapes and ignore unrelated errors.
func TestIsMissingColumnErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"questdb invalid column", errors.New("Invalid column: taker_sell_volume"), true},
		{"does not exist phrasing", errors.New("column 'taker_sell_volume' does not exist"), true},
		{"unknown column phrasing", errors.New("Unknown column [name=taker_sell_volume]"), true},
		{"unrelated error", errors.New("connection refused"), false},
		{"different column missing", errors.New("Invalid column: some_other_col"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isMissingColumnErr(tt.err, "taker_sell_volume"); got != tt.want {
				t.Fatalf("isMissingColumnErr(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// TestShouldFallbackToLegacy locks the retry decision: fall back to the compatible read ONLY
// when the optimistic (column-present) read failed because the column is absent. A real error
// must NOT be masked as a fallback, and a compatible read never retries.
func TestShouldFallbackToLegacy(t *testing.T) {
	missing := errors.New("Invalid column: taker_sell_volume")
	other := errors.New("connection refused")

	tests := []struct {
		name         string
		hasTakerSell bool
		err          error
		want         bool
	}{
		{"optimistic read, column absent -> fall back", true, missing, true},
		{"optimistic read, unrelated error -> propagate", true, other, false},
		{"optimistic read, no error -> no fallback", true, nil, false},
		{"compatible read, column absent err -> no retry (already compatible)", false, missing, false},
		{"compatible read, unrelated error -> propagate", false, other, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldFallbackToLegacy(tt.hasTakerSell, tt.err); got != tt.want {
				t.Fatalf("shouldFallbackToLegacy(%v, %v) = %v, want %v", tt.hasTakerSell, tt.err, got, tt.want)
			}
		})
	}
}

// TestScanCandlesCompat_LegacyShapeReconstructsSell proves the row scanner handles BOTH
// shapes: the current one (taker_sell_volume present) and the compatible one (column absent,
// so the read runs against a table that predates it). A table missing the column must degrade,
// not fail at scan time — that is what keeps a host whose ALTER has not run yet serving reads
// instead of returning 500 for the whole fleet.
func TestScanCandlesCompat_LegacyShapeReconstructsSell(t *testing.T) {
	c := buildColdCandle(1_700_000_000_000, 1, 2, 0.5, 1.5, 100, ptrF(60), nil)
	if c.TakerSellVolume != 40 {
		t.Fatalf("compatible shape must reconstruct sell = volume - buy: got %v want 40",
			c.TakerSellVolume)
	}
}

func ptrF(f float64) *float64 { return &f }
