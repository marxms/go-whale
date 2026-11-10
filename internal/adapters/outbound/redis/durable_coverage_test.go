package redisstore

import (
	"reflect"
	"sort"
	"testing"

	"github.com/marxms/go-whale/internal/derivatives"
)

// ============================================================================
// DURABLE COVERAGE, CHECKED MECHANICALLY
// ============================================================================
//
// The gap was declared in prose in the design. Prose does not run: an assertion describing a
// state a later commit changed is the defect class this repository has paid for three times.
// These cases exist so that the `DerivativesDurableCoverage` declaration cannot age silently.

// isQuantity says whether a snapshot field is a MAGNITUDE — what durable coverage must declare
// — rather than a dimension (exchange, symbol) or an instant (`*int64`).
//
// The rule is by TYPE, not a list: a list of exceptions is what would let a new field enter
// without being declared.
func isQuantity(f reflect.StructField) bool {
	switch f.Type.String() {
	case "*float64", "*bool", "derivatives.NDerivationStep":
		return true
	}
	return false
}

func jsonName(f reflect.StructField) string {
	tag := f.Tag.Get("json")
	for i := 0; i < len(tag); i++ {
		if tag[i] == ',' {
			return tag[:i]
		}
	}
	return tag
}

// TestDerivativesDurableCoverage_DeclaresEveryQuantityOfTheSnapshot.
//
// KILLS the new field that enters the snapshot without a coverage declaration — which is how
// the gap stops being known without anyone resolving it.
func TestDerivativesDurableCoverage_DeclaresEveryQuantityOfTheSnapshot(t *testing.T) {
	t.Parallel()

	coverage := DerivativesDurableCoverage()
	tipo := reflect.TypeOf(derivatives.DerivativesSnapshot{})

	var faltando []string
	declarados := map[string]struct{}{}
	for i := 0; i < tipo.NumField(); i++ {
		f := tipo.Field(i)
		if !isQuantity(f) {
			continue
		}
		name := jsonName(f)
		declarados[name] = struct{}{}
		if _, ok := coverage[name]; !ok {
			faltando = append(faltando, name)
		}
	}
	sort.Strings(faltando)
	if len(faltando) > 0 {
		t.Fatalf("snapshot magnitude(s) without a declared coverage: %v — declare them in DerivativesDurableCoverage", faltando)
	}

	// AND THE OTHER SIDE: a name in the declaration that is not a snapshot field is coverage
	// asserted over nothing.
	var fantasmas []string
	for name := range coverage {
		if _, ok := declarados[name]; !ok {
			fantasmas = append(fantasmas, name)
		}
	}
	sort.Strings(fantasmas)
	if len(fantasmas) > 0 {
		t.Fatalf("the declaration covers name(s) that are not snapshot magnitudes: %v", fantasmas)
	}
}

// TestDerivativesDurableCoverage_TheMeasuredShapeIsTwelveOfThirteen pins the numbers the cap
// decision consumes. A cap decided over "3 of 8 timeframes covered" erases magnitudes no
// database stores.
//
// KILLS the declaration that gains a column "by optimism" without the table gaining it.
//
// The equality is of an EXACT SET and the count is exact. What the cap decision still hinges
// on is the THIRTEENTH field, `oiChangePct`: `UncoveredMemberFields` iterates ALL fields, so
// one empty row is enough to keep the cell uncappable. Twelve of thirteen is not a released
// cap.
func TestDerivativesDurableCoverage_TheMeasuredShapeIsTwelveOfThirteen(t *testing.T) {
	t.Parallel()

	coverage := DerivativesDurableCoverage()
	var comColuna, semColuna []string
	for name, column := range coverage {
		if column == "" {
			semColuna = append(semColuna, name)
			continue
		}
		comColuna = append(comColuna, column)
	}
	sort.Strings(comColuna)

	querido := []string{
		"funding_at_anchor", "funding_interval_hours", "funding_rate", "funding_rate_8h",
		"interest_rate", "long_short_ratio", "ls_long_pct", "ls_short_pct", "n_step",
		"next_funding_rate", "open_interest", "open_interest_base",
	}
	if !reflect.DeepEqual(comColuna, querido) {
		t.Fatalf("declared measurement columns = %v, want %v", comColuna, querido)
	}
	if len(semColuna) != 1 {
		t.Fatalf("%d magnitude(s) without a column, want 1 (13 magnitudes - 12 columns): %v", len(semColuna), semColuna)
	}
	if semColuna[0] != "oiChangePct" {
		t.Fatalf("the magnitude without a column is %q, want oiChangePct — it is the only one that does not reach the series member", semColuna[0])
	}
}

// TestDerivativesDurableCoverage_TheHotOnlyFieldsAreNamed: the fields the live route
// measurement showed falling at index 800 — the ZSET cap — must be declared WITHOUT a column.
//
// KILLS the declaration that promotes one of them to covered: that would be written permission
// to cap the cell and shorten the horizon of a field no database stores.
//
// `fundingIntervalHours` and `nStep` are NOT in this list, and the difference is factual rather
// than tolerance: they stopped being hot-only because the table gained the columns and the
// durable write emits them. Promoting a field here without that counterpart is what keeps
// failing.
func TestDerivativesDurableCoverage_TheHotOnlyFieldsAreNamed(t *testing.T) {
	t.Parallel()

	// THE LIST SHRANK TO ONE, and the difference is factual: the other six stopped being
	// hot-only because the table gained the columns, the durable write emits them and the read
	// query asks for them. `oiChangePct` stays, and it will NOT leave: the series path nils it
	// on the write and on the read, so there is nothing to store and nothing to read.
	coverage := DerivativesDurableCoverage()
	for _, name := range []string{"oiChangePct"} {
		column, declared := coverage[name]
		if !declared {
			t.Errorf("%s is not declared", name)
			continue
		}
		if column != "" {
			t.Errorf("%s was declared with column %q — the `derivatives` table does not have it", name, column)
		}
	}
}
