package questdb

import (
	"strings"
	"testing"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/derivatives"
)

// TestNormalizingRungLiterals_ComeFromTheLadderAndNotFromALiteralList — the query that looks
// for the incoherent row asks for the rungs that NORMALIZE, and that list is the filtered
// ladder.
//
// A list typed inside the query is the usual divergence: a new rung that normalizes would be
// counted as incoherent, and the dimension would accuse the entire table.
func TestNormalizingRungLiterals_ComeFromTheLadderAndNotFromALiteralList(t *testing.T) {
	got := normalizingRungLiterals()

	var want []string
	for _, rung := range derivatives.NDerivationSteps() {
		if rung.Normalizes() {
			want = append(want, "'"+string(rung)+"'")
		}
	}
	if len(want) == 0 {
		t.Fatal("no rung normalizes — the comparison would pass vacuously")
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("rungs in the query = %v, wanted %v — the list must come from the ladder", got, want)
	}
	// AND NO RUNG THAT DOES NOT NORMALIZE ENTERS: including `inherited` here would make the
	// query declare coherent exactly the row the check exists to catch.
	for _, rung := range derivatives.NDerivationSteps() {
		if rung.Normalizes() {
			continue
		}
		if strings.Contains(strings.Join(got, ","), string(rung)) {
			t.Errorf("rung %q does not normalize and entered the list of those that authorize", rung)
		}
	}
}

// ============================================================================
// THE CHECKER'S PROJECTION COUNTS COLUMNS OF ANY TYPE
//
// THE LIVE DEFECT, measured against the production database:
//
//	count(open_interest_base)  →  7,662,191   (DOUBLE: accepted)
//	count(funding_at_anchor)   →  ERROR: there is no matching function `count`
//	                              with the argument types: (BOOLEAN)
//
// The twelve columns travel in a SINGLE query with ONE `QueryRow().Scan(...)`, so the boolean
// column alone zeroed the ENTIRE timeframe — the checker was blind in production, and blind
// without saying so. The fix is in the QUERY: `funding_at_anchor` stays BOOLEAN.
//
// The REAL EXECUTION of the query against QuestDB is explicit integration and does not belong
// here: these cases assert the SHAPE of the SQL, which is what can be asserted without a
// network or database.
// ============================================================================

// TestDurableFillCountExpression_TheBooleanColumnIsCountedLikeAnyOther is the case that kills
// the `count(column)` mutant: with the bare form, `funding_at_anchor` takes down the whole
// query.
func TestDurableFillCountExpression_TheBooleanColumnIsCountedLikeAnyOther(t *testing.T) {
	got, err := derivativesFillCountExpression("funding_at_anchor")
	if err != nil {
		t.Fatalf("the boolean column produced no projection: %v", err)
	}
	// THE SHAPE, word for word. `count(column)` does not exist for BOOLEAN; the conditional
	// expression counts the same thing — the NON-NULLs — and returns an integer.
	const want = "count(case when funding_at_anchor is not null then 1 end)"
	if got != want {
		t.Fatalf("projection for %s = %q, wanted %q", "funding_at_anchor", got, want)
	}

	// AND THE COLUMN STAYS `BOOLEAN`: the fix is in the QUERY, and a schema "fixed" to DOUBLE
	// would turn a predicate into a number — 0/1 over which someone would do arithmetic. See
	// the `derivativesRequiredColumns` block in `derivatives_schema.go`.
	var tipo string
	for _, coluna := range derivativesRequiredColumns() {
		if coluna.name == "funding_at_anchor" {
			tipo = coluna.typ
		}
	}
	if tipo != "BOOLEAN" {
		t.Fatalf("`funding_at_anchor` is declared as %q; it IS a predicate and the fix belonged "+
			"in the query, not the schema", tipo)
	}
}

// AND THE SHAPE DOES NOT DEPEND ON THE TYPE: the expression for a DOUBLE column is the SAME,
// with the name swapped.
//
// Without this case, an implementation that handled BOOLEAN and DOUBLE through different
// branches would pass the case above and bring back a per-type decision — the second list that
// is refused elsewhere.
func TestDurableFillCountExpression_TheFormDoesNotDependOnTheColumnType(t *testing.T) {
	booleana, err := derivativesFillCountExpression("funding_at_anchor")
	if err != nil {
		t.Fatalf("boolean column: %v", err)
	}
	numerica, err := derivativesFillCountExpression("open_interest_base")
	if err != nil {
		t.Fatalf("numeric column: %v", err)
	}

	mascarar := func(expr, coluna string) string { return strings.Replace(expr, coluna, "<column>", 1) }
	if got, want := mascarar(booleana, "funding_at_anchor"), mascarar(numerica, "open_interest_base"); got != want {
		t.Fatalf("the projection of a BOOLEAN column (%q) differs from that of a DOUBLE one (%q) — the "+
			"expression became chosen by type", got, want)
	}
}

// TestDurableFillCountQuery_EveryDeclaredColumnTravelsInOneQuery — the query carries one
// projection per DECLARED column, all in the countable form, and the boolean one is among
// them.
//
// The list comes from the declaration (`redisstore.DeclaredDurableColumns`), not from one
// written here: a second list in this file would be the divergence the checker exists to
// catch, written inside the test itself.
func TestDurableFillCountQuery_EveryDeclaredColumnTravelsInOneQuery(t *testing.T) {
	colunas := redisstore.DeclaredDurableColumns()
	if len(colunas) == 0 {
		t.Fatal("no column declared — the comparison would pass vacuously")
	}
	// THE COLUMN THAT BREAKS THE BARE FORM is in the declaration: without this premise the
	// case would pass without ever looking at the type that requires the conditional form.
	var temBooleana bool
	for _, coluna := range colunas {
		if coluna == "funding_at_anchor" {
			temBooleana = true
		}
	}
	if !temBooleana {
		t.Fatal("`funding_at_anchor` is not in the declaration — the case would not cover the BOOLEAN type")
	}

	query, err := derivativesFillCountQuery(colunas)
	if err != nil {
		t.Fatalf("derivativesFillCountQuery: %v", err)
	}

	for _, coluna := range colunas {
		if !strings.Contains(query, "count(case when "+coluna+" is not null then 1 end)") {
			t.Errorf("the query does not count %q in the form that serves every type: %q", coluna, query)
		}
		if strings.Contains(query, "count("+coluna+")") {
			t.Errorf("the bare form `count(%s)` came back — it is the one the database refuses for BOOLEAN", coluna)
		}
	}
	// ONE query: a single `FROM` and a single parameterized timeframe. Two queries per timeframe
	// would pay for the isolation the correct expression makes unnecessary.
	if got := strings.Count(query, " FROM "); got != 1 {
		t.Errorf("the query has %d `FROM`; the checker's counter is ONE query per timeframe", got)
	}
	if !strings.Contains(query, "WHERE timeframe = $1") {
		t.Errorf("the timeframe does not travel as a parameter: %q", query)
	}
}

// TestDurableFillCountQuery_AnInvalidColumnNameIsRefused — the boundary remains validated
// even with the new expression: the name is interpolated, and an identifier accepts no
// parameter.
func TestDurableFillCountQuery_AnInvalidColumnNameIsRefused(t *testing.T) {
	if _, err := derivativesFillCountQuery([]string{"ok_col", "1; DROP TABLE derivatives"}); err == nil {
		t.Fatal("an invalid column name entered the query")
	}
}

// ============================================================================
// THE CHECKER'S QUERY IS ASSERTED BY THE SCHEMA
//
// A double's FAKE counter proves nothing about the SQL: while it was the only coverage, the
// real query was never exercised and the boolean column could take it down in production with
// no red anywhere. What this block adds is the other half — the SHAPE of the query, asserted
// against the TYPES the schema declares.
//
// The REAL EXECUTION of the query against QuestDB 8.2.3 is EXPLICIT INTEGRATION and does not
// run in this file: it requires a database, and a unit test that depended on the network would
// measure database availability, not the query. It runs as part of closing the work, with the
// raw output recorded, so that its absence does not pass as coverage.
//
// THE LIST COMES FROM THE SCHEMA (`derivatives_schema.go`), not from a sample chosen here: a
// new column enters this enumeration by itself. The defect the case exists to catch is exactly
// this one — a column of a new type that the projection does not know how to count. While the
// implementation chooses the expression BY TYPE, this is where it breaks; while the expression
// comes from the column NAME, no new type can blind production.
// ============================================================================

// TestDurableFillCount_EveryDeclaredTypeSharesOneExpression is the case that replaces the fake
// counter as proof of the query.
func TestDurableFillCount_EveryDeclaredTypeSharesOneExpression(t *testing.T) {
	if len(derivativesRequiredColumns()) == 0 {
		t.Fatal("the schema declares no column — the enumeration would pass vacuously")
	}

	// THE TYPES ARE READ FROM THE SCHEMA, and the map is just the count per type.
	tipos := map[string]int{}
	for _, coluna := range derivativesRequiredColumns() {
		tipos[coluna.typ]++
	}
	// THE SINGLE FORM IS ONLY DISCRIMINATED IF THERE IS MORE THAN ONE TYPE: with only one, "the
	// expression does not depend on the type" would be vacuously true, and a per-type branch
	// would pass the same.
	if len(tipos) < 2 {
		t.Fatalf("the schema declares a single type (%v) — the cross-type comparison would have nothing to compare", tipos)
	}
	// AND THE THREE TYPES THIS CHECKER CROSSES MUST BE HERE: the boolean that breaks the bare
	// query form, the numeric of measurements, and the rung's state name.
	for _, exigido := range []string{"BOOLEAN", "DOUBLE", "SYMBOL"} {
		if tipos[exigido] == 0 {
			t.Errorf("the schema declares no %s column — the enumeration stopped covering a "+
				"type the checker's query meets", exigido)
		}
	}

	// THE MASKED FORM — the expression with the column name replaced by a marker — must be the
	// SAME for every declared type. It is this equality that a per-type decision breaks.
	forma := ""
	for _, coluna := range derivativesRequiredColumns() {
		expr, err := derivativesFillCountExpression(coluna.name)
		if err != nil {
			t.Fatalf("column %s (%s) produced no projection: %v — the whole query would fall with "+
				"it, and that is how production goes blind", coluna.name, coluna.typ, err)
		}
		// THE SEMANTICS REMAIN THOSE OF `count(column)`: NON-NULL rows. The new expression must
		// not count the row, or a table full of rows without the column would answer like a
		// healthy table — the defect the bare form had to avoid.
		if !strings.Contains(expr, "when "+coluna.name+" is not null") {
			t.Errorf("the projection of %s (%s) does not count the NON-NULLs: %q", coluna.name, coluna.typ, expr)
		}
		mascarada := strings.Replace(expr, coluna.name, "<column>", 1)
		if forma == "" {
			forma = mascarada
		}
		if mascarada != forma {
			t.Fatalf("the expression for %s (%s) is %q and the one for the other types is %q — the projection "+
				"became chosen by TYPE, and that choice is what a column of a new type blinds",
				coluna.name, coluna.typ, mascarada, forma)
		}
	}
	// AND NO TYPE NAME SURVIVES IN THE EXPRESSION: a `cast(... as int)` or a per-type variant
	// would leave a mark.
	for tipo := range tipos {
		if strings.Contains(forma, tipo) {
			t.Errorf("the expression carries type %q: %q — the form must come from the column NAME", tipo, forma)
		}
	}

	// THE QUERY ASSEMBLED WITH THE SCHEMA'S COLUMNS: one projection per column, one query.
	nomes := make([]string, 0, len(derivativesRequiredColumns()))
	for _, coluna := range derivativesRequiredColumns() {
		nomes = append(nomes, coluna.name)
	}
	query, err := derivativesFillCountQuery(nomes)
	if err != nil {
		t.Fatalf("derivativesFillCountQuery: %v", err)
	}
	if got := strings.Count(query, "count("); got != len(nomes) {
		t.Errorf("the query has %d projection(s) for %d schema column(s): %q", got, len(nomes), query)
	}
	if got := strings.Count(query, " FROM "); got != 1 {
		t.Errorf("the query has %d `FROM`: that is two database round trips per timeframe where the "+
			"correct expression allows one", got)
	}
}
