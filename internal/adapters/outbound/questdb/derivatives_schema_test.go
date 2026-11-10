package questdb

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/derivatives"
)

// fakeTableSchema records what was asked and what was altered, and lets the test decide what
// the database answers AFTER the ALTER — which is the fact that matters.
type fakeTableSchema struct {
	present map[string]bool // key: "table.column"
	// landsAfterAlter says whether the column comes to exist after an accepted ALTER. False is
	// the case the mechanism must name instead of swallowing.
	landsAfterAlter bool
	addErr          error
	presentErr      error

	probed  []string
	altered []string
}

func (f *fakeTableSchema) columnPresent(_ context.Context, table, column string) (bool, error) {
	f.probed = append(f.probed, table+"."+column)
	if f.presentErr != nil {
		return false, f.presentErr
	}
	return f.present[table+"."+column], nil
}

func (f *fakeTableSchema) addColumn(_ context.Context, table, column, typ string) error {
	f.altered = append(f.altered, table+"."+column+" "+typ)
	if f.addErr != nil {
		return f.addErr
	}
	if f.landsAfterAlter {
		if f.present == nil {
			f.present = map[string]bool{}
		}
		f.present[table+"."+column] = true
	}
	return nil
}

// TestEnsureDerivativesSchema_AlteraDerivativesENaoCandles — the scope is the table.
//
// `EnsureSchema` is pinned to `q.tableName`, and its store answers `"candles"`. A derivatives
// column added to that list would alter the wrong table and complain about nothing: `ADD
// COLUMN` on a table that accepts the column works.
func TestEnsureDerivativesSchema_AlteraDerivativesENaoCandles(t *testing.T) {
	fake := &fakeTableSchema{landsAfterAlter: true}
	store := &Store{tableName: "candles", schema: fake}

	if err := store.EnsureDerivativesSchema(context.Background()); err != nil {
		t.Fatalf("EnsureDerivativesSchema: %v", err)
	}

	// EIGHT columns. The shape of the assertion is exact set, exact type, and nothing outside
	// `derivatives`.
	if len(fake.altered) != 8 {
		t.Fatalf("%d ALTER(s) emitted (%v), wanted 8", len(fake.altered), fake.altered)
	}
	for _, alvo := range append(append([]string{}, fake.altered...), fake.probed...) {
		if strings.HasPrefix(alvo, "candle.") {
			t.Fatalf("the mechanism touched %q — the scope is the `derivatives` table", alvo)
		}
		if !strings.HasPrefix(alvo, "derivatives.") {
			t.Fatalf("the mechanism touched %q, which is not the `derivatives` table", alvo)
		}
	}

	// The columns, with the type each must be born with: the rung is a state name, not a
	// number, and storing it as DOUBLE would lose the whole value.
	querido := map[string]bool{
		"derivatives.funding_interval_hours DOUBLE": true,
		"derivatives.n_step SYMBOL":                 true,
		"derivatives.open_interest DOUBLE":          true,
		"derivatives.next_funding_rate DOUBLE":      true,
		"derivatives.interest_rate DOUBLE":          true,
		"derivatives.funding_at_anchor BOOLEAN":     true,
		"derivatives.ls_long_pct DOUBLE":            true,
		"derivatives.ls_short_pct DOUBLE":           true,
	}
	for _, alter := range fake.altered {
		if !querido[alter] {
			t.Fatalf("unexpected ALTER: %q — wanted %v", alter, querido)
		}
		delete(querido, alter)
	}
	if len(querido) > 0 {
		t.Fatalf("declared columns the mechanism did not create: %v", querido)
	}
}

// TestEnsureColumns_OAlterAceitoQueNaoAterrissaEhNomeado asserts the accepted-but-not-landed
// ALTER is named, which keeps the fix from reproducing the defect.
//
// The command's nil error says it was ACCEPTED. On a WAL table, accepted and applied are two
// facts, and a declared column that never lands reads as zero rows — which is exactly the state
// of `funding_rate_8h` that originated this behavior.
func TestEnsureColumns_OAlterAceitoQueNaoAterrissaEhNomeado(t *testing.T) {
	fake := &fakeTableSchema{landsAfterAlter: false}

	err := ensureColumns(context.Background(), fake, "derivatives", derivativesRequiredColumns(), janelaCurta())
	if err == nil {
		t.Fatal("the ALTER was accepted, the column does not exist, and the mechanism declared success")
	}
	for _, col := range derivativesRequiredColumns() {
		if !strings.Contains(err.Error(), col.name) {
			t.Errorf("the error does not name %q: %v", col.name, err)
		}
	}
	if !strings.Contains(err.Error(), "still absent") {
		t.Errorf("the error does not say what happened: %v", err)
	}
}

// TestEnsureColumns_OAlterQueFalhaEhNomeado — the command's own failure also comes out with
// the column name, not as a generic schema error.
func TestEnsureColumns_OAlterQueFalhaEhNomeado(t *testing.T) {
	fake := &fakeTableSchema{addErr: errors.New("permission denied")}

	err := ensureColumns(context.Background(), fake, "derivatives",
		[]schemaColumn{{name: "n_step", typ: "SYMBOL"}}, janelaCurta())
	if err == nil {
		t.Fatal("an ALTER refused by the database passed as success")
	}
	if !strings.Contains(err.Error(), "n_step") || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("the error carries neither the column nor the cause: %v", err)
	}
}

// TestEnsureColumns_ColunaPresenteNaoEhAlterada — the lesson already paid for: `ADD COLUMN IF
// NOT EXISTS` is REFUSED by QuestDB on a WAL table even when the column already exists, so
// altering without probing produces an alarm that fires on every boot of a healthy system.
func TestEnsureColumns_ColunaPresenteNaoEhAlterada(t *testing.T) {
	// The already complete table is DERIVED from the declaration, not typed out: a new column
	// added to `derivativesRequiredColumns` enters here by itself, instead of making this case
	// fail for being out of date and inviting someone to loosen it.
	fake := &fakeTableSchema{present: map[string]bool{}}
	for _, col := range derivativesRequiredColumns() {
		fake.present["derivatives."+col.name] = true
	}

	if err := ensureColumns(context.Background(), fake, "derivatives", derivativesRequiredColumns(), janelaCurta()); err != nil {
		t.Fatalf("ensureColumns on an already complete table: %v", err)
	}
	if len(fake.altered) != 0 {
		t.Fatalf("altered %v on a table that already has the columns", fake.altered)
	}
	if len(fake.probed) != len(derivativesRequiredColumns()) {
		t.Fatalf("%d probe(s) (%v), wanted %d — one per column", len(fake.probed), fake.probed, len(derivativesRequiredColumns()))
	}
}

// TestEnsureColumns_SondaQueNaoResponde — not knowing is a third state, and it cannot become
// "it is there".
func TestEnsureColumns_SondaQueNaoResponde(t *testing.T) {
	fake := &fakeTableSchema{presentErr: errors.New("connection refused")}

	err := ensureColumns(context.Background(), fake, "derivatives",
		[]schemaColumn{{name: "n_step", typ: "SYMBOL"}}, janelaCurta())
	if err == nil {
		t.Fatal("the probe failed and the mechanism declared the schema complete")
	}
	if len(fake.altered) != 0 {
		t.Fatalf("altered %v without knowing whether the column existed", fake.altered)
	}
}

// ============================================================================
// A DURABLE COLUMN FOR EVERY QUANTITY THE HOT TIER CARRIES
// ============================================================================

// ddlMeasurementColumns are the MEASUREMENT columns `questdb-init.sql` creates with the table.
// The other six in the DDL name the row (who, where, when, under which convention).
func ddlMeasurementColumns() []string {
	return []string{
		"open_interest_base", "funding_rate", "funding_rate_8h", "long_short_ratio",
	}
}

// hotQuantityColumns maps, field by field, which durable column carries each quantity the hot
// member of the SERIES carries.
//
// Written by hand on purpose. A camelCase → snake_case convention would derive the names and
// pass with any of them; what is asserted here is the EXACT NAME the write and the read will
// use, and a typo in one of them is a column that never lands.
func hotQuantityColumns() map[string]string {
	return map[string]string{
		"openInterestBase":     "open_interest_base",
		"fundingRate":          "funding_rate",
		"fundingRate8h":        "funding_rate_8h",
		"longShortRatio":       "long_short_ratio",
		"fundingIntervalHours": "funding_interval_hours",
		"nStep":                "n_step",
		"openInterest":         "open_interest",
		"nextFundingRate":      "next_funding_rate",
		"interestRate":         "interest_rate",
		"fundingAtAnchor":      "funding_at_anchor",
		"lsLongPct":            "ls_long_pct",
		"lsShortPct":           "ls_short_pct",
	}
}

// THE ENUMERATION COMES FROM THE STRUCT, not from a list. A new quantity in the snapshot that
// is born without a durable column fails here instead of becoming the ninth gap nobody noticed.
//
// The rule for "what is a quantity" is by TYPE — `*float64`, `*bool`, `NDerivationStep` — the
// same one `storage/durable_coverage_test.go` applies on the other side. An instant (`*int64`)
// and a dimension (`string`) are not measurements.
func TestDerivativesRequiredColumns_EveryHotQuantityHasADurableColumn(t *testing.T) {
	disponiveis := map[string]bool{}
	for _, coluna := range ddlMeasurementColumns() {
		disponiveis[coluna] = true
	}
	for _, col := range derivativesRequiredColumns() {
		disponiveis[col.name] = true
	}

	tipo := reflect.TypeOf(derivatives.DerivativesSnapshot{})
	var semColuna []string
	for i := 0; i < tipo.NumField(); i++ {
		campo := tipo.Field(i)
		switch campo.Type.String() {
		case "*float64", "*bool", "derivatives.NDerivationStep":
		default:
			continue
		}
		nome := jsonFieldName(campo)

		// `oiChangePct` IS THE ONLY EXCEPTION, and it is MEASURED. The requirement is a column
		// for the quantity the hot tier carries AND THAT THE READ ROUTE SERVES; this one is
		// neither on the series path. See the dedicated assertion below.
		if nome == "oiChangePct" {
			continue
		}

		coluna, declarada := hotQuantityColumns()[nome]
		if !declarada {
			semColuna = append(semColuna, nome+" (no column named)")
			continue
		}
		if !disponiveis[coluna] {
			semColuna = append(semColuna, nome+" → "+coluna+" (the table does not have it)")
		}
	}
	sort.Strings(semColuna)
	if len(semColuna) > 0 {
		t.Fatalf("hot-member quantity(ies) without a durable column: %v — when the cap bites they disappear and come back from nowhere", semColuna)
	}
}

// THE EXCEPTION, ASSERTED INSTEAD OF ASSUMED.
//
// `oiChangePct` stays OUT of the durable schema because `dropPollToPollOIChange` zeroes it on
// the series path — on write and on read —, so neither the hot member carries it nor the
// series route returns it. Writing it per bucket would stamp a poll delta onto a window it
// never measured.
//
// Without this case, "we forgot the seventh" and "we deliberately excluded the seventh" would
// be the same file.
func TestDerivativesRequiredColumns_ThePollToPollDeltaIsDeliberatelyAbsent(t *testing.T) {
	for _, col := range derivativesRequiredColumns() {
		if strings.Contains(col.name, "oi_change") || strings.Contains(col.name, "change_pct") {
			t.Fatalf("the durable schema gained %q — a poll delta written per bucket asserts a window it never measured", col.name)
		}
	}
	if _, nomeada := hotQuantityColumns()["oiChangePct"]; nomeada {
		t.Fatal("oiChangePct was named with a durable column — it never reaches the series member")
	}
}

// NO ADDITIVE COLUMN IS A DEDUP KEY. That is what makes them safe on a live table: re-seed
// idempotency is the UPSERT key, and touching it would change what a rewrite replaces — in
// production, over 200,854 rows.
func TestDerivativesRequiredColumns_NoneIsADedupKey(t *testing.T) {
	chaves := map[string]bool{}
	for _, k := range derivativesDedupKeys() {
		chaves[k] = true
	}
	for _, col := range derivativesRequiredColumns() {
		if chaves[col.name] {
			t.Errorf("additive column %q is a DEDUP key — adding it changes re-seed idempotency", col.name)
		}
	}
}

// jsonFieldName returns the field's JSON name, which is what the route delivers and what the
// coverage declaration uses as a key.
func jsonFieldName(f reflect.StructField) string {
	tag := f.Tag.Get("json")
	if i := strings.IndexByte(tag, ','); i >= 0 {
		return tag[:i]
	}
	return tag
}

// janelaCurta is the landing window for unit cases: short enough that the TIMEOUT path is
// exercised in milliseconds, and non-zero so it is actually exercised — with zero, the loop
// would probe once and the "the wait does not exist" mutant would survive the entire suite.
func janelaCurta() schemaSettle {
	return schemaSettle{total: 20 * time.Millisecond, step: time.Millisecond}
}

// ============================================================================
// THE WAIT FOR WAL-APPLY, WHICH THE LIVE ENGINE FORCED INTO EXISTENCE
// ============================================================================

// THE COLUMN THAT APPEARS ON THE THIRD PROBE IS ACCEPTED.
//
// Measured against `questdb/questdb:8.2.3`: `ALTER TABLE ... ADD COLUMN` on a WAL table
// returns OK and `table_columns()` answers ZERO the next instant. Over pgwire the immediate
// re-probe lost the race on all EIGHT columns.
//
// The discriminating assertion is not "no error": it is the PROBE COUNT. An implementation
// that probed once and gave up would also not error if the column were already there.
func TestEnsureColumns_TheColumnThatLandsLateIsAccepted(t *testing.T) {
	fake := &tardyTableSchema{landsOnProbe: 3}

	if err := ensureColumns(context.Background(), fake, "derivatives",
		[]schemaColumn{{name: "open_interest", typ: "DOUBLE"}}, janelaCurta()); err != nil {
		t.Fatalf("the column landed on the third probe and the mechanism failed: %v", err)
	}
	// 1 probe before the ALTER + 3 after, the third being the one that finds it.
	if fake.probes != 4 {
		t.Fatalf("%d probe(s), wanted 4 — the wait for WAL-apply is not happening", fake.probes)
	}
}

// AND THE WINDOW CLOSES. The wait cannot become an infinite wait: a column that never lands is
// still named, which is the half of the mechanism this behavior exists to preserve.
func TestEnsureColumns_TheWindowClosesOnAColumnThatNeverLands(t *testing.T) {
	fake := &tardyTableSchema{landsOnProbe: 1 << 30} // never

	inicio := time.Now()
	err := ensureColumns(context.Background(), fake, "derivatives",
		[]schemaColumn{{name: "open_interest", typ: "DOUBLE"}}, janelaCurta())
	decorrido := time.Since(inicio)

	if err == nil {
		t.Fatal("the column never landed and the mechanism declared success")
	}
	if !strings.Contains(err.Error(), "still absent") {
		t.Errorf("the error does not say what happened: %v", err)
	}
	if decorrido > time.Second {
		t.Errorf("the wait took %v with a window of %v — it stopped being bounded", decorrido, janelaCurta().total)
	}
	if fake.probes < 3 {
		t.Errorf("%d probe(s) — the window closed before giving WAL-apply a chance", fake.probes)
	}
}

// tardyTableSchema reproduces WAL-apply: the `ALTER` is accepted and the column only appears
// from the Nth following probe on.
type tardyTableSchema struct {
	landsOnProbe int // how many POST-ALTER probes until the column appears
	probes       int
	altered      int
	postAlter    int
}

func (f *tardyTableSchema) columnPresent(_ context.Context, _, _ string) (bool, error) {
	f.probes++
	if f.altered == 0 {
		return false, nil
	}
	f.postAlter++
	return f.postAlter >= f.landsOnProbe, nil
}

func (f *tardyTableSchema) addColumn(_ context.Context, _, _, _ string) error {
	f.altered++
	return nil
}
