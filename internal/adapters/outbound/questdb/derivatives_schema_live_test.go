package questdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	questdb "github.com/questdb/go-questdb-client/v3"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/marxms/go-whale/internal/derivatives"
)

// ============================================================================
// THE PROOF AGAINST THE LIVE ENGINE — QuestDB 8.2.3, the SAME production tag
// ============================================================================
//
// # WHY IT EXISTS, GIVEN THE TEST WITH A DOUBLE
//
// The double proves the DECISION — probe before, alter, re-probe, name the one that did not
// land. It cannot prove the FACT that 8.2.3 accepts `ALTER TABLE ... ADD COLUMN` on a WAL
// table with DEDUP and that the column appears in `table_columns()`. That fact has already
// cost this repository dearly once: `ADD COLUMN IF NOT EXISTS` is REFUSED on this version,
// and nobody knew until running against it.
//
// # WHY IT SKIPS BY DEFAULT
//
// A test that requires a running database turns every machine without Docker into a red that
// says nothing. It runs when `QDB_LIVE_PG` points at a QuestDB 8.2.3 — and that is how the
// evidence is collected:
//
//	QDB_LIVE_PG='postgres://admin:quest@localhost:18812/qdb' go test -run Live ./internal/candle/engine/
//
// # WHY ON ITS OWN TABLE
//
// It CREATES the table with the same DDL as `derivatives` and DROPS it at the end. The
// property under test belongs to the engine, not to that specific table, and a test that
// alters the real table leaves residue for whoever comes next.

const liveProbeTable = "derivatives_clo32_live_probe"

// liveProbeDDL mirrors `scripts/questdb/questdb-init.sql` in the form that matters for this
// proof: the twelve initial columns, `PARTITION BY DAY`, `WAL` and `DEDUP UPSERT KEYS`. It is
// on a WAL table with dedup that `ADD COLUMN` must work.
const liveProbeDDL = `CREATE TABLE IF NOT EXISTS ` + liveProbeTable + ` (
    timestamp TIMESTAMP, source SYMBOL, exchange SYMBOL, market_type SYMBOL,
    symbol SYMBOL, timeframe SYMBOL, open_interest_base DOUBLE, funding_rate DOUBLE,
    funding_rate_8h DOUBLE, long_short_ratio DOUBLE, denominated_in SYMBOL,
    label_convention SYMBOL
) timestamp(timestamp) PARTITION BY DAY WAL
DEDUP UPSERT KEYS(timestamp, source, exchange, market_type, symbol, timeframe)`

// TestEnsureColumnsLive_TheEightColumnsLandOnAWalDedupTable is the mechanism's proof against
// 8.2.3: the eight declared columns land, and the second pass emits no `ALTER` at all.
func TestEnsureColumnsLive_TheEightColumnsLandOnAWalDedupTable(t *testing.T) {
	pool := liveQuestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := pool.Exec(ctx, liveProbeDDL); err != nil {
		t.Fatalf("probe CREATE TABLE: %v", err)
	}
	t.Cleanup(func() {
		limpeza, cancelar := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelar()
		if _, err := pool.Exec(limpeza, "DROP TABLE IF EXISTS "+liveProbeTable); err != nil {
			t.Errorf("the probe left residue: DROP TABLE failed: %v", err)
		}
	})

	schema := pgTableSchema{pool: pool}

	// The table is born WITHOUT any of the eight. Without this the case would pass against a
	// table that already had them, without exercising a single `ALTER`.
	for _, col := range derivativesRequiredColumns() {
		presente, err := schema.columnPresent(ctx, liveProbeTable, col.name)
		if err != nil {
			t.Fatalf("columnPresent(%s): %v", col.name, err)
		}
		if presente {
			t.Fatalf("the table was born with %q — the probe would not exercise the ALTER", col.name)
		}
	}

	if err := ensureColumns(ctx, schema, liveProbeTable, derivativesRequiredColumns(), derivativesSchemaSettle()); err != nil {
		t.Fatalf("ensureColumns against live 8.2.3: %v", err)
	}

	// THE ROUND TRIP IS THE ASSERTION. `ensureColumns` already re-probes, but it re-probes
	// through the same seam; here the metadata is read again, from scratch, and compared as a
	// SET.
	lidas := liveColumnsOf(ctx, t, pool, liveProbeTable)
	for _, col := range derivativesRequiredColumns() {
		tipo, presente := lidas[col.name]
		if !presente {
			t.Errorf("column %q is not in table_columns() after the accepted ALTER", col.name)
			continue
		}
		if tipo != col.typ {
			t.Errorf("column %q was born %s, declared %s — the read type is what the write will find",
				col.name, tipo, col.typ)
		}
	}

	// CONVERGENT: the second pass alters nothing. It is what lets the mechanism run on every
	// boot of a binary in crashloop without becoming an alarm.
	segunda := &countingSchema{inner: schema}
	if err := ensureColumns(ctx, segunda, liveProbeTable, derivativesRequiredColumns(), derivativesSchemaSettle()); err != nil {
		t.Fatalf("second ensureColumns pass: %v", err)
	}
	if segunda.alters > 0 {
		t.Errorf("the second pass emitted %d ALTER(s) on an already complete table", segunda.alters)
	}
}

// TestEnsureColumnsLive_TheDedupKeysSurviveTheAdditiveColumns: additive columns must not touch
// the UPSERT key, because it is what makes re-seeding idempotent over the 200,854 rows
// measured in production.
func TestEnsureColumnsLive_TheDedupKeysSurviveTheAdditiveColumns(t *testing.T) {
	pool := liveQuestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := pool.Exec(ctx, liveProbeDDL); err != nil {
		t.Fatalf("probe CREATE TABLE: %v", err)
	}
	t.Cleanup(func() {
		limpeza, cancelar := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelar()
		if _, err := pool.Exec(limpeza, "DROP TABLE IF EXISTS "+liveProbeTable); err != nil {
			t.Errorf("the probe left residue: DROP TABLE failed: %v", err)
		}
	})

	if err := ensureColumns(ctx, pgTableSchema{pool: pool}, liveProbeTable, derivativesRequiredColumns(), derivativesSchemaSettle()); err != nil {
		t.Fatalf("ensureColumns: %v", err)
	}

	var nomes []string
	rows, err := pool.Query(ctx,
		`SELECT "column", upsertKey FROM table_columns('`+liveProbeTable+`')`)
	if err != nil {
		t.Fatalf("table_columns: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var nome string
		var chave bool
		if err := rows.Scan(&nome, &chave); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if chave {
			nomes = append(nomes, nome)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iteration: %v", err)
	}

	sort.Strings(nomes)
	querido := append([]string(nil), derivativesDedupKeys()...)
	sort.Strings(querido)
	if fmt.Sprint(nomes) != fmt.Sprint(querido) {
		t.Fatalf("UPSERT keys after the additive columns = %v, wanted %v", nomes, querido)
	}
}

// countingSchema counts the `ALTER`s without preventing any: convergence is about what the
// mechanism EMITS, not about what the database would accept.
type countingSchema struct {
	inner  tableSchema
	alters int
}

func (c *countingSchema) columnPresent(ctx context.Context, table, column string) (bool, error) {
	return c.inner.columnPresent(ctx, table, column)
}

func (c *countingSchema) addColumn(ctx context.Context, table, column, typ string) error {
	c.alters++
	return c.inner.addColumn(ctx, table, column, typ)
}

// liveColumnsOf reads name → type from `table_columns()`.
func liveColumnsOf(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string) map[string]string {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT "column", type FROM table_columns('`+table+`')`)
	if err != nil {
		t.Fatalf("table_columns(%s): %v", table, err)
	}
	defer rows.Close()

	out := map[string]string{}
	for rows.Next() {
		var nome, tipo string
		if err := rows.Scan(&nome, &tipo); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out[nome] = tipo
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iteration: %v", err)
	}
	return out
}

// liveQuestDB opens the pool, or skips the case.
func liveQuestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("QDB_LIVE_PG")
	if dsn == "" {
		t.Skip("QDB_LIVE_PG not declared — the proof against the live engine needs a running QuestDB 8.2.3")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New(%s): %v", dsn, err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping at %s: %v", dsn, err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// ============================================================================
// THE ROUND TRIP AGAINST THE LIVE ENGINE
// ============================================================================
//
// # WHY IT IS THE ONLY ONE THAT PROVES THE "READABLE" HALF
//
// The cases with a double prove that the requested column list covers the emitted list, and
// that the translation of the read row carries the fields. None of them touches the `Scan`,
// which is POSITIONAL and typed by the driver — the exact place where a BOOLEAN column read
// as `*float64` blows up on every row, and where a swapped order delivers the right value in
// the wrong column.
//
// This case writes through ILP and reads through the REAL query (`derivativesRangeQuery`),
// against the same production tag. It is the demonstration that writing **and** readability
// both hold.

// liveProbeSymbol and liveProbeDay isolate what this case writes into the real table.
//
// The dedicated partition is what makes cleanup EXACT: `DROP PARTITION LIST` removes the
// whole day, and no real data lives in 2001.
const (
	liveProbeSymbol = "CLO33/PROBE"
	liveProbeDay    = "2001-01-01"
)

func TestDerivativesRoundTripLive_TheSixNewColumnsAreWrittenAndReadBack(t *testing.T) {
	pool := liveQuestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	sender, err := questdb.LineSenderFromConf(ctx, "http::addr="+liveILPAddr(t)+";")
	if err != nil {
		t.Fatalf("LineSenderFromConf: %v", err)
	}
	defer func() { _ = sender.Close(context.Background()) }()

	store := &Store{ilpSender: sender, pgPool: pool, tableName: "candles", priors: emptyPriorSource{}}

	if err := ensureColumns(ctx, pgTableSchema{pool: pool}, derivativesTableName,
		derivativesRequiredColumns(), derivativesSchemaSettle()); err != nil {
		t.Fatalf("EnsureDerivativesSchema against the real table: %v", err)
	}
	t.Cleanup(func() { restoreLiveDerivatives(t, pool) })

	bucket, err := time.Parse(time.RFC3339, liveProbeDay+"T00:00:00Z")
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	anchor := true
	ponto := derivatives.DerivativesColdPoint{
		Timestamp:            bucket,
		Source:               "coinalyze",
		Exchange:             "binance",
		MarketType:           "future",
		Symbol:               liveProbeSymbol,
		Timeframe:            "1d",
		DenominatedIn:        "BASE_ASSET",
		LabelConvention:      "interval-start",
		OpenInterestBase:     fp(106117.954),
		FundingRate:          fp(0.0001),
		FundingRate8h:        fp(0.0003),
		LongShortRatio:       fp(0.9554),
		FundingIntervalHours: fp(8),
		NStep:                derivatives.NFromMeasuredSpacing,
		OpenInterest:         fp(5_432_100_000),
		NextFundingRate:      fp(0.00012),
		InterestRate:         fp(0.0001),
		FundingAtAnchor:      &anchor,
		LSLongPct:            fp(48.86),
		LSShortPct:           fp(51.14),
	}

	if err := store.WriteDerivativesBatch(ctx, []derivatives.DerivativesColdPoint{ponto}); err != nil {
		t.Fatalf("WriteDerivativesBatch: %v", err)
	}

	escopo := derivatives.DerivativesScope{
		Source: "coinalyze", Exchange: "binance", MarketType: "future",
		Symbol: liveProbeSymbol, Timeframe: "1d",
	}
	de, ate := bucket.Add(-24*time.Hour), bucket.Add(24*time.Hour)

	// THE WAL-APPLY OF THE WRITE HAS THE SAME ASYNCHRONY AS THE `ALTER`'S. Bounded wait, and
	// its failure is a case failure — not a `skip`.
	var lidos []*derivatives.DerivativesSnapshot
	prazo := time.Now().Add(30 * time.Second)
	for {
		lidos, err = store.QueryDerivativesRange(ctx, escopo, de, ate)
		if err != nil {
			t.Fatalf("QueryDerivativesRange: %v", err)
		}
		if len(lidos) > 0 || time.Now().After(prazo) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(lidos) != 1 {
		t.Fatalf("%d row(s) read by the real route, wanted 1 — the written row is not reachable through the query", len(lidos))
	}

	got := lidos[0]
	for _, caso := range []struct {
		nome string
		got  *float64
		want float64
	}{
		{"openInterest", got.OpenInterest, 5_432_100_000},
		{"nextFundingRate", got.NextFundingRate, 0.00012},
		{"interestRate", got.InterestRate, 0.0001},
		{"lsLongPct", got.LSLongPct, 48.86},
		{"lsShortPct", got.LSShortPct, 51.14},
		// The older ones keep crossing: the list grew and the positional `Scan` order had to
		// follow. A one-slot shift shows up here.
		{"openInterestBase", got.OpenInterestBase, 106117.954},
		{"fundingRate", got.FundingRate, 0.0001},
		{"fundingRate8h", got.FundingRate8h, 0.0003},
		{"longShortRatio", got.LongShortRatio, 0.9554},
		{"fundingIntervalHours", got.FundingIntervalHours, 8},
	} {
		if caso.got == nil {
			t.Errorf("%s came back NULL from the real route — the column is written and unreachable", caso.nome)
			continue
		}
		if *caso.got != caso.want {
			t.Errorf("%s = %v, wanted %v", caso.nome, *caso.got, caso.want)
		}
	}
	if got.FundingAtAnchor == nil || !*got.FundingAtAnchor {
		t.Errorf("fundingAtAnchor = %v, wanted true — the BOOLEAN column is the first the positional `Scan` breaks", got.FundingAtAnchor)
	}
	if got.NStep != derivatives.NFromMeasuredSpacing {
		t.Errorf("nStep = %q, wanted %q", got.NStep, derivatives.NFromMeasuredSpacing)
	}
}

// restoreLiveDerivatives returns the real table to the state the DDL creates: the probe
// partition leaves, the eight additive columns leave, and the count is CHECKED — a cleanup
// that does not verify itself is residue with good intentions.
func restoreLiveDerivatives(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := pool.Exec(ctx,
		"ALTER TABLE "+derivativesTableName+" DROP PARTITION LIST '"+liveProbeDay+"'"); err != nil {
		t.Errorf("the probe partition stayed behind: %v", err)
	}
	for _, col := range derivativesRequiredColumns() {
		if _, err := pool.Exec(ctx,
			"ALTER TABLE "+derivativesTableName+" DROP COLUMN "+col.name); err != nil {
			t.Errorf("column %q stayed behind: %v", col.name, err)
		}
	}

	prazo := time.Now().Add(20 * time.Second)
	for {
		var n int64
		if err := pool.QueryRow(ctx,
			`SELECT count() FROM table_columns('`+derivativesTableName+`')`).Scan(&n); err != nil {
			t.Fatalf("checking the cleanup: %v", err)
		}
		if n == 12 {
			return
		}
		if time.Now().After(prazo) {
			t.Fatalf("the table ended with %d columns, wanted the 12 from the DDL", n)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// liveILPAddr derives the ILP address from the DSN's host: QuestDB's HTTP port (9000 in the
// container) is declared in `QDB_LIVE_ILP`, and without it the case does not guess.
func liveILPAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("QDB_LIVE_ILP")
	if addr == "" {
		t.Skip("QDB_LIVE_ILP not declared — the round trip needs the ILP address of the same QuestDB")
	}
	return addr
}

// ============================================================================
// ABSENCE OF A DURABLE ROW IS A STATE, NOT AN ERROR
// ============================================================================
//
// # WHY THIS CASE MUST BE LIVE
//
// `GetOldestTimestamp` promises, in its comment, *"returns zero time if no data exists"*, and
// the `ts == nil` branch implements that promise FOR POSTGRES — which returns ONE row with
// `NULL` when the `WHERE` of a `MIN()` matches nothing.
//
// QuestDB returns ZERO ROWS. It is an ENGINE difference, and that is why no double catches
// it: a double that returns `NULL` in one row passes the defective code, and the whole suite
// stays green.
//
// The cost of that blindness was measured in PRODUCTION. On the LAN deployment, the sole-copy
// migration aborted on EVERY pass, always at the same point:
//
//	migration: durable boundary for MRNA/USDT 1w: questdb oldest ts: no rows in result set
//
// `MRNA/USDT` on `1w` had never been written to the durable tier — which is exactly the state
// the migration exists to repair. The abort withheld the retention license, so nothing was
// destroyed, but the service did not come up.
//
// THE MUTANT IT KILLS: the one that removes the `pgx.ErrNoRows` branch and treats absence as
// an error again. Without a live engine, that mutant survives the entire suite.
func TestGetOldestTimestampLive_AusenciaDeLinhaNaoEhErro(t *testing.T) {
	pool := liveQuestDB(t)

	// The scope is deliberately nonexistent: no row matches, and that is the point.
	const naoExiste = "scope-that-does-not-exist-" + "TestGetOldestTimestampLive"
	var ts *time.Time
	err := pool.QueryRow(context.Background(),
		"SELECT MIN(timestamp) FROM candles WHERE exchange = $1 AND market_type = $2 AND symbol = $3 AND timeframe = $4",
		naoExiste, "future", "NAO/EXISTE", "1w").Scan(&ts)

	// FIRST the engine semantics, because they are what justifies the branch in production
	// code. If QuestDB ever starts returning `NULL` in a row like Postgres, this case warns —
	// and the branch becomes dead code instead of continuing to be faith.
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("MIN() over a WHERE that matches nothing returned err=%v (ts=%v), and this case exists because "+
			"QuestDB 8.2.3 returns ZERO ROWS (`pgx.ErrNoRows`) where Postgres would return one "+
			"row with NULL — if the semantics changed, the ErrNoRows branch in GetOldestTimestamp "+
			"needs to be reassessed, not kept out of habit", err, ts)
	}

	// THEN the contract the product promises over that semantics.
	store := &Store{pgPool: pool, tableName: "candles"}
	got, err := store.GetOldestTimestamp(context.Background(), naoExiste, "future", "NAO/EXISTE", "1w")
	if err != nil {
		t.Fatalf("GetOldestTimestamp returned an error for a scope WITHOUT a durable row: %v — absence "+
			"is the state the migration exists to repair, and treating it as an error takes down the whole "+
			"pass (measured in the LAN: `MRNA/USDT 1w` aborted 8 boots in a row)", err)
	}
	if !got.IsZero() {
		t.Errorf("GetOldestTimestamp = %v, wanted the ZERO time — that is what the function's comment "+
			"promises for absent data", got)
	}
}
