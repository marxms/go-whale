package questdb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// ─────────────────────────────────────────────────────────────────────────────
// THE TABLE CONTRACT — and why it is NOT the same problem as the column one
//
// `ensureColumns` proves convergence: probe, alter, re-probe. The proof holds because
// `ADD COLUMN` CONVERGES — a missing column can come to exist. `PARTITION BY` does not:
// it is IMMUTABLE after `CREATE`, and QuestDB has no `ALTER TABLE … PARTITION BY`.
//
// Add `CREATE TABLE IF NOT EXISTS`, which is SILENT SUCCESS over a table that already
// exists with the wrong shape, and `line.auto.create.new.tables=true` measured on the
// server — the first ILP line creates the table with whatever shape ILP infers, and there
// is no way back. The correct `CREATE` emitted afterwards returns a NIL error and changes
// nothing.
//
// That is why this file checks the RESULT and not the CALL: after `CREATE`, it re-reads
// `partitionBy`, `walEnabled` and `dedup` from `tables()` and compares them with what was
// declared. "The command was accepted" and "the table has the declared shape" are two
// facts, and only the second one matters.
//
// # THE TWO FAILURE CLASSES
//
// A read divergence is the SECOND class: the schema is wrong, and continuing means writing
// wrong data irreversibly. It is `ErrSchemaContract`, it reaches `Run` and refuses startup —
// it never becomes `qdbStore = nil`, because `nil` is exactly the state that makes
// `sizing_sheet.go` return `Evaluated` on the zero value and `gates.go` downgrade `GateFatal`
// to `GateError`. The graceful-degradation path is the disarm path.
//
// Being unable to READ `tables()` is the FIRST class — unavailability. It asserts nothing
// wrong about the table, only that it could not be inspected, and it is handled here:
// reported loudly and not propagated. `EnsureTables` returns, and only returns, contract
// divergence.
// ─────────────────────────────────────────────────────────────────────────────

// ErrSchemaContract marks table contract divergence — the class that is fatal to startup.
// `errors.Is` against it is what separates "the schema is wrong" from "the database did not
// answer", and it is the only question the caller needs to ask.
var ErrSchemaContract = errors.New("questdb: table contract diverged")

// SchemaContractError names ONE divergent field with the two values read: what was declared
// and what the live table answered. It is the shape the design requires — declare the
// divergence for a human decision instead of converging it.
type SchemaContractError struct {
	Table    string
	Field    string
	Declared string
	Read     string
}

func (e *SchemaContractError) Error() string {
	return fmt.Sprintf(
		"table %q: %s declared %q, read %q — %s does not converge through ALTER; the table "+
			"must be recreated, and that is a human decision because it destroys what the "+
			"table already holds",
		e.Table, e.Field, e.Declared, e.Read, e.Field)
}

// Is makes every `SchemaContractError` recognizable by the sentinel without forcing the
// caller to know the type.
func (e *SchemaContractError) Is(target error) bool { return target == ErrSchemaContract }

// tableFacts is what `tables()` answers about a table. The three fields are exactly the ones
// the DDL declares and none of them converges later: `partitionBy` is immutable, and
// `walEnabled` and `dedup` change only by recreation or by `ALTER … DEDUP`, which the
// sidecar already emits.
type tableFacts struct {
	PartitionBy string
	WAL         bool
	Dedup       bool
}

// tableContract is the shape a table MUST HAVE, together with the DDL that creates it that
// way.
type tableContract struct {
	name        string
	partitionBy string
	wal         bool
	dedup       bool
	ddl         string
}

// tableCatalog is the seam through which the mechanism talks to the database.
//
// `execDDL` is generic on purpose: it COULD carry an `ALTER`, and that is what makes the
// promise that none is emitted verifiable. A seam that only knew how to create a table
// would prove the absence of `ALTER` by construction, which is the same as not proving it.
type tableCatalog interface {
	execDDL(ctx context.Context, ddl string) error
	readTableFacts(ctx context.Context, table string) (tableFacts, bool, error)
	// readUpsertKeys returns the set of columns that compose the DEDUP key. It lives here,
	// and not next to `VerifyDedup`, because it is metadata of the SAME table read through
	// the same seam — and because without it the DEDUP verification would have no double.
	readUpsertKeys(ctx context.Context, table string) (map[string]bool, error)
}

// candlesTableContract mirrors `scripts/questdb/questdb-init.sql:9`. The table name comes from
// configuration and is validated by `validTableName` in the constructor, so the
// interpolation is safe — and validated again here, because a boundary is validated by what
// it is.
func candlesTableContract(table string) tableContract {
	return tableContract{
		name:        table,
		partitionBy: "DAY",
		wal:         true,
		dedup:       true,
		ddl: fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    timestamp TIMESTAMP,
    exchange SYMBOL,
    market_type SYMBOL,
    symbol SYMBOL,
    timeframe SYMBOL,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE,
    taker_buy_volume DOUBLE,
    taker_sell_volume DOUBLE
) timestamp(timestamp)
PARTITION BY DAY
WAL
DEDUP UPSERT KEYS(timestamp, exchange, market_type, symbol, timeframe)`, table),
	}
}

// derivativesTableContract mirrors `scripts/questdb/questdb-init.sql:115`.
//
// IT EXISTS BECAUSE THE SIDECAR MAY NOT RUN: under `--components 3`, the most used mode, it
// does not run, and the table is born from ILP with the shape ILP infers.
func derivativesTableContract() tableContract {
	return tableContract{
		name:        derivativesTableName,
		partitionBy: "DAY",
		wal:         true,
		dedup:       true,
		ddl: fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    timestamp TIMESTAMP,
    source SYMBOL,
    exchange SYMBOL,
    market_type SYMBOL,
    symbol SYMBOL,
    timeframe SYMBOL,
    open_interest_base DOUBLE,
    funding_rate DOUBLE,
    funding_rate_8h DOUBLE,
    long_short_ratio DOUBLE,
    denominated_in SYMBOL,
    label_convention SYMBOL
) timestamp(timestamp)
PARTITION BY DAY
WAL
DEDUP UPSERT KEYS(timestamp, source, exchange, market_type, symbol, timeframe)`, derivativesTableName),
	}
}

// tableContractsFor lists EVERY table this binary writes. Two, and the second one is the
// derivatives tier.
func tableContractsFor(candlesTable string) []tableContract {
	return []tableContract{
		candlesTableContract(candlesTable),
		derivativesTableContract(),
	}
}

// ensureTable emits the contract's `CREATE TABLE IF NOT EXISTS` and RE-READS what remains.
//
// It returns `ErrSchemaContract` when what was read diverges from what was declared, and a
// plain error when the database could not be read — the two classes, distinguishable through
// `errors.Is`.
func ensureTable(ctx context.Context, catalog tableCatalog, contract tableContract) error {
	if !validTableName.MatchString(contract.name) {
		return fmt.Errorf("questdb: invalid table name: %q", contract.name)
	}
	if err := catalog.execDDL(ctx, contract.ddl); err != nil {
		return fmt.Errorf("questdb: CREATE TABLE %s: %w", contract.name, err)
	}

	facts, presente, err := catalog.readTableFacts(ctx, contract.name)
	if err != nil {
		return fmt.Errorf("questdb: reading the contract of %s from tables(): %w", contract.name, err)
	}
	if !presente {
		// ABSENCE IS NOT APPROVAL. The `CREATE` was accepted and `tables()` does not know
		// the table: either the command did not do what it says, or we are looking at
		// another database. Both outcomes are contract, not unavailability.
		return &SchemaContractError{
			Table: contract.name, Field: "presence",
			Declared: "the table exists after CREATE",
			Read:     "tables() did not return a row",
		}
	}
	return compareTableContract(contract, facts)
}

// compareTableContract is the pure decision: three declared fields against three read
// fields.
//
// NO `ALTER` LEAVES HERE, and that is the property. The convergence that holds for a column
// does not transfer to a table: `PARTITION BY` is immutable, and a mechanism that "fixed"
// the divergence would be recreating the table — that is, erasing what it holds — without
// anyone having decided that.
func compareTableContract(contract tableContract, facts tableFacts) error {
	var divergencias []error
	if !strings.EqualFold(strings.TrimSpace(facts.PartitionBy), contract.partitionBy) {
		divergencias = append(divergencias, &SchemaContractError{
			Table: contract.name, Field: "partitionBy",
			Declared: contract.partitionBy, Read: facts.PartitionBy,
		})
	}
	if facts.WAL != contract.wal {
		divergencias = append(divergencias, &SchemaContractError{
			Table: contract.name, Field: "walEnabled",
			Declared: boolWord(contract.wal), Read: boolWord(facts.WAL),
		})
	}
	if facts.Dedup != contract.dedup {
		divergencias = append(divergencias, &SchemaContractError{
			Table: contract.name, Field: "dedup",
			Declared: boolWord(contract.dedup), Read: boolWord(facts.Dedup),
		})
	}
	return errors.Join(divergencias...)
}

func boolWord(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// pgTableCatalog talks to the real database.
type pgTableCatalog struct{ pool *pgxpool.Pool }

func (c pgTableCatalog) execDDL(ctx context.Context, ddl string) error {
	_, err := c.pool.Exec(ctx, ddl)
	return err
}

// readTableFacts reads the authoritative metadata. `pgx.ErrNoRows` becomes "not present",
// not an error: the distinction matters because the caller handles the two in opposite ways.
func (c pgTableCatalog) readTableFacts(ctx context.Context, table string) (tableFacts, bool, error) {
	var f tableFacts
	err := c.pool.QueryRow(ctx,
		"SELECT partitionBy, walEnabled, dedup FROM tables() WHERE table_name = $1", table,
	).Scan(&f.PartitionBy, &f.WAL, &f.Dedup)
	if errors.Is(err, pgx.ErrNoRows) {
		return tableFacts{}, false, nil
	}
	if err != nil {
		return tableFacts{}, false, err
	}
	return f, true, nil
}

// readUpsertKeys reads which columns compose the DEDUP key.
//
// The table name is interpolated because `table_columns()` does not accept a parameter
// there, and it is therefore validated here — a boundary is validated by what it is, not by
// what today's caller passes.
func (c pgTableCatalog) readUpsertKeys(ctx context.Context, table string) (map[string]bool, error) {
	if !validTableName.MatchString(table) {
		return nil, fmt.Errorf("questdb: invalid table name: %q", table)
	}
	rows, err := c.pool.Query(ctx, `SELECT "column", upsertKey FROM table_columns('`+table+`')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	keys := make(map[string]bool)
	for rows.Next() {
		var col string
		var isKey bool
		if err := rows.Scan(&col, &isKey); err != nil {
			return nil, err
		}
		if isKey {
			keys[col] = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return keys, nil
}

// EnsureTables guarantees the contract of EVERY table this binary writes, and returns — and
// only returns — contract divergence.
//
// Unavailability is HANDLED HERE: reported at the highest level that does not take the
// process down, and not propagated. It is the two-class separation applied at the boundary:
// whatever leaves `EnsureTables` is always the class that must refuse startup, so the caller
// needs to reclassify nothing.
func (q *Store) EnsureTables(ctx context.Context) error {
	catalog := q.catalogOf()
	var contratos []error
	for _, contract := range tableContractsFor(q.tableName) {
		err := ensureTable(ctx, catalog, contract)
		switch {
		case err == nil:
			continue
		case errors.Is(err, ErrSchemaContract):
			contratos = append(contratos, err)
		default:
			logger.OrDiscard(q.log).Error("questdb: table contract could not be verified; boot continues and the table may have the wrong shape",
				"table", contract.name, "error", err)
		}
	}
	return errors.Join(contratos...)
}

// catalogOf returns this store's seam. Nil in production, where it is the pool.
func (q *Store) catalogOf() tableCatalog {
	if q.catalog != nil {
		return q.catalog
	}
	return pgTableCatalog{pool: q.pgPool}
}
