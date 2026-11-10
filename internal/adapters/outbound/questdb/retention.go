package questdb

import (
	"context"
	"fmt"
	"time"
)

// COLD TIER RETENTION — the database side.
//
// SEPARATE FROM THE DECISION on purpose: what needs testing is which cutoff, against which
// table and under which license — not the driver. Only SQL lives here, and it reuses the
// shapes this repository has already proven against QuestDB 8.2.3.
//
// Store is the executor: the three methods below are structural and satisfy the retention
// port declared by the consumer without coupling the adapter to it.

// TableExists uses the SAME shape the DEDUP verification already uses, not an invented one:
// `tables()` with a parameterized `table_name`.
//
// Checked against QuestDB 8.2.3 — it returns 1 for `derivatives` and 0 for a nonexistent
// name, with no error in either case. That is why the distinction retention needs does not
// have to parse an `ALTER` error message.
func (q *Store) TableExists(ctx context.Context, table string) (bool, error) {
	var n int64
	err := q.PgPool().QueryRow(ctx,
		"SELECT count() FROM tables() WHERE table_name = $1", table).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("reading tables() for %q: %w", table, err)
	}
	return n > 0, nil
}

// OldestTimestamp reads the minimum of the designated time column.
//
// The table name is INTERPOLATED because `FROM` accepts no parameter in QuestDB — the same
// reason `table_columns('`+table+`')` already carries in derivatives_schema.go. The tables
// that arrive here come from policies declared in code, never from external input.
func (q *Store) OldestTimestamp(ctx context.Context, table string) (time.Time, bool, error) {
	var oldest *time.Time
	err := q.PgPool().QueryRow(ctx,
		"SELECT min(timestamp) FROM "+table).Scan(&oldest)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("reading the oldest value of %q: %w", table, err)
	}
	if oldest == nil {
		return time.Time{}, false, nil
	}
	return oldest.UTC(), true, nil
}

// DropPartitionsBefore drops every partition older than the cutoff.
//
// It is only emitted after `OldestTimestamp` proves there is something to drop, so an error
// here is a REAL error — not the "QuestDB returns an error if no partition matches, that is
// normal" that buried everything at Info level.
func (q *Store) DropPartitionsBefore(ctx context.Context, table string, cutoff time.Time) error {
	query := fmt.Sprintf("ALTER TABLE %s DROP PARTITION WHERE timestamp < '%s'",
		table, cutoff.UTC().Format("2006-01-02"))
	if _, err := q.PgPool().Exec(ctx, query); err != nil {
		return fmt.Errorf("dropping partitions of %q older than %s: %w",
			table, cutoff.UTC().Format("2006-01-02"), err)
	}
	return nil
}

// DerivativesTableName exposes the durable tier table to whoever declares its retention.
//
// The constant stays private and single: a second string with the table name would be the
// divergence retention exists to avoid — a policy pointing at a name the database does not
// have comes out as `table_absent`, and nobody would think to look for the defect in a
// single letter.
func DerivativesTableName() string { return derivativesTableName }
