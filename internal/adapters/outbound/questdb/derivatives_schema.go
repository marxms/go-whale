package questdb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// ─────────────────────────────────────────────────────────────────────────────
// THE `derivatives` TABLE SCHEMA
//
// `EnsureSchema` does not reach this table and could not: it is pinned to `q.tableName`,
// which is `"candles"`, and no `ALTER` in this module addresses another one. A new
// derivatives column added to that list would alter the wrong table — and would pass,
// because `ADD COLUMN` on a table that accepts the column complains about nothing.
//
// # WHY THIS CHECKS THE RESULT, AND NOT THE CALL
//
// The `ALTER` on that path is NON-FATAL by declared decision ("cold continues without it"),
// and the decision is right: the cold tier serves without the column. What cannot continue
// is the other half — declaring the column covered. A declared column that never lands
// reads as zero rows, and zero rows is exactly the defect this guard exists to prevent.
// Skipping the check would reproduce the defect.
//
// That is why the mechanism RE-PROBES after the `ALTER`: the command's nil error says it was
// accepted, not that the column exists. On a WAL table the step is asynchronous, and
// "accepted" and "applied" are two facts.
// ─────────────────────────────────────────────────────────────────────────────

// schemaColumn is an additive column and the type it is born with.
type schemaColumn struct {
	name string
	typ  string
}

// derivativesRequiredColumns are the columns the durable derivatives tier writes and the
// table may not have, because it was created before them.
//
// `n_step` is `SYMBOL` and not `DOUBLE`: it is a STATE NAME — which rung produced the
// interval —, and the column that carries it has cardinality four. See
// `domain.NDerivationStep`.
//
// # THE SIX COLUMNS, AND WHY SIX AND NOT SEVEN
//
// The series' hot member carries thirteen quantities and the table covered six. The missing
// ones live ONLY inside the ZSET member: when the cap bites, they disappear and come back
// from nowhere — measured on the live route, `openInterest`/`lsLongPct`/`lsShortPct` arrive
// in 800 of 2,000 points, and 800 is the ZSET cap digit for digit.
//
// `oi_change_pct` does NOT go in, and the exclusion is measured, not forgotten. The column
// exists for the quantity the hot tier carries **and that the read route serves**, and
// `oiChangePct` is neither on the series path: `dropPollToPollOIChange`
// (`storage/redis_derivatives.go:848`) zeroes it when writing the series (`:509`) and when
// reading it (`:674`), so it never reaches the member. It lives in the `latest` record, with
// a TTL. Writing it per bucket would stamp a poll delta onto a window it never measured —
// the fabrication this repository removed on purpose.
//
// ADDITIVE: an old row reads NULL. None of them enters the DEDUP key set, so re-seed
// idempotency stays where it was.
func derivativesRequiredColumns() []schemaColumn {
	return []schemaColumn{
		{name: "funding_interval_hours", typ: sqlTypeDouble},
		{name: "n_step", typ: "SYMBOL"},

		{name: "open_interest", typ: sqlTypeDouble},
		{name: "next_funding_rate", typ: sqlTypeDouble},
		{name: "interest_rate", typ: sqlTypeDouble},
		// BOOLEAN and not DOUBLE: `fundingAtAnchor` asserts that funding is pinned to the clamp's
		// dead band, not how much it is worth. A 0/1 in a floating-point column would invite
		// arithmetic over a predicate.
		{name: "funding_at_anchor", typ: "BOOLEAN"},
		{name: "ls_long_pct", typ: sqlTypeDouble},
		{name: "ls_short_pct", typ: sqlTypeDouble},
	}
}

// tableSchema is the seam through which the mechanism talks to the database.
//
// It exists for the same reason `newPacer` and `now` exist in the collectors: the property
// under test is WHICH TABLE is altered and WHETHER the column landed, and both are invisible
// behind a connection pool.
type tableSchema interface {
	columnPresent(ctx context.Context, table, column string) (bool, error)
	addColumn(ctx context.Context, table, column, typ string) error
}

// schemaSettle is the window in which a column may land AFTER the `ALTER` is accepted.
//
// # IT EXISTS BECAUSE THE LIVE ENGINE REFUTED THE IMMEDIATE SECOND PROBE
//
// Measured against `questdb/questdb:8.2.3`, the same production tag: on a WAL table,
// `ALTER TABLE ... ADD COLUMN` returns `{"ddl":"OK"}` and `table_columns()` answers ZERO for
// that column the next instant; two seconds later it answers one. The step is an
// asynchronous WAL-apply job, and "accepted" and "applied" are two facts — exactly what the
// comment at the top of this file already said, and what the IMMEDIATE second probe did not
// respect.
//
// The consequence was not theoretical: over pgwire, where the round trip is sub-millisecond,
// all EIGHT columns were reported as "the ALTER was accepted and the column is still absent"
// — an `schema INCOMPLETE` at startup that ADDS them, followed by a green next startup
// because by then the first probe already finds them. An alarm that fires once per new
// column, over a system that is working.
//
// THE ASSERTION DID NOT WEAKEN: the column still has to be present, and absence is still
// named. What changed is that the wait is declared instead of assumed to be zero.
type schemaSettle struct {
	// total is how long the column has to appear. Zero probes ONCE.
	total time.Duration
	// step is the interval between probes.
	step time.Duration
}

// derivativesSchemaSettle is the production window.
//
// Five seconds against the two seconds measured in the worst observed case — the margin
// exists because the measurement was on an empty, idle database, and production WAL-apply
// shares the queue with ingestion. It is the ONLY wait in schema startup, and it happens
// only when a column has just been added: on an already complete table no probe repeats.
func derivativesSchemaSettle() schemaSettle {
	return schemaSettle{total: 5 * time.Second, step: 50 * time.Millisecond}
}

// landed re-probes until the column appears or the window closes.
func (s schemaSettle) landed(ctx context.Context, schema tableSchema, table, column string) (bool, error) {
	prazo := time.Now().Add(s.total)
	for {
		present, err := schema.columnPresent(ctx, table, column)
		if err != nil || present {
			return present, err
		}
		if s.step <= 0 || !time.Now().Add(s.step).Before(prazo) {
			return false, nil
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(s.step):
		}
	}
}

// ensureColumns guarantees that `table` has every column in `cols`, and returns an error
// naming those still absent after the attempt.
//
// PROBE BEFORE ALTERING, from the lesson already paid for in this repository: `ADD COLUMN IF
// NOT EXISTS` is refused by QuestDB 8.2.3 on a WAL table even when the column already exists,
// and treating that as a failure produces an alarm that fires on every boot of a healthy
// system.
//
// AND PROBE AFTER, INSIDE A DECLARED WINDOW. Without the second probe, "the command returned
// no error" becomes "the column exists", an inference already proven wrong in three other
// places. Without the WINDOW, the second probe runs ahead of WAL-apply and fails an `ALTER`
// that lands milliseconds later — see `schemaSettle`.
func ensureColumns(ctx context.Context, schema tableSchema, table string, cols []schemaColumn, settle schemaSettle) error {
	var problems []error
	for _, col := range cols {
		present, err := schema.columnPresent(ctx, table, col.name)
		if err != nil {
			problems = append(problems, fmt.Errorf("%s.%s: could not verify: %w", table, col.name, err))
			continue
		}
		if present {
			continue
		}

		if err := schema.addColumn(ctx, table, col.name, col.typ); err != nil {
			problems = append(problems, fmt.Errorf("%s.%s: ADD COLUMN failed: %w", table, col.name, err))
			continue
		}

		landed, err := settle.landed(ctx, schema, table, col.name)
		if err != nil {
			problems = append(problems, fmt.Errorf("%s.%s: could not confirm after ALTER: %w",
				table, col.name, err))
			continue
		}
		if !landed {
			problems = append(problems, fmt.Errorf("%s.%s: the ALTER was accepted and the column is still absent",
				table, col.name))
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("questdb schema %s: %w", table, errors.Join(problems...))
	}
	return nil
}

// pgTableSchema talks to the real database.
type pgTableSchema struct{ pool *pgxpool.Pool }

// columnPresent reads QuestDB's authoritative metadata.
//
// The table name is interpolated because `table_columns()` does not accept a parameter there,
// so it is validated against the same pattern the store constructor uses. The COLUMN is
// passed as a parameter.
func (s pgTableSchema) columnPresent(ctx context.Context, table, column string) (bool, error) {
	if !validTableName.MatchString(table) {
		return false, fmt.Errorf("invalid table name: %q", table)
	}
	var n int64
	if err := s.pool.QueryRow(ctx,
		`SELECT count() FROM table_columns('`+table+`') WHERE "column" = $1`, column,
	).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

// addColumn adds a NON-KEY column, which is what makes it safe on a live table: it touches
// neither the DEDUP key set nor the idempotency that set sustains.
//
// Table, column and type are interpolated because DDL accepts no parameter. All three come
// from constants in this package, and they are validated anyway: a boundary is validated by
// what it is, not by what today's caller passes.
func (s pgTableSchema) addColumn(ctx context.Context, table, column, typ string) error {
	if !validTableName.MatchString(table) {
		return fmt.Errorf("invalid table name: %q", table)
	}
	if !validTableName.MatchString(column) {
		return fmt.Errorf("invalid column name: %q", column)
	}
	if !validColumnType.MatchString(typ) {
		return fmt.Errorf("invalid column type: %q", typ)
	}
	_, err := s.pool.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, typ))
	return err
}

// EnsureDerivativesSchema adds to the `derivatives` table the columns the durable tier
// writes, and returns an error naming the one that did not land.
//
// THE SCOPE IS THE TABLE, and that is what distinguishes it from `EnsureSchema`: that one
// talks about `q.tableName`, this one talks about `derivatives`, and the two are never the
// same.
func (q *Store) EnsureDerivativesSchema(ctx context.Context) error {
	return ensureColumns(ctx, q.schemaOf(), derivativesTableName, derivativesRequiredColumns(), derivativesSchemaSettle())
}

// schemaOf returns this store's schema seam. Nil in production, where it is the pool.
func (q *Store) schemaOf() tableSchema {
	if q.schema != nil {
		return q.schema
	}
	return pgTableSchema{pool: q.pgPool}
}

// ReportDerivativesSchema guarantees the durable schema and records the verdict.
//
// LEVEL Error on failure, not Warn like the candles path: there the absent column degrades
// the read to an older shape that still serves; here it is the unit of the number, and
// without the column the durable funding row does not say how much it costs. It is not fatal
// — the repository's doctrine reserves fatal for the comparison of two read values — but it
// is not routine either.
func (q *Store) ReportDerivativesSchema(ctx context.Context) {
	reportDerivativesSchema(q.log, q.EnsureDerivativesSchema(ctx))
}

func reportDerivativesSchema(log *slog.Logger, err error) {
	log = logger.OrDiscard(log)
	if err == nil {
		log.Info("questdb: derivatives schema complete",
			"declared_columns", len(derivativesRequiredColumns()))
		return
	}
	log.Error("questdb: derivatives schema incomplete", "error", err)
}
