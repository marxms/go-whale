// Package questdb implements the cold-tier candle and derivatives stores on QuestDB.
package questdb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"
	logger "github.com/marxms/go-whale/internal/platform/logging"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	questdb "github.com/questdb/go-questdb-client/v3"
)

// Durable-tier schema names. The DDL, writes and reads share them, so they are declared
// once instead of repeated as literals across the package.
const (
	sqlTypeDouble         = "DOUBLE"
	columnTimestamp       = "timestamp"
	columnExchange        = "exchange"
	columnMarketType      = "market_type"
	columnSymbol          = "symbol"
	columnTimeframe       = "timeframe"
	columnOpen            = "open"
	columnHigh            = "high"
	columnLow             = "low"
	columnClose           = "close"
	columnVolume          = "volume"
	columnTakerBuyVolume  = "taker_buy_volume"
	columnTakerSellVolume = "taker_sell_volume"
)

// Compile-time check: Store satisfies candle.ColdStore.
var (
	_ candle.ColdStore             = (*Store)(nil)
	_ candle.ColdStoreApplyBarrier = (*Store)(nil)
)

// validColumnType restricts DDL type names to the QuestDB spellings this module uses; the type
// is interpolated into DDL.
var validColumnType = regexp.MustCompile(`^[A-Z]{3,16}$`)

// validTableName restricts table names to safe SQL identifiers.
var validTableName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,126}$`)

// Store implements candle.ColdStore using QuestDB.
// Write path: ILP (InfluxDB Line Protocol) via go-questdb-client.
// Read path:  PgWire (PostgreSQL-compatible) via pgx.
type Store struct {
	log        *slog.Logger
	ilpSender  questdb.LineSender
	ilpConfStr string // stored for reconnection
	pgPool     *pgxpool.Pool
	// schema is the seam through which schema guarantees talk to the database. Nil in
	// production, where it is the pool itself; a test swaps it to observe WHICH table is
	// altered. See tableSchema.
	schema tableSchema
	// catalog is the TABLE CONTRACT seam — partition, WAL and dedup. Nil in production,
	// where it is the pool itself. See tableCatalog.
	catalog tableCatalog
	// walPosition is a read seam for the visibility barrier; production uses pgPool.
	walPosition func(context.Context) (coldWALPosition, error)
	// rawSeedRead is a test seam for the physical-row no-op check. Production uses pgPool.
	rawSeedRead func(context.Context, candle.SeriesKey, time.Time, time.Time, int) ([]string, [][]any, error)
	// writeBatch is a test seam for the coordinated DAY state machine.
	writeBatch func(context.Context, map[candle.SeriesKey][]candle.Candle) error
	// priors is the source of truth about what a durable row already carries. Nil in
	// production, where it is the store itself. See derivativesPriorSource.
	priors    derivativesPriorSource
	tableName string
	// metrics is the injected observability port. Zero value = safe no-op.
	metrics Metrics

	// ilpMu serializes the sender, which is not concurrency-safe: two writers on the same store
	// race in the shared buffer and can panic the process. It is held across the entire batch
	// build and flush.
	ilpMu sync.Mutex
	// Shared between boot migration and the online pipeline. The permit is cancellable;
	// pendingPlan retains the existing sweep and cursor after its caller returns.
	candleDayOnce   sync.Once
	candleDayPermit chan struct{}
	pendingPlan     *coldDayPlan

	mu        sync.RWMutex
	lastFlush time.Time

	rowsWritten atomic.Int64
	errCount    atomic.Int64
	reconnCount atomic.Int64

	// takerSellAbsent caches whether the cold table lacks taker_sell_volume. Default false is
	// optimistic; EnsureSchema pre-warms it and QueryRange self-heals it.
	takerSellAbsent atomic.Bool

	ctx    context.Context
	cancel context.CancelFunc
}

// SetMetrics injects the observability port. The composition root calls it after construction.
func (q *Store) SetMetrics(m Metrics) { q.metrics = m }

// candleILPConfig keeps one explicit transmission per DAY. SDK retries after a
// lost ACK would replay before the pipeline can fence, so retry ownership is here.
func candleILPConfig(address string) string {
	return fmt.Sprintf("http::addr=%s;auto_flush=off;retry_timeout=0;", address)
}

// NewStore creates a Store connected to a QuestDB instance.
// ILP sender handles writes, pgx pool handles reads.
func NewStore(cfg config.QuestDBConfig, logs ...*slog.Logger) (*Store, error) {
	log := logger.OrDiscard(logs...)
	if !validTableName.MatchString(cfg.Table) {
		return nil, fmt.Errorf("questdb: invalid table name %q", cfg.Table)
	}

	ctx, cancel := context.WithCancel(context.Background())

	confStr := candleILPConfig(cfg.ILPAddress) // HTTP transport for error feedback
	sender, err := questdb.LineSenderFromConf(ctx, confStr)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("questdb ilp sender: %w", err)
	}

	poolSize := cfg.PGPoolSize
	if poolSize <= 0 {
		poolSize = 4
	}
	if cfg.PGPassword == nil {
		cancel()
		return nil, fmt.Errorf("questdb: PGPassword is nil — config validation bypassed?")
	}
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?pool_max_conns=%d&sslmode=disable",
		cfg.PGUser, *cfg.PGPassword, cfg.PGAddress, cfg.PGDatabase, poolSize)

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		_ = sender.Close(ctx)
		cancel()
		return nil, fmt.Errorf("questdb pg pool: %w", err)
	}

	log.Info("questdb connected via HTTP",
		"ilp_address", cfg.ILPAddress,
		"pg_address", cfg.PGAddress,
		"table", cfg.Table,
		"pool_size", poolSize)

	q := &Store{
		log:        log,
		ilpSender:  sender,
		ilpConfStr: confStr,
		pgPool:     pool,
		tableName:  cfg.Table,
		ctx:        ctx,
		cancel:     cancel,
		// Seeded at construction so cold_last_flush_age_seconds measures "since this store
		// was able to write", not "since the epoch". A zero time would make the gauge open at
		// ~1.8 billion seconds and fire every alert on it during a perfectly normal boot,
		// which is how a new signal earns the mute it never comes back from.
		lastFlush: time.Now(),
	}
	// The table contract comes before everything, and before VerifyDedup: PARTITION BY is
	// immutable, and with auto-created tables the first ILP line would create the table before
	// the correct CREATE could. Contract divergence is a constructor error, because a divergent
	// schema writes wrong data irreversibly.
	func() {
		ensureCtx, ensureCancel := context.WithTimeout(ctx, ensureTablesBudget)
		defer ensureCancel()
		err = q.EnsureTables(ensureCtx)
	}()
	if err != nil {
		_ = sender.Close(ctx)
		pool.Close()
		cancel()
		return nil, fmt.Errorf("questdb: schema contract refused at startup: %w", err)
	}

	// Publish for the scrape-time age gauge. Last store wins; the process builds at most one.
	return q, nil
}

// ensureTablesBudget is the ceiling of the contract check at startup. It emits two
// `CREATE TABLE IF NOT EXISTS` and two reads of `tables()` — metadata work, not data — and a
// QuestDB recovering its WAL takes a while to answer the first command.
const ensureTablesBudget = 15 * time.Second

// isConnectionError reports whether err is a transport failure. Everything downstream
// (reconnect, retry) is gated on it, so a phrasing it fails to recognize disables the whole
// recovery path. The list is phrasing-based because the SDK wraps transport errors in plain
// strings. Row-level rejections (bad column, missing table) are deliberately excluded so one
// unwritable row cannot abort the sweep forever. See
// docs/decisions/0033-cold-write-transport-failures.md.
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "connection reset") ||
		strings.Contains(s, "use of closed") ||
		strings.Contains(s, "connection refused") ||
		strings.Contains(s, "i/o timeout") ||
		// ILP-over-HTTP phrasings.
		strings.Contains(s, "context deadline exceeded") ||
		strings.Contains(s, "retry timeout reached") ||
		strings.Contains(s, "no such host") ||
		strings.Contains(s, "EOF")
}

// reconnectILP closes the stale ILP sender and creates a fresh TCP connection.
// Caller must hold no locks — this method acquires mu internally. ctx bounds the
// sender close and is the caller's write context, so a canceled write never waits
// for a detached teardown.
func (q *Store) reconnectILP(ctx context.Context) error {
	// The sender first: swapping the field while another path writes to it is the same race.
	q.ilpMu.Lock()
	defer q.ilpMu.Unlock()
	return q.reconnectILPLocked(ctx)
}

// reconnectILPLocked also discards an unsent prefix before another user of the
// shared sender can append derivatives and flush it. Caller holds ilpMu.
func (q *Store) reconnectILPLocked(ctx context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// auto_flush=off: Close discards unsent rows rather than transmitting them.
	_ = q.ilpSender.Close(ctx)

	// The sender outlives this write: WithoutCancel keeps the caller's values while detaching
	// the cancellation that belongs to one batch.
	sender, err := questdb.LineSenderFromConf(context.WithoutCancel(ctx), q.ilpConfStr)
	if err != nil {
		return fmt.Errorf("questdb reconnect: %w", err)
	}

	q.ilpSender = sender
	q.reconnCount.Add(1)
	logger.OrDiscard(q.log).Info("questdb ILP/HTTP reconnected", "total_reconnects", q.reconnCount.Load())
	return nil
}

// WriteBatches submits the DAY assembled by the pipeline. The sender is serialized
// with derivatives, but no ILP mutex is held while the pipeline awaits WAL apply.
// A sent request whose ACK is lost is fenced rather than replayed after a revision.
func (q *Store) WriteBatches(ctx context.Context, groups map[candle.SeriesKey][]candle.Candle) error {
	if len(groups) == 0 {
		return nil
	}
	if q.writeBatch != nil {
		return q.writeBatch(ctx, groups)
	}
	return q.writeBatchesOnce(ctx, groups)
}

// ilpRow is one row flattened out of the per-key groups so the whole sweep can be sorted by
// timestamp before it is handed to ILP.
type ilpRow struct {
	key    candle.SeriesKey
	candle candle.Candle
}

// flattenSorted preserves timestamp order within a submitted DAY. This avoids
// a needless timestamp zig-zag; it cannot turn historical O3 data into an append.
// Stable equal timestamps preserve the original-close then revision order.
func flattenSorted(groups map[candle.SeriesKey][]candle.Candle) []ilpRow {
	total := 0
	for _, batch := range groups {
		total += len(batch)
	}
	rows := make([]ilpRow, 0, total)
	for key, batch := range groups {
		for _, c := range batch {
			rows = append(rows, ilpRow{key: key, candle: c})
		}
	}
	// Stable, and that is not stylistic: one sweep can carry a bucket's original close and its
	// correction at the same key and timestamp, and the table dedups so the row written last
	// wins. sort.Slice leaves equal elements unordered, discarding the correction about half
	// the time. Enqueue order is already correct, so preserving it is enough.
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].candle.Timestamp < rows[j].candle.Timestamp })
	return rows
}

// writeBatchesOnce adds every candle of the sweep to the ILP buffer in timestamp order and
// flushes once. ILP has no NULL: taker_buy_volume is written as 0 when CVD is unavailable, and
// readers treat it as absent, not as a genuine zero.
func (q *Store) writeBatchesOnce(ctx context.Context, groups map[candle.SeriesKey][]candle.Candle) error {
	// The lock covers build and flush: the sender's buffer accumulates rows between calls.
	q.ilpMu.Lock()
	defer q.ilpMu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	var rowErrors int
	rows := flattenSorted(groups)
	total := len(rows)
	for _, row := range rows {
		key, c := row.key, row.candle
		exchange, marketType := key.Scope.Exchange.String(), key.Scope.MarketType.String()
		symbol, timeframe := key.Scope.Symbol.String(), key.Timeframe.String()
		{
			ts := time.UnixMilli(c.Timestamp)
			err := q.ilpSender.
				Table(q.tableName).
				Symbol(columnExchange, exchange).
				Symbol(columnMarketType, marketType).
				Symbol(columnSymbol, symbol).
				Symbol(columnTimeframe, timeframe).
				Float64Column(columnOpen, c.Open).
				Float64Column(columnHigh, c.High).
				Float64Column(columnLow, c.Low).
				Float64Column(columnClose, c.Close).
				Float64Column(columnVolume, c.Volume).
				Float64Column(columnTakerBuyVolume, c.TakerBuyVolume).
				Float64Column(columnTakerSellVolume, c.TakerSellVolume).
				At(ctx, ts)
			if err != nil {
				q.errCount.Add(1)
				// A transport failure is not a property of this row: skipping rows one at a
				// time silently discards the whole sweep. auto_flush=off leaves the prefix
				// unsent, so abort and replace the sender.
				if isConnectionError(err) {
					if resetErr := q.reconnectILPLocked(ctx); resetErr != nil {
						return fmt.Errorf("questdb unsent row ts=%d: %w; sender reset: %w", c.Timestamp, err, resetErr)
					}
					return fmt.Errorf("questdb unsent row ts=%d: %w", c.Timestamp, err)
				}
				// A row-level rejection is a property of this row: keep it a counted, logged
				// skip so one bad candle cannot wedge the pipeline.
				rowErrors++
				logger.OrDiscard(q.log).Warn("questdb: skipping ILP row", columnTimestamp, c.Timestamp, "error", err)
				continue
			}
		}
	}

	// Cancellation while serializing is still pre-send. Discard the buffered
	// prefix under ilpMu before derivatives can share the sender again.
	if err := ctx.Err(); err != nil {
		if resetErr := q.reconnectILPLocked(ctx); resetErr != nil {
			return fmt.Errorf("questdb cancelled unsent batch: %w; sender reset: %w", err, resetErr) //nolint:misspell // error string is caller-observable; do not rewrite it
		}
		return err
	}
	// auto_flush=off makes this the only transmission point for this DAY.
	// An HTTP ACK acknowledges ingestion, not persisted WAL application.
	if err := q.ilpSender.Flush(ctx); err != nil {
		q.errCount.Add(1)
		return &candle.ColdWriteUncertainError{Err: fmt.Errorf("questdb flush: %w", err)}
	}

	written := int64(total - rowErrors)
	if written > 0 {
		q.rowsWritten.Add(written)
	}
	q.mu.Lock()
	q.lastFlush = time.Now()
	q.mu.Unlock()

	return nil
}

// requiredDedupKeys are the UPSERT keys the cold store depends on for idempotent
// re-writes (seed / backfill MUST overwrite, not duplicate). Mirrors questdb-init.sql.
func requiredDedupKeys() []string {
	return []string{columnTimestamp, columnExchange, columnMarketType, columnSymbol, columnTimeframe}
}

// derivativesDedupKeys are the UPSERT keys of the derivatives table. They differ from candles:
// source is in the key because two provenances write the same bucket of the same symbol.
func derivativesDedupKeys() []string {
	return []string{columnTimestamp, "source", columnExchange, columnMarketType, columnSymbol, columnTimeframe}
}

// dedupContract is the UPSERT key ONE table must have.
type dedupContract struct {
	table string
	keys  []string
}

// dedupContracts lists every table this binary writes. A table without DEDUP does not refuse
// writes: it duplicates, and the duplicate only shows up later in what the series sums.
func (q *Store) dedupContracts() []dedupContract {
	return []dedupContract{
		{table: q.tableName, keys: requiredDedupKeys()},
		{table: derivativesTableName, keys: derivativesDedupKeys()},
	}
}

// VerifyDedup asserts that EVERY table this binary writes has WAL + DEDUP on exactly the
// required UPSERT keys. The whole cold-durability fix depends on this idempotency: a table
// ILP-auto-created without WAL (the bootstrap race) silently disables DEDUP and would
// DUPLICATE every re-seeded bucket.
func (q *Store) VerifyDedup(ctx context.Context) error {
	catalog := q.catalogOf()
	var problems []error
	for _, contract := range q.dedupContracts() {
		if err := verifyTableDedup(ctx, q.log, catalog, contract); err != nil {
			problems = append(problems, err)
		}
	}
	return errors.Join(problems...)
}

// verifyTableDedup checks one table.
//
// Absence of a row is not approval: under the most used component set the sidecar does not
// run, so ILP would create the table with an inferred shape and no DEDUP. Unreadable metadata
// remains fail-open, because a renamed column says nothing about the table; the contract class
// disables the cold tier instead of refusing startup, which EnsureTables already did.
func verifyTableDedup(ctx context.Context, log *slog.Logger, catalog tableCatalog, contract dedupContract) error {
	log = logger.OrDiscard(log)
	facts, presente, err := catalog.readTableFacts(ctx, contract.table)
	if err != nil {
		log.Warn("questdb DEDUP verify: tables() metadata unreadable (non-fatal)",
			"table", contract.table, "error", err)
		return nil
	}
	if !presente {
		return fmt.Errorf("questdb table %q is ABSENT from tables() — the DEDUP check found NO ROW, "+
			"and a table that does not exist yet will be ILP-auto-created without WAL, which silently "+
			"disables DEDUP and DUPLICATES every re-seeded bucket", contract.table)
	}

	upsertKeys, keysErr := catalog.readUpsertKeys(ctx, contract.table)
	if keysErr != nil {
		// Keys unverifiable, but wal+dedup were read above — enforce those, skip keys.
		log.Warn("questdb DEDUP verify: table_columns() unreadable (non-fatal)",
			"table", contract.table, "error", keysErr)
		return resolveDedupUnverified(log, contract.table,
			validateDedupMeta(contract.table, facts.WAL, facts.Dedup, nil, nil, false))
	}

	return validateDedupMeta(contract.table, facts.WAL, facts.Dedup, upsertKeys, contract.keys, true)
}

// resolveDedupUnverified downgrades the unverified-keys sentinel to a non-fatal nil after a
// loud warning, while still propagating a definitive WAL/DEDUP error.
func resolveDedupUnverified(log *slog.Logger, table string, err error) error {
	log = logger.OrDiscard(log)
	if errors.Is(err, errDedupKeysUnverified) {
		log.Warn("questdb DEDUP verify: upsert keys unverified; relying on the init sidecar",
			"table", table)
		return nil
	}
	return err
}

// errDedupKeysUnverified marks an outcome where WAL and DEDUP are on but the key set could not
// be read, distinct from "verified, all keys present".
var errDedupKeysUnverified = errors.New("questdb DEDUP upsert keys could not be verified (table_columns unreadable)")

// validateDedupMeta is the pure decision: WAL on, DEDUP on, and the key set covers every
// required key. When keysVerifiable is false the per-key check is skipped and the distinct
// sentinel is returned, so the caller can tell "verified" from "could not verify".
func validateDedupMeta(table string, wal, dedup bool, upsertKeys map[string]bool, required []string, keysVerifiable bool) error {
	if !wal {
		return fmt.Errorf("questdb table %q has WAL disabled — DEDUP requires WAL; the table was likely ILP-auto-created before the init sidecar ran", table)
	}
	if !dedup {
		return fmt.Errorf("questdb table %q has DEDUP disabled — re-seeds would DUPLICATE; run the init sidecar (ALTER TABLE %s DEDUP ENABLE)", table, table)
	}
	if !keysVerifiable {
		return errDedupKeysUnverified
	}
	var missing []string
	for _, k := range required {
		if !upsertKeys[k] {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("questdb table %q missing required DEDUP upsert keys %v", table, missing)
	}
	return nil
}

// requiredColumns are non-key columns that may be absent on an older table. Adding them does
// not touch the DEDUP key set or WAL.
func requiredColumns() []struct {
	name string
	typ  string
} {
	return []struct {
		name string
		typ  string
	}{
		{name: columnTakerBuyVolume, typ: sqlTypeDouble},
		{name: columnTakerSellVolume, typ: sqlTypeDouble},
	}
}

// columnExists reports whether the cold table currently has the named column, via the
// authoritative QuestDB table_columns() metadata. The table name is validated at
// construction (validTableName), so the interpolation is injection-safe.
func (q *Store) columnExists(ctx context.Context, col string) (bool, error) {
	var n int64
	if err := q.pgPool.QueryRow(ctx,
		`SELECT count() FROM table_columns('`+q.tableName+`') WHERE "column" = $1`, col,
	).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

// EnsureSchema adds missing additive columns to the live cold table and caches whether
// taker_sell_volume landed, so QueryRange can select the compatible shape. It is fail-open for
// cold availability but returns a real error for the caller to log; the cached flag keeps reads
// working without the column.
func (q *Store) EnsureSchema(ctx context.Context) error {
	var alterErrs []error
	for _, col := range requiredColumns() {
		// Probe first, then ALTER only if genuinely absent: QuestDB 8.2.3 rejects ADD COLUMN
		// IF NOT EXISTS on a WAL table even when the column exists, which would log a false
		// failure on every healthy boot.
		present, err := q.columnExists(ctx, col.name)
		if err != nil {
			logger.OrDiscard(q.log).Warn("questdb EnsureSchema: could not verify column", "column", col.name, "error", err)
			alterErrs = append(alterErrs, fmt.Errorf("verify %s: %w", col.name, err))
			continue
		}
		if present {
			continue
		}

		stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", q.tableName, col.name, col.typ)
		if _, err := q.pgPool.Exec(ctx, stmt); err != nil {
			// Non-fatal: the cold tier still works without the column; the derived/absolute
			// field for that column is simply void until the column lands.
			logger.OrDiscard(q.log).Warn("questdb EnsureSchema: ADD COLUMN failed (non-fatal)",
				"column", col.name, "error", err)
			alterErrs = append(alterErrs, fmt.Errorf("ADD COLUMN %s: %w", col.name, err))
		}
	}

	// Authoritatively cache whether taker_sell_volume is present so cold reads pick the right
	// SELECT shape from the first read instead of relying on the lazy self-heal on the hot
	// path.
	present, probeErr := q.columnExists(ctx, columnTakerSellVolume)
	if probeErr != nil {
		// Could not verify. Leave the flag at its optimistic default (present) and let the
		// read-path self-heal handle a genuine absence; surface the probe failure so the
		// caller can log it rather than silently assuming success.
		logger.OrDiscard(q.log).Warn("questdb EnsureSchema: could not verify taker_sell_volume presence",
			"error", probeErr)
		alterErrs = append(alterErrs, fmt.Errorf("verify taker_sell_volume: %w", probeErr))
	} else {
		q.takerSellAbsent.Store(!present)
		if !present {
			logger.OrDiscard(q.log).Warn("questdb EnsureSchema: taker_sell_volume still absent after ALTER")
			alterErrs = append(alterErrs, fmt.Errorf("taker_sell_volume column absent after ALTER"))
		}
	}

	if err := q.ensureSymbolIndex(ctx); err != nil {
		alterErrs = append(alterErrs, err)
	}

	if len(alterErrs) > 0 {
		return fmt.Errorf("questdb EnsureSchema: %w", errors.Join(alterErrs...))
	}
	return nil
}

// ensureSymbolIndex guarantees the symbol column carries an index. The init sidecar declares it
// too, but it is a one-shot whose failure leaves no log, so EnsureSchema holds the invariant on
// every boot.
//
// Without the index the cold read plan is a row-by-row filter over four symbol columns, which
// becomes pathological for a symbol with fewer rows than the limit: measured, PANW/USDT 4h took
// 3.9s against 254ms indexed. A failure joins alterErrs instead of being logged and forgotten.
//
// On a WAL table the ALTER is accepted synchronously and applied asynchronously, so the verify
// is separate and tolerates a not-yet-visible index; only the ALTER itself is an error.
func (q *Store) ensureSymbolIndex(ctx context.Context) error {
	indexed, err := q.symbolIndexed(ctx)
	if err != nil {
		logger.OrDiscard(q.log).Warn("questdb EnsureSchema: could not verify the symbol index", "error", err)
		return fmt.Errorf("verify symbol index: %w", err)
	}
	if indexed {
		return nil
	}

	logger.OrDiscard(q.log).Warn("questdb EnsureSchema: symbol column is not indexed; adding the index")

	stmt := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN symbol ADD INDEX", q.tableName)
	if _, err := q.pgPool.Exec(ctx, stmt); err != nil {
		logger.OrDiscard(q.log).Error("questdb EnsureSchema: ADD INDEX on symbol failed", "error", err)
		return fmt.Errorf("ADD INDEX symbol: %w", err)
	}

	// Applied through the WAL, so indexed may still read false here; a persistent false is what
	// the next boot catches.
	if nowIndexed, verifyErr := q.symbolIndexed(ctx); verifyErr == nil && nowIndexed {
		logger.OrDiscard(q.log).Warn("questdb EnsureSchema: symbol index created and already visible")
	} else {
		logger.OrDiscard(q.log).Warn("questdb EnsureSchema: symbol index accepted; the WAL applies it asynchronously")
	}
	return nil
}

// symbolIndexed reports whether symbol carries an index. SHOW COLUMNS is authoritative; the
// ALTER's return value is not, because a WAL table applies after acknowledging.
func (q *Store) symbolIndexed(ctx context.Context) (bool, error) {
	rows, err := q.pgPool.Query(ctx, fmt.Sprintf("SHOW COLUMNS FROM %s", q.tableName))
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		values, scanErr := rows.Values()
		if scanErr != nil {
			return false, scanErr
		}
		if len(values) < 3 {
			continue
		}
		name, _ := values[0].(string)
		if name != columnSymbol {
			continue
		}
		switch v := values[2].(type) {
		case bool:
			return v, nil
		case string:
			return strings.EqualFold(v, "true"), nil
		}
		return false, nil
	}
	return false, rows.Err()
}

// QueryRange reads candles within [from, to], newest first. WHERE parameters are bound and the
// table name is validated at construction. It never hard-requires taker_sell_volume: an
// optimistic SELECT that hits a missing column flips the cached flag and retries with the
// compatible shape instead of returning an API 500.
func (q *Store) QueryRange(ctx context.Context, exchange, marketType, symbol, timeframe string, from, to time.Time, limit int) ([]candle.Candle, error) {
	if limit <= 0 {
		limit = 1000
	}

	hasTakerSell := !q.takerSellAbsent.Load()
	result, err := q.queryRangeOnce(ctx, hasTakerSell, exchange, marketType, symbol, timeframe, from, to, limit)
	if shouldFallbackToLegacy(hasTakerSell, err) {
		// The optimistic SELECT hit a table without the column. Cache it so future reads
		// skip the column entirely, and retry THIS read with the compatible shape.
		if q.takerSellAbsent.CompareAndSwap(false, true) {
			logger.OrDiscard(q.log).Warn("questdb cold read: taker_sell_volume column absent; falling back to the compatible read and caching",
				"error", err)
		}
		return q.queryRangeOnce(ctx, false, exchange, marketType, symbol, timeframe, from, to, limit)
	}
	return result, err
}

// shouldFallbackToLegacy falls back only when the optimistic read failed specifically because
// taker_sell_volume is absent; any other error propagates unchanged.
func shouldFallbackToLegacy(hasTakerSell bool, err error) bool {
	return err != nil && hasTakerSell && isMissingColumnErr(err, columnTakerSellVolume)
}

// queryRangeOnce runs one cold read with the column shape selected by hasTakerSell.
// timestampLiteral renders t as a QuestDB timestamp literal in UTC and microsecond precision.
// Concatenation is safe by construction: the input is a time.Time and the layout is fixed.
func timestampLiteral(t time.Time) string {
	return "'" + t.UTC().Format("2006-01-02T15:04:05.000000Z") + "'"
}

// coldRangeQuery builds the cold read. The timestamp interval is a literal while symbol
// predicates stay bound parameters.
//
// QuestDB prunes partitions only when it can resolve the designated-timestamp predicate while
// planning; with bound timestamps it scans every partition row by row, at the CPU ceiling.
// With the interval as a literal the plan becomes an interval scan. The symbol columns are
// deliberately not interpolated: they are a linear filter either way, and interpolating them
// would turn exchange-supplied strings into SQL text.
func coldRangeQuery(table string, hasTakerSell bool, from, to time.Time) string {
	return "SELECT " + coldSelectColumns(hasTakerSell) + " FROM " + table +
		" WHERE exchange = $1 AND market_type = $2 AND symbol = $3 AND timeframe = $4" +
		" AND timestamp >= " + timestampLiteral(from) +
		" AND timestamp <= " + timestampLiteral(to) +
		" ORDER BY timestamp DESC" +
		" LIMIT $5"
}

// completenessQuery builds the per-scope completeness scan, with the lower bound as a
// literal so QuestDB prunes partitions. Same rationale as coldRangeQuery.
func completenessQuery(table string, from time.Time) string {
	return "SELECT symbol, MIN(timestamp), MAX(timestamp), COUNT(*) FROM " + table +
		" WHERE exchange = $1 AND market_type = $2 AND timeframe = $3" +
		" AND timestamp >= " + timestampLiteral(from) +
		" GROUP BY symbol"
}

func (q *Store) queryRangeOnce(ctx context.Context, hasTakerSell bool, exchange, marketType, symbol, timeframe string, from, to time.Time, limit int) ([]candle.Candle, error) {
	query := coldRangeQuery(q.tableName, hasTakerSell, from, to)

	rows, err := q.pgPool.Query(ctx, query, exchange, marketType, symbol, timeframe, limit)
	if err != nil {
		q.errCount.Add(1)
		return nil, fmt.Errorf("questdb query: %w", err)
	}
	defer rows.Close()

	return scanCandlesCompat(rows, hasTakerSell, limit)
}

// GetOldestTimestamp returns the earliest candle timestamp for the given key.
// Returns zero time if no data exists.
func (q *Store) GetOldestTimestamp(ctx context.Context, exchange, marketType, symbol, timeframe string) (time.Time, error) {
	query := "SELECT MIN(timestamp) FROM " + q.tableName +
		" WHERE exchange = $1 AND market_type = $2 AND symbol = $3 AND timeframe = $4"

	var ts *time.Time
	err := q.pgPool.QueryRow(ctx, query, exchange, marketType, symbol, timeframe).Scan(&ts)
	// Absence of a durable row is a state, not an error. A MIN() matching nothing returns one
	// NULL row on Postgres but ZERO rows on QuestDB, so Scan gets pgx.ErrNoRows. Treating that
	// as an error aborted the sole-copy migration on symbols never written to the durable tier.
	// The case pinning this is live, under QDB_LIVE_PG.
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, nil
	}
	if err != nil {
		q.errCount.Add(1)
		return time.Time{}, fmt.Errorf("questdb oldest ts: %w", err)
	}
	if ts == nil {
		return time.Time{}, nil
	}
	return *ts, nil
}

// OldestTimestampsByScope returns MIN(timestamp) per symbol in one grouped query, because the
// sole-copy guard needs the boundary for every symbol of 1d and 1w without ~1,000 round trips.
// A symbol with no rows does not appear: that absence means its whole hot series is a sole
// copy, and a zero time would make it indistinguishable from a 1970 row.
func (q *Store) OldestTimestampsByScope(ctx context.Context, exchange, marketType, timeframe string) (map[string]time.Time, error) {
	query := "SELECT symbol, MIN(timestamp) FROM " + q.tableName +
		" WHERE exchange = $1 AND market_type = $2 AND timeframe = $3 GROUP BY symbol"

	rows, err := q.pgPool.Query(ctx, query, exchange, marketType, timeframe)
	if err != nil {
		q.errCount.Add(1)
		return nil, fmt.Errorf("questdb oldest ts by scope: %w", err)
	}
	defer rows.Close()

	out := make(map[string]time.Time)
	for rows.Next() {
		var symbol string
		var ts *time.Time
		if err := rows.Scan(&symbol, &ts); err != nil {
			q.errCount.Add(1)
			return nil, fmt.Errorf("questdb oldest ts by scope: scan: %w", err)
		}
		if ts != nil {
			out[symbol] = *ts
		}
	}
	if err := rows.Err(); err != nil {
		q.errCount.Add(1)
		return nil, fmt.Errorf("questdb oldest ts by scope: %w", err)
	}
	return out, nil
}

// ScanScopeCompleteness measures cold-tier completeness for one scope in a single grouped
// query: missing = expected buckets between each symbol's MIN and MAX minus rows present, so a
// dropped bottom partition never reports a phantom gap. It also reports how many symbols
// answered, since a symbol answering with nothing is invisible to an interior-gap measurement.
func (q *Store) ScanScopeCompleteness(ctx context.Context, exchange, marketType, timeframe string) (candle.ScopeCompleteness, error) {
	var out candle.ScopeCompleteness

	tfMs, ok := candle.ParseTimeframeMs(timeframe)
	if !ok {
		// Strict: an unrecognized timeframe must NOT silently fall back to 1h (which
		// would report phantom gaps for coarser frames). Fail loud for this scope.
		return out, fmt.Errorf("unrecognized timeframe %q", timeframe)
	}

	// Bounded to a recent window: the unbounded form scanned every partition and never
	// converged, since a deranked symbol kept reporting a permanent hole. The lower bound is a
	// literal for the same partition-pruning reason as the cold read; this scan runs on a timer.
	from := time.Now().Add(-candle.CompletenessLookbackFor(timeframe))

	query := completenessQuery(q.tableName, from)

	rows, err := q.pgPool.Query(ctx, query, exchange, marketType, timeframe)
	if err != nil {
		q.errCount.Add(1)
		return candle.ScopeCompleteness{}, fmt.Errorf("questdb completeness scan: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var sym string
		var minT, maxT *time.Time
		var count int64
		if err := rows.Scan(&sym, &minT, &maxT, &count); err != nil {
			return candle.ScopeCompleteness{}, fmt.Errorf("questdb completeness scan row: %w", err)
		}
		if minT == nil || maxT == nil {
			// No bounds means no measurable window, so the symbol is not counted as present.
			continue
		}
		out.SymbolsPresent++
		missing := candle.ExpectedBuckets(minT.UnixMilli(), maxT.UnixMilli(), tfMs) - count
		if missing > 0 {
			out.MissingBuckets += missing
			out.SymbolsWithGaps++
		}
	}
	if err := rows.Err(); err != nil {
		return candle.ScopeCompleteness{}, fmt.Errorf("questdb completeness rows: %w", err)
	}
	return out, nil
}

// Close flushes the ILP sender and closes the pgx pool.
func (q *Store) Close() error {
	// The sender close runs before q.cancel(): it is the final flush and owns the
	// store-lifetime context, so canceling first would make the last write race it.
	var firstErr error
	if err := q.ilpSender.Close(q.ctx); err != nil {
		firstErr = fmt.Errorf("questdb ilp close: %w", err)
	}
	q.cancel()
	q.pgPool.Close()

	logger.OrDiscard(q.log).Info("questdb closed",
		"rows_written", q.rowsWritten.Load(), "errors", q.errCount.Load())
	return firstErr
}

// Stats returns goroutine-safe operational metrics.
func (q *Store) Stats() candle.ColdStoreStats {
	q.mu.RLock()
	lastFlush := q.lastFlush
	q.mu.RUnlock()

	return candle.ColdStoreStats{
		Connected:     q.pgPool.Stat().TotalConns() > 0,
		RowsWritten:   q.rowsWritten.Load(),
		LastFlushTime: lastFlush,
		PendingWrites: 0,
		Errors:        q.errCount.Load(),
		Reconnects:    q.reconnCount.Load(),
	}
}

// PgPool returns the underlying pgx connection pool for administrative queries.
func (q *Store) PgPool() *pgxpool.Pool {
	return q.pgPool
}

// coldSelectColumns returns the read column list. When includeTakerSell is false the
// compatible list omits only taker_sell_volume, so the read succeeds and cumulative maps to
// void instead of returning a 500.
//
// number_of_trades is not selected: it has no consumer and the write path does not produce it,
// and not selecting a column is always valid.
func coldSelectColumns(includeTakerSell bool) string {
	if includeTakerSell {
		return "timestamp, open, high, low, close, volume, taker_buy_volume, taker_sell_volume"
	}
	return "timestamp, open, high, low, close, volume, taker_buy_volume"
}

// isMissingColumnErr reports whether err is a missing-column rejection for the named column,
// across QuestDB pgwire phrasings. The read uses it as a safety net when the boot probe was
// wrong.
func isMissingColumnErr(err error, col string) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	if !strings.Contains(s, col) {
		return false
	}
	return strings.Contains(s, "does not exist") ||
		strings.Contains(s, "Invalid column") ||
		strings.Contains(s, "Unknown column")
}

// buildColdCandle maps one cold row to a candle.Candle. Nullable columns scan as nil on rows
// written before they existed. Ordering is load-bearing: the persisted absolute cumulative is
// applied after DeriveCVD, which never sets it.
func buildColdCandle(tsMs int64, open, high, low, closePrice, volume float64, takerBuy *float64, takerSell *float64) candle.Candle {
	c := candle.Candle{
		Timestamp: tsMs,
		Open:      open,
		High:      high,
		Low:       low,
		Close:     closePrice,
		Volume:    volume,
	}
	if takerBuy != nil {
		c.TakerBuyVolume = *takerBuy
	}

	// NULL versus present is an era discriminator: a non-NULL taker_sell_volume means both
	// sides were measured and are taken verbatim, while NULL predates the column and the
	// missing side is reconstructed by DeriveTakerSell. The shim declines when taker-buy is
	// zero, keeping an OHLCV-only row flow-void.
	if takerSell != nil {
		c.TakerSellVolume = *takerSell
	} else {
		candle.DeriveTakerSell(&c)
	}
	return c
}

// scanCandlesCompat iterates pgx rows and converts timestamps to milliseconds. hasTakerSell
// selects the row shape; the two SELECT lists differ only by the trailing taker_sell_volume
// column.
func scanCandlesCompat(rows pgx.Rows, hasTakerSell bool, capacityHint int) ([]candle.Candle, error) {
	var result []candle.Candle
	for rows.Next() {
		var (
			ts                                  time.Time
			open, high, low, closePrice, volume float64
			// Nullable: rows written before the CVD columns exist scan as NULL.
			takerBuy *float64
			// Nullable: rows written before taker_sell_volume exists scan as NULL.
			cvdCum *float64
			err    error
		)
		if hasTakerSell {
			err = rows.Scan(&ts, &open, &high, &low, &closePrice, &volume, &takerBuy, &cvdCum)
		} else {
			// Legacy shape: the column is absent from the SELECT, so cvdCum stays nil →
			// buildColdCandle leaves CvdCumulative void (0, unset).
			err = rows.Scan(&ts, &open, &high, &low, &closePrice, &volume, &takerBuy)
		}
		if err != nil {
			return nil, fmt.Errorf("questdb scan: %w", err)
		}
		if result == nil {
			// Avoid repeated growth for warm reads, without reserving an unbounded
			// caller limit or allocating for empty results. This never limits rows.
			result = make([]candle.Candle, 0, min(capacityHint, 1024))
		}
		result = append(result, buildColdCandle(ts.UnixMilli(), open, high, low, closePrice, volume, takerBuy, cvdCum))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("questdb rows: %w", err)
	}
	return result, nil
}

// coldWALPosition comes from one metadata row; no candle scan or moving target.
type coldWALPosition struct {
	writer    int64
	sequencer int64
	suspended bool
}

func (q *Store) readColdWALPosition(ctx context.Context) (coldWALPosition, error) {
	var position coldWALPosition
	var err error
	switch {
	case q.walPosition != nil:
		position, err = q.walPosition(ctx)
	case q.pgPool == nil:
		return position, fmt.Errorf("questdb WAL barrier: no metadata connection")
	default:
		err = q.pgPool.QueryRow(ctx, "SELECT writerTxn, sequencerTxn, suspended FROM wal_tables() WHERE name = $1", q.tableName).Scan(&position.writer, &position.sequencer, &position.suspended)
	}
	if err != nil {
		return position, fmt.Errorf("questdb WAL metadata: %w", err)
	}
	if position.writer < 0 || position.sequencer < 0 || position.writer > position.sequencer {
		return position, fmt.Errorf("questdb WAL metadata invalid: writer=%d sequencer=%d", position.writer, position.sequencer)
	}
	if position.suspended {
		return position, fmt.Errorf("questdb WAL table %s suspended", q.tableName)
	}
	return position, nil
}

// CaptureApplyTarget returns the WAL sequencer position to wait for.
func (q *Store) CaptureApplyTarget(ctx context.Context) (int64, error) {
	position, err := q.readColdWALPosition(ctx)
	if err != nil {
		return 0, err
	}
	return position.sequencer, nil
}

// WaitApplied blocks until the WAL writer has applied target or the context ends.
func (q *Store) WaitApplied(ctx context.Context, target int64) error {
	if target < 0 {
		return fmt.Errorf("questdb WAL target must be nonnegative")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		position, err := q.readColdWALPosition(ctx)
		if err != nil {
			return err
		}
		if position.writer >= target {
			return nil
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// applyColdPlan retains the whole bounded predecessor sweep across callers.
// Retaining only its first ACKed DAY would let a later, older online DAY move MIN
// past the migration's unsent middle and hide it from the next reconstruction.
// No ILP mutex is held while this cancellable admission guard waits for WAL.
func (q *Store) applyColdPlan(ctx context.Context, plan *coldDayPlan) (string, error) {
	return q.applyColdPlanLimit(ctx, plan, false)
}

func (q *Store) applyColdPlanStep(ctx context.Context, plan *coldDayPlan) (string, error) {
	return q.applyColdPlanLimit(ctx, plan, true)
}

func (q *Store) applyColdPlanLimit(ctx context.Context, plan *coldDayPlan, oneDay bool) (string, error) {
	release, err := q.acquireCandleDayPermit(ctx)
	if err != nil {
		return "COORDINATOR", err
	}
	defer release()
	return q.applyColdPlanLimitHeld(ctx, plan, oneDay)
}

// acquireCandleDayPermit serializes a cold comparison with its first DAY write.
// The caller must release it after that write has reached the apply barrier.
func (q *Store) acquireCandleDayPermit(ctx context.Context) (func(), error) {
	q.candleDayOnce.Do(func() { q.candleDayPermit = make(chan struct{}, 1) })
	select {
	case q.candleDayPermit <- struct{}{}:
		return func() { <-q.candleDayPermit }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// applyColdPlanLimitHeld is the same state machine with the permit already owned.
func (q *Store) applyColdPlanLimitHeld(ctx context.Context, plan *coldDayPlan, oneDay bool) (string, error) {
	if q.pendingPlan != nil && q.pendingPlan != plan {
		// A pipeline retry wraps the same stable DAY pointer in a fresh one-DAY plan.
		sameDay := len(plan.days) == 1 && q.pendingPlan.current() == plan.current()
		phase, err := applyColdPlanUncoordinatedLimit(ctx, q, q.pendingPlan, oneDay)
		if err != nil {
			if sameDay {
				return phase, err
			}
			return "PREVIOUS_" + phase, fmt.Errorf("questdb previous candle sweep remains fenced: %w", err)
		}
		if q.pendingPlan.current() == nil {
			q.pendingPlan = nil
		}
		if sameDay {
			plan.days[0] = nil
			plan.next = 1
			return "", nil
		}
		if oneDay {
			return "", nil
		}
	}
	q.pendingPlan = plan
	phase, err := applyColdPlanUncoordinatedLimit(ctx, q, plan, oneDay)
	current := plan.current()
	// Before any transmission, keep the fallback. Once a prefix was confirmed, preserve its
	// tail even for a later pre-send error: MIN has already moved.
	if current == nil || (err != nil && plan.next == 0 && current != nil && !current.acknowledged && !current.uncertain) {
		q.pendingPlan = nil
	}
	return phase, err
}
