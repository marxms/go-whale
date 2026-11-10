# QuestDB schemas

QuestDB is the optional cold tier. It is written over HTTP ILP and read over the
PostgreSQL wire protocol by `internal/candle/engine`. The init file
`scripts/questdb/questdb-init.sql` creates the base tables; the binary verifies the
table contract at startup and adds additive columns.

The cold writer is single: one `QuestDBStore` owns the ILP sender, and its mutex
serializes candle and derivatives batches. A second writer would corrupt the
shared ILP buffer.

## Table contract

Both tables are created with:

- `PARTITION BY DAY`, which is immutable after `CREATE`;
- `WAL` enabled, required for `DEDUP`;
- `DEDUP UPSERT KEYS` that make re-seeds and repairs idempotent.

At startup the binary reads `tables()` and refuses to start when `partitionBy`,
`walEnabled` or `dedup` diverge from the contract. It does not emit an `ALTER` to
"fix" the partition: recreating the table would destroy its contents. A
divergence is a human decision.

`questdb.partition_by` and `questdb.wal_enabled` exist in the configuration
struct but do not override this contract; the contract is `DAY` + WAL. The
configured table name (`questdb.table`, default `candles`) is validated as a SQL
identifier.

Sources: `internal/candle/engine/questdb_table.go`
(`candlesTableContract`, `derivativesTableContract`, `EnsureTables`),
`scripts/questdb/questdb-init.sql`.

## `candles`

### Base DDL

```sql
CREATE TABLE IF NOT EXISTS candles (
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
DEDUP UPSERT KEYS(timestamp, exchange, market_type, symbol, timeframe);
```

The live table may have a different name when `questdb.table` is set. The DDL
above is the shape the contract requires, not a claim that the physical table is
named `candles`.

### Columns

| Column | Type | Written by this binary | Notes |
| --- | --- | --- | --- |
| `timestamp` | TIMESTAMP | yes | designated timestamp; candle bucket start |
| `exchange` | SYMBOL | yes | market identity |
| `market_type` | SYMBOL | yes | `spot` or `future` |
| `symbol` | SYMBOL | yes | `BASE/QUOTE`; indexed |
| `timeframe` | SYMBOL | yes | candle interval |
| `open`, `high`, `low`, `close`, `volume` | DOUBLE | yes | OHLCV |
| `taker_buy_volume` | DOUBLE | yes | taker buy volume |
| `taker_sell_volume` | DOUBLE | yes | taker sell volume; may be absent on a table created before the column existed |
| `number_of_trades` | legacy numeric | no | retired from the contract; may still physically exist on an old table and is not selected |

`EnsureSchema` probes and adds `taker_buy_volume` and `taker_sell_volume` when
they are absent. The read path also has a fallback: if a table lacks
`taker_sell_volume`, it selects the legacy column list instead of failing the
query.

### Write semantics

The candle writer always emits the taker pair. ILP has no `NULL`, so a candle
that has no CVD is written with `taker_buy_volume = 0`. On read this zero is
treated as absent by the CVD derivation, not as a measured zero; the raw stored
zero is not authoritative.

`sideUnknownCount` and `lateTradeCount` are telemetry on the live candle and are
**not** written to the cold tier. They read back as zero for historical rows.

Source: `internal/candle/engine/questdb_store.go` (`writeBatchesOnce`,
`EnsureSchema`, `coldSelectColumns`), `internal/candle/types.go`.

## `derivatives`

### Base DDL

```sql
CREATE TABLE IF NOT EXISTS derivatives (
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
DEDUP UPSERT KEYS(timestamp, source, exchange, market_type, symbol, timeframe);
```

The table name is fixed: `derivatives`.

### Additive columns

The columns below are not in the base DDL. `EnsureDerivativesSchema` probes
`table_columns()` and adds any that are missing; it re-probes within a declared
settle window because a WAL `ALTER` is applied asynchronously. They are
non-key columns, so they do not change the dedup key set.

| Column | Type | Source field |
| --- | --- | --- |
| `funding_interval_hours` | DOUBLE | `fundingIntervalHours` |
| `n_step` | SYMBOL | `nStep` (`measured`, `seed`, `inherited`, `unavailable`) |
| `open_interest` | DOUBLE | `openInterest` (notional, display only) |
| `next_funding_rate` | DOUBLE | `nextFundingRate` |
| `interest_rate` | DOUBLE | `interestRate` |
| `funding_at_anchor` | BOOLEAN | `fundingAtAnchor` (a predicate, not a number) |
| `ls_long_pct` | DOUBLE | `lsLongPct` |
| `ls_short_pct` | DOUBLE | `lsShortPct` |

### Full effective column list

| Column | Type | Presence | Notes |
| --- | --- | --- | --- |
| `timestamp` | TIMESTAMP | required | bucket-aligned instant, derived with the same rule as the Redis series |
| `source` | SYMBOL | required | `venue` or `coinalyze`; part of the dedup key |
| `exchange`, `market_type`, `symbol`, `timeframe` | SYMBOL | required | market identity |
| `denominated_in` | SYMBOL | required | `BASE_ASSET` or `QUOTE_ASSET`; the unit of `open_interest` |
| `label_convention` | SYMBOL | required | which end of the interval the feed timestamp names |
| `open_interest_base` | DOUBLE | optional | open interest in base units |
| `funding_rate` | DOUBLE | optional | raw funding |
| `funding_rate_8h` | DOUBLE | optional | normalized funding |
| `long_short_ratio` | DOUBLE | optional | long/short ratio |
| `funding_interval_hours` | DOUBLE | optional | settlement interval used |
| `n_step` | SYMBOL | optional | derivation rung; omitted when no funding is present |
| `open_interest` | DOUBLE | optional | notional |
| `next_funding_rate` | DOUBLE | optional | next predicted funding |
| `interest_rate` | DOUBLE | optional | contract interest rate |
| `funding_at_anchor` | BOOLEAN | optional | funding pinned at the clamp anchor |
| `ls_long_pct` | DOUBLE | optional | long share |
| `ls_short_pct` | DOUBLE | optional | short share |

`observedAt`, `oiChangePct`, `oiChangePctWindowMs` and `fieldSources` are **not**
cold columns. They exist only in the hot path (the `latest` key) or are
reconstructed at read time.

`source` is part of the dedup key so two provenances cannot overwrite each
other. `denominated_in` and `label_convention` are stored because a series read
later must not infer today's unit or label convention for an old point.

### Write semantics

- Absent is omitted, never zeroed. A missing `funding_rate` is not a funding rate
  of zero, and a missing `funding_at_anchor` is not `false`.
- A row with none of the base measurements (`open_interest_base`, `funding_rate`,
  `funding_rate_8h`, `long_short_ratio`) is not written. A row missing `source`,
  `denominated_in` or `label_convention` is refused as undeclared.
- Omitting a column on a DEDUP rewrite erases the value that was there. The
  writer has an omission guard that preserves prior columns, but the rule is
  part of the schema contract: do not treat an omitted optional column as
  "leave the old value alone" by default.

Source: `internal/candle/engine/derivatives_cold_store.go`
(`DerivativesColdPoint`, `writeDerivativesBatchOnce`, `coldPointOf`),
`internal/candle/engine/derivatives_schema.go`
(`derivativesRequiredColumns`, `EnsureDerivativesSchema`).

## Indexes

`scripts/questdb/questdb-init.sql` adds an index on `candles.symbol` and
`derivatives.symbol`. `EnsureSchema` also ensures the candle `symbol` index at
boot, because the init sidecar is optional and a missing index turns cold reads
into a full scan. The first query after index creation pays a one-time warm-up.

## Retention

- `candles`: `questdb.retention_days`, default `90` (the example file sets
  `365`). Retention drops whole partitions with
  `ALTER TABLE ... DROP PARTITION WHERE timestamp < '<date>'`.
- `derivatives`: `questdb.derivatives_retention_days`. When zero/undeclared, the
  window is derived from the durable seed horizon; it is not a literal default.
- Retention executes only after the safety and integrity gates; it never runs
  before them.

Sources: `internal/candle/engine/retention_questdb.go`,
`internal/app/retention_wiring.go`.
