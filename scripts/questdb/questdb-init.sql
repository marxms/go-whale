-- QuestDB DDL: candles table for cold storage.
-- Partition by DAY for retention management.
-- Dedup key on (timestamp, exchange, market_type, symbol, timeframe).
--
-- Usage:
--   curl -G --data-urlencode "query@scripts/questdb/questdb-init.sql" http://localhost:9000/exec
--   or via psql: psql -h localhost -p 8812 -U admin -d qdb -f scripts/questdb/questdb-init.sql

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

-- Enable dedup on existing tables. Idempotent and safe to re-run; it is required for
-- tables created before DEDUP was added to CREATE TABLE.
ALTER TABLE candles DEDUP ENABLE UPSERT KEYS(timestamp, exchange, market_type, symbol, timeframe);

-- ============================================================================
-- NO `ALTER TABLE ... ADD COLUMN` IN THIS FILE. EVER.
--
-- `ADD COLUMN IF NOT EXISTS` is not idempotent on a WAL table. Measured against
-- QuestDB 8.2.3 in the measurement deployment, with the same statement run twice:
--
--   first run (column absent):  {"ddl":"OK"}
--   second run (column present): ERROR "statements containing multiple transactions, such as
--                                'alter table add column col1, col2' are currently not
--                                supported for WAL tables [oldStructureVersion=N,
--                                newStructureVersion=N]"
--
-- The message mentions multiple columns and misleads; what it actually says is that
-- the statement produced no structural change (old == new) and the WAL path rejects it.
--
-- THE DAMAGE: the whole file used to go in a SINGLE /exec request (see the
-- integration compose, `--data-urlencode "query@.../init.sql"`), and /exec aborts on
-- the first failing statement. On a database where the table already exists — that
-- is, every live database — the two lines that used to live HERE turned the REST OF
-- THE FILE into dead code, including the index on `symbol` below, which is required,
-- not optional. That is exactly how the index went missing in production and had to
-- be applied by hand.
--
-- Adding a column is QuestDBStore.EnsureSchema's responsibility
-- (internal/adapters/outbound/questdb/store.go): it probes then ALTERs — it asks
-- whether the column exists and emits the ALTER only when it is missing. That is the
-- only applier that works in both cases, and it runs on every streamer boot. This
-- file handles only what EnsureSchema does not: creating the table from scratch and
-- the index.
-- ============================================================================

-- number_of_trades was REMOVED from the contract. The column carried TWO units —
-- aggregated event count on the live path and raw trade count on the REST path — and
-- no consumer read it: no indicator, no signal, no screen. It was removed instead of
-- fixed.
--
-- The column is NOT dropped by this script: the DROP is irreversible and depends on
-- confirming that no binary on any host still writes it. It stays as a DELIBERATE
-- operational step:
--
--   ALTER TABLE candles DROP COLUMN number_of_trades;
--
-- WHAT THIS COMMENT USED TO SAY WAS WRONG, and the correction matters because the
-- same premise appeared in two independent migration plans. It said: "the column
-- remains and simply stops being filled — which is safe: older reads see the
-- historical values". Measured on 2026-07-31 against the measurement deployment, on a
-- disposable table of the same shape (WAL + DEDUP UPSERT KEYS):
--
--   write  a=1.0, b=2.0  -> read (1.0, 2.0)
--   write  a=9.0         -> read (9.0, NULL)   <- the OMITTED column was ERASED
--
-- That is: stopping the emission of a column over ILP does not freeze it, it ERODES
-- it. Every row that is rewritten loses the value. And the streamer rewrites history
-- on EVERY boot (TieredStore.SeedCandlesBatch, depth buffer_sizes.default × symbols ×
-- timeframes), so the erosion reaches precisely the most-queried window.
--
-- Practical consequence: a retired column has no stable "dead but preserved" state.
-- Either the data is archived before the writes stop, or it is lost gradually and
-- silently. That is why the policy here is to DROP what is not used, instead of
-- letting a dead column accumulate in the table.

-- ============================================================================
-- derivatives TABLE — the durable tier for funding, open interest and long/short.
--
-- WHY IT EXISTS. The derivatives ZSET has no TTL: the CAP is the retention
-- mechanism, and the cap is a member count. A `1d` with 200 members covers 200 days,
-- and a `5m` with 200 members covers 16 hours — the horizon is a consequence of the
-- cap, not a decision. The user complaint is about a regime months ago, and no
-- memory-affordable cap serves that. Here the axis is time, not count.
--
-- WHY `source` IS IN THE DEDUP KEY. Without that term, Coinalyze and Binance collide
-- in the same row and the last write erases the other — exactly the last-writer-wins
-- that the ZSET already suffers and that this phase exists to avoid repeating in the
-- durable layer. With it, the coexistence of the two provenances is a SCHEMA fact,
-- not a convention someone has to remember.
--
-- WHY `denominated_in` AND `label_convention` ARE COLUMNS. Both are the unit and the
-- time anchor of the point. A series written without them is a number without a
-- unit: `oi_lq_vol_denominated_in` varies by market (measured: of Binance's 759
-- markets, 739 are BASE_ASSET and 20 are QUOTE_ASSET) and changed between 2024 and
-- 2025, and the two sources label opposite ends of the interval. Deriving them at
-- read time would infer today what was true at write time.
--
-- NO `ALTER TABLE ... ADD COLUMN` HERE — see the block above. A new column in this
-- table is EnsureSchema's responsibility, through the same probe-then-ALTER that
-- `candles` uses.
-- ============================================================================
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

-- Idempotent, for the same reason as the equivalent line on `candles`: a table created
-- before DEDUP entered CREATE does not gain the key on its own.
ALTER TABLE derivatives DEDUP ENABLE UPSERT KEYS(timestamp, source, exchange, market_type, symbol, timeframe);

-- ============================================================================
-- INDEX ON symbol — required, not an optional optimization.
--
-- LEFT LAST ON PURPOSE, and both original reasons FELL — the position stays because
-- it is still the safest, not because the justification holds.
--
-- It used to say here (a) that the whole file went in a SINGLE /exec request and
-- (b) that `ADD INDEX` was NOT idempotent, failing on re-run. Both were measured and
-- are false today: (a) the init now sends ONE REQUEST PER STATEMENT, because /exec
-- applies only the FIRST of a batch and returns 200 for the rest — sending the whole
-- file created `candles` and silently discarded everything else; (b) the 2026-08-23
-- measurement on 8.2.3 is recorded in the block below, and a repeated `ADD INDEX`
-- applies with OK.
--
-- Neither correction makes the position wrong. See the measurement block below.
--
-- WHY IT IS REQUIRED: without it the cold-read plan is `Filter` row by row on the
-- four SYMBOL columns. That is tolerable while the query reaches the requested LIMIT
-- and the scan stops early, and becomes pathological when it does NOT: a symbol with
-- fewer rows than the LIMIT (a recent listing, a tokenized equity) forces the scan of
-- the whole window just to prove there is nothing more.
--
-- Measured on a 10.8M-row test deployment, before and after this ALTER:
--   PANW/USDT 4h (3 rows)    3,859ms -> 254ms  (mean of 5 repetitions)
--   DIS/USDT  4h (5 rows)    3,976ms -> 264ms
--   BTC/USDT  4h (deep)        258ms -> 222ms
-- and EXPLAIN moves from `Filter` to `Index backward scan on: symbol`.
--
-- In production the same case reached 9,577ms — above the analyzer's 5s timeout — so
-- the indicator was never computed and the series froze, which appeared on screen as
-- a stale badge. A NEW database without this index reproduces the defect from day
-- one, which is why it entered here instead of remaining a manual step.
--
-- CAUTION when validating: the FIRST query after creation pays the index warm-up
-- (measured 7,656ms once, against a 254ms mean over the next five). Measuring once
-- leads to the wrong conclusion that the index made things worse.
--
-- MEASURED ON 2026-08-23 against QuestDB 8.2.3 of the measurement deployment, over
-- the PG protocol: the whole file applies with SIX `OK`s on an empty database, and
-- with the SAME six on an already-applied database. That is, `ALTER COLUMN … ADD
-- INDEX` IS idempotent in this version — the note above, which says the opposite,
-- does not reproduce. The third scenario, the one that actually exists today
-- (`candles` created and indexed, `derivatives` still absent), also applies the six
-- and ends with the new table created and indexed.
--
-- The order between the two indexes stays as it is anyway, and costs nothing: if a
-- future version again refuses a repeated `ADD INDEX`, /exec aborts on the first
-- failing statement, and the only state where the `derivatives` line would fail is
-- the one where it is already applied.
-- ============================================================================
ALTER TABLE derivatives ALTER COLUMN symbol ADD INDEX;
ALTER TABLE candles ALTER COLUMN symbol ADD INDEX;
