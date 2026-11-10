# Redis schemas

Redis is the hot tier behind the API and the store for the latest values and
series. This document records the key families and member formats used by the
code. The adapter files are the authority; when this document and the code
disagree, fix the document.

Key building blocks are the exchange id, the canonical market type (`spot` or
`future`), the symbol (`BASE/QUOTE`) and the timeframe (`5m`, `1h`, `1d`, ...).
Components are joined with `:`; no component is escaped.

## Candle keys

### `candles:{exchange}:{marketType}:{symbol}:{timeframe}`

- Type: ZSET.
- Score: the candle timestamp in milliseconds since the Unix epoch. This is the
  bucket start.
- Member: the versioned positional encoding described below.
- TTL: none. Retention is the member cap.
- Cap resolution, highest priority first:
  `candle_engine.redis_cap_by_timeframe[tf]` →
  `candle_engine.redis_cap_by_timeframe["default"]` →
  `candle_engine.buffer_sizes[tf]` →
  `candle_engine.buffer_sizes["default"]` → the fallback passed to
  `NewRedisStore` (201 when that argument is zero).
- Writes remove the member at the same score before adding the merged member,
  then trim the lowest scores. A candle at the same score is one bucket, not a
  second member.

Source: `internal/adapters/outbound/redis/redis_store.go` (`candlesKeyV2`,
`candleCapFor`), `internal/adapters/outbound/redis/candle_member_codec.go`.

### Candle member, `v1` positional format

The current writer emits `v1` followed by 19 fields, joined by `|` (20 fields in
total). Field 1, `timestamp`, is the member identity inside the ZSET and is never
omitted. Fields are written with `strconv.FormatFloat(..., 'g', -1, 64)` or
`strconv.FormatInt`; an empty position means zero or absent for the fields that
allow it.

| Position | Field | Encoding | Absence rule |
| --- | --- | --- | --- |
| 0 | version | literal `v1` | always present |
| 1 | `timestamp` | int64, milliseconds, bucket start | always present |
| 2 | `open` | float | empty means zero |
| 3 | `high` | float | empty means zero |
| 4 | `low` | float | empty means zero |
| 5 | `close` | float | empty means zero |
| 6 | `volume` | float | empty means zero |
| 7 | `createdAt` | int64, milliseconds | empty means zero |
| 8 | `lastUpdate` | int64, milliseconds | empty means zero |
| 9 | `takerBuyVolume` | float | **always written**, including `0` |
| 10 | `takerSellVolume` | float | **always written**, including `0` |
| 11 | `source` | text | empty means absent; must not contain `|` |
| 12 | `aggregateMethod` | text | empty means absent; must not contain `|` |
| 13 | `fetchedAt` | int64, milliseconds | empty means zero |
| 14 | `requestedAt` | int64, milliseconds | empty means zero |
| 15 | `lastTradeAt` | int64, milliseconds | empty means zero |
| 16 | `klineEventAt` | int64, milliseconds | empty means zero |
| 17 | `sideUnknownCount` | int64 | empty means zero |
| 18 | `lateTradeCount` | int64 | empty means zero |
| 19 | `hasGap` | `1` for true, empty for false | reader also accepts `0` for false |

The taker pair intentionally has no absence form: zero is a measured market
state (all aggression on one side) and must remain distinguishable from an older
payload that predates the field. `Origin` is process-local and is never
persisted; a candle read back from Redis has `OriginUnknown`.

### Legacy candle member (read-only)

A member whose first byte is `{` is decoded as the old JSON form. The current
binary does not write it. Its fields are the JSON names of the old
`candleLegacyWire`: `timestamp`, `open`, `high`, `low`, `close`, `volume`,
`createdAt`, `lastUpdate`, `takerBuyVolume`, `takerSellVolume`, `source`,
`aggregateMethod`, `fetchedAt`, `requestedAt`, `lastTradeAt`, `klineEventAt`,
`sideUnknownCount`, `lateTradeCount`, `hasGap`. `timestamp`, OHLCV, `createdAt`,
`lastUpdate` and the taker pair are always present; the remaining optional names
are omitted when zero/empty. A key that has not been rewritten may contain this
form; consumers must tolerate it until the key rolls over.

An unknown version fails the read; it is never skipped or treated as a shorter
series.

## Last trade key

### `last_trade:{exchange}:{marketType}:{symbol}`

- Type: STRING.
- TTL: 1 hour, refreshed on every write.
- Value: JSON, produced from `trade.LastTrade`.

| JSON field | Type | Meaning |
| --- | --- | --- |
| `timestamp` | number | venue trade time, milliseconds |
| `price` | number | trade price |
| `volume` | number | trade amount (from `Trade.Amount`) |
| `side` | string | `buy`, `sell` or `unknown` |
| `tradeId` | string | venue trade id |
| `receivedAt` | number | process receive time, milliseconds |
| `latency` | number | `receivedAt - timestamp`, milliseconds |

All fields are emitted; there is no `omitempty` on this struct. A missing key
means no trade has been observed within the TTL.

Source: `internal/adapters/outbound/redis/redis_store.go` (`lastTradeKeyV2`,
`SetLastTrade`, `GetLastTrade`), `internal/trade/trade.go` (`LastTrade`).

## Derivatives keys

### `derivatives:snapshot:latest:{exchange}:{marketType}:{symbol}`

- Type: STRING.
- TTL: 900 seconds (15 minutes).
- Value: JSON of `derivatives.DerivativesSnapshot`.
- Write semantics: field-level merge against the previous value. A field that
  the incoming poll did not carry keeps its previous value. The TTL bounds the
  key only when writes stop entirely; while other fields keep arriving, a field
  that the feed stopped publishing can remain in the merged record beyond one
  TTL window.

| JSON field | Presence | Meaning |
| --- | --- | --- |
| `exchange`, `marketType`, `symbol` | always | market identity |
| `timestamp` | always | poll instant, milliseconds |
| `observedAt` | optional (`omitempty`) | observation instant when it differs from `timestamp` |
| `source` | optional (`omitempty`) | provenance: `venue` or `coinalyze` |
| `openInterestBase` | optional (`omitempty`) | open interest in base units; decision input |
| `openInterestObservedAtMs` | optional (`omitempty`) | instant of the open-interest reading |
| `openInterest` | optional (`omitempty`) | notional open interest; display only |
| `oiChangePct` | optional (`omitempty`) | legacy poll-to-poll change; **latest key only** |
| `oiChangePctWindowMs` | optional (`omitempty`) | measured span of that change |
| `fundingRate` | optional (`omitempty`) | raw per-interval funding |
| `nextFundingRate` | optional (`omitempty`) | next predicted funding |
| `fundingRate8h` | optional (`omitempty`) | 8-hour equivalent, only when the interval permits |
| `fundingIntervalHours` | optional (`omitempty`) | settlement interval actually used |
| `nStep` | optional (`omitempty`) | derivation rung: `measured`, `seed`, `inherited`, `unavailable` |
| `interestRate` | optional (`omitempty`) | contract interest rate |
| `fundingAtAnchor` | optional (`omitempty`) | funding is pinned at the clamp anchor |
| `longShortRatio` | optional (`omitempty`) | long/short ratio |
| `lsLongPct` | optional (`omitempty`) | long share |
| `lsShortPct` | optional (`omitempty`) | short share |
| `fieldSources` | optional (`omitempty`) | per-field provenance map, only on genuinely mixed points |

A missing field is absent, never zero. A zero that was measured is present as
`0`.

Source: `internal/adapters/outbound/redis/redis_derivatives.go`
(`saveSnapshotOnce`), `internal/derivatives/derivatives.go`
(`DerivativesSnapshot`).

### `derivatives:series:{exchange}:{marketType}:{symbol}:{timeframe}`

- Type: ZSET.
- Score: the bucket-aligned timestamp in milliseconds. Buckets are floored to
  the start of the interval; `1w` is aligned to Monday 00:00 UTC.
- TTL: none. Retention is the member cap.
- Cap: `derivatives.series_cap_by_timeframe[tf]` when declared, otherwise
  `derivatives.max_series_size`. A value below the timeframe's baseline window
  is raised to that floor.
- The member does not repeat `exchange`, `marketType` or `symbol`: they are in
  the key. `timestamp` remains in the member because it is the identity inside
  the ZSET.
- `oiChangePct` and `oiChangePctWindowMs` are stripped before writing the series
  (a poll-to-poll delta is not a per-bucket measurement). They remain on the
  `latest` key.

Member, `v1` positional format: 19 fields joined by `|`.

| Position | Field | Encoding | Absence rule |
| --- | --- | --- | --- |
| 0 | version | literal `v1` | always present |
| 1 | `timestamp` | int64, milliseconds, bucket-aligned | always present |
| 2 | `observedAt` | int64, milliseconds | empty means zero/absent |
| 3 | `source` | text | empty means absent; must not contain `|` |
| 4 | `openInterestBase` | float | empty means absent |
| 5 | `openInterestObservedAtMs` | int64 | empty means absent |
| 6 | `openInterest` | float | empty means absent |
| 7 | `oiChangePct` | float | always empty in a series written by this binary |
| 8 | `oiChangePctWindowMs` | int64 | always empty in a series written by this binary |
| 9 | `fundingRate` | float | empty means absent |
| 10 | `nextFundingRate` | float | empty means absent |
| 11 | `fundingRate8h` | float | empty means absent |
| 12 | `fundingIntervalHours` | float | empty means absent |
| 13 | `nStep` | text | empty means absent; must not contain `|` |
| 14 | `interestRate` | float | empty means absent |
| 15 | `fundingAtAnchor` | `1` true, `0` false | empty means absent |
| 16 | `longShortRatio` | float | empty means absent |
| 17 | `lsLongPct` | float | empty means absent |
| 18 | `lsShortPct` | float | empty means absent |

A member whose first byte is `{` is the legacy JSON form and is still readable.
An unknown version fails the read. A malformed member is skipped by readers that
can tolerate a short series (with a warning), so the missing observation is
visible rather than silently converted into data. Non-finite floats are written
as absence, never as `NaN`/`Inf` and never as zero.

Source: `internal/adapters/outbound/redis/derivatives_keys.go`
(`derivSeriesZsetKey`), `internal/adapters/outbound/redis/derivatives_series_codec.go`
(`encodeDerivativesSeriesMember`), `internal/adapters/outbound/redis/redis_derivatives.go`
(`upsertSeriesInto`, `upsertSeriesBatch`).

### `derivatives:baseline:{exchange}:{marketType}:{symbol}:{timeframe}:v1`

- Type: STRING.
- TTL: none.
- Value: JSON of `derivatives.DerivativesBaseline`.
- The version is a **suffix**, so positional parses of the key families keep the
  symbol in the same segment.

| JSON field | Presence | Meaning |
| --- | --- | --- |
| `source` | optional | provenance when the baseline was built from an alternative source |
| `seriesTail` | optional | recent points copied with an alternative-source baseline |
| `exchange`, `marketType`, `symbol`, `timeframe` | always | identity |
| `windowN` | always | declared window size |
| `nMin` | always | minimum sample size |
| `openInterest`, `openInterestState` | axis may be absent | robust ruler and state |
| `longShortRatio`, `longShortRatioState` | axis may be absent | robust ruler and state |
| `price`, `priceState` | axis may be absent | robust ruler and state |
| `updatedAt` | always | recompute instant, milliseconds |

Each `Baseline` object is `{median, mad, n, spanMs, updatedAt}`; `spanMs` and
`updatedAt` are omitted when zero. The state is `mature`, `immature` or
`degenerate`; a nil axis is not a zeroed ruler. The key is written even when the
sample is too short, so "not enough history" is distinguishable from "not
computed yet".

Source: `internal/adapters/outbound/redis/derivatives_baseline.go`,
`internal/derivatives/baseline.go`, `internal/derivatives/robuststat.go`.

### Coinalyze families (cleanup only)

The current binary does not write these families. They are retained because a
long-lived database can still contain them and every cleanup sweep must cover
them:

- `derivatives:coinalyze:snapshot:latest:{exchange}:{marketType}:{symbol}`
- `derivatives:coinalyze:series:{exchange}:{marketType}:{symbol}:{timeframe}`
- `derivatives:coinalyze:baseline:{exchange}:{marketType}:{symbol}:{timeframe}:v1`

The series family was write-only; the deep history now comes from the QuestDB
tier. Do not add a reader that assumes these keys are fresh.

Source: `internal/adapters/outbound/redis/derivatives_keys.go`.

## Metadata keys

These keys support the catalog and are not time series:

| Key | Type | Contents |
| --- | --- | --- |
| `active_symbols:{exchange}:{marketType}` | SET | symbols with observed data |
| `timeframes:{exchange}:{marketType}:{symbol}` | SET | timeframes observed for that symbol |
| `active_symbol_scopes` | SET | `{exchange}:{marketType}` scopes, used to avoid a full keyspace `SCAN` |

They have no TTL in this code; removal is explicit (`RemoveActiveSymbol`,
`ClearActiveSymbols`).

Source: `internal/adapters/outbound/redis/redis_store.go` (`activeSymbolsKeyV2`,
`timeframesKeyV2`, `scopeRegistryKey`).

## TTL and retention summary

| Family | TTL | Retention mechanism |
| --- | --- | --- |
| `candles:*` | none | per-timeframe ZSET member cap |
| `last_trade:*` | 1 hour | TTL |
| `derivatives:snapshot:latest:*` | 900 seconds | TTL |
| `derivatives:series:*` | none | per-timeframe ZSET member cap |
| `derivatives:baseline:*` | none | rewritten in place |
| metadata sets | none | explicit removal |
