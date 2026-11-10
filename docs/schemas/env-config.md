# Configuration and environment contract

This document records the configuration contract implemented by
`internal/platform/config`. The loader is strict: there is one YAML shape, no
`version` field, no v1/v2 parser and no `STRICT_CONFIG` mode.

## Selection and precedence

1. `--config <path>` on `gowhale` / `gowhale serve`.
2. `GOWHALE_CONFIG_FILE`.
3. Environment-only mode when neither is set.

A selected file is authoritative. A read, parse or validation error is returned;
the loader never falls back to environment values. `KnownFields(true)` rejects
unknown YAML keys.

`gowhale doctor` reads a file with `-config`; it reports what it read instead of
exiting on the first config error.

```bash
gowhale serve --config /etc/gowhale/config.yaml
gowhale doctor -config /etc/gowhale/config.yaml
```

## YAML sections

`config.example.yaml` is the complete example. The loader accepts these
top-level sections and no others.

| Section | Purpose |
| --- | --- |
| `server` | HTTP port and log level |
| `redis` | hot-tier connection, pool and timeouts |
| `stream` | WebSocket buffer and connection limits |
| `exchanges` | enabled venues, symbols, discovery and REST limits |
| `derivatives` | polling cadence, series caps and bootstrap depth |
| `candle_engine` | in-memory ring, timeframes, write-through and snapshot cadence |
| `questdb` | cold-tier addresses, table, retention and WAL switch |
| `retention` | budget ratios and the measured Redis container ceiling |

### `server`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `port` | string | `"3001"` | HTTP port |
| `log_level` | string | `"info"` | `debug`, `info`, `warn` or `error` |
| `log_format` | string | `"text"` | `text` or `json` |

### `redis`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `addr` | string | `"localhost:6379"` | `host:port` |
| `password` | string | empty | password |
| `db` | int | `0` | database index |
| `pool_size` | int | `256` | maximum open connections |
| `min_idle_conns` | int | `32` | warm idle connections |
| `dial_timeout_s` | int | `5` | dial timeout |
| `read_timeout_s` | int | `3` | per-command read timeout |
| `write_timeout_s` | int | `3` | per-command write timeout |
| `pool_timeout_s` | int | `4` | wait for a pool slot |
| `max_retries` | int | `2` | client-level retries |

### `stream`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `max_symbols_per_instance` | int | `50` | symbols per connection |
| `max_connections_per_ip` | int | `60` | connections per IP |
| `trade_buffer_size` | int | `5000` | per-symbol trade ring capacity |

### `exchanges[]`

| Key | Type | Meaning |
| --- | --- | --- |
| `id` | string | venue id (`binance`, `bingx`, ...) |
| `enabled` | bool | enable this feed |
| `market_type` | string | `spot` or the futures vocabulary |
| `symbols` | list of strings | explicit symbol list |
| `discovery.enabled` | bool | periodic discovery |
| `discovery.max_assets` | int | discovery ceiling |
| `discovery.refresh_interval` | string | discovery cadence, e.g. `6h` |
| `limits.max_symbols_per_instance` | int | per-venue override |
| `limits.max_connections_per_ip` | int | per-venue override |

There is no default exchange list. Environment-only mode starts with an empty
list.

### `derivatives`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `enabled` | bool | `false` | enable collection |
| `poll_interval_sec` | int | `300` | poll cadence |
| `max_series_size` | int | `800` | ZSET cap where no per-timeframe cap is declared |
| `series_cap_by_timeframe` | map | empty | per-timeframe ZSET cap |
| `bootstrap_limit` | int | derived | boot fetch depth; derived from the deepest declared cap when zero |
| `bootstrap_concurrency` | int | `10` | parallel bootstrap workers |

A per-timeframe cap below that timeframe's baseline window is raised to the
baseline floor by the store.

### `candle_engine`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `enabled` | bool | `false` | enable in-process aggregation |
| `timeframes` | list | `["5m","15m","30m","1h","2h","4h","1d","1w"]` | supported intervals |
| `buffer_sizes` | map | `{"default": 800}` | in-process ring depth and seed depth |
| `redis_write_through` | bool | `false` | write candle updates to Redis |
| `snapshot_interval_ms` | int | `5000` | snapshot cadence |
| `bingx_min_interval_ms` | int | `300` | minimum BingX update interval |
| `redis_cap_by_timeframe` | map | empty | resident candle ZSET cap per timeframe |

The resident cap resolution order is documented in
[redis.md](redis.md#candlesexchange-marketsymbol-timeframe).

### `questdb`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `enabled` | bool | `false` | enable the cold tier |
| `ilp_address` | string | none | HTTP ILP endpoint; required when enabled |
| `pg_address` | string | none | PostgreSQL-wire endpoint; required when enabled |
| `pg_user` | string | none | PostgreSQL user |
| `pg_password` | string | none | secret; environment is the published path |
| `pg_database` | string | none | database name used in the connection string |
| `pg_pool_size` | int | `5` | PostgreSQL pool size |
| `retention_days` | int | `90` | candle partition retention |
| `derivatives_retention_days` | int | `0` (derived) | derivatives partition retention |
| `table` | string | `"candles"` | candle table name |
| `partition_by` | string | `"DAY"` | accepted, but the table contract is fixed at `DAY` |
| `wal_enabled` | bool | `false` | accepted, but the table contract requires WAL |

The YAML type accepts `pg_password`, but the published contract keeps secrets in
the environment. When both are present, `GOWHALE_QUESTDB_PG_PASSWORD` wins.

### `retention`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `budget_warn_ratio` | float | `0` (undeclared) | projected occupancy warning |
| `budget_error_ratio` | float | `0` (undeclared) | projected occupancy error line |
| `occupancy_alert_ratio` | float | `0` (undeclared) | measured occupancy alert |
| `measured_startup_warn_ratio` | float | `0` (undeclared) | measured startup warning |
| `budget_sample_interval_seconds` | int | `0` (undeclared) | observer cadence |
| `redis_container_memory_limit_mib` | int | `0` (undeclared) | measured Redis container ceiling |

These are policy ratios, not retention depths. Zero means "not declared"; the
sheet reports the absence instead of inventing a value.

## Environment variables

"Both paths" means the variable is applied after either load path. "YAML
override" means it overrides the value read from a YAML file and also works in
environment-only mode. "Env-only" means it is read only when no file is
selected.

### Process and server

| Variable | Applies | Meaning |
| --- | --- | --- |
| `GOWHALE_CONFIG_FILE` | selection | YAML path when `--config` is absent |
| `GOWHALE_SERVER_PORT` | YAML override | HTTP port |
| `GOWHALE_LOG_LEVEL` | YAML override | `debug`, `info`, `warn`, `error` |
| `GOWHALE_LOG_FORMAT` | YAML override | `text` or `json`; invalid values fail validation |
| `GOWHALE_WEBSOCKET_MAX_ASSETS_PER_CONNECTION` | env-only | symbols per connection |
| `GOWHALE_WEBSOCKET_MAX_CONNECTIONS_PER_IP` | env-only | connections per IP |

The discovery path is `exchanges[].discovery`; there is no automatic-subscription
environment surface.

### Redis

| Variable | Applies | Meaning |
| --- | --- | --- |
| `GOWHALE_REDIS_ADDR` | YAML override | `host:port` |
| `GOWHALE_REDIS_HOST` | env-only | host, used only when `ADDR` is empty |
| `GOWHALE_REDIS_PORT` | env-only | port, used only when `ADDR` is empty |
| `GOWHALE_REDIS_PASSWORD` | YAML override | password |
| `GOWHALE_REDIS_DB` | YAML override | database index |

### QuestDB

| Variable | Applies | Meaning |
| --- | --- | --- |
| `GOWHALE_QUESTDB_ENABLED` | both paths | enable the cold tier |
| `GOWHALE_QUESTDB_ILP_ADDRESS` | both paths | HTTP ILP endpoint |
| `GOWHALE_QUESTDB_PG_ADDRESS` | both paths | PostgreSQL-wire endpoint |
| `GOWHALE_QUESTDB_PG_USER` | both paths | user |
| `GOWHALE_QUESTDB_PG_PASSWORD` | both paths | password; required when enabled |

### Realtime edge

| Variable | Applies | Meaning |
| --- | --- | --- |
| `GOWHALE_REALTIME_CHANNEL_PREFIX` | both paths | Pub/Sub prefix, default `gowhale:realtime` |
| `GOWHALE_REALTIME_WS_TICKET_SECRET` | both paths | HMAC secret for WS tickets; empty disables the authenticated edge unless auth is explicitly disabled |
| `GOWHALE_REALTIME_WS_TICKET_TTL_S` | both paths | ticket lifetime, seconds, default `30` |
| `GOWHALE_REALTIME_WS_ALLOWED_ORIGINS` | both paths | comma-separated exact origins |
| `GOWHALE_REALTIME_WS_MAX_CONN_AGE_S` | both paths | maximum session age, seconds, default `3600` |
| `GOWHALE_REALTIME_WS_AUTH_DISABLED` | both paths | explicit local-only auth disable, default `false` |

`GOWHALE_REALTIME_WS_AUTH_DISABLED=true` is a development/LAN escape hatch: the
WebSocket edge mounts without a secret, skips ticket validation and accepts any
origin. It is never derived from an empty secret.

### Coinalyze

| Variable | Applies | Meaning |
| --- | --- | --- |
| `GOWHALE_COINALYZE_ENABLED` | both paths | enable the provider, default `false` |
| `GOWHALE_COINALYZE_API_KEY` | both paths | API key; when absent the provider is skipped with a warning and the boot continues |

### Binance WebSocket hosts

| Variable | Applies | Meaning |
| --- | --- | --- |
| `GOWHALE_BINANCE_FUTURES_WS_URL` | adapter init | futures stream base, default `wss://fstream.binance.com` |
| `GOWHALE_BINANCE_SPOT_WS_URL` | adapter init | spot stream base, default `wss://stream.binance.com:9443` |

These are read when the Binance adapter package initializes; they do not depend
on the configuration file. The `binancefuture.com` host is the futures testnet
and is not supported in any environment; its candle volume is synthetic. It is
never the default, and configuring it emits a loud boot warning.

## Validation rules

- A selected file must be readable, parse as YAML and contain no unknown keys.
- `server.port` and `redis.addr` must be non-empty after defaults.
- `server.log_format` must be `text` or `json`.
- Stream limits must be non-negative.
- `candle_engine.timeframes` must contain at least one interval; each interval
  must match `Nm`, `Nh`, `Nd` or `Nw` with a positive number.
- Every `exchanges[].market_type` must parse as `spot` or the futures vocabulary
  (`swap`, `perpetual`, `future`, `delivery`). Duplicate `id` + `market_type`
  pairs are rejected.
- `questdb.enabled: true` requires a non-empty PostgreSQL password.
- A trade buffer above the advisory threshold logs a warning; it is not fatal.

## Failure semantics

A validation error returns from `Load`; the process exits non-zero and does not
start. `doctor` reports unreadable declarations instead of printing a healthy
verdict. There is no fallback from a selected file to environment values, no
version negotiation and no legacy parser.

Source: `internal/platform/config/config.go`, `internal/platform/config/types.go`,
`internal/platform/config/declared_depth.go`, `config.example.yaml`.
