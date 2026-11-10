# Configuration

GoWhale reads one YAML file and applies environment overrides on top. The
loader rejects unknown YAML fields and invalid timeframes or market types. A
file selected with `--config` or `GOWHALE_CONFIG_FILE` is authoritative: read,
parse or validation errors are returned, never replaced by environment values.

## Precedence

1. `--config <path>` (serve/doctor flag)
2. `GOWHALE_CONFIG_FILE`
3. environment-only mode when neither is set

Within a loaded file, `GOWHALE_*` environment variables override the matching
values. The published examples keep secrets in the environment; the YAML loader
also accepts `redis.password` and `questdb.pg_password`, and an environment
variable wins when both are set.

## Sections

| Section | Purpose |
| --- | --- |
| `server` | HTTP port and log level |
| `redis` | hot-tier connection, pool and timeouts |
| `stream` | WebSocket buffer and connection limits |
| `exchanges` | enabled venues, symbols, discovery and REST limits |
| `derivatives` | polling cadence, series caps and bootstrap depth |
| `candle_engine` | in-memory ring, timeframes, write-through and snapshot cadence |
| `questdb` | cold-tier addresses, table, retention and WAL |
| `retention` | budget ratios and the measured Redis container ceiling |

`config.example.yaml` is a complete, safe local example. Private environment
files live outside the product tree and are mounted read-only at deploy time.

## Environment catalogue

### Process and server

| Variable | Meaning |
| --- | --- |
| `GOWHALE_CONFIG_FILE` | YAML path when no `--config` is given |
| `GOWHALE_SERVER_PORT` | HTTP port |
| `GOWHALE_LOG_LEVEL` | `debug`, `info`, `warn`, `error` |
| `GOWHALE_LOG_FORMAT` | `text` or `json` |

### Redis

| Variable | Meaning |
| --- | --- |
| `GOWHALE_REDIS_ADDR` | `host:port` |
| `GOWHALE_REDIS_HOST` / `GOWHALE_REDIS_PORT` | alternative to `ADDR` |
| `GOWHALE_REDIS_PASSWORD` | password |
| `GOWHALE_REDIS_DB` | database index |

### QuestDB

| Variable | Meaning |
| --- | --- |
| `GOWHALE_QUESTDB_ENABLED` | enable the cold tier |
| `GOWHALE_QUESTDB_ILP_ADDRESS` | HTTP ILP endpoint |
| `GOWHALE_QUESTDB_PG_ADDRESS` | PostgreSQL-wire endpoint |
| `GOWHALE_QUESTDB_PG_USER` | user |
| `GOWHALE_QUESTDB_PG_PASSWORD` | password (required when QuestDB is enabled) |

### Binance WebSocket hosts

| Variable | Meaning |
| --- | --- |
| `GOWHALE_BINANCE_FUTURES_WS_URL` | futures stream base, default `wss://fstream.binance.com` |
| `GOWHALE_BINANCE_SPOT_WS_URL` | spot stream base, default `wss://stream.binance.com:9443` |

The production host is the only supported source, in every environment. The
`binancefuture.com` names resolve to the futures **testnet**; it is NOT used by
this project, because its prices track production while its volume, trade count
and taker split come from a synthetic order book (measured 2026-11-07: BCH 15m
reported 4.7M BCH over 4,031 trades, ~1,164 BCH/trade, against ~0.5 BCH/trade on
production). GoWhale warns loudly at boot if one is configured, and no
environment should set it. An unreachable production stream is a network
incident to report, not a reason to switch venues.

### Realtime edge

| Variable | Meaning |
| --- | --- |
| `GOWHALE_REALTIME_CHANNEL_PREFIX` | Pub/Sub prefix, default `gowhale:realtime` |
| `GOWHALE_REALTIME_WS_TICKET_SECRET` | HMAC secret for WS tickets; empty disables the edge |
| `GOWHALE_REALTIME_WS_MINT_TOKEN` | server-to-gateway secret for the ticket-mint endpoint; both WS secrets are required |
| `GOWHALE_REALTIME_WS_TICKET_TTL_S` | ticket lifetime |
| `GOWHALE_REALTIME_WS_ALLOWED_ORIGINS` | comma-separated origin allow-list |
| `GOWHALE_REALTIME_WS_MAX_CONN_AGE_S` | maximum session age |
| `GOWHALE_REALTIME_WS_MAX_CONNECTIONS` | global WS connection cap, default `4096` |
| `GOWHALE_REALTIME_WS_MAX_SCOPES_PER_CONNECTION` | per-connection subscription cap, default `128` |
| `GOWHALE_REALTIME_WS_AUTH_DISABLED` | explicit local-only auth disable |

The WebSocket edge mounts only when **both** `GOWHALE_REALTIME_WS_TICKET_SECRET` and
`GOWHALE_REALTIME_WS_MINT_TOKEN` are set to at least 32 random bytes. The
authenticating gateway must inject `X-Gowhale-Mint-Token` on `/api/ws-ticket`; the
mint endpoint is not a user-auth boundary by itself. Empty secrets disable the edge.
Placeholders such as `replace-with-…` are rejected at boot.

### Discovery and subscriptions

| Variable | Meaning |
| --- | --- |
| `GOWHALE_WEBSOCKET_MAX_ASSETS_PER_CONNECTION` | per-connection stream budget |
| `GOWHALE_WEBSOCKET_MAX_CONNECTIONS_PER_IP` | per-IP connection budget |

### Coinalyze

| Variable | Meaning |
| --- | --- |
| `GOWHALE_COINALYZE_ENABLED` | enable the provider |
| `GOWHALE_COINALYZE_API_KEY` | API key; when absent, the provider is skipped with a warning and the boot continues |

## Validation rules

- Timeframes are one of `Nm`, `Nh`, `Nd`, `Nw` with a positive number.
- Market types accept `spot` and the futures vocabulary (`swap`, `perpetual`,
  `future`, `delivery`); unknown values fail at the boundary.
- `questdb.enabled: true` requires a non-empty PostgreSQL password.
- Unknown YAML keys fail the load.
