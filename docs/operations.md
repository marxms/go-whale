# Operations

## Process

`gowhale serve` is the default mode. `gowhale doctor` reads the compose policy,
the configuration and the measured boot facts and exits non-zero when a
declaration is unreadable.

```bash
gowhale doctor -compose compose.example.yaml -service redis \
  -config config.yaml -indicator-cap 400 -sole-copy-keys-at-risk 0
```

## Boot migration (sole-copy)

The boot migration runs at construction, before `preflightGates`. That order is
deliberate: with `SoleCopyKeysAtRisk > 0` the gates refuse the boot, so a
migration placed after them would sit behind the refusal it exists to remove.

The stage must stay until the owner proves, against production data, that
`SoleCopyKeysAtRisk == 0`:

1. Read the gate's measured risk on the target deployment (the `doctor`
   invocation above takes `-sole-copy-keys-at-risk`).
2. Record the command, the timestamp, the deployment and the observed value in
   the deploy evidence.
3. Only then may a release remove `internal/app/boot_migration.go` and the
   QuestDB migration. Removal is an owner decision and must be called out in the
   release notes.

Without step 1 and 2 the migration is not removed; a green local suite does not
substitute for the production measurement.

## Health and metrics

| Endpoint | Meaning |
| --- | --- |
| `/health` | process and timeframe checksum |
| `/ready` | dependencies and expected scopes |
| `/metrics` | Prometheus registry |
| `/openapi.json` | generated route contract |

Health distinguishes absent coverage, stale data and a healthy component. A
component that is not mounted is never reported healthy.

The fleet-liveness gate uses the per-symbol persisted candle on the fastest
timeframe and reports a scope unhealthy when more than 20% of its **monitored**
symbols are stale. Venue TradFi markets (Binance `underlyingType` other than
`COIN`, e.g. tokenized equities and commodities) are excluded from that
denominator: they follow their own trading hours, and a closed equity market is
not a broken crypto feed. Their WebSocket connection health and gap recovery
still apply, and `/api/subscriptions` lists them.

## Shutdown

SIGTERM cancels the root context. The process stops ingestion, drains bounded
queues, flushes snapshots, closes transports and then the Redis client. Close
is idempotent; a second signal is not required.

## Docker

```bash
docker compose -f compose.example.yaml up --build
docker compose -f compose.example.yaml ps
curl -s http://127.0.0.1:3000/ready
docker compose -f compose.example.yaml down
```

The example stack binds the HTTP port to loopback, keeps Redis on a private
network and mounts the configuration read-only. It contains no production
credentials. QuestDB is the optional cold tier and lives in the separate
integration compose; it is not part of the minimal example journey.

## Key layout

- Candles: `candles:{exchange}:{marketType}:{symbol}:{timeframe}` (ZSET)
- Last trade: `last_trade:{exchange}:{marketType}:{symbol}`
- Derivatives latest: `derivatives:snapshot:latest:{exchange}:{marketType}:{symbol}` (TTL)
- Derivatives series: `derivatives:series:{exchange}:{marketType}:{symbol}:{timeframe}` (ZSET)
- Realtime events: `gowhale:realtime:*` (Pub/Sub)

The storage catalogue is the authority for exact layouts and codecs.
