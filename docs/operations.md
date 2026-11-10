# Operations

## Process

`gowhale serve` is the default mode. `gowhale doctor` reads the compose policy,
the configuration and the measured boot facts and exits non-zero when a
declaration is unreadable.

```bash
gowhale doctor -compose compose.example.yaml -service redis \
  -config config.yaml -immortal-families '' -indicator-cap 512 -sole-copy-keys-at-risk 0
```

The example Compose deliberately uses `noeviction` while the service has resident
families without TTL. Run `doctor` against that example **without**
`-immortal-families ''` and it refuses with exit `1`
(`eviction-policy-vs-immortal-families`). That refusal is the gate working, not a
broken probe: the example is a throwaway stack, and a production deployment must
choose an eviction policy and prove the sole-copy risk. The empty value above
diagnoses the other gates without pretending the example is a deployment.

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
| `/health` | process status, timeframe checksum and per-component state (`starting`, `healthy`, `degraded`, `unhealthy`) |
| `/ready` | whether at least one venue stream is connected |
| `/metrics` | Prometheus registry |
| `/openapi.json` | generated route contract; query parameters are in [`api.md`](api.md) |

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

## WebSocket edge

The WS edge is opt-in. It mounts only when both
`GOWHALE_REALTIME_WS_TICKET_SECRET` and `GOWHALE_REALTIME_WS_MINT_TOKEN` are set
to at least 32 random bytes, and `GOWHALE_REALTIME_WS_ALLOWED_ORIGINS` lists the
browser origins. The authenticating gateway must verify the user and inject
`X-Gowhale-Mint-Token` on `/api/ws-ticket`; the mint endpoint is not a user-auth
boundary by itself. Tickets are short-lived and the response carries
`Cache-Control: no-store`. Connections and subscriptions are capped by
`GOWHALE_REALTIME_WS_MAX_CONNECTIONS` and
`GOWHALE_REALTIME_WS_MAX_SCOPES_PER_CONNECTION`; scopes are validated against the
capability matrix. The WS feed itself is the in-process distribution hub; Redis
Pub/Sub is a separate producer for external consumers. The example Compose does not
set the secrets, so its WS routes are absent by design.

## `noeviction` boot refusal (M10)

The service has resident families without TTL (`candles`, `derivatives:series`).
Under `noeviction`, Redis fails writes once memory is full instead of evicting a
candle that may be the only copy; the startup gate therefore refuses to boot when
the declared policy cannot keep those families. The example Compose uses
`noeviction` on a throwaway volume, so `doctor` refuses it by default. To diagnose a
deployment, declare the real policy and the live resident families:

1. Read the destination policy from the Compose service `command`.
2. Measure the live families and the sole-copy risk before changing the policy.
3. Run `doctor` with `-sole-copy-keys-at-risk <measured>`; never presume zero.
4. Use `-immortal-families ''` only for the disposable example, never as a way to
   silence a production refusal.

## Redis ownership and pruning

The pruner only unlinks families this service owns: candles, last trades and
derivatives. It never deletes `indicators:*`, `analyzer:*` or `signalEvidence:*`
keys; those belong to another application. A GoWhale instance pointed at a shared
Redis must not be given ownership of those families.

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
