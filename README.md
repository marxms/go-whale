<p align="center">
  <img src="docs/assets/brand/gowhale-hero.webp" alt="GoWhale mascot riding a whale through market data" width="720">
</p>
<p align="center"><em>Illustrative artwork. Only Binance and BingX are supported today; the other venues shown are planned or illustrative, not supported. The capability matrix below is the source of truth.</em></p>

# GoWhale

**Real-time, correctness-first crypto market data in Go.**

GoWhale ingests public exchange streams, aggregates trades into canonical OHLCV
candles with measured taker flow, serves them over HTTP and WebSocket, and keeps a
Redis hot tier with an optional QuestDB cold tier.

![Go 1.26.10](https://img.shields.io/badge/Go-1.26.10-00ADD8)
![License MIT](https://img.shields.io/badge/license-MIT-blue)
![Status pre-1.0](https://img.shields.io/badge/status-pre--1.0-orange)

## Contents

- [What it does](#what-it-does)
- [Status](#status)
- [Supported venues and capabilities](#supported-venues-and-capabilities)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [HTTP and WebSocket API](#http-and-websocket-api)
- [How it works](#how-it-works)
- [Repository layout](#repository-layout)
- [Operations](#operations)
- [Development](#development)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [Security](#security)
- [License and brand](#license-and-brand)

## What it does

- **Ingests** public trade streams from Binance USD-M futures, Binance spot and
  BingX, plus the Binance futures kline feed, through a cancellation-safe
  WebSocket transport.
- **Aggregates** trades into configurable timeframes with deterministic
  open/close election by trade id and explicit taker buy/sell attribution.
- **Repairs** gaps through venue REST APIs, seeds history, and keeps the live
  measurement when a closed bucket is recovered.
- **Serves** a small, consumer-driven HTTP surface and an opt-in WebSocket feed fed
  by the in-process fan-out; a separate Redis Pub/Sub producer carries candle events
  to external consumers.
- **Persists** candles and derivatives snapshots to Redis; QuestDB is an optional
  cold tier behind ports.

## Status

GoWhale is **pre-1.0**. The API and configuration may change between minor
versions. The Binance futures ingestion path has been exercised in a private
deployment; this repository is prepared for its first public release, its GitHub
Actions have not yet executed, and publication is pending owner authorization. The
public gate, the contract harness and the BCHUSDT 1w fixture are the reproducible
evidence. The local gate is `make verify`.

The reference scope is `binance:future`. Other scopes are supported only where
the capability matrix says so; see below.

## Supported venues and capabilities

The matrix is the single source of truth: every row is declared in
[`internal/market/capability.go`](internal/market/capability.go), and an absent
capability carries a reason instead of being discovered in production.

| Scope | Kline feed | CVD | Funding | Open interest | OI history | L/S ratio | Markets | Listing floors |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `binance:future` | yes | yes | yes | yes | yes | yes | yes | yes |
| `binance:spot` | no | yes | no | no | no | no | yes | no |
| `bingx:swap` | no | no | yes | yes | no | no | yes | no |
| `bingx:spot` | no | no | no | no | no | no | yes | no |

The `binance:future` scope is the reference implementation. "No" means the venue
does not publish the data or this service does not transport it; the adapter
returns a typed absence instead of inventing a value. The full connection policy
and the porting checklist are in [`docs/adapters.md`](docs/adapters.md).

Planned venues — Bybit, OKX, Coinbase, Kraken, Bitget, Gate.io, Hyperliquid,
Backpack and Deribit — are **not supported yet**. Their priorities and
certification criteria are in the [roadmap](ROADMAP.md).

## Quick start

### Docker Compose

The example stack runs Redis and GoWhale. QuestDB is optional and has its own
integration compose.

```bash
docker compose -f compose.example.yaml up --build
curl -sS http://127.0.0.1:3000/health
```

The process answers `/health` as soon as it is up. The JSON body distinguishes
`starting`, `healthy`, `degraded` and `unhealthy`; `503` means the process is up but
at least one declared scope is unhealthy (for example, a venue is unreachable from
your network). The configuration is mounted read-only from `config.example.yaml`;
the image never contains a private configuration. Stop with:

```bash
docker compose -f compose.example.yaml down -v
```

`down -v` deletes the example Redis volume; it is safe only for a throwaway stack.

Once discovery/seed has populated the fleet, the core read path is:

```bash
curl -sS 'http://127.0.0.1:3000/api/timeseries?symbol=BTC%2FUSDT&timeframe=5m&limit=2'
```

The first calls may return an empty series until the symbol is discovered, seeded
and has a closed bucket.

### From source

Requirements: Go 1.26.10 (the pinned toolchain; `GOTOOLCHAIN=auto` downloads it if
needed) and Redis 8.x.

```bash
cp config.example.yaml config.yaml
# Point GOWHALE_REDIS_ADDR at your Redis, then:
go run ./cmd/gowhale serve -config config.yaml
```

Build the binary and print its identity:

```bash
make build
./bin/gowhale version
```

`gowhale serve` is the default mode. `gowhale doctor` diagnoses the boot
preconditions without opening a port; it is intentionally strict and exits `3`
when an input was not measured. The example Compose uses `noeviction` with resident
families that have no TTL, so `doctor` refuses that example by default (exit `1`);
pass `-immortal-families ''` to diagnose the example's other gates.

## Configuration

Configuration is one YAML file plus `GOWHALE_*` environment overrides. The loader
is strict: unknown YAML fields and invalid timeframes or market types are errors,
and an explicitly selected file is never silently replaced by environment values.

The published examples keep credentials in the environment; the loader also accepts
`redis.password` and `questdb.pg_password` in YAML, and an environment variable wins
when both are set. The examples use:

```bash
export GOWHALE_CONFIG_FILE=./config.yaml
export GOWHALE_REDIS_ADDR=localhost:6379
export GOWHALE_QUESTDB_PG_PASSWORD=...   # only when QuestDB is enabled
export GOWHALE_COINALYZE_API_KEY=...     # only when Coinalyze is enabled
```

See [`docs/configuration.md`](docs/configuration.md) for the full catalogue and
[`config.example.yaml`](config.example.yaml) for a complete example.

## HTTP and WebSocket API

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | process status and timeframe checksum |
| `GET` | `/ready` | whether at least one venue stream is connected |
| `GET` | `/metrics` | Prometheus registry |
| `GET` | `/openapi.json` | generated route contract |
| `GET` | `/api/subscriptions` | active symbols grouped by exchange/market, timeframes and optional margin rates |
| `GET` | `/api/timeseries` | candle series for one symbol and timeframe |
| `GET` | `/api/batch/prices` | latest prices for a symbol set |
| `GET` | `/api/batch/derivatives-series` | derivatives series for a symbol set |
| `GET` | `/api/ws-ticket` | short-lived ticket (opt-in; requires both WS secrets and the gateway header) |
| `GET` | `/api/ws` | opt-in WebSocket feed, mounted with the ticket endpoint |

The WebSocket edge is opt-in: empty `GOWHALE_REALTIME_WS_TICKET_SECRET` and
`GOWHALE_REALTIME_WS_MINT_TOKEN` disable it. The authenticating gateway must inject
`X-Gowhale-Mint-Token` on `/api/ws-ticket`; the server never treats the mint endpoint
as a user-auth boundary. The OpenAPI document at `/openapi.json` lists the routes;
query parameters and examples are in [`docs/api.md`](docs/api.md).

## How it works

```text
exchange WebSocket ──► ingestion ──► candle aggregation ──► Redis hot tier
                                      │                        │
                                      ├── derivatives          ├── HTTP API
                                      └── recovery ◄── venue REST
                                                               └── QuestDB cold tier
```

- Domain packages (`market`, `trade`, `candle`, `derivatives`) do not import
  configuration, adapters, Redis, QuestDB or Prometheus.
- Adapters convert venue payloads into canonical values at the boundary. The one
  exception, the Binance SDK gap filler in `internal/recovery`, stays behind the
  recovery port and does not leak a vendor type into the domain.
- Absent, zero, transient, contract divergence and uncertain write stay distinct
  states. Startup gates refuse to serve when a declaration cannot be read.
- Redis is the hot tier and the source for `/api/*` reads. QuestDB is optional; the
  engine owns the single writer, WAL barrier and retention policy.

Read [`docs/architecture.md`](docs/architecture.md) for the package graph and
[`docs/domain.md`](docs/domain.md) for the canonical values.

## Repository layout

```text
cmd/gowhale/              serve, doctor, version
internal/market/          exchange, market type, timeframe and capability matrix
internal/trade/           canonical trade, side and last-trade values
internal/candle/          candle values, aggregation and storage ports
internal/candle/engine/   ring, hot/cold tiers and QuestDB adapters
internal/derivatives/     derivatives values, collectors and baselines
internal/ingestion/       transports and trade admission
internal/recovery/        gap detection, seeding and repair
internal/health/          liveness, readiness and budget signals
internal/adapters/inbound/{httpapi,realtime,scheduler}/
internal/adapters/outbound/{binance,binancews,binancerest,bingx,venue,redis,questdb,prometheus,coinalyze}/
internal/platform/{config,doctor,logging,netdiag,ops,version}/
internal/pruning/         retention-aware pruning and the sole-copy guard
internal/retention/       retention policy builders
internal/distribution/    realtime fan-out
scripts/                  boundary, health and QuestDB helpers
docs/                     architecture, adapters, operations, schemas and decisions
```

## Operations

- `GET /health` distinguishes absent coverage, stale data, degraded and healthy;
  a component that is not mounted is never reported healthy. `GET /ready` reports
  whether at least one venue stream is connected.
- `GET /metrics` exposes the Prometheus registry.
- `gowhale doctor` reads the compose policy, the configuration and the measured
  boot facts. It exits `0` (approved), `1` (fatal), `2` (unreadable declaration) or
  `3` (indeterminate because an input was not measured).
- QuestDB is the optional cold tier. The integration stack is in
  [`compose.integration.yaml`](compose.integration.yaml); apply the DDL with a
  QuestDB pgwire client before `make integration`:

  ```bash
  psql "postgres://admin:quest@localhost:8812/qdb" \
    -f scripts/questdb/questdb-init.sql
  ```

See [`docs/operations.md`](docs/operations.md) for health semantics, shutdown and
the boot-migration runbook.

## Development

```bash
make help        # list targets
make fmt-check   # gofumpt + goimports
make vet         # go vet
make lint        # golangci-lint v2.13.2
make test        # unit suite
make race        # unit suite with the race detector
make integration # tests behind the integration build tag
make build       # bin/gowhale
make verify      # all gates
```

`make verify` is the product gate. It pins the tool versions, checks formatting,
lint, import boundaries, module hygiene, vulnerabilities, vet, unit and race
suites, the executed test-case count, integration compilation and the binary
build. It stops at the first failing stage with a message naming the stage.

See [`docs/development.md`](docs/development.md) for the toolchain and the test
policy.

## Roadmap

The roadmap separates what is shipped, what is broken, what is blocked and what is
planned. It includes the identified reliability debt, the venue expansion plan and
the certification checklist a new adapter must pass.

Read [`ROADMAP.md`](ROADMAP.md). Planned support is not a promise of a date, and a
venue is not "supported" until the capability matrix declares it and the contract
harness certifies it.

## Contributing

Issues and pull requests are welcome. Start with an issue that describes the
behaviour and the evidence that it is wrong or missing. Keep the change inside its
scope, add a test that fails without it, and include the result of `make verify`.

Read [`CONTRIBUTING.md`](CONTRIBUTING.md) and the
[`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md).

## Security

Do not open a public issue for a vulnerability. Use GitHub's private vulnerability
reporting on the repository; see [`SECURITY.md`](SECURITY.md). This project never
asks for exchange credentials, API keys or production database access in a report.

## License and brand

The source code and documentation are MIT; see [`LICENSE`](LICENSE). The GoWhale
name and the original elements of the artwork are project marks and are not
licensed by MIT. The artwork also contains the Go logo, a trademark of Google LLC,
and a gopher derivative created by Renee French and licensed under CC BY 4.0.
GoWhale is an independent project and is not affiliated with or endorsed by Google
or the Go project. The exchange names and logos are illustrative and belong to
their owners; GoWhale is not affiliated with or endorsed by any venue.

See [`docs/branding.md`](docs/branding.md).

## What GoWhale is not

GoWhale is market data infrastructure. It does **not** place orders, execute
trades, hold funds or custody assets. It does not provide portfolio management,
investment signals or financial advice. Use it as a data service, and evaluate any
trading decision with your own risk controls.
