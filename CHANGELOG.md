# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Planned

- Recovery over explicit windows: every missing bucket is written and the series
  head advances.
- Seed normalization at the write gate, with the BCHUSDT 1w regression as the
  proof.
- Capability contract exercised for every declared scope.
- Certification of the first additional venue (Bybit and/or OKX).

## [0.1.0] - 2026-11-09

The first public tree. The code was extracted from a private monorepo and is
published as the standalone module `github.com/marxms/go-whale`.

### Added

- Standalone Go module with one entrypoint, `cmd/gowhale`.
- Binance USD-M futures as the reference scope; Binance spot and BingX spot/swap
  through the native REST adapter.
- Owned WebSocket transport with bounded buffers, deadlines, renewal and
  idempotent close.
- Canonical `trade.Trade`, `trade.Side`, `candle.Candle` and derivatives values.
- Trade aggregation into configurable timeframes, deterministic open/close
  election by trade id, and taker buy/sell attribution.
- Gap detection, historical seeding and closed-bucket repair.
- Funding, open interest, open-interest history and long/short ratio where the
  venue declares them.
- Redis hot tier and optional QuestDB cold tier behind ports.
- HTTP read API, ticket-authenticated WebSocket feed, Prometheus metrics,
  `/health`, `/ready` and `/openapi.json`.
- Strict configuration loader with `GOWHALE_*` environment overrides.
- `gowhale serve`, `gowhale doctor` and `gowhale version`.
- Example Dockerfile, Compose stack, configuration and environment files.
- QuestDB DDL fixture inside the repository, exercised by the schema test.
- Architecture, adapter, operations, schema and decision documentation.
- Brand assets, README, roadmap and branding policy.
- Contribution, code-of-conduct, security, issue and pull-request templates.
- CI, integration, schema and security workflows, validated locally with
  `actionlint`.

### Changed

- The module path is `github.com/marxms/go-whale`; the binary, environment prefix
  and wire names keep the GoWhale spelling.
- Public comments, CLI output and documentation are in English.
- `doctor` defaults to the public example (`compose.example.yaml`, service
  `redis`, `config.yaml`) instead of a deployment-specific path.
- The example Compose declares the ZSET listpack step so `doctor` can compare it
  with the indicator cap.

### Removed

- CCXT and the `go-ethereum` (GPL-3.0) dependency; every venue read is native.
- `third_party/` and the local module replacement.
- Private configuration, `.env` and build output from the published tree.
- Monorepo-only CI conformance tests; `actionlint` and the public workflows replace
  them.

### Known limitations

- The API and configuration are pre-1.0 and may change between minor versions.
- Only `binance:future` has a transportable kline feed; the other scopes declare
  the absence and aggregate from trades.
- The boot migration remains until `SoleCopyKeysAtRisk == 0` is proven against
  production data.
- The GitHub workflows have not yet executed on the public remote.
- Ten benchmarks remain inconclusive under the measurement protocol; none is
  reported as a pass.
