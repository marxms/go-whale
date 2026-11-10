# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Planned

- Certification of the first additional venue (Bybit first, then OKX).

## [0.1.0] - 2026-11-10

The first public tree. The code was extracted from a private monorepo and is
prepared for publication as the standalone module `github.com/marxms/go-whale`;
this is the release candidate that the first authorized push publishes.

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
- Recovery over explicit windows, seed normalization at the write gate and boot gap
  marking across every configured timeframe, each covered by a regression test.
- Venue contract harness exercising the four scopes and every declared driver
  absence.
- Funding, open interest, open-interest history and long/short ratio where the
  venue declares them.
- Redis hot tier and optional QuestDB cold tier behind ports.
- HTTP read API, opt-in ticket-authenticated WebSocket feed, Prometheus metrics,
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
- The testnet is no longer a configuration default: production hosts are the only
  default, and a legacy testnet host is reported loudly when configured.
- Documentation that described CCXT as a supported adapter was removed; the
  historical ADR remains marked as history.
- Task and phase narrative was removed from the tree, and the 37 remaining
  `deadcode` survivors are triaged in `docs/dead-code.md`.

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
- Interior gaps behind the head are not located; recovery windows derive from the
  head and need a coverage index for the middle of a series.
- The boot migration remains until `SoleCopyKeysAtRisk == 0` is proven against
  production data.
- The GitHub workflows have not yet executed on the public remote.
- Ten benchmarks remain inconclusive under the measurement protocol; none is
  reported as a pass.
