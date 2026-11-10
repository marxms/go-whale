# Architecture decision records

These records capture durable decisions. They describe the reasoning that is
already in force; they are not proposals and they are not a migration plan.
Historical decisions are recorded as history, not rewritten as if they had
originated in GoWhale.

The numbers are historical. Gaps in the sequence are intentional: records that
were superseded, merged or never accepted were removed, and the remaining numbers
were not renumbered. A missing number does not imply a missing decision.

| ADR | Title | Status |
| --- | --- | --- |
| [0001](0001-standalone-module.md) | Extract GoWhale as a standalone Go module | Accepted |
| [0002](0002-owned-websocket-transport.md) | Own the Binance WebSocket transport | Accepted |
| [0003](0003-canonical-fail-fast-config.md) | One canonical, fail-fast configuration | Accepted |
| [0004](0004-hot-and-cold-tiers.md) | Redis hot tier and QuestDB cold tier behind ports | Accepted |
| [0005](0005-canonical-domain-and-wire-dtos.md) | Canonical domain values and DTOs at the boundary | Accepted |
| [0006](0006-ccxt-gpl-license-blocker.md) | Remove CCXT to keep the MIT license | Accepted |
| [0010](0010-taker-sell-legacy-reconstruction.md) | Reconstruct taker-sell only for legacy and REST candles | Accepted |
| [0011](0011-settled-kline-authority.md) | Exchange kline authority requires a settled record | Accepted |
| [0012](0012-sole-copy-guard-before-lru.md) | Guard sole-copy hot candles before an eviction-policy change | Accepted |
| [0030](0030-process-wide-rest-budget.md) | One process-wide REST weight budget for Binance | Accepted |
| [0031](0031-coinalyze-request-budget.md) | Coinalyze request budget and refusal classification | Accepted |
| [0032](0032-migrate-sole-copy-before-retention.md) | Migrate sole-copy candles before QuestDB retention runs | Accepted |
| [0033](0033-cold-write-transport-failures.md) | Treat cold-write transport failures as a recoverable process condition | Accepted |
| [0040](0040-single-resident-depth.md) | One resident depth constant, enforced by a conformance test | Accepted |
| [0041](0041-member-lane-fanout-cost.md) | Hub fan-out keeps the transport-free Member/Lane seam | Accepted |
| [0042](0042-open-tick-benchmark-path.md) | OpenTickEndToEnd measures the real poll path since the fan-out change | Accepted |
