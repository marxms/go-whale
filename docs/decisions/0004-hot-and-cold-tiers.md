# 0004 — Redis hot tier and QuestDB cold tier behind ports

- **Status:** Accepted
- **Date:** 2026-11-05

## Context

The service serves recent candles and derivatives snapshots to the API while
also retaining a longer history. Recent reads are latency-sensitive and fit in
memory; long history has a different cost, retention rule and failure mode. A
single store abstraction hid that difference and made one implementation own
unrelated concerns.

## Decision

- Redis is the hot tier. It stores candle ZSETs, the latest derivatives
  snapshot, derivatives series, last trade and the active-symbol metadata. The
  API reads the latest values and series from it; candle reads go through the
  tiered store, which starts at the in-memory ring.
- QuestDB is the optional cold tier. It stores the durable candle table and the
  durable derivatives table, and serves historical windows that the hot tier no
  longer retains.
- Domain packages declare the narrow ports they consume:
  `candle.Writer`, `candle.Reader`, `candle.Catalog`, `candle.SeedWriter`,
  `candle.SeedSplitter`, `derivatives.Writer`, `derivatives.Reader`,
  `derivatives.BaselineReader`, `derivatives.PriceReader` and
  `trade.LastTradeStore`.
- Adapters implement those ports. `internal/adapters/outbound/redis`,
  `internal/adapters/outbound/binance` and `internal/candle/engine` own the
  concrete stores; `internal/app` is the only composition root.
- `engine.TieredStore` combines the in-memory ring, Redis and, when configured,
  QuestDB. The candle engine owns the single cold writer, the WAL visibility
  barrier and the retention policy.
- Cold-tier absence is an explicit degraded state, not an empty result. Schema
  divergence is refused at startup rather than repaired by guessing.

## Consequences

- Redis and QuestDB each have their own codec and schema, documented under
  `docs/schemas/`. Their adapters are covered by contract tests; Redis uses
  miniredis for unit tests and a real instance for integration, and QuestDB
  tests run against the real engine.
- QuestDB can be disabled. When disabled, range reads fall back to the hot tier
  and the API must report the absence rather than fabricate a complete history.
- Domain packages do not import adapters or `platform/config`.
- Retention is per tier and per table: Redis retention is a member cap, QuestDB
  retention drops partitions.
