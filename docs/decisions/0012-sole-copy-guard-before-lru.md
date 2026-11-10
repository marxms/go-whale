# 0012 — Guard sole-copy hot candles before an eviction-policy change

- **Status:** Accepted
- **Date:** 2026-11-06

## Context

Redis can be configured with `--maxmemory-policy allkeys-lru`, which evicts the
least-touched keys first. A compose file can deploy that policy even when the
person deploying it never read the migration design.

The hot tier can hold candle cells that do not exist in the durable tier. Those
cells are the least-touched keys in the keyspace — a weekly chart is opened
rarely — so an LRU policy destroys their only copy with no durable fallback.

An ordering written in prose does not stop a `docker compose up`.

## Decision

- The service measures at startup whether a sole-copy cell still exists and
  cross-references that result with the CURRENT policy read from the server.
- A sole copy under a policy that evicts everything is a FATAL gate.
- The measurement is a boundary comparison, not a candle count: the lowest score
  of each hot ZSET against the durable table's `MIN(timestamp)`. Counting the
  candles would turn a startup guard into a scan.
- A symbol with no durable row is the most severe case, not the rarest. It does
  not appear in the `GROUP BY`; a map returning zero for it would make it look
  covered since 1970, so it is counted separately and named.
- "Could not measure" alarms and does not fail. A fatal gate on the ABSENCE of a
  measurement would turn momentary cold-tier unavailability into a service that
  does not come up.

## Consequences

- The migration must precede any policy change; the guard enforces the order.
- A deployment without a durable tier is reported as NOT EVALUATED, never as
  clean.
- The guard reads two boundaries per timeframe, not the whole hot keyspace.
