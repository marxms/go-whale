# 0011 — Exchange kline authority requires a settled record

- **Status:** Accepted
- **Date:** 2026-11-06

## Context

A bucket closing and the exchange finishing its kline are different facts. A
REST kline fetched moments after a bucket rolls is still being written:
convergence takes about 5s. Treating bucket closure as authority made a
targeted repair replace a correct live reconstruction with a short record and
report it as a heal.

The failure is especially hard to detect because no trade is lost: the
aggregate-trade tape is contiguous, the discard counters are zero and
`hasGap` is null. The measurement is overwritten, not dropped, so every
detector is correctly silent.

Two clocks bracket the instant the exchange computed the total:

```
RequestedAt ≤ H ≤ FetchedAt
```

`RequestedAt` is the instant the request was issued; `FetchedAt` is the instant
the response was parsed.

## Decision

- A REST total is authoritative for a closed bucket only when
  `FetchedAt >= bucketEnd + KlineSettleGraceMs`.
- `KlineSettleGraceMs` is 10s, twice the measured ~5s convergence, because
  being late costs one cycle while being early installs a wrong number.
- An unstamped total (`FetchedAt == 0`) is never authoritative; it falls back to
  the conservative union, which can only raise a bucket.
- Authority also requires a declared total source with a measured taker pair;
  a record that did not measure order flow cannot settle order flow.
- The same settle-time gate is applied in the seed union and the engine.

## Consequences

- A short REST kline cannot replace a correct live reconstruction.
- A genuine authoritative correction still settles after the grace; the guard
  does not become a ratchet that can only move upward.
- Repair and seed paths must carry `RequestedAt` and `FetchedAt`.
