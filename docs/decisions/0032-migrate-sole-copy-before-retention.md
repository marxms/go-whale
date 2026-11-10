# 0032 — Migrate sole-copy candles before QuestDB retention runs

- **Status:** Accepted
- **Date:** 2026-11-06

## Context

`retention_days: 365` is not only a ceiling; QuestDB reapplies it as a window on every
boot. The retention sweep drops partitions with `ALTER TABLE candles DROP PARTITION WHERE
timestamp < ...` without a timeframe filter, so the cutoff is the moving edge of that
window, not the date when writing started.

A migration tool wrote 130,284 candles from Redis and the retention sweep erased them on
the next boot because they were older than 365 days. At the same time, the read path has no
REST fallback for the `1d` and `1w` cells: `SeedWindow` seeds, it does not read. Redis is the
only copy of 95,811 members in `candles:*:1d` and 33,379 in `:1w` (129,190 candles across 297
of 500 keys each) that do not exist in QuestDB. Under `allkeys-lru`, a weekly chart — the
least-touched series — is at the front of the eviction queue.

The sole-copy-vs-evict-everything gate refuses to run retention while a sole copy remains.
That refusal is the feature working; what was missing was the work that removes the
condition, and it has to happen before the gate, during construction, because the preflight
returns immediately when a sole copy remains.

## Decision

- Stage 1 of boot is the sole-copy migration. It runs before the preflight gates and before
  retention, because retention is global and would delete what the migration just wrote.
- The boundary is `MIN(timestamp)` of the durable tier per `(symbol, timeframe)`, read from
  the data and never written as a literal. A symbol admitted later has shorter coverage than
  its cell, and a symbol with zero durable rows has its entire series as a sole copy.
- The migration reads Redis and writes QuestDB; it deletes nothing. Caps and eviction belong
  to later stages, and their order is what makes both safe.
- Retention receives a licence only when the boot-migration outcome does not withhold it. A
  pass cut by the row or pass ceiling is incomplete, not successful; the next boot resumes
  from the re-read boundary.

## Consequences

- While retention remains global and 365 days, the migration does not by itself preserve a
  sole copy older than the window; it must run first and be allowed to finish, which is why
  an incomplete outcome denies retention.
- The hot read is identical before and after the migration; the contract is pinned by tests
  on both tiers.
