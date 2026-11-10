# 0033 — Treat cold-write transport failures as a recoverable process condition

- **Status:** Accepted
- **Date:** 2026-11-06

## Context

The QuestDB store writes through ILP-over-HTTP. When QuestDB's workers died while its
process stayed up, every write failed with:

```
retry timeout reached: 10s. Post "http://questdb:9000/write": context deadline exceeded
```

The classifier that decides whether an error is a transport failure still knew only the
TCP-era phrasings, so that message matched nothing. No reconnect ever ran, and the cold tier
silently lost every row for thirteen hours, including after QuestDB was healthy again. Each
dropped row logged one tidy line, so every dashboard looked healthy.

Transport failures are not a property of the row being written: the next row fails the same
way, and skipping them one at a time discards the entire sweep and then the next sweep,
indefinitely. Row-level rejections (bad column, missing table, unrepresentable value) are a
property of the row and must stay non-retryable, or one unwritable row aborts the sweep
forever.

## Decision

- `isConnectionError` recognises transport failures across the transports the store uses,
  including the retry-timeout and context-deadline shapes the HTTP client emits. The list is
  phrasing-based because the SDK wraps transport errors in plain strings.
- A transport failure aborts the current sweep and replaces the sender. With
  `auto_flush=off` the unsent prefix would otherwise stay queued; the pipeline retains the
  DAY for a later pre-submission retry.
- Row-level rejections remain non-retryable and are counted/logged without replacing the
  sender.
- Cold-write health is published independently of transport liveness: the last successful
  commit age climbs when writes fail or when no closed candles are produced, and the probe
  degrades on it without returning 503, because restarting under memory pressure restarts
  the spiral at the cost of seeding again.

## Consequences

- A silent cold-write outage is visible as a rising `cold_last_flush_age_seconds` and
  pipeline counters instead of a green probe.
- Recovery is automatic once the link works again; the retained DAY is retried before
  submission.
- The classifier must be revisited whenever the transport changes, because an unrecognised
  phrasing turns the whole recovery path into dead code again.
