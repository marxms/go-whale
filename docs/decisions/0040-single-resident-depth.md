# 0040 — One resident depth constant, enforced by a conformance test

- **Status:** Accepted
- **Date:** 2026-11-06

## Context

The resident depth of every series (candle ring, candle ZSET, derivatives series and
derivatives bootstrap) was declared in five places with four different numbers: a
comment claiming a ZSET cap of 200, `candleCap := 200`, `defaultBufferSize = 500`,
`seedDepth := 200`, and config fallbacks of 200. Individually each was a plausible
fallback; together they contradicted one another and contradicted production. Anyone
who read one of them to make a decision decided on a number that never ran.

The failure mode is a present-tense claim about state that a later commit changed.
It recurred across review rounds because none of the five sites was authoritative.

## Decision

- There is exactly one fallback constant, `DefaultResidentDepth = 800`, in
  `internal/platform/config/declared_depth.go`.
- Its value is not a local decision: it is the depth production runs.
- The mounted YAML (`config.${ENV:-production}.yaml`) may override it through
  `derivatives.max_series_size` and `candle_engine.buffer_sizes`.
- `declared_values_test.go` cross-reads the constant against the mounted YAML and
  fails on divergence. The coupling is a test, not a second copy — the same
  discipline used for `MIN_WARMUP_CANDLES`.

## Consequences

- Changing the production depth requires changing the declaration and the constant
  in the same commit, or the conformance test fails.
- A reader looking for "how deep is resident data" has one place to look.
- Adding another resident series means reusing the constant, not inventing a local
  fallback.
