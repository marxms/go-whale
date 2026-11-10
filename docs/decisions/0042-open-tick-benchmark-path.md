# 0042 — OpenTickEndToEnd measures the real poll path since the fan-out change

- **Status:** Accepted
- **Date:** 2026-11-06

## Context

The baseline measured `BenchmarkOpenTickEndToEnd` by calling the hub's
private `handleOpenBatch` directly and then flushing every member
synchronously from the benchmark goroutine. That priced the actor conflation
plus a serialized flush, not a whole poll tick.

The fan-out change moved the conflation into `internal/distribution` and made
`handleOpenBatch` private. The benchmark was changed to call `Hub.PollOnce()`
(scope request → engine peek → actor batch) and to drive the real `writePump`
goroutines. The change record documents it; it did not record numbers.

The re-run compared the post-change benchmark with the baseline under the same
`-count=5 -benchtime=300ms` protocol. The small matrices look like large
regressions while the large matrices improve:

| Subcase | Baseline median | Re-run median | Δ |
| --- | ---: | ---: | ---: |
| `clients=1/scopes=1` | 1,350 ns | 5,198 ns | +285.0% |
| `clients=1/scopes=10` | 12,402 ns | 22,877 ns | +84.5% |
| `clients=1/scopes=100` | 116,289 ns | 159,969 ns | +37.6% |
| `clients=10/scopes=1` | 11,297 ns | 20,332 ns | +80.0% |
| `clients=10/scopes=10` | 112,496 ns | 72,723 ns | -35.4% |
| `clients=10/scopes=100` | 994,661 ns | 395,793 ns | -60.2% |
| `clients=100/scopes=1` | 113,210 ns | 78,999 ns | -30.2% |
| `clients=100/scopes=10` | 969,906 ns | 393,415 ns | -59.4% |
| `clients=100/scopes=100` | 9,388,437 ns | 4,593,100 ns | -51.1% |

All CVs are at most 7.8% and the ranges are disjoint. The cause is coverage,
not a production change: `pollOnce` already existed in the baseline tree and still
runs on the same 2 s tick. The small matrices now pay the fixed cost of the
real tick (two channel hand-offs, the engine peek and the actor turn), while
the large matrices benefit from the real parallel pumps instead of the
benchmark's synchronous flush loop.

## Decision

- Keep the post-change benchmark shape. It measures the path production actually
  runs, which the baseline shape did not.
- Treat the post-change numbers as the operational baseline for
  `BenchmarkOpenTickEndToEnd`. The baseline small-matrix numbers are not comparable
  and must not be read as a production regression.

## Consequences

- `BenchmarkOpenTickEndToEnd/clients=1/*` and `clients=10/scopes=1` are
  `accepted-with-ADR` in the re-run.
- Any future `PollOnce` optimization must be judged against the post-change shape;
  reverting the benchmark would price a code path production no longer uses.
- Future baseline comparisons start from the re-run numbers, not from the pre-change commit.
