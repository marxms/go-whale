# 0041 — Hub fan-out keeps the transport-free Member/Lane seam

- **Status:** Accepted
- **Date:** 2026-11-06

## Context

The baseline measured `BenchmarkHubFanoutClose` inside `adapters/inbound/realtime`, where
the hub held `*Client` values and fan-out deleted the conflated open and pushed
to the client's channel directly. The fan-out change moved the actor to `internal/distribution`
and introduced two seams: `Member`, which hides the transport, and `Lane`, whose
`RWMutex` makes a send and the close of the same channel non-overlapping even if
a caller violates the single-owner rule.

The re-run used the baseline protocol on the post-change tree (`-count=5 -benchtime=300ms`).
At 100 clients the fan-out regressed, while the 1-client and 10-client cases
improved or stayed equal:

| Subcase | Baseline median | Re-run median | Δ | CV (baseline → re-run) |
| --- | ---: | ---: | ---: | --- |
| `clients=100/scopes=10` | 3,610 ns | 4,840 ns | +34.1% | 2.4% → 0.8% |
| `clients=100/scopes=100` | 3,694 ns | 4,890 ns | +32.4% | 3.9% → 0.4% |

The ranges are disjoint. A same-registry probe isolated the cost per member:

| Inner loop, 100 clients | ns/op |
| --- | ---: |
| Pre-change body (one member mutex + direct channel send) | 3,250 |
| Current body with `Lane.Push`'s `RWMutex` removed | 4,110 |
| Current body (`Member.DeleteOpen` + `Lane().Push`) | 4,690 |
| Current `Hub.fanout` over the interface-keyed index | 5,000 |

So the seam costs about 14 ns per member: roughly 6 ns for the lane's
close-safety lock and 8 ns for interface dispatch, the extra lane indirection
and the interface-keyed registry map. A combined `Member.PushClose` method was
also measured and did not help (about 5.4 µs with the lock); the lock and the
dispatch remain.

The absolute number matters more than the ratio: one close at 100 clients costs
4.9 µs on the actor, and the boundary burst benchmark prices the real 18-scope
burst end to end. The actor is still the global ceiling, but 4.9 µs is not a
liveness risk and the lanes stay lossless.

## Decision

- Keep the `Member`/`Lane` seam and accept the 100-client fan-out cost. It is the
  price of a hub that no longer depends on a WebSocket connection and of a lane
  whose close cannot race a send.
- Do not remove the lane lock or reintroduce `*Client` in the hot loop without a
  measurement showing that the invariant is enforceable some other way.
- Record the post-change numbers as the fan-out baseline; the baseline numbers are
  pre-extraction history.

## Consequences

- `BenchmarkHubFanoutClose/clients=100/scopes={10,100}` is
  `accepted-with-ADR` in the re-run, not a silently ignored regression.
- Future work that needs the ratio back must target the two costs the probe
  identified: a safe way to prove the single-owner invariant to the lane, and a
  cheaper delivery seam than two interface calls per member.
- The benchmark itself is unchanged; the measurement and the alternatives
  considered live here, not in a comment.
