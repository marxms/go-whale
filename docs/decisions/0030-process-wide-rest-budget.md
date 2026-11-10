# 0030 — One process-wide REST weight budget for Binance

- **Status:** Accepted
- **Date:** 2026-11-06

## Context

Binance bills USDT-M futures request weight per IP, not per component. Before
the shared budget, every REST consumer paced itself in isolation: the seed used
`errgroup.SetLimit`, the derivatives poller used its own interval, gap recovery
used its own batching and periodic discovery had no limit at all. Each policy
was defensible on its own, but their sum was not bounded by anything.

On 2026-07-29 a 500-symbol deploy produced 4,602 HTTP 429 responses in its boot
burst and rolled back 117 symbols, because the boot-gap batch fired about 30
seconds after boot while the seed was still running. After a power outage the
same collision cut the fleet to 34 symbols. In both cases no single component
was misconfigured; the aggregate was unbounded.

## Decision

- The process keeps one `exchange.RestBudget`, a token bucket sized at 75% of
  Binance's documented 2,400 weight/min per-IP ceiling. The remaining 600
  weight/min absorb what the process cannot meter: the websocket control plane,
  retries inside the venue adapter and the requests Binance rejects but still
  counts.
- The bucket starts empty. A process that boots and immediately spends a full
  bucket reproduces the burst above, so the first callers wait for refill.
- Consumers reserve the weight of a call immediately before issuing it, so the
  reservation and the spend stay adjacent and two consumers cannot interleave
  past the cap.
- A klines call is billed by half-open limit tiers (`[1,100) -> 1`,
  `[100,500) -> 2`, `[500,1000] -> 5`, above 1000 -> 10). Reading the 500
  boundary as inclusive under-counts an 801-bar seed by 2.5x.
- A 429/418 suspends spending for every consumer for at least two minutes.
  Binance's documented minimum IP ban is two minutes and escalates on repeat,
  so backing off per component guarantees walking into the next tier.

## Consequences

- When the fleet grows, every consumer slows proportionally instead of the
  fleet exceeding the cap.
- A refusal is a process-wide event: one consumer observing 429/418 brakes all
  REST spending, which is the only backoff that works against a per-IP limit.
- The budget is package-level state on purpose; threading it through every
  signature on the seed -> gap recovery -> derivatives -> discovery path would
  still resolve to the same single instance at runtime.
