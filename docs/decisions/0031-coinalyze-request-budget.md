# 0031 — Coinalyze request budget and refusal classification

- **Status:** Accepted
- **Date:** 2026-11-06

## Context

Coinalyze accepts up to 20 symbols per request and bills one call per symbol
against a documented ceiling of 40 calls per minute per API key. A successful
response carries no `X-RateLimit-*`, `RateLimit-*` or equivalent counter; the
only rate-limit signal is the `Retry-After` of a 429, which arrives after the
quota has already been spent. The only other consumer of this host in the
process is the markets table, one call every six hours.

Treating a request as one call makes the process spend up to 20x what it
believes it spends. A bucket that banks a whole minute of allowance paces
nothing: it accumulates while the process waits and dumps forty calls the
instant the wait ends. A control probe waited the full 39.992 seconds the
provider announced and the first following call was refused 0.18 seconds later,
with zero successes; the window reopens and the fleet re-exhausts it at once.
Reading a fractional `Retry-After` without a burst ceiling makes that storm
worse, because shorter correct waits hit the provider sooner. In a LAN
measurement, 82% of resends were refused again and 38% of the budget went to
refused requests.

## Decision

- Budget calls per symbol, not per request. A request above 20 symbols is
  refused rather than clamped: clamping would send 21 symbols to the network
  having paid for 20.
- Keep local accounting. Each attempt is charged, including a resend of a 429,
  because the refused request reached the provider and counts against its
  ceiling.
- The bucket starts empty and accumulates at most one full request. The rate
  stays the provider's; calls leave spread across it instead of in minute-sized
  blocks.
- A 429 suspends all spending for at least the provider's `Retry-After`, or one
  minute when the header is absent. The suspension is process-wide because the
  provider's ceiling is per process; the resend wait alone is per request.
- The suspension is checked before the balance. Checking the balance first
  lets the next batch spend a refilled bucket against a provider that has just
  refused.
- The suspension duration comes from the provider, never from a local tuned
  value. Shortening it is resending before the provider allows.
- When a 429 arrives while local spend in the previous minute is strictly below
  the configured ceiling, the refusal cannot be explained by this process's
  emission: the quota was consumed by another consumer of the same key. The
  process reports that cause instead of blaming its own fleet size. It cannot
  identify the other consumer, and it does not try.

## Consequences

- The process can state what it spent and how many refusals it received; the
  remainder is inference from those two measurements.
- Resolving a shared-key refusal is an operational decision: a dedicated key per
  environment, or disabling collection in the laboratory.
- Budget tallies are monotonic. A cycle's spend is the difference between two
  readings; resetting them would make concurrent readers steal counts from each
  other.
