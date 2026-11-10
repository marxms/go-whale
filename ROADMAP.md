# GoWhale Roadmap

This is a working roadmap, not a release calendar. It separates what is shipped,
what is known to be broken, what is blocked by an external proof and what is
planned. A venue is not "supported" until the capability matrix declares it and
the contract harness certifies it.

## How to read this document

| Status | Meaning |
| --- | --- |
| **Shipped** | Present in the current tree and covered by the product gate |
| **Resolved** | A known problem whose fix is present and covered by a regression test |
| **Documented** | A known operator condition with a runbook, not a code fix |
| **Next** | Agreed and unblocked; the next work to be scheduled |
| **Later** | Agreed, but depends on a prerequisite or on demand |
| **Blocked** | Needs an external proof, an owner decision or access we do not have |
| **Not planned** | Explicitly out of scope, with the reason recorded |

Priority is about correctness and user impact, not about how easy the work is.

## Principles

1. **Correctness before coverage.** A venue that publishes an unmeasured number is
   worse than a venue that declares the number absent.
2. **One canonical path.** No second API, configuration format or fallback to keep
   an old client alive. Migrate the consumer in the same change.
3. **Declared absence over guessed default.** Absent, zero, transient, contract
   divergence and uncertain write stay distinct states.
4. **Normalize once, at the write gate.** Venue adapters return canonical values;
   impossible OHLC is normalized or rejected in one place, not per venue.
5. **Production-only data.** The production venue hosts are the default. The
   testnet is not used in any environment; a production chart fed by testnet
   candles is worse than a visible absence.
6. **No fabricated PASS.** A benchmark with a coefficient of variation above the
   protocol threshold is inconclusive, not a pass.

## Data-integrity invariants

These invariants constrain every current and future venue adapter. A change that
cannot satisfy one of them ships a declared absence instead. Normalization at the
write gate (principle 4 above) is the fourth.

1. **Settled kline authority.** A venue kline feed is a hint, not history. The
   bucket written by trade aggregation is authoritative; a kline may only fill a
   bucket the trade stream cannot observe, and it never overwrites a settled
   candle. Recovery and seeding use the same rule.
2. **Taker-buy/sell identity.** CVD is the sum of attributable taker buy and sell
   volume and must reconcile with total volume for the same trade set. A venue
   that cannot attribute both sides declares CVD absent; the adapter does not
   estimate it.
3. **Sole-copy guard.** Data with a single durable copy is protected. Pruning and
   migration refuse to delete a key that no other tier holds, so a future tier or
   venue cannot silently become the only copy.

## Current state

Prepared for the first public release (publication pending owner authorization):

- Standalone Go module `github.com/marxms/go-whale`, MIT, no CCXT and no GPL-3.0
  dependency.
- Binance USD-M futures as the reference scope; Binance spot, BingX spot and
  BingX swap through the same native REST adapter.
- Owned WebSocket transport for Binance and BingX, with cancellation-safe
  lifecycle, deadlines, renewal and idempotent close.
- Trade aggregation into configurable timeframes, deterministic open/close
  election by trade id, and taker buy/sell attribution.
- Gap detection, historical seeding and closed-bucket repair.
- Derivatives collection: funding, open interest, OI history and long/short ratio
  where the venue declares them.
- Redis hot tier, optional QuestDB cold tier, Prometheus metrics, `/health`,
  `/ready`, `/openapi.json`, WebSocket fan-out and the HTTP read API.
- Strict configuration loader with `GOWHALE_*` environment overrides.
- `gowhale serve`, `gowhale doctor` and `gowhale version`.
- Unit, race and integration-compile gates; `make verify` runs them all.

The capability matrix and the connection policy are in
[`docs/adapters.md`](docs/adapters.md) and
[`internal/market/capability.go`](internal/market/capability.go).

## Known problems

These are the items carried from the extraction debt register (2026-11-06), the
LAN validation (2026-11-07) and the multi-exchange recovery session. The table is
the public record; an item leaves it only when its exit criterion is met.

| ID | Problem | Status | Why it matters | Exit criterion |
| --- | --- | --- | --- | --- |
| P1 | CCXT pulled `go-ethereum` (GPL-3.0) into an MIT binary | **Resolved** | The license inventory could not describe the binary as MIT-only | Removed; `go list -m all` contains no CCXT or `go-ethereum` (ADR 0006) |
| P2 | Boot migration remains in the tree | **Blocked** | Removing it before proving `SoleCopyKeysAtRisk == 0` against production can destroy the only copy of a candle | A production measurement recorded with the `doctor` command, then an owner decision to remove the migration |
| P3 | CI workflows have never executed on GitHub | **Next** | Local `actionlint` is not a substitute for a real runner | The first push runs `ci`, `integration`, `schema` and `security`; failures are fixed at the cause |
| P4 | The public repository was never published | **Blocked** | The code existed only as an internal module | The launch commits exist in a local repository; publication requires an owner-authorized push |
| P5 | LAN derivatives data was not audited at the destination | **Blocked** | Local proof does not prove the deployed data | A read-only audit of the destination, with freshness, parity and absence recorded |
| P6 | Ten benchmarks are inconclusive (CV > 10%) | **Later** | A number outside the protocol cannot become a PASS by repetition | Re-run with the same protocol and publish PASS, inconclusive or accepted-with-ADR per case |
| P7 | Comment debt outside the Go module | **Not planned here** | The TypeScript/React corpus is a separate codebase, not this repository | Out of scope; tracked by the owner outside this repository |
| P8 | Stale CCXT references in docs | **Resolved** | Documentation that names a removed adapter sends contributors to a dead end | Zero current-support CCXT references; the historical ADR stays marked as history |
| M1 | Recovery can no-op on an explicit window | **Resolved** | A backstop job with `window.start > 0` can refetch only the straddling bucket, so the series freezes behind a stale head while the process still reports healthy | Every missing bucket in `[window.start, now]` is written; the head advances; a no-progress breaker suspends and records the scope |
| M2 | Seed writes do not normalize impossible OHLC | **Resolved** | A venue can publish a weekly candle with `high < open`; an impossible candle must not enter Redis or the engine | Every seeded candle passes the write gate; the real BCHUSDT 1w line from 2020-01-13 is a regression fixture |
| M3 | The driver capability contract is not exercised for every scope | **Resolved** | A matrix row without a harness is a declaration, not a certification | The four scopes pass `TestVenueContractFourScopes` for every driver capability; every absent driver capability returns its typed error and records the metric |
| M13 | Boot marking covers only the kline feed interval | **Resolved** | Coarser buckets stay unmarked and their repair is vetoed, so the series can freeze while the process is healthy | Every configured timeframe of the open bucket is marked; a coarser bucket's repair is proven not to be vetoed |
| M4 | The testnet was reachable as a configuration default | **Resolved** | Testnet prices are production-like but not production data | Production hosts are the default; `.env.example` no longer advertises the testnet; a test asserts the policy |
| M5 | Only `binance:future` has a transportable kline feed | **Later** | The other scopes need trade aggregation and REST candles; the matrix declares the absence | A venue publishes a kline stream this service can transport, or the scope stays declared absent |
| M6 | Interior gaps behind the head are not located | **Later** | The recovery window derives from the head; a hole in the middle needs a coverage index | A coverage index answers "which buckets exist" without scanning the whole series |
| M7 | Historical open-interest backfill is absent | **Later** | Current OI and OI history are not the same product | A per-venue historical source, its budget and its absence semantics are designed |
| M8 | The REST budget and the pacer are split | **Later** | Two budget mechanisms are two ways to be wrong about the same limit | One process-wide budget owns every venue read, with the same classification |
| M10 | A reused Redis volume can refuse boot under `noeviction` | **Documented** | Resident families without TTL can hit the ceiling after a restart | The operator runbook and the Compose warning; the startup gate stays fatal by design |
| M11 | The binary had no public version surface | **Resolved** | A user could not identify the build in a report | `gowhale version` prints version, commit and build date; `go version -m` shows the module |
| M12 | Dead code and task narrative remained | **Resolved** | A public tree should not carry abandoned paths or phase labels | `go tool deadcode ./cmd/...` is clean or every survivor is justified; no `TASK`/`Fase`/`Trilha` narrative |

## Venue expansion

### Selection criteria

A venue enters the plan when all of the following are true:

1. It publishes public market data without requiring an account for the scope we
   want.
2. Its WebSocket or REST surface can be transported without a vendor SDK in the
   core.
3. Its rate limits and connection policy are documented.
4. Its license terms allow a native, MIT-compatible adapter.
5. There is a real user need for the venue in the scope we can certify.

### Candidate venues

This is a plan, not a support matrix. Only the four scopes in the README are
supported today.

| Venue | Market types | Expected capabilities | Priority | Why this position |
| --- | --- | --- | --- | --- |
| **Bybit** | spot, linear and inverse perpetuals | trades, klines, funding, OI, L/S ratio, markets | **P0** | Large public derivatives venue with a documented WS/REST surface; the closest second reference scope to Binance futures |
| **OKX** | spot, swap, futures | trades, klines, funding, OI, L/S ratio, markets | **P0** | Broad public market data and a mature API; covers spot and derivatives with one vocabulary |
| **Coinbase** | spot | trades, REST klines, markets | **P1** | High-quality spot reference and a large user base; no funding/OI in the spot scope |
| **Kraken** | spot, futures | trades, REST OHLC, markets; funding where the futures API declares it | **P1** | Long-standing public data and a conservative API; useful as a second spot reference |
| **Hyperliquid** | perpetuals (on-chain order book) | trades, candles, funding, OI, markets | **P2** | A different market model and a real user demand for on-chain perps; needs its own absence and integrity analysis |
| **Backpack** | spot, perpetuals | trades, klines, funding where declared, markets | **P2** | Smaller venue with a modern API; valuable to prove the adapter seam is not Binance-shaped |
| **Bitget** | spot, swap | trades, klines, funding, OI, markets | **P3** | Broad coverage; behind the P0/P1 venues on public-data maturity |
| **Gate.io** | spot, swap | trades, klines, funding, OI, markets | **P3** | Broad coverage; the adapter cost is similar to Bitget, so sequence by demand |
| **Deribit** | futures, options | trades, OHLC, funding; options need a new domain model | **P4** | Options are not in the canonical candle/derivatives vocabulary; only pursue if that model is designed |

**Not planned:** venues whose public data requires an account, venues whose terms
forbid a native adapter, and venues whose only differentiator is a logo in the
README. The hero artwork is illustrative; it is not a support claim.

### Venue certification checklist

A new scope is certified only when every item below has evidence. The checklist is
the same for every venue; the reference implementation is `binance:future`.

1. **Route classes.** Declare the WebSocket route per stream in
   `market.ConnPolicyFor` (`/public`, `/market`, `/private`) and dial it. A
   connection without the documented route returns 101 and delivers only the
   public class.
2. **Connection policy.** Declare server ping, pong deadline, read idle, socket
   lifetime, stream and connection limits, and keepalive encoding. Each value
   carries the `DocsURL` that sustains it; zero means "not documented".
3. **Capability matrix.** Add the row in `internal/market/capability.go`. Every
   capability the reference scope has must be present or declared absent with a
   reason; an absent method returns `CapabilityAbsentError` and records the metric.
4. **Canonical DTOs.** The adapter returns `trade.Trade`, `candle.Candle` and the
   derivatives values. No vendor type crosses the package boundary.
5. **Integrity.** Normalization and rejection happen once at the write gate
   (`recovery.prepareCandles`), never per venue. The BCHUSDT 1w fixture proves the
   impossible-OHLC path.
6. **Contract harness.** Add the scope to `TestVenueContractFourScopes`. Every
   present capability answers with a typed, non-empty result from a recorded
   fixture; every absent capability returns its typed error.
7. **Live smoke.** A read-only smoke against the public venue records markets,
   OHLCV, funding/OI/LSR where present, and the taker-flow identity where CVD is
   present.
8. **Budget and failure classes.** The venue read goes through the shared request
   budget or a declared local limiter, and 429/418/-1003, 5xx/timeout and 4xx are
   classified as rate-limited, transient and permanent.
9. **License and documentation.** The dependency and license inventory stays
   MIT-compatible; the adapter page, fixture provenance and operational notes are
   updated in the same change.

A scope that fails any item does not ship.

## Data plane

| Item | Status | Next step |
| --- | --- | --- |
| Generic recovery over explicit windows (M1) | **Resolved** | Make a job with `window.start > 0` fetch the whole `[start, now]`; keep the one-bucket shortcut only for a short reconnect without an explicit window. Add a no-progress breaker test. |
| Seed normalization (M2) | **Resolved** | Normalize or reject every seeded candle at the write gate; keep the BCHUSDT 1w regression. |
| Driver capability harness for every scope (M3) | **Resolved** | `TestVenueContractFourScopes` exercises the four scopes and every declared driver absence. |
| In-flight timeframe marking at boot (M13) | **Resolved** | Mark every configured timeframe of the open bucket, not only the kline feed interval; prove that a coarser bucket's repair is not vetoed. |
| Interior gap coverage (M6) | **Later** | Design a coverage index keyed by symbol and timeframe; do not scan the whole series per cycle. |
| Historical OI backfill (M7) | **Later** | Pick the first venue with a documented historical endpoint; design its budget and absence semantics. |
| Kline feeds beyond the reference scope (M5) | **Later** | Transport a venue kline stream only where it exists; otherwise keep the declared absence. |
| REST budget consolidation (M8) | **Later** | Move the pacer behind the same process-wide budget, with one classification table. |

## Reliability and operations

| Item | Status | Exit criterion |
| --- | --- | --- |
| Boot migration removal (P2) | **Blocked** | Production proof of `SoleCopyKeysAtRisk == 0` and an owner decision |
| CI on GitHub (P3) | **Next** | The four workflows run on the first push; a red job is fixed, not ignored |
| Release automation | **Later** | A tag, changelog, checksums and a GitHub release produced by the [release checklist](#release-checklist); no prebuilt binary is claimed before it exists |
| Container image | **Later** | A published image is built from the same Dockerfile and tagged with the commit; the README only claims it after publication |
| Soak and load | **Later** | A repeatable soak records restarts, goroutines, GC headroom and endpoint latency; the result is published with its protocol |
| Benchmarks (P6) | **Later** | Every case is PASS, inconclusive or accepted-with-ADR; no inconclusive case is reported as PASS |
| LAN derivatives audit (P5) | **Blocked** | Read-only destination audit with freshness, parity and absence recorded |

## Developer experience

| Item | Status | Notes |
| --- | --- | --- |
| `gowhale version` (M11) | **Shipped** | Version, commit and build date; `go version -m` also identifies the module |
| OpenAPI examples | **Later** | The route contract is served at `/openapi.json`; examples can follow without changing the wire |
| Client SDK | **Later** | Only after the v1 API is stable; the OpenAPI document is the source |
| Grafana dashboard | **Later** | Publish the dashboard JSON next to the metric names it queries |
| Reproducible local stack | **Shipped** | `compose.example.yaml` for Redis + GoWhale; `compose.integration.yaml` for QuestDB checks |

## Milestones

Milestones are exit criteria, not dates. A milestone can slip; a criterion cannot
be weakened to make it pass.

### v0.1.0 — first public tree (publication pending)

- [x] Standalone MIT module, no CCXT/GPL.
- [x] Binance futures reference scope; Binance spot and BingX spot/swap declared.
- [x] Capability matrix and connection policy documented.
- [x] Venue contract harness exercises the four scopes and every declared driver absence.
- [x] Recovery over explicit windows, seed normalization and boot gap marking covered by regression tests.
- [x] `serve`, `doctor` and `version`.
- [x] `make verify` green in the new repository.
- [x] Brand assets, README, roadmap, community files and local CI validation.
- [ ] Four independent reviews and an independent verifier.
- [ ] Publication, after the reviews and the owner's explicit authorization.

### v0.2.0 — the first new venue and the first public CI run

- [ ] Bybit (the first P0 target) passes the full certification checklist.
- [ ] GitHub Actions runs the same gates locally validated at launch.
- [ ] `make verify` green with the new scope.

### v0.3.0 — spot coverage and release engineering

- [ ] Coinbase and Kraken certified for their declared scopes.
- [ ] Release automation and a published container image.
- [ ] P2 either closed with the production proof or explicitly carried with the
      same blocker.
- [ ] A published benchmark report with PASS, inconclusive and accepted-with-ADR
      verdicts.
- [ ] A repeatable soak report.

### v1.0.0 — stable contract

- [ ] At least six certified venues across spot and derivatives.
- [ ] Stable HTTP/WebSocket and configuration contracts with a documented
      compatibility policy.
- [ ] No open blocker or high-severity issue.
- [ ] The recovery and seed invariants are covered by regression tests that fail
      when the behaviour is removed.

## Release checklist

Not executed in this cycle. A release happens only after the reviews, the
owner's explicit authorization and a green `make verify` on the tagged commit.

1. **Version bump.** Set the semantic version in the linker flags
   (`-X .../internal/platform/version.Version=…`) and confirm `gowhale version`
   and `go version -m bin/gowhale` agree.
2. **Changelog.** Move `Unreleased` entries under a dated heading in
   `CHANGELOG.md`, keeping Keep a Changelog sections.
3. **Gate.** Run `make verify` and the local workflow lint on the exact commit;
   record the log next to the tag.
4. **Tag.** Create an annotated tag on that commit; never tag a dirty tree.
5. **Checksums.** Build the binary from the tag and publish its SHA-256, plus the
   asset checksums in `docs/branding.md` when the artwork changed.
6. **GitHub release.** Notes come from the changelog; the release links the
   checksums and states the supported scopes from the capability matrix.
7. **Container image (future).** Only after an image workflow exists: build from
   the same `Dockerfile`, tag it with the commit, and update the README before
   claiming it.
8. **Post-release.** Verify the published artifacts against the checksums and
   record the result.

## What this roadmap does not promise

- Order execution, custody, portfolio management or trading signals.
- A hosted service, an SLA or paid support.
- A date for any venue or milestone.
- Support for a venue that the capability matrix does not declare.

## How to influence the roadmap

Open an issue with the problem, the evidence and the scope you are proposing. For a
durable architectural decision, propose an ADR under `docs/decisions/` using the
same format as the existing records. A roadmap change that adds a venue should name
the certification checklist item it satisfies and the fixture that proves it.
