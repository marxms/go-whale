# GoWhale Roadmap

This is a working roadmap, not a release calendar. It separates what is shipped,
what is known to be broken, what is blocked by an external proof and what is
planned. A venue is not "supported" until the capability matrix declares it and
the contract harness certifies it.

## How to read this document

| Status | Meaning |
| --- | --- |
| **Shipped** | Present in the current tree and covered by the product gate |
| **Documented** | A known operator condition with a runbook, not a code fix |
| **Next** | Agreed and unblocked; the next work to be scheduled |
| **Later** | Agreed, but depends on a prerequisite or on demand |
| **Blocked** | Needs an external proof, an owner decision or access we do not have |
| **Not planned** | Explicitly out of scope, with the reason recorded |

Work that is already fixed is not repeated here. The first public tree carries the
fixes recorded in [`CHANGELOG.md`](CHANGELOG.md); this roadmap lists only what is
still open, planned or blocked.

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

## Open work

| Area | Item | Impact | Source | Status | Exit criterion |
| --- | --- | --- | --- | --- | --- |
| CI | The workflows have not executed on GitHub | `actionlint` is not a runner; a job can fail on first contact | Owner decision: local-only until publication | **Blocked** | The first authorized push runs `ci`, `integration`, `schema` and `security`; a red job is fixed at the cause |
| Storage | The boot migration remains in the tree | Removing it before the proof can destroy the only copy of a candle | ADR 0012, ADR 0032 | **Blocked** | A production measurement of `SoleCopyKeysAtRisk == 0` recorded with `doctor`, then an owner decision to remove the migration |
| Operations | The LAN deployment has no read-only derivatives audit | Local proofs do not prove the destination | `docs/operations.md` | **Blocked** | Freshness, parity and absence recorded from the destination |
| Operations | A reused Redis volume can refuse boot under `noeviction` | A restart can refuse boot until the operator reconciles the volume | `docs/operations.md`, `compose.example.yaml` | **Documented** | The runbook and the Compose warning; the startup gate stays fatal by design |
| Operations | No repeatable soak result | No evidence about restarts, goroutine growth or latency under load | None published | **Later** | Restarts, goroutines, GC headroom and endpoint latency recorded and published with the protocol |
| Performance | Ten benchmarks are inconclusive under the protocol | An unmeasured number cannot be reported as a pass | ADR 0041, ADR 0042 | **Later** | Every case published as PASS, inconclusive or accepted-with-ADR; no inconclusive case reported as PASS |
| Data plane | No transportable kline feed outside `binance:future` | Scopes without a feed depend on REST candles and aggregation | `internal/market/capability.go` | **Later** | A venue publishes a kline stream this service can transport, or the matrix keeps the absence |
| Data plane | Interior gaps behind the head are not located | A hole behind the head can survive while the process reports healthy | `docs/adapters.md` | **Later** | A coverage index answers which buckets exist without scanning the whole series |
| Data plane | The process-wide REST budget still lives in the composition root | The cap is enforced there, but a new driver caller can bypass it | `docs/adapters.md` | **Later** | The same budget and classification move behind the driver without changing the cap |
| Release | Release automation and a published container image | No tag, notes or checksums exist until the checklist runs | [Release checklist](#release-checklist) | **Later** | The checklist produces a tag, notes and checksums; an image is built from the same Dockerfile and tagged with the commit |
| Developer experience | `/openapi.json` has routes but no examples | Consumers must infer payloads from `docs/api.md` | `docs/api.md` | **Later** | Examples can be added without changing the wire |
| Developer experience | No client SDK | Every consumer writes its own HTTP/WS client | OpenAPI contract | **Later** | Only after the v1 API is stable; the OpenAPI document remains the source |
| Developer experience | No Grafana dashboard | The metric names exist, but there is no ready dashboard | `docs/operations.md` | **Later** | The dashboard JSON is published next to the metric names it queries |

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
- [x] Four independent reviews and an independent verifier.
- [ ] Publication, after the reviews and the owner's explicit authorization.

### v0.2.0 — the first new venue and the first public CI run

- [ ] Bybit (the first P0 target) passes the full certification checklist.
- [ ] GitHub Actions runs the same gates locally validated at launch.
- [ ] `make verify` green with the new scope.

### v0.3.0 — spot coverage and release engineering

- [ ] Coinbase and Kraken certified for their declared scopes.
- [ ] Release automation and a published container image.
- [ ] The boot-migration blocker either closed with the production proof or
      explicitly carried with the same blocker.
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
2. **Changelog.** Confirm the `0.1.0` entry is dated, move any `Unreleased`
   entries into it, and keep the Keep a Changelog sections.
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
