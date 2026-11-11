# Contributing

Thanks for taking the time to improve GoWhale. The project is a market-data
service; a change is judged by the behaviour it proves, not by the size of the
diff.

## Before you start

1. Open an issue describing the behaviour you want to change and the evidence that
   it is wrong or missing. A screenshot, a log line, a failing test or a documented
   venue contract is evidence; a preference is not.
2. For a change that alters a durable decision (a port, a storage contract, a
   configuration rule or a venue boundary), propose an ADR under `docs/decisions/`
   using the format of the existing records. A decision that will outlive the pull
   request belongs in an ADR, not only in the discussion.
3. Keep the change inside its scope. Do not mix a refactor with a behaviour change;
   split the pull request when the two are separable.

## Setup

Requirements: Go 1.26.10 (the `toolchain` directive lets Go download it), Redis
8.x, and Python 3 for the import-boundary gate. QuestDB is only needed for the
integration suite.

```bash
cp config.example.yaml config.yaml
make tools-check
make verify
```

`make verify` is the product gate. It runs formatting, lint, import boundaries,
module hygiene, vulnerabilities, vet, the unit and race suites, the executed
test-case count, integration compilation and the binary build. A pull request that
does not include its result is not ready for review.

## Test first

Add a test that fails without your change and passes with it. The test must assert
an observable effect: a value, a persisted state, a typed error, a metric or a
wire response. A test that only exercises a helper without a consumer effect is not
proof.

Race-sensitive packages (`ingestion`, `recovery`, `distribution`, `realtime`,
`app`, the engine) also run under `make race`. Integration tests use real Redis and
QuestDB; they are never replaced by mocks in the verification report.

## Code rules

- Application code stays under `internal/`.
- Domain packages (`market`, `trade`, `candle`, `derivatives`) do not import
  configuration, adapters, Redis, QuestDB or Prometheus.
- New ports are declared by the consumer and kept as small as the consumer needs.
- Absent, zero, transient, contract divergence and uncertain write stay distinct
  states. Do not collapse them into a default.
- Normalize or reject venue data once, at the write gate. Do not add a per-venue
  normalizer.
- Never add a second API, configuration format or fallback path to keep an old
  client alive. Migrate the consumer in the same change.
- Comments explain the why, in one sentence, next to the code. A long rationale
  belongs in an ADR or a document.

## Adding a venue

A new venue is not a registry row plus a driver file. It must pass every item of
the certification checklist in [`docs/adapters.md`](docs/adapters.md): route
classes, connection policy with documented limits, capability matrix row, canonical
DTOs, normalization at the write gate, contract harness, live smoke, request budget
and license compatibility.

Do not open a pull request that adds a venue stub returning "not implemented" and
describes it as support. The capability matrix is the only support claim.

## Commits

Use [Conventional Commits](https://www.conventionalcommits.org/) in English, for
example:

```text
fix(recovery): fetch every missing bucket on an explicit window
feat(venue): add the OKX capability row
docs(roadmap): move Bybit to the next milestone
```

Keep the subject in the imperative mood and under 72 characters. Explain the why in
the body when the change is not obvious.

## Licensing and provenance

There is no CLA. Contributions are accepted under the project's MIT license
(inbound = outbound). A `Signed-off-by` line is welcome and encouraged, but it is
not enforced by CI. Do not paste code from a project with an incompatible license;
the dependency and license inventory must stay MIT-compatible.

## Security and conduct

Do not open a public issue for a vulnerability. Follow
[`SECURITY.md`](SECURITY.md). All participants are expected to follow the
[`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md).

## Review

A reviewer will check the behaviour, the proof, the scope and the failure modes
(ordering, restart, partial write, absence vs zero, cancellation). A feature that
changes the product contract also needs an independent verification before it is
declared done. The author does not verify their own feature.
