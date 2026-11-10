# Development

## Toolchain

- Go 1.26.10 (pinned by the `toolchain` directive in `go.mod`)
- Docker and Docker Compose (for the example stack and integration checks)
- Python 3 (for the import-boundary gate)
- `golangci-lint` v2.13.2, `govulncheck` v1.1.4 and `actionlint` v1.7.7 are
  invoked by pinned version from the Makefile; `make tools-check` verifies the Go
  toolchain and fails if a tool version is not pinned

## Gates

```bash
make tools-check
make fmt-check
make lint
make boundaries
make workflow-lint
make tidy-check
make mod-verify
make vuln
make vet
make test
make race
make test-count
make integration-compile
make build
make verify
```

`make verify` runs every gate without skipping: toolchain/tool pins, formatting,
lint, import boundaries, workflow lint, `go mod tidy -diff`, `go mod verify`,
govulncheck, vet, the unit suite, the race suite, the executed test-case count,
compilation of the integration-tagged tests, and the binary build. It stops at
the first failing stage with a message naming the stage.

`make test-count` derives the count from `go test -list '^Test' ./...` and
fails when zero cases are discovered. `make integration-compile` compiles the
`integration` build tag without running the suites; it also proves that
`cmd/gowhale` and every test package still build.

`make integration` runs the suites behind the `integration` build tag; they
expect the services named in their fixtures.

## Local Compose

The minimal journey is Redis plus GoWhale:

```bash
docker compose -f compose.example.yaml up -d --build
curl -fsS http://127.0.0.1:3000/health
docker compose -f compose.example.yaml down
```

Redis binds only to the private Compose network; GoWhale publishes HTTP on
`127.0.0.1`. The config is mounted read-only, and Redis data lives in the named
volume `redis-data`. `down -v` deletes that volume and is only safe on a
throwaway environment.

QuestDB is not part of the example journey. Integration and schema checks use
the separate stack:

```bash
docker compose -f compose.integration.yaml up -d
make integration
docker compose -f compose.integration.yaml down
```

Apply the GoWhale DDL with a QuestDB pgwire client before the integration run:

```bash
psql "postgres://admin:quest@localhost:8812/qdb" \
  -f scripts/questdb/questdb-init.sql
```

## Tests

- Unit tests use `miniredis` for Redis behaviour and fakes for externals.
- Race-sensitive paths are covered by `make race`; run it after touching
  streaming, lifecycle or tier code.
- Integration tests are opt-in and never silently downgraded to mocks.

## Dead code

`make dead-code` runs the pinned `deadcode` tool over `./cmd/...`. It is advisory:
a static reachability tool cannot see test-only helpers, interface implementations
or the public constructors a consumer may use. An unreachable function must still
have a caller, a test that exercises it, or a documented interface role before it
enters the tree; "it might be useful" is not a reason to keep it.

## Adding an exchange

1. Declare a value object in `market` if the venue adds vocabulary.
2. Implement the exchange client behind the existing ingestion interfaces.
3. Translate venue payloads into `trade.Trade` and `candle.Candle` in the
   adapter only; the domain must not import vendor types.
4. Add characterization tests for ordering, reconnect, gap detection and
   shutdown before wiring it into `app`.
