# 0001 — Extract GoWhale as a standalone Go module

- **Status:** Accepted
- **Date:** 2026-11-05

## Context

GoWhale grew inside a private monorepo, in an application directory that also
carried deployment and sibling-service code. The product needs to be buildable and
publishable without the monorepo, its sibling directories, private configuration or
a vendored local module. Keeping a second copy of the application inside the
monorepo would create two active implementations of the same contract.

The extraction target is a normal Go module whose repository root is the
application root. The module path is `github.com/marxms/go-whale`, as declared in
`go.mod`.

## Decision

- The extracted repository is one Go module, `github.com/marxms/go-whale`, with
  no `replace`, no `go.work`, no nested module and no `third_party/` tree.
- The application root is the directory that held the application inside the
  monorepo: `cmd/`, `internal/`, `docs/`, the build files and the example
  configuration move together.
- There is exactly one executable, `cmd/gowhale`. Running it with no arguments
  starts the server; `gowhale doctor` runs the diagnostic mode.
- Imports use the module path. Application code stays under `internal/`, so the
  module makes no library compatibility promise.
- The extraction itself is performed outside the public repository, from a
  filtered clone with an explicit denylist. The public repository receives the
  application tree, not the monorepo history or private migration artifacts.

## Consequences

- Analyzer, web and deployment consumers must be adapted before cutover; the
  extraction of the code is not the migration of the running system.
- Paths in the documentation are written relative to the application root
  (`internal/...`, not a monorepo-relative prefix).
- A dependency that only builds through a local replacement is not eligible for
  the public candidate.
- Build, test, container and release commands must be exercised on the extracted
  tree, not only inside the monorepo.
