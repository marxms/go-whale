# Security policy

## Supported versions

| Version | Supported |
| --- | --- |
| `0.1.x` | Yes |
| Older / unversioned builds | No |

GoWhale is pre-1.0. Security fixes land in the latest minor release; backports are
not promised before 1.0.

## Reporting a vulnerability

Do **not** open a public issue for a vulnerability.

Use GitHub's private vulnerability reporting on this repository
(**Security → Report a vulnerability**). Include:

- the affected version and commit (`gowhale version` and `go version -m`);
- the configuration shape, with credentials and host names redacted;
- a minimal reproduction and the impact you observed;
- whether the issue is reachable from a public network or requires local access.

The project never asks for exchange credentials, API keys, private keys or
production database access in a report. Do not include them.

We will acknowledge the report and coordinate a fix before public disclosure. There
is no bug bounty.

## Scope

This policy covers the GoWhale server, its container image and the code in this
repository. It does not cover the exchanges it reads from; report a venue issue to
the venue.

## Dependency and license posture

The module is MIT and must stay MIT-compatible. A GPL-3.0 dependency in the binary
is a release blocker. The gate runs `make vuln` (`govulncheck`), and the dependency
inventory is reviewed when a module is added or upgraded. `make verify` also runs
the module-hygiene checks and the import-boundary check.

## WebSocket edge

The chart WebSocket edge is opt-in. It mounts only when both
`GOWHALE_REALTIME_WS_TICKET_SECRET` and `GOWHALE_REALTIME_WS_MINT_TOKEN` are set to
at least 32 random bytes. The authenticating gateway must verify the user and inject
`X-Gowhale-Mint-Token` on `/api/ws-ticket`; the mint endpoint is not a user-auth
boundary by itself, so do not expose it directly to the internet. The server sets
`Cache-Control: no-store` on tickets, validates scopes against the capability
matrix, and caps connections and per-connection subscriptions. Ticket secrets and
mint tokens are environment-only in the published examples and must never be
committed.

## Operational hardening

- Run the binary as a non-root user; the example image uses an unprivileged user.
- Mount the configuration read-only and keep credentials in environment variables.
- Put Redis on a private network and enable authentication outside a throwaway
  stack.
- Do not expose `/metrics` publicly without a reverse proxy or network policy.
- The `/api/ws` endpoint requires a short-lived ticket; keep the ticket secret out
  of logs.
