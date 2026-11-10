# 0002 — Own the Binance WebSocket transport

- **Status:** Accepted
- **Date:** 2026-11-05

## Context

The previous tree depended on a vendored WebSocket fork to control connection
lifecycle, buffers and dispatch. The upstream Binance Go SDK WebSocket client was
measured to allocate fixed 16 MiB read and write buffers per connection,
dispatch each message on its own goroutine and keep reconnection private. Those
properties reintroduce ordering and memory defects that the application had
already paid to remove.

Keeping a fork as the long-term solution was rejected: it is an unmaintained
second implementation of the transport and prevents ordinary `go.mod` upgrades.

## Decision

- Binance market streams use an owned transport in
  `internal/adapters/outbound/binancews`, built on
  `github.com/gorilla/websocket`.
- The transport installs handlers before the first frame is read, uses one
  sequential reader, bounded read and write buffers, read deadlines, ping/pong,
  explicit subscription management, connection renewal before the venue's
  24-hour limit and idempotent close.
- `internal/adapters/outbound/binance` decodes a minimal adapter DTO for the
  fields it consumes and converts it to `trade.Trade` and `candle.Candle`.
  Vendor WebSocket models never cross the adapter boundary.
- The upstream Binance SDK may remain in `go.mod` for REST calls where it still
  has an advantage. It is resolved from the module cache, with no local
  `replace`.
- No vendored WebSocket SDK, fork, nested module or second transport is kept as
  a silent fallback. If transport parity is not achieved, the extraction stops
  and the design is revised.

## Consequences

- The project owns connection lifecycle, backpressure and reconnection tests.
  Frame ordering, renewal, close idempotency and memory behaviour are covered by
  the package's own tests and by characterization against the previous
  behaviour.
- Binance SDK upgrades affect REST only; a WebSocket regression cannot be fixed
  by restoring the fork.
- Operational overrides for the public stream hosts remain available as
  environment variables, so a venue host change does not require a rebuild.
