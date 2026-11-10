# 0006 — Remove CCXT to keep the MIT license

- **Status:** Accepted — Option A (remove CCXT)
- **Date:** 2026-11-06

## Context

The Go module depended on `github.com/ccxt/ccxt/go/v4`. CCXT pulled
`github.com/ethereum/go-ethereum` (GPL-3.0) into the linked binary through
packages such as `common/hexutil`, `crypto` and `rlp`. The license inventory
therefore could not describe the distributed binary as MIT-only.

The source repository carries an MIT `LICENSE`. Linking GPL-3.0 code makes the
combined work subject to GPL-3.0 when distributed; publishing the binary under
MIT alone was not an option. The owner chose to keep the product MIT and remove
the GPL dependency instead of relicensing the whole distribution.

## Decision

The CCXT adapter was removed. Every venue call the service needs is now served
by `internal/adapters/outbound/venue`, a native REST adapter with the same
consumer-owned ports:

- `recovery.HistorySource` — OHLCV and CVD-complete klines;
- `derivatives.Source` — funding, open interest and long/short ratio;
- `app.ListingSource` — market metadata;
- `market.DiscoverySource` — liquidity-ranked symbol discovery.

The adapter speaks HTTP directly to Binance spot, Binance USD-M futures and
BingX spot/swap. It keeps the request-weight budget shared with seed and gap
recovery, classifies rate-limit/transient/permanent failures, and never lets a
vendor type cross the boundary.

BingX publishes a base-asset open-interest value rather than a contract count,
so its `OpenInterest.Amount` remains absent; the canonical field keeps its
meaning instead of being filled with the wrong unit. BingX does not serve open
interest history or long/short ratio history; those methods return a typed
permanent error.

## Consequences

- `go.mod` no longer contains CCXT or any `go-ethereum` module.
- The product remains MIT; no `NOTICE` or license change is needed.
- The adapter is now owned code: venue changes are a normal maintenance item,
  and the contract tests in `internal/adapters/outbound/venue` are the guard.
- The official Binance SDK used by the SDK gap filler remains; it is not GPL.
- The license report must be re-run after dependency changes; a GPL-3.0 package
  reappearing in the graph is a release blocker again.

## Proof

- `go list -m all` in the extracted module contains no `ccxt` and no
  `ethereum/go-ethereum`.
- `go mod tidy -diff` is clean.
- `make verify` passes with the native adapter.
- `internal/adapters/outbound/venue/*_test.go` covers every endpoint, the
  success shape and the three error classes.
- The live smoke (`go test -tags=smoke -run TestSmoke ...`) passed on Binance
  spot/futures and BingX spot/swap, including the CVD identity.
- `go-licenses report` lists 30 packages and zero GPL-3.0.
