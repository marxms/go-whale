# Adapters

## Binance

`internal/adapters/outbound/binance` implements futures, spot and kline feeds over
`internal/adapters/outbound/binancews`, an owned Gorilla/WebSocket transport. The
transport never reads a frame before handlers are installed, dispatches frames
sequentially, renews connections before the venue's 24-hour limit, and closes
idempotently.

REST access uses the upstream Binance Go SDK (`go.mod`, no local `replace`).
Venue DTOs are decoded in the adapter and converted to `trade.Trade` and
`candle.Candle`; vendor models do not cross the package boundary. Public
trade streams are subscribed as `@trade`; the adapter accepts both the
individual id (`t`) and the aggregate id (`a`) when building the canonical
trade id.

## BingX

`internal/adapters/outbound/bingx/bingx_client.go` and `bingx_pool.go` own the BingX
WebSocket pool, gzip framing, aggregation and health. Trades are converted to
the canonical `trade.Trade` at the boundary.

## Venue REST

`internal/adapters/outbound/venue` owns the native REST reads: market metadata,
liquidity discovery, OHLCV/CVD, funding, open interest and long/short ratio.
It speaks HTTP to Binance spot, Binance USD-M futures and BingX spot/swap,
keeps the Binance request-weight budget shared with seed and gap recovery, and
returns only canonical types. The Binance SDK gap filler used by historical
recovery remains in `internal/recovery/sdk_filler.go`; it stays behind the
recovery port and does not leak a vendor type into the domain. BingX has no
open-interest history or long/short ratio history; those calls return a typed
permanent error.

Requests dispatch through a `driver` resolved by `registry.go` for the
`(exchange, market type)` scope. `Client` keeps the public signatures and
delegates every venue request; a driver that cannot serve a capability
declares it in the matrix and returns `CapabilityAbsentError`.

## Porting checklist

A new venue is one `registry.go` row plus one driver file. The checklist is the
same for every scope:

1. **Route.** Declare the WebSocket route class per stream in
   `market.ConnPolicyFor` (`/public` for trade and depth, `/market` for kline,
   aggTrade and markPrice, `/private` for account) and dial it. A connection
   without the documented route returns 101 and delivers only the public class.
2. **Connection policy.** Declare server ping, pong deadline, read idle, socket
   lifetime, stream and connection limits, and the keepalive encoding, each
   with the `DocsURL` that sustains it. Zero means "not documented"; the
   transport never substitutes a default.
3. **Capabilities.** Add the matrix row in `internal/market/capability.go`.
   Every capability the reference scope (Binance USD-M futures) has must be
   present or declared absent with a reason, and the driver must return
   `CapabilityAbsentError` from the method.
4. **Integrity.** The driver returns canonical `candle.Candle`. Normalization
   and rejection happen once at the write gate (`recovery.prepareCandles`),
   never per venue.
5. **Harness.** Add the scope to `TestVenueContractFourScopes` and run
   `venue_test`. The store conformance suite (`candle/storetest`,
   `derivatives/storetest`) is the persistence adapter's gate, not the venue
   adapter's. The harness is the certification gate, not a review convention.

## Contract harness

`contract_test.go` runs one table for the four scopes. For every capability the
matrix declares present, a recorded fixture must answer with a typed,
non-empty result. For every driver capability declared absent, the method must
return `CapabilityAbsentError` and record the `venue_capability_absent_total` metric. Listing
floors must come from the matrix, and a venue row with impossible OHLC must be
normalized, not dropped: the fixture is the real BCHUSDT 1w line from
2020-01-13.

`driver_test.go` proves the `driver` interface is satisfiable outside the two
venue implementations, and `registry_test.go` proves a scope resolves through
the registry without touching `internal/recovery`.

A venue adapter is certified by `venue_test`. The store it writes to is
certified separately by the applicable conformance suite (`candle/storetest`,
`derivatives/storetest`). A scope that fails either does not ship.

## Capability matrix

`binance:future` is the reference scope; every absence carries a reason in
`internal/market/capability.go`.

| Scope | Kline feed | CVD | Funding | Open interest | OI history | LSR | Markets | Listing floors |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `binance:spot` | absent | present | absent | absent | absent | absent | present | absent |
| `binance:future` | present | present | present | present | present | present | present | present |
| `bingx:spot` | absent | absent | absent | absent | absent | absent | present | absent |
| `bingx:swap` | absent | absent | present | present | absent | absent | present | absent |

## Connection policy

Zero means the venue does not document the value.

| Scope | Routes | Server ping | Pong timeout | Read idle | Max lifetime | Max streams | Max conns/IP | Keepalive |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `binance:spot` | no split | 20 s | 60 s | 60 s | 24 h | 1024 | 300 | protocol |
| `binance:future` | `/public`, `/market`, `/private` | 3 min | 10 min | 240 s | 24 h | 1024 | 300 | protocol |
| `bingx:spot` | no split | 5 s | not documented | not documented | not documented | 200 | not documented | JSON `{"ping": ...}` |
| `bingx:swap` | no split | 5 s | not documented | not documented | not documented | 200 | not documented | text `Ping` |

## Left for later

- **Interior gaps behind the head.** The recovery window derives from the head;
  locating an interior hole needs a coverage index this change does not build.
- **Kline feed for other venues.** BingX publishes no candle stream this
  service can transport; the matrix declares the absence and the tape keeps
  writing.
- **Consolidating `exchange.RestBudget` and the pacer into the driver.** The
  refactor moves the request bodies; it does not redesign the budget.

## Adding a persistence or venue adapter

Implement the relevant port from `internal/candle`, `internal/derivatives` or
`internal/trade`. Adapters may import domain packages; domain packages never
import adapters or configuration. A venue adapter also follows the porting
checklist above.
