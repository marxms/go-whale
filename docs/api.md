# HTTP API

The OpenAPI document served at `/openapi.json` lists the routes and path
parameters. This page documents the query parameters and the response shapes that a
consumer needs to build a request; the handler code is the final authority.

Base URL in the example stack: `http://127.0.0.1:3000`.

## Common query parameters

| Parameter | Meaning | Default |
| --- | --- | --- |
| `exchange` | venue id (`binance`, `bingx`) | `binance` |
| `marketType` | `spot` or `future` | `future` |
| `symbol` | canonical `BASE/QUOTE` (URL-encode the slash) | required for single-symbol reads |
| `timeframe` | configured timeframe (`5m`, `1h`, `1d`, …) | endpoint-specific |
| `from`, `to` | Unix epoch milliseconds; include either or both for a range | latest window |
| `limit` | maximum points (`1`–`5000`) | `800` |
| `symbols` | comma-separated canonical symbols | all active symbols |

The symbol is `BTC/USDT`, not `BTCUSDT`; URL-encode the slash as `%2F`.

## `GET /api/timeseries`

One candle window for one symbol and timeframe.

```bash
curl -sS 'http://127.0.0.1:3000/api/timeseries?symbol=BTC%2FUSDT&timeframe=5m&limit=2'
```

Response shape:

```json
{
  "symbol": "BTC/USDT",
  "timeframe": "5m",
  "count": 2,
  "total": 2,
  "lastTrade": {"timestamp": 0, "price": 0, "volume": 0, "side": "buy"},
  "data": [
    {"timestamp": 0, "open": 0, "high": 0, "low": 0, "close": 0, "volume": 0,
     "takerBuyVolume": 0, "takerSellVolume": 0}
  ]
}
```

Values are examples of the fields only. A first request can return `count: 0`
until discovery, seed and a closed bucket exist for the symbol. An unsupported
timeframe returns `400` with `{"error":"unsupported timeframe","code":"invalid_timeframe"}`.

## `GET /api/batch/prices`

Latest close and 24 h volume for a set of symbols. `symbols` is optional; without
it the endpoint uses the active symbols for the scope.

```bash
curl -sS 'http://127.0.0.1:3000/api/batch/prices?exchange=binance&marketType=future&timeframe=30m&symbols=BTC%2FUSDT,ETH%2FUSDT'
```

## `GET /api/batch/derivatives-series`

Derivatives series for a set of symbols. Required: `symbols`; optional:
`exchange`, `marketType`, `timeframe` (defaults to the configured default) and
`limit` (default 30). A missing `symbols` returns `400`.

## `GET /api/subscriptions`

Active symbols grouped by exchange and market type, with the configured
timeframes and, when a margin-bracket source is wired, the tier-1 maintenance
margin rate per symbol.

```json
{
  "total": 1,
  "exchanges": {
    "binance": {
      "total_symbols": 1,
      "markets": {
        "future": {
          "total_symbols": 1,
          "symbols": ["BTC/USDT"],
          "timeframes": ["5m", "1h"],
          "maintenance_margin_rate": {"BTC/USDT": 0.004}
        }
      }
    }
  }
}
```

## Health and metrics

- `GET /health`: process status, timeframe checksum and per-component state. The
  body can be `starting`, `healthy`, `degraded` or `unhealthy`.
- `GET /ready`: whether at least one venue stream is connected.
- `GET /metrics`: Prometheus exposition.
- `GET /openapi.json`: route contract.

## WebSocket edge

The edge is opt-in and mounts only when both
`GOWHALE_REALTIME_WS_TICKET_SECRET` and `GOWHALE_REALTIME_WS_MINT_TOKEN` are set
(at least 32 random bytes each).

1. The authenticating gateway verifies the user and proxies `GET /api/ws-ticket`
   with `X-Gowhale-Mint-Token: <mint token>`.
2. The server returns `{"ticket":"…","expiresAt":…}` with `Cache-Control: no-store`.
3. The browser opens `GET /api/ws?ticket=<ticket>` from an origin in
   `GOWHALE_REALTIME_WS_ALLOWED_ORIGINS`.

The mint endpoint is not a user-auth boundary by itself; a direct client that knows
the mint token can mint a ticket, so the gateway must not expose either secret.
Connections and per-connection subscriptions are capped by
`GOWHALE_REALTIME_WS_MAX_CONNECTIONS` and
`GOWHALE_REALTIME_WS_MAX_SCOPES_PER_CONNECTION`.

## Errors

Errors use JSON with an `error` string; some routes also return a stable `code`.
The HTTP status is the contract: `400` for an invalid request, `401` for an invalid
ticket or missing mint token, `403` for a rejected origin, `404` when the edge is not
mounted (both WS secrets unset), `503` when it is mounted but its auth is not
configured, and `500` for a read failure.
