# Architecture

GoWhale is a server, not a library: application code stays under `internal/`.
Packages follow capabilities, and dependencies point inward.

```text
cmd/gowhale
  -> internal/app (composition root and lifecycle)
       -> internal/adapters/inbound/httpapi, internal/adapters/inbound/realtime   inbound adapters
       -> internal/candle/engine, internal/adapters/outbound/redis   persistence and tiers
       -> internal/adapters/outbound/binance, internal/adapters/outbound/binancews,
          internal/adapters/outbound/bingx, internal/adapters/outbound/venue   venue adapters
       -> internal/ingestion, internal/recovery,
          internal/derivatives/collector   ingestion and recovery
       -> internal/candle, internal/trade,
          internal/market, internal/derivatives   domain
```

The import-boundary gate covers the four pure domain packages (`market`, `trade`,
`candle`, `derivatives`). `candle/engine` and the adapters are deliberate layers:
the engine may import the Redis adapter and the configuration, and the inbound HTTP
adapter may import the Redis adapter. That is a layer boundary, not a domain-purity
violation; the gate is narrower than the whole tree by design.

## Domain

- `market` owns exchange vocabulary, market types and timeframes. Timeframes
  are parsed strictly and weekly buckets start Monday 00:00 UTC.
- `trade` owns `Trade`, `Side` and `LastTrade`. Unknown side means "not
  classified", never a default buy or sell.
- `candle` owns candle values, aggregation, taker-flow attribution and the
  storage ports (`Writer`, `Reader`, `Catalog`, `SeedWriter`, `SeedSplitter`).
- `derivatives` owns snapshots, funding normalization, label conventions and
  the robust baseline values, plus its own storage ports.

Ports are declared by the domain package that defines the values, and adapters
implement them. Consumers depend on the narrowest port they use; there is no
composite storage interface.

## Ingestion

The Binance adapter uses an owned Gorilla transport
(`internal/adapters/outbound/binancews`) with bounded read/write buffers, one
sequential reader, deadlines, ping/pong, subscription management, renewal and
idempotent close. The adapter
installs handlers before the first frame is read and decodes a minimal DTO for
public trade and kline payloads. The adapter subscribes to `@trade`: measured
on 2026-11-05, the official futures host delivered that stream while
`@aggTrade` produced no frames. No vendor WebSocket SDK is linked.

`internal/ingestion` admits trades, deduplicates by id, aggregates every
configured timeframe and writes batches. `internal/recovery` detects holes and
repairs them through the venue REST adapter; `internal/candle/engine` performs
the boot-time historical load with the fold → open-bucket → persist order.
`internal/adapters/outbound/venue` owns the native REST reads: market metadata,
discovery, OHLCV/CVD, funding, open interest and long/short ratio. The Binance
SDK gap filler in `internal/recovery` remains for the historical kline path; it
stays behind the recovery port, and no vendor type reaches the domain from
either path.

## Persistence

- Redis is the hot tier and the source for `/api/*` reads. Key layout and
  codecs live in `internal/adapters/outbound/redis`.
- QuestDB is the optional cold tier. `engine` owns the single writer, WAL
  barrier and retention policy.
- `engine.TieredStore` combines the in-memory ring with Redis and, when
  configured, QuestDB. Its Redis dependency is the `redisTier` interface it
  actually calls, so the tier can be exercised with a fake.

## Failure semantics

Absent, zero, transient, contract divergence and uncertain write are distinct
states. Startup gates refuse to serve when a declaration cannot be read;
`doctor` exits non-zero instead of printing an unread verdict as healthy.
