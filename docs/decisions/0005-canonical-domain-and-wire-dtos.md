# 0005 — Canonical domain values and DTOs at the boundary

- **Status:** Accepted
- **Date:** 2026-11-05

## Context

Venue payloads, HTTP responses, Redis members and QuestDB rows use different
names, orders and optional-field rules. Letting those shapes live on domain
structs made a serialization change look like a domain change, and copying venue
models into the core made two vocabularies authoritative at once.

## Decision

- `market`, `trade`, `candle` and `derivatives` own the canonical values.
  Market type and timeframe are parsed and validated; unknown values are errors,
  not defaults.
- `trade.Trade` and `trade.LastTrade` are the canonical trade values. `Side` is
  `buy`, `sell` or `unknown`; `unknown` is an explicit absence of
  classification.
- `candle.Candle` carries no serialization tags. Its JSON names, order and
  `omitempty` rules live in the adapter DTOs that own each boundary: the HTTP
  API, the realtime envelope, the legacy Redis member and the positional Redis
  member codec.
- A candle series is identified by `(exchange, marketType, symbol, timeframe)`,
  the `SeriesKey` concept. Adapters project that value into their own key
  formats; no concatenated string becomes a domain value.
- Venue DTOs stay inside the venue adapter and are converted explicitly at the
  boundary. Vendor models do not cross the package boundary.
- `trade.LastTrade` and `derivatives.DerivativesSnapshot` are intentionally
  wire-shaped today: they are the published JSON contract for the last-trade
  key, the derivatives `latest` key, the baseline document and the HTTP
  responses. Splitting them into separate DTOs is allowed, but it must preserve
  those contracts byte for byte.

## Consequences

- Adding or renaming a wire field requires editing the owning DTO and its tests;
  editing a domain struct cannot silently change a payload.
- Redis member codecs and QuestDB column writers are the authority for persisted
  formats. Their schemas are documented under `docs/schemas/`.
- The four components of a series identity must agree across Redis keys, QuestDB
  dedup keys and HTTP query parameters. Tests pin each projection.
- The two wire-shaped types remain a known boundary exception until they are
  split; that split is a compatibility change and is not done silently.
