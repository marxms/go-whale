# 0010 — Reconstruct taker-sell only for legacy and REST candles

- **Status:** Accepted
- **Date:** 2026-11-06

## Context

A candle can carry total volume and taker-buy volume without a measured
taker-sell volume. Two producers are in that situation:

- Cold rows written before `taker_sell_volume` existed. The column was added on
  2026-07-31, and the cold tier retains 365 days.
- REST/SDK klines, whose payload carries a taker-buy base volume and no sell
  side.

Live data does not need reconstruction: the aggregator classifies each trade by
its aggressor side and measures both sides directly.

The cold store has no NULL. A candle with no measured order flow is stored with
`taker_buy = 0`, which is indistinguishable from a genuine 100%-sell bar.
Reconstructing `sell = volume - buy` unconditionally would turn every legacy
OHLCV-only row into a claim of 100% sell aggression that no measurement
supports.

## Decision

- `candle.DeriveTakerSell` reconstructs the sell side only when
  `TakerBuyVolume > 0` and `Volume > 0`.
- Taker-buy is clamped to total volume before the subtraction to absorb
  floating-point rounding.
- The shim is not applied to live aggregation.
- The guard is deliberately conservative: a genuine 100%-sell bar written
  before the column existed remains flow-void because `SideUnknownCount` was
  never persisted to disambiguate it.

## Consequences

- The shim has a finite life. Once the oldest surviving cold row post-dates
  2026-07-31, the legacy branch is unreachable and can be deleted.
- A fleet-wide false claim of sell aggression is prevented at the cost of
  leaving unmeasured legacy flow unclassified.
