# Domain

## Market

`MarketType` accepts `spot` and the futures vocabulary. `ParseTimeframe`
accepts positive `m`, `h`, `d` and `w` intervals. Unknown values are errors, not
defaults. Weekly buckets are aligned to Monday 00:00 UTC.

## Trade

```go
type Trade struct {
    ID        string
    Symbol    string
    Timestamp int64 // milliseconds since Unix epoch, venue trade time
    Price     float64
    Amount    float64
    Side      Side
}
```

`Side` is `buy`, `sell` or `unknown`. Unknown is an explicit absence of
classification and is never folded into either side.

## Candle

A candle carries OHLCV, creation/update instants, the measured taker buy/sell
pair, provenance, aggregate method, gap/late counters and the request/fetch/
trade/kline timestamps that decide authority during merges. The taker pair has
no `omitempty`: zero is a real market state and absence means "older contract".

The aggregator elects open and close by `(timestamp, trade id)`, not by slice
order. When ids are not orderable it preserves the incumbent instead of
inventing an order.

## Derivatives

Snapshots carry open interest, funding, long/short ratios and provenance per
point and per field. Funding normalization emits an 8-hour equivalent only when
the derivation step permits it. A baseline with mixed provenance cannot be
used as if it came from a single ruler.
