# Dead code

`make dead-code` runs the pinned `deadcode` tool over `./cmd/...`. It is advisory:
static reachability cannot see test-only helpers, interface implementations or
`fmt` reflection. The v0.1.0 tree has **37** survivors, grouped here so the policy is
public and checkable.

| Category | Symbols | Why it stays |
| --- | --- | --- |
| Test capture helper | `internal/platform/logging/capture.go` (12 symbols) | Used by tests in other packages; a `_test.go` file would not be visible to them |
| Test constructors and options | `NewRouterWithMetrics`, `realtime.NewAsyncProducer`, `WithEventsBufSize`, `WithPingPeriod`, `distribution.WithQueueSize`, `withColdTierConstructor`, `buildColdTier`, `binance.IsBatchSubscribeUnavailable`, `coinalyze.WithCoinalyzeBaseURL` | Exercised directly by the test suites; some are the only way to drive a failure branch without a live venue |
| Pure seams | `candle.BuildOHLCVC`, `candle.ValidateCandleAggregates`, `ContinuityTracker.LatestClosed`, `ContinuityTracker.Len`, `AsyncProducer.Dropped` | Test and observability seams for the pure arithmetic |
| `fmt.Stringer` implementations | `periodSupport.String`, `config.Secret.String`, `logging.Level.String`, `trade.Side.String` | Called through `fmt` reflection, which RTA cannot follow; `Secret.String` is the redaction path |
| Interface and lifecycle roles | `binance.ClientFactory.Close`, `bingx.ClientFactory.Close`, `bingXDriver.keepaliveEncoding`, `venue.NewClient` | Satisfy a lifecycle or driver interface, or are the stable public constructor while the composition root uses the options variant |
| Wire codecs | `realtime.Unmarshal`, `realtime.Channel`, `distribution.Unmarshal` | Consumed by tests and external consumers of the versioned frames |

A new unreachable symbol must have a caller, a test that exercises it, or a
documented interface role. “It might be useful” is not a reason to keep it, and a
symbol outside these categories is removed before the next release.
