package binancesdk

// KlineFeedInterval is the ONE interval subscribed per symbol.
//
// Every timeframe this service publishes is an exact multiple of 5m, and folding a finer
// bucket into a coarser one is exact. One stream per symbol is therefore a sufficient
// source for all eight. It is a vendor wire detail, so it lives with the adapter that
// dials the stream.
const KlineFeedInterval = "5m"

// KlineSilenceLimitSeconds is how long the feed may say nothing before it is declared
// dead.
//
// The venue pushes an update roughly every 250 ms per stream, so across a 500-symbol
// fleet a full minute of silence is thousands of missing frames — never market quiet,
// always transport. It is deliberately far above any plausible gap and far below the
// 5-minute bucket, so a dead partition is caught before it can cost a whole candle.
const KlineSilenceLimitSeconds = 60

// KlineFaultRedialWindow is how long a terminal transport fault stays actionable for an
// immediate partition re-dial. The owner tick is 30s, so a fault is caught on the next tick
// instead of waiting for the 60s silence limit; the window keeps a stale fault from
// re-triggering a redial after the partition has already been replaced.
const KlineFaultRedialWindow = 60 // seconds
