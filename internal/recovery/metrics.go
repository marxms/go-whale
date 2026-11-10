package recovery

// openBucketSnapshotLagMs measures THE RESIDUAL — the only part of the boot open bucket that
// no ordering can close.
//
// Once the seed waits for the stream to deliver, the REST window [bucket start, T_fetch] and
// the live window [T_deliver, ...] overlap, and the engine's MAX-on-declared-total stitches
// them. What survives the stitch is the interval between the exchange computing the snapshot
// (T_fetch) and this process applying it (T_apply): the MAX elects the REST total, which does
// not contain the trades of that interval, and the live partial that did contain them is the
// loser of the comparison.
//
// Closing it would require a cut point BOTH sources agree on at trade granularity. A kline
// carries none — no trade id, no snapshot instant, and its trade COUNT is raw trades while the
// stream delivers aggTrades, so the two cannot be aligned. The residual is therefore bounded
// by one network leg and is irreducible against this source; it is not bounded by anything we
// choose, which is exactly why it is measured rather than asserted.
//
// This histogram IS the proof obligation. Multiply an observation by the timeframe to get the
// worst-case share of a bucket it can represent: 150ms against a 5m bucket is 0,05%, against
// 1h it is 0,004%. If a post-deploy parity check shows a divergent open bucket materially
// larger than this histogram allows for, the cause is NOT this residual and the investigation
// should look elsewhere.
