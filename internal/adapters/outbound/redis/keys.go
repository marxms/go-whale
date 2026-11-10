package redisstore

// Redis key layout owned by the adapter. The composition root may name these constants
// but never spell the patterns itself: a second literal is how the streamer and the
// analyzer drift apart on the same keyspace.

// ActiveSymbolsKeyPrefix is the per-scope set the streamer publishes and the analyzer
// reads.
const ActiveSymbolsKeyPrefix = "active_symbols:"

// ActiveSymbolsKey returns the set key for an "exchange:marketType" scope.
func ActiveSymbolsKey(scope string) string {
	return ActiveSymbolsKeyPrefix + scope
}

// LegacySwapMarketType is the pre-canonicalization market-type label whose active set
// the streamer still clears when a future scope starts. It is Redis key layout, not
// market vocabulary: the key exists in the keyspace and must be removed.
const LegacySwapMarketType = "swap"
