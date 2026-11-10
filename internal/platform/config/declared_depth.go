package config

// declared_depth.go — the RESIDENT DEPTH, in one source.
//
// See docs/decisions/0040-single-resident-depth.md for why there is exactly one fallback.

// DefaultResidentDepth is the single fallback depth for every resident series: the candle ring,
// the candle ZSET, the derivatives series and the derivatives bootstrap.
//
// Its value is not a decision taken here — it is what production runs, and the conformance test
// is what keeps that true.
//
// OVERRIDABLE DEFAULT: `derivatives.max_series_size` and
// `candle_engine.buffer_sizes` in the mounted YAML replace it, and
// TestDeclaredDepth_MatchesTheMountedConfiguration fails if the two diverge.
const DefaultResidentDepth = 800
