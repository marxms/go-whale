package engine

import "github.com/marxms/go-whale/internal/candle"

// Merge-source values as they travel on candle.Candle.Source. The single source of truth is
// internal/candle, so the pure fold/merge package never imports a storage adapter; these aliases
// exist only for the engine's own label wiring and tests.
const (
	sourceWSDelta   = candle.CandleMergeSourceWSDelta
	sourceRESTTotal = candle.CandleMergeSourceRESTTotal
)
