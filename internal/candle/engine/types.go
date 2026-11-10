package engine

// CompositeKey uniquely identifies a candle stream.
type CompositeKey struct {
	Exchange   string
	MarketType string
	Symbol     string
	Timeframe  string
}

// EngineStats exposes runtime metrics for the CandleEngine.
type EngineStats struct {
	TotalBuffers     int
	TotalCandles     int
	MemoryEstimateKB int64
	BuffersByTF      map[string]int // timeframe → count of buffers
}
