package bingx

// Metrics is the optional drop-observability port. The zero value is a no-op,
// so a fixture built without metrics keeps working.
type Metrics struct {
	TradesDroppedClassified func(exchange, marketType, symbol string, partition int, reason string)
}

func (m Metrics) incTradesDroppedClassified(exchange, marketType, symbol string, partition int, reason string) {
	if m.TradesDroppedClassified != nil {
		m.TradesDroppedClassified(exchange, marketType, symbol, partition, reason)
	}
}
