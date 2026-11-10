package venue

// Metrics is the adapter's observability port. The zero value is a safe no-op.
type Metrics struct {
	LongShortShareSumViolation func(exchange, symbol, reason string)
	CapabilityAbsent           func(exchange, marketType, capability string)
}

func (m Metrics) longShortShareSumViolation(exchange, symbol, reason string) {
	if m.LongShortShareSumViolation != nil {
		m.LongShortShareSumViolation(exchange, symbol, reason)
	}
}

func (m Metrics) capabilityAbsent(exchange, marketType, capability string) {
	if m.CapabilityAbsent != nil {
		m.CapabilityAbsent(exchange, marketType, capability)
	}
}
