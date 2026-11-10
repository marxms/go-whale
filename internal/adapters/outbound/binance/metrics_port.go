package binancesdk

// Metrics is the Binance adapter's observability port. Every field is a function so the
// owning package never imports Prometheus and a zero value is a usable no-op.
type Metrics struct {
	GoroutinesTotal               func(exchange, marketType string, n float64)
	ReconnectAttemptsTotal        func(exchange, marketType, reason string)
	ReconnectFailuresTotal        func(exchange, marketType string)
	DisconnectCauseTotal          func(exchange, marketType, cause string)
	LastMessageAgeSeconds         func(exchange, marketType string, age float64)
	PartitionStaleFraction        func(exchange, marketType, partition string, fraction float64)
	PartitionStaleFractionDelete  func(exchange, marketType string)
	HealthState                   func(exchange, marketType string, state float64)
	MessagesInTotal               func(exchange, marketType string)
	TradesDroppedTotal            func(exchange, marketType string)
	TradesIngestedTotal           func(exchange, marketType string)
	ReconnectSignalsDropped       func(exchange, marketType string)
	ResubscribeIncompleteTotal    func(exchange, marketType string, n float64)
	ResubscribeUnverifiedTotal    func(exchange, marketType string, n float64)
	DynamicSubscribeDeferredTotal func(exchange, marketType string)
	StarvationSuppressedTotal     func(exchange, marketType string)

	KlineFramesTotal     func(exchange, marketType string)
	KlineFramesRejected  func(exchange, marketType string)
	KlineSymbolsAttached func(exchange, marketType string, n float64)
	KlinePartitionFrame  func(exchange, marketType string, partition int)
	KlineRoute           func(exchange, marketType, route string)
	KlineInit            func(exchange, marketType string)
	// InitScope materializes the fixed-label series at zero before first resolution.
	InitScope func(exchange, marketType string)
}

func (m Metrics) incKlinePartitionFrame(exchange, marketType string, partition int) {
	if m.KlinePartitionFrame != nil {
		m.KlinePartitionFrame(exchange, marketType, partition)
	}
}

func (m Metrics) setKlineRoute(exchange, marketType, route string) {
	if m.KlineRoute != nil {
		m.KlineRoute(exchange, marketType, route)
	}
}

func (m Metrics) initScope(exchange, marketType string) {
	if m.InitScope != nil {
		m.InitScope(exchange, marketType)
	}
}

func (m Metrics) setGoroutines(exchange, marketType string, n float64) {
	if m.GoroutinesTotal != nil {
		m.GoroutinesTotal(exchange, marketType, n)
	}
}

func (m Metrics) incReconnectAttempt(exchange, marketType, reason string) {
	if m.ReconnectAttemptsTotal != nil {
		m.ReconnectAttemptsTotal(exchange, marketType, reason)
	}
}

func (m Metrics) incReconnectFailure(exchange, marketType string) {
	if m.ReconnectFailuresTotal != nil {
		m.ReconnectFailuresTotal(exchange, marketType)
	}
}

func (m Metrics) incDisconnectCause(exchange, marketType, cause string) {
	if m.DisconnectCauseTotal != nil {
		m.DisconnectCauseTotal(exchange, marketType, cause)
	}
}

func (m Metrics) setLastMessageAge(exchange, marketType string, age float64) {
	if m.LastMessageAgeSeconds != nil {
		m.LastMessageAgeSeconds(exchange, marketType, age)
	}
}

func (m Metrics) resetPartitionStale(exchange, marketType string) {
	if m.PartitionStaleFractionDelete != nil {
		m.PartitionStaleFractionDelete(exchange, marketType)
	}
}

func (m Metrics) setPartitionStale(exchange, marketType, partition string, fraction float64) {
	if m.PartitionStaleFraction != nil {
		m.PartitionStaleFraction(exchange, marketType, partition, fraction)
	}
}

func (m Metrics) setHealthState(exchange, marketType string, state float64) {
	if m.HealthState != nil {
		m.HealthState(exchange, marketType, state)
	}
}

func (m Metrics) incMessagesIn(exchange, marketType string) {
	if m.MessagesInTotal != nil {
		m.MessagesInTotal(exchange, marketType)
	}
}

func (m Metrics) incTradesDropped(exchange, marketType string) {
	if m.TradesDroppedTotal != nil {
		m.TradesDroppedTotal(exchange, marketType)
	}
}

func (m Metrics) incTradesIngested(exchange, marketType string) {
	if m.TradesIngestedTotal != nil {
		m.TradesIngestedTotal(exchange, marketType)
	}
}

func (m Metrics) incReconnectSignalDropped(exchange, marketType string) {
	if m.ReconnectSignalsDropped != nil {
		m.ReconnectSignalsDropped(exchange, marketType)
	}
}

func (m Metrics) addResubscribeIncomplete(exchange, marketType string, n float64) {
	if m.ResubscribeIncompleteTotal != nil {
		m.ResubscribeIncompleteTotal(exchange, marketType, n)
	}
}

func (m Metrics) addResubscribeUnverified(exchange, marketType string, n float64) {
	if m.ResubscribeUnverifiedTotal != nil {
		m.ResubscribeUnverifiedTotal(exchange, marketType, n)
	}
}

func (m Metrics) incDynamicSubscribeDeferred(exchange, marketType string) {
	if m.DynamicSubscribeDeferredTotal != nil {
		m.DynamicSubscribeDeferredTotal(exchange, marketType)
	}
}

func (m Metrics) incStarvationSuppressed(exchange, marketType string) {
	if m.StarvationSuppressedTotal != nil {
		m.StarvationSuppressedTotal(exchange, marketType)
	}
}

func (m Metrics) incKlineFrame(exchange, marketType string) {
	if m.KlineFramesTotal != nil {
		m.KlineFramesTotal(exchange, marketType)
	}
}

func (m Metrics) incKlineRejected(exchange, marketType string) {
	if m.KlineFramesRejected != nil {
		m.KlineFramesRejected(exchange, marketType)
	}
}

func (m Metrics) setKlineAttached(exchange, marketType string, n float64) {
	if m.KlineSymbolsAttached != nil {
		m.KlineSymbolsAttached(exchange, marketType, n)
	}
}
