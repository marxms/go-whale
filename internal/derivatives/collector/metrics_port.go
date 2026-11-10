package collector

// Metrics is the derivatives collector's observability port. The zero value is a safe
// no-op.
type Metrics struct {
	FeedAvailable                   func(source, feed, timeframe string, available bool)
	FeedRequestTotal                func(exchange, feed, timeframe, outcome string)
	CollectedPoints                 func(exchange, feed, symbol, timeframe string, points float64)
	CollectedSpanSeconds            func(exchange, feed, symbol, timeframe string, seconds float64)
	HistoryPagesTotal               func(exchange, feed, timeframe string)
	FundingIntervalComparisonTotal  func(exchange, result string)
	FundingIntervalMeasurementTotal func(exchange, outcome string)
	FundingProfileRegistryEntries   func(n float64)
	FundingIntervalDivergenceTotal  func(exchange, symbol, measuredHours, declaredHours string)

	CoinalyzeCallsSpentTotal            func(n float64)
	CoinalyzeRateRefusalsTotal          func(n float64)
	CoinalyzeExternalQuotaRefusalsTotal func(n float64)
}

func (m Metrics) setFeedAvailable(source, feed, timeframe string, available bool) {
	if m.FeedAvailable != nil {
		m.FeedAvailable(source, feed, timeframe, available)
	}
}

func (m Metrics) incFeedRequest(exchange, feed, timeframe, outcome string) {
	if m.FeedRequestTotal != nil {
		m.FeedRequestTotal(exchange, feed, timeframe, outcome)
	}
}

func (m Metrics) setCollectedPoints(exchange, feed, symbol, timeframe string, points float64) {
	if m.CollectedPoints != nil {
		m.CollectedPoints(exchange, feed, symbol, timeframe, points)
	}
}

func (m Metrics) setCollectedSpanSeconds(exchange, feed, symbol, timeframe string, seconds float64) {
	if m.CollectedSpanSeconds != nil {
		m.CollectedSpanSeconds(exchange, feed, symbol, timeframe, seconds)
	}
}

func (m Metrics) incHistoryPage(exchange, feed, timeframe string) {
	if m.HistoryPagesTotal != nil {
		m.HistoryPagesTotal(exchange, feed, timeframe)
	}
}

func (m Metrics) incFundingIntervalComparison(exchange, result string) {
	if m.FundingIntervalComparisonTotal != nil {
		m.FundingIntervalComparisonTotal(exchange, result)
	}
}

func (m Metrics) incFundingIntervalMeasurement(exchange, outcome string) {
	if m.FundingIntervalMeasurementTotal != nil {
		m.FundingIntervalMeasurementTotal(exchange, outcome)
	}
}

func (m Metrics) setFundingProfileRegistryEntries(n int) {
	if m.FundingProfileRegistryEntries != nil {
		m.FundingProfileRegistryEntries(float64(n))
	}
}

func (m Metrics) incFundingIntervalDivergence(exchange, symbol, measuredHours, declaredHours string) {
	if m.FundingIntervalDivergenceTotal != nil {
		m.FundingIntervalDivergenceTotal(exchange, symbol, measuredHours, declaredHours)
	}
}

func (m Metrics) addCoinalyzeCallsSpent(n float64) {
	if m.CoinalyzeCallsSpentTotal != nil {
		m.CoinalyzeCallsSpentTotal(n)
	}
}

func (m Metrics) addCoinalyzeRateRefusals(n float64) {
	if m.CoinalyzeRateRefusalsTotal != nil {
		m.CoinalyzeRateRefusalsTotal(n)
	}
}

func (m Metrics) addCoinalyzeExternalQuotaRefusals(n float64) {
	if m.CoinalyzeExternalQuotaRefusalsTotal != nil {
		m.CoinalyzeExternalQuotaRefusalsTotal(n)
	}
}
