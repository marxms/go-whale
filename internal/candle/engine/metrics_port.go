package engine

// Metrics is the candle engine's observability port. Every field is a function so the
// domain package never imports Prometheus and a zero value is a usable no-op.
type Metrics struct {
	WarmSeedTotal                 func(exchange, marketType, timeframe, outcome string)
	CandleDeltaInsideHorizonTotal func(exchange, marketType, timeframe string)
	EngineAppliedDeltaVolumeTotal func(exchange, marketType, timeframe string, volume float64)
	DeleteAppliedDeltaVolume      func(exchange, marketType, timeframe string)
	PreregisterLateUpdate         func(exchange, marketType, timeframe, reason string)
	CandleLateUpdateTotal         func(exchange, marketType, timeframe, reason string)
	CandleSeedMergeTotal          func(exchange, marketType, timeframe, outcome string, n float64)
	FoldSeedRevisionsUnconfirmed  func()
	SeedRepairEvicted             func()
	SeedRepairOutsideRetention    func()
	ColdAppendAborted             func()

	ColdMissingBuckets  func(exchange, marketType, timeframe string, v float64)
	ColdSymbolsWithGaps func(exchange, marketType, timeframe string, v float64)
	ColdSymbolsExpected func(exchange, marketType, timeframe string, v float64)
	ColdSymbolsAbsent   func(exchange, marketType, timeframe string, v float64)

	DurableRows               func(timeframe string, v float64)
	DurableMissingBuckets     func(timeframe string, v float64)
	DurableColumnCovered      func(timeframe, column string, v float64)
	DurableTimeframeCovered   func(timeframe string, v float64)
	DurableIncoherentFunding  func(timeframe string, v float64)
	DurableDimensionsMeasured func(v float64)
}

func (m Metrics) incWarmSeed(exchange, marketType, timeframe, outcome string) {
	if m.WarmSeedTotal != nil {
		m.WarmSeedTotal(exchange, marketType, timeframe, outcome)
	}
}

func (m Metrics) incDeltaInsideHorizon(exchange, marketType, timeframe string) {
	if m.CandleDeltaInsideHorizonTotal != nil {
		m.CandleDeltaInsideHorizonTotal(exchange, marketType, timeframe)
	}
}

func (m Metrics) addAppliedDeltaVolume(exchange, marketType, timeframe string, volume float64) {
	if m.EngineAppliedDeltaVolumeTotal != nil {
		m.EngineAppliedDeltaVolumeTotal(exchange, marketType, timeframe, volume)
	}
}

func (m Metrics) deleteAppliedDeltaVolume(exchange, marketType, timeframe string) {
	if m.DeleteAppliedDeltaVolume != nil {
		m.DeleteAppliedDeltaVolume(exchange, marketType, timeframe)
	}
}

func (m Metrics) preregisterLateUpdate(exchange, marketType, timeframe, reason string) {
	if m.PreregisterLateUpdate != nil {
		m.PreregisterLateUpdate(exchange, marketType, timeframe, reason)
	}
}

func (m Metrics) incLateUpdate(exchange, marketType, timeframe, reason string) {
	if m.CandleLateUpdateTotal != nil {
		m.CandleLateUpdateTotal(exchange, marketType, timeframe, reason)
	}
}

func (m Metrics) addSeedMerge(exchange, marketType, timeframe, outcome string, n float64) {
	if m.CandleSeedMergeTotal != nil {
		m.CandleSeedMergeTotal(exchange, marketType, timeframe, outcome, n)
	}
}

func (m Metrics) incFoldSeedRevision() {
	if m.FoldSeedRevisionsUnconfirmed != nil {
		m.FoldSeedRevisionsUnconfirmed()
	}
}

func (m Metrics) incSeedRepairEvicted() {
	if m.SeedRepairEvicted != nil {
		m.SeedRepairEvicted()
	}
}

func (m Metrics) incSeedRepairOutsideRetention() {
	if m.SeedRepairOutsideRetention != nil {
		m.SeedRepairOutsideRetention()
	}
}

func (m Metrics) incColdAppendAborted() {
	if m.ColdAppendAborted != nil {
		m.ColdAppendAborted()
	}
}

func (m Metrics) setColdMissingBuckets(exchange, marketType, timeframe string, v float64) {
	if m.ColdMissingBuckets != nil {
		m.ColdMissingBuckets(exchange, marketType, timeframe, v)
	}
}

func (m Metrics) setColdSymbolsWithGaps(exchange, marketType, timeframe string, v float64) {
	if m.ColdSymbolsWithGaps != nil {
		m.ColdSymbolsWithGaps(exchange, marketType, timeframe, v)
	}
}

func (m Metrics) setColdSymbolsExpected(exchange, marketType, timeframe string, v float64) {
	if m.ColdSymbolsExpected != nil {
		m.ColdSymbolsExpected(exchange, marketType, timeframe, v)
	}
}

func (m Metrics) setColdSymbolsAbsent(exchange, marketType, timeframe string, v float64) {
	if m.ColdSymbolsAbsent != nil {
		m.ColdSymbolsAbsent(exchange, marketType, timeframe, v)
	}
}

func (m Metrics) setDurableRows(timeframe string, v float64) {
	if m.DurableRows != nil {
		m.DurableRows(timeframe, v)
	}
}

func (m Metrics) setDurableMissingBuckets(timeframe string, v float64) {
	if m.DurableMissingBuckets != nil {
		m.DurableMissingBuckets(timeframe, v)
	}
}

func (m Metrics) setDurableColumnCovered(timeframe, column string, v float64) {
	if m.DurableColumnCovered != nil {
		m.DurableColumnCovered(timeframe, column, v)
	}
}

func (m Metrics) setDurableTimeframeCovered(timeframe string, v float64) {
	if m.DurableTimeframeCovered != nil {
		m.DurableTimeframeCovered(timeframe, v)
	}
}

func (m Metrics) setDurableIncoherentFunding(timeframe string, v float64) {
	if m.DurableIncoherentFunding != nil {
		m.DurableIncoherentFunding(timeframe, v)
	}
}

func (m Metrics) setDurableDimensionsMeasured(v float64) {
	if m.DurableDimensionsMeasured != nil {
		m.DurableDimensionsMeasured(v)
	}
}
