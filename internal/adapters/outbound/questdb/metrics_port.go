package questdb

// Metrics is the QuestDB adapter's observability port. The zero value is a safe no-op.
type Metrics struct {
	DerivativesColdDegraded func(outcome string, n float64)
	ColdPipelineBlocked     func(v float64)
	ColdPipelinePendingRows func(v float64)
	ColdSeedNoopRows        func(n float64)
}

func (m Metrics) addDerivativesColdDegraded(outcome string, n float64) {
	if m.DerivativesColdDegraded != nil {
		m.DerivativesColdDegraded(outcome, n)
	}
}

func (m Metrics) setColdPipelineBlocked(v float64) {
	if m.ColdPipelineBlocked != nil {
		m.ColdPipelineBlocked(v)
	}
}

func (m Metrics) setColdPipelinePendingRows(v float64) {
	if m.ColdPipelinePendingRows != nil {
		m.ColdPipelinePendingRows(v)
	}
}

func (m Metrics) addColdSeedNoopRows(n float64) {
	if m.ColdSeedNoopRows != nil {
		m.ColdSeedNoopRows(n)
	}
}
