package engine

import (
	"fmt"
	"sync"
)

// engineTestMetrics is an in-memory implementation of the engine Metrics port.
type engineTestMetrics struct {
	mu sync.Mutex

	late       map[string]float64
	latePrereg map[string]bool

	coldMissing   map[string]float64
	coldGaps      map[string]float64
	coldExpected  map[string]float64
	coldAbsent    map[string]float64
	durableRows   map[string]float64
	durableMiss   map[string]float64
	durableColumn map[string]float64
	durableTF     map[string]float64
	durableIncoh  map[string]float64
	durableDims   float64
	appliedVolume map[string]float64
	appliedGone   map[string]bool
	coldAborted   float64
	warmSeed      map[string]float64
}

func newEngineTestMetrics() *engineTestMetrics {
	return &engineTestMetrics{
		late:          map[string]float64{},
		latePrereg:    map[string]bool{},
		coldMissing:   map[string]float64{},
		coldGaps:      map[string]float64{},
		coldExpected:  map[string]float64{},
		coldAbsent:    map[string]float64{},
		durableRows:   map[string]float64{},
		durableMiss:   map[string]float64{},
		durableColumn: map[string]float64{},
		durableTF:     map[string]float64{},
		durableIncoh:  map[string]float64{},
		appliedVolume: map[string]float64{},
		appliedGone:   map[string]bool{},
		warmSeed:      map[string]float64{},
	}
}

func key(parts ...string) string {
	return fmt.Sprint(parts)
}

func (m *engineTestMetrics) port() Metrics {
	return Metrics{
		WarmSeedTotal: func(ex, mt, tf, outcome string) {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.warmSeed[key(ex, mt, tf, outcome)]++
		},
		CandleDeltaInsideHorizonTotal: func(_, _, _ string) {},
		EngineAppliedDeltaVolumeTotal: func(ex, mt, tf string, volume float64) {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.appliedVolume[key(ex, mt, tf)] += volume
		},
		DeleteAppliedDeltaVolume: func(ex, mt, tf string) {
			m.mu.Lock()
			defer m.mu.Unlock()
			delete(m.appliedVolume, key(ex, mt, tf))
			m.appliedGone[key(ex, mt, tf)] = true
		},
		PreregisterLateUpdate: func(ex, mt, tf, reason string) {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.latePrereg[key(ex, mt, tf, reason)] = true
		},
		CandleLateUpdateTotal: func(ex, mt, tf, reason string) {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.late[key(ex, mt, tf, reason)]++
		},
		CandleSeedMergeTotal:         func(_, _, _, _ string, _ float64) {},
		FoldSeedRevisionsUnconfirmed: func() {},
		SeedRepairEvicted:            func() {},
		SeedRepairOutsideRetention:   func() {},
		ColdAppendAborted: func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.coldAborted++
		},
		ColdMissingBuckets: func(ex, mt, tf string, v float64) {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.coldMissing[key(ex, mt, tf)] = v
		},
		ColdSymbolsWithGaps: func(ex, mt, tf string, v float64) {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.coldGaps[key(ex, mt, tf)] = v
		},
		ColdSymbolsExpected: func(ex, mt, tf string, v float64) {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.coldExpected[key(ex, mt, tf)] = v
		},
		ColdSymbolsAbsent: func(ex, mt, tf string, v float64) {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.coldAbsent[key(ex, mt, tf)] = v
		},
		DurableRows: func(tf string, v float64) {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.durableRows[tf] = v
		},
		DurableMissingBuckets: func(tf string, v float64) {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.durableMiss[tf] = v
		},
		DurableColumnCovered: func(tf, column string, v float64) {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.durableColumn[key(tf, column)] = v
		},
		DurableTimeframeCovered: func(tf string, v float64) {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.durableTF[tf] = v
		},
		DurableIncoherentFunding: func(tf string, v float64) {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.durableIncoh[tf] = v
		},
		DurableDimensionsMeasured: func(v float64) {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.durableDims = v
		},
	}
}

func (m *engineTestMetrics) lateTotal() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var total float64
	for _, v := range m.late {
		total += v
	}
	return total
}

func (m *engineTestMetrics) lateReason(reason string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var total float64
	suffix := " " + reason + "]"
	for k, v := range m.late {
		if len(k) >= len(suffix) && k[len(k)-len(suffix):] == suffix {
			total += v
		}
	}
	return total
}

func (m *engineTestMetrics) applied(ex, mt, tf string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.appliedVolume[key(ex, mt, tf)]
}
