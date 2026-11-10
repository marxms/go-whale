package engine

import (
	"fmt"
	"sort"

	"github.com/marxms/go-whale/internal/candle"
)

func (ce *CandleEngine) noteSeedRepairEvictionLocked(key CompositeKey) {
	if !ce.seedDirtyLost[key] {
		ce.metrics.incSeedRepairEvicted()
	}
	ce.seedDirtyLost[key] = true
}

type seedRepairAdmission struct {
	key        CompositeKey
	candle     candle.Candle
	generation uint64
}

// SeedRepairStatus reports obligations that were not admitted to the cold queue.
func (ce *CandleEngine) SeedRepairStatus() (pending, evicted int) {
	ce.mu.Lock()
	defer ce.mu.Unlock()
	for key, entries := range ce.seedDirty {
		for ts := range entries {
			if candle.FindByTimestamp(ce.buffers[key], ts) == nil {
				delete(entries, ts)
				ce.noteSeedRepairEvictionLocked(key)
			} else {
				pending++
			}
		}
	}
	for _, lost := range ce.seedDirtyLost {
		if lost {
			evicted++
		}
	}
	return pending, evicted
}

// markSeedRepairLocked retains only identities and generations, bounded by the
// resident ring. An evicted obligation remains a reported error, not success.
func (ce *CandleEngine) markSeedRepairLocked(key CompositeKey, timestamp int64) {
	dirty := ce.seedDirty[key]
	if dirty == nil {
		dirty = make(map[int64]uint64)
		ce.seedDirty[key] = dirty
	}
	buf := ce.buffers[key]
	if buf == nil {
		ce.noteSeedRepairEvictionLocked(key)
		return
	}
	if len(dirty) >= buf.Cap() {
		for ts := range dirty {
			if candle.FindByTimestamp(ce.buffers[key], ts) == nil {
				delete(dirty, ts)
				ce.noteSeedRepairEvictionLocked(key)
			}
		}
	}
	ce.seedRevision++
	dirty[timestamp] = ce.seedRevision
}

func (ce *CandleEngine) pendingSeedRepairs(exchange, marketType, symbol, childTF string, batch []candle.Candle) ([]seedRepairAdmission, error) {
	ce.mu.Lock()
	defer ce.mu.Unlock()
	timestamps := make(map[int64]struct{}, len(batch))
	for _, c := range batch {
		timestamps[c.Timestamp] = struct{}{}
	}
	var repairs []seedRepairAdmission
	for _, step := range candle.FoldChain() {
		if step.Child != childTF {
			continue
		}
		next := make(map[int64]struct{}, len(timestamps))
		for ts := range timestamps {
			next[candle.BucketTimestamp(ts, step.TF)] = struct{}{}
		}
		key := CompositeKey{exchange, marketType, symbol, step.TF}
		for ts := range ce.seedDirty[key] {
			if candle.FindByTimestamp(ce.buffers[key], ts) == nil {
				delete(ce.seedDirty[key], ts)
				ce.noteSeedRepairEvictionLocked(key)
			}
		}
		if ce.seedDirtyLost[key] {
			return nil, fmt.Errorf("seed ancestor %v: unadmitted repair was evicted", key)
		}
		for ts := range next {
			generation, ok := ce.seedDirty[key][ts]
			if !ok {
				continue
			}
			c := candle.FindByTimestamp(ce.buffers[key], ts)
			if c == nil {
				delete(ce.seedDirty[key], ts)
				ce.noteSeedRepairEvictionLocked(key)
				return nil, fmt.Errorf("seed ancestor %v timestamp %d: unadmitted repair was evicted", key, ts)
			}
			repairs = append(repairs, seedRepairAdmission{key: key, candle: *c, generation: generation})
		}
		timestamps, childTF = next, step.TF
	}
	sort.Slice(repairs, func(i, j int) bool {
		if repairs[i].key.Timeframe != repairs[j].key.Timeframe {
			return repairs[i].key.Timeframe < repairs[j].key.Timeframe
		}
		return repairs[i].candle.Timestamp < repairs[j].candle.Timestamp
	})
	return repairs, nil
}

func (ce *CandleEngine) finishSeedRepair(repair seedRepairAdmission) error {
	ce.mu.Lock()
	defer ce.mu.Unlock()
	current := candle.FindByTimestamp(ce.buffers[repair.key], repair.candle.Timestamp)
	if current == nil {
		delete(ce.seedDirty[repair.key], repair.candle.Timestamp)
		ce.noteSeedRepairEvictionLocked(repair.key)
		return fmt.Errorf("seed ancestor %v: repair evicted during admission", repair.key)
	}
	if ce.seedDirty[repair.key][repair.candle.Timestamp] != repair.generation || !candle.SameExactSeedValues(*current, repair.candle) || current.HasGap != repair.candle.HasGap || current.Source != repair.candle.Source {
		return fmt.Errorf("seed ancestor %v: changed during admission; retry required", repair.key)
	}
	delete(ce.seedDirty[repair.key], repair.candle.Timestamp)
	return nil
}
