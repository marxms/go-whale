package engine

import (
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// FOLDING THE VENUE'S CANDLE UP THE TIMEFRAME CHAIN.
//
// The algorithms live in candle.FoldFromChildren / candle.CollectFoldChildren; this file owns the
// engine side: the buffer map, the write lock, the callbacks and the counters. A parent is
// REBUILT from its children every time, which is idempotent by construction — running it twice
// gives the same answer, so no delivery guarantee is required of the transport.

// FoldUp rebuilds every coarser bucket that contains the given child bucket, in order.
//
// Call it after a child bucket has been written. It walks the chain once, so one 5m close settles
// 15m through 1w in a single pass, and stops at the first level whose children are incomplete —
// beyond that point the data to build an exact bucket does not exist yet.
func (ce *CandleEngine) FoldUp(exchange, marketType, symbol, childTF string, childTS int64) {
	for _, step := range candle.FoldChain() {
		if step.Child != childTF {
			continue
		}
		parentTS := candle.BucketTimestamp(childTS, step.TF)
		if !ce.rebuildFromChildren(exchange, marketType, symbol, step, parentTS, false) {
			return
		}
		childTF, childTS = step.TF, parentTS
	}
}

// rebuildFromChildren recomputes one parent bucket and reports whether it was written.
func (ce *CandleEngine) rebuildFromChildren(exchange, marketType, symbol string, step candle.FoldStep, parentTS int64, existingOnly bool) bool {
	ce.mu.Lock()
	var callback func()
	defer func() {
		ce.mu.Unlock()
		if callback != nil {
			callback()
		}
	}()
	key := CompositeKey{exchange, marketType, symbol, step.TF}
	if existingOnly && candle.FindByTimestamp(ce.buffers[key], parentTS) == nil {
		return false
	}

	now := time.Now().UnixMilli()
	childBuf := ce.buffers[CompositeKey{exchange, marketType, symbol, step.Child}]
	kids, ok := candle.CollectFoldChildren(
		childBuf, step.Child, step.N, parentTS, now,
		ce.listingFloors[exchange+":"+marketType+":"+symbol],
	)
	if !ok {
		return false
	}
	parent := candle.FoldFromChildren(kids, parentTS)

	buf := ce.getOrCreate(key)
	for i := 0; i < buf.Len(); i++ {
		old, _ := buf.At(i)
		if old.Timestamp != parentTS {
			continue
		}
		if old.Origin == candle.OriginNative {
			return true
		}
		if candle.SameFoldValues(old, parent) {
			// Complete measured children establish derived provenance even when
			// the financial payload is unchanged. A completeness change is
			// observable; source/origin alone do not emit a revision.
			gapChanged := old.HasGap != parent.HasGap
			old.Source, old.Origin, old.HasGap = parent.Source, parent.Origin, parent.HasGap
			buf.SetAt(i, old)
			tfMs, _ := candle.ParseTimeframeMs(step.TF)
			if gapChanged && tfMs > 0 && parentTS+tfMs <= now {
				if existingOnly {
					ce.markSeedRepairLocked(key, parentTS)
					ce.metrics.incFoldSeedRevision()
				}
				fn := ce.onRevised
				if existingOnly && ce.onSeedRevised != nil {
					fn = ce.onSeedRevised
				}
				if fn != nil {
					revised := old
					callback = func() { fn(key, revised) }
				}
			}
			return true
		}
		parent.CreatedAt, parent.LastUpdate = old.CreatedAt, now
		parent.LateTradeCount = old.LateTradeCount
		buf.SetAt(i, parent)
		tfMs, _ := candle.ParseTimeframeMs(step.TF)
		if tfMs > 0 && parentTS+tfMs <= now {
			if existingOnly {
				ce.markSeedRepairLocked(key, parentTS)
				ce.metrics.incFoldSeedRevision()
			}
			fn := ce.onRevised
			if existingOnly && ce.onSeedRevised != nil {
				fn = ce.onSeedRevised
			}
			if fn != nil {
				revised := parent
				callback = func() { fn(key, revised) }
			}
		}
		return true
	}
	latest, hasLatest := buf.PeekLatest()
	if !hasLatest || parentTS > latest.Timestamp {
		parent.CreatedAt, parent.LastUpdate = now, now
		buf.Push(parent)
		if hasLatest && ce.onClose != nil {
			fn, closed := ce.onClose, latest
			callback = func() { fn(key, closed) }
		}
		return true
	}
	// The search above held the same lock and found no parent. This is insertion,
	// for which mergeHistorical returns no revision and never fabricates a close.
	ce.mergeHistorical(key, buf, parent, now)
	return true
}

// repairSeedAncestors visits each affected existing parent once per level.
func (ce *CandleEngine) repairSeedAncestors(exchange, marketType, symbol, childTF string, timestamps []int64) {
	for _, step := range candle.FoldChain() {
		if step.Child != childTF {
			continue
		}
		parents := candle.UniqueSortedParentTimestamps(step.TF, timestamps)
		// Avoid scanning the ring for every absent parent in a historical seed.
		// A parent that appears concurrently is built by its own producer; each
		// candidate still rechecks existence and children under the write lock.
		existing := make(map[int64]struct{})
		ce.mu.RLock()
		if buf := ce.buffers[CompositeKey{exchange, marketType, symbol, step.TF}]; buf != nil {
			for i := 0; i < buf.Len(); i++ {
				c, _ := buf.At(i)
				existing[c.Timestamp] = struct{}{}
			}
		}
		ce.mu.RUnlock()
		timestamps = timestamps[:0]
		for _, ts := range parents {
			if _, ok := existing[ts]; !ok {
				continue
			}
			if ce.rebuildFromChildren(exchange, marketType, symbol, step, ts, true) {
				timestamps = append(timestamps, ts)
			}
		}
		if len(timestamps) == 0 {
			return
		}
		childTF = step.TF
	}
}
