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

// RepairAncestors rebuilds every coarser bucket containing each marked child bucket.
//
// FoldUp runs only for a live frame, so a closed parent otherwise keeps its pre-mark value and
// still claims HasGap=false. Call it outside the engine lock: FoldUp takes ce.mu itself.
func (ce *CandleEngine) RepairAncestors(exchange, marketType, childTF string, marks []MarkedGap) {
	for _, m := range marks {
		ce.FoldUp(exchange, marketType, m.Symbol, childTF, m.Timestamp)
		// FoldUp stops at the first level whose children are incomplete. A provisional child is
		// not foldable, but every ancestor that CONTAINS it is still unproven: mark the existing
		// ones so a coarser level cannot publish them as clean.
		ce.markAncestorsIncomplete(exchange, marketType, m.Symbol, childTF, m.Timestamp)
	}
}

// markAncestorsIncomplete marks every EXISTING ancestor that contains the given marked bucket as
// provisional, without rebuilding its values. Rebuilding from a provisional child is forbidden;
// leaving the ancestor unmarked is what made a coarser timeframe fold it as clean.
func (ce *CandleEngine) markAncestorsIncomplete(exchange, marketType, symbol, childTF string, childTS int64) {
	now := time.Now().UnixMilli()
	for _, step := range candle.FoldChain() {
		if step.Child != childTF {
			continue
		}
		parentTS := candle.BucketTimestamp(childTS, step.TF)
		key := CompositeKey{exchange, marketType, symbol, step.TF}
		var callback func()
		ce.mu.Lock()
		if buf, ok := ce.buffers[key]; ok {
			for i := 0; i < buf.Len(); i++ {
				old, _ := buf.At(i)
				if old.Timestamp != parentTS {
					continue
				}
				hasGap, gapKind := candle.MergeGapMark(old, candle.Candle{HasGap: true, GapKind: candle.GapProvisional})
				if hasGap == old.HasGap && gapKind == old.GapKind {
					break
				}
				old.HasGap, old.GapKind = hasGap, gapKind
				buf.SetAt(i, old)
				callback = ce.notifyGapChange(key, old, step, parentTS, now, false)
				break
			}
		}
		ce.mu.Unlock()
		if callback != nil {
			callback()
		}
		childTF, childTS = step.TF, parentTS
	}
}

// notifyGapChange arms the revision callback for a completeness change on a stored parent. A bucket
// that has not closed yet is not announced. Call it while holding ce.mu.
func (ce *CandleEngine) notifyGapChange(
	key CompositeKey,
	revised candle.Candle,
	step candle.FoldStep,
	parentTS, now int64,
	existingOnly bool,
) func() {
	tfMs, _ := candle.ParseTimeframeMs(step.TF)
	if tfMs <= 0 || parentTS+tfMs > now {
		return nil
	}
	if existingOnly {
		ce.markSeedRepairLocked(key, parentTS)
		ce.metrics.incFoldSeedRevision()
	}
	fn := ce.onRevised
	if existingOnly && ce.onSeedRevised != nil {
		fn = ce.onSeedRevised
	}
	if fn == nil {
		return nil
	}
	return func() { fn(key, revised) }
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
	// The fold is the only writer of a derived timeframe, so without this the parent's freshness
	// watermark never advances and /health reports a healthy 5m beside a frozen 30m.
	ce.noteBucketTimestamp(exchange, marketType, step.TF, parentTS)

	// A folded bar that contradicts its children is never published as clean: marking keeps the
	// value fresh while telling consumers it is unproven, where refusing would freeze the bucket.
	if violations := candle.ValidateFoldedParent(parent, kids); len(violations) > 0 {
		for _, violation := range violations {
			ce.metrics.incCandleSanityViolation(exchange, marketType, step.TF, string(violation))
		}
		parent.HasGap = true
		if parent.GapKind != candle.GapProvisional {
			parent.GapKind = candle.GapProvisional
		}
	}

	buf := ce.getOrCreate(key)
	for i := 0; i < buf.Len(); i++ {
		old, _ := buf.At(i)
		if old.Timestamp != parentTS {
			continue
		}
		if old.Origin == candle.OriginNative {
			// A native parent keeps its VALUES, but completeness is not one of them: a child heard
			// partially makes the parent's coverage unproven, so the mark travels even when the
			// rebuild is skipped.
			hasGap, gapKind := candle.MergeGapMark(old, parent)
			if hasGap == old.HasGap && gapKind == old.GapKind {
				return true
			}
			gapChanged := old.HasGap != hasGap || old.GapKind != gapKind
			old.HasGap, old.GapKind = hasGap, gapKind
			buf.SetAt(i, old)
			if gapChanged {
				callback = ce.notifyGapChange(key, old, step, parentTS, now, existingOnly)
			}
			return true
		}
		if candle.SameFoldValues(old, parent) {
			// Complete measured children establish derived provenance even when
			// the financial payload is unchanged. A completeness change is
			// observable; source/origin alone do not emit a revision.
			gapChanged := old.HasGap != parent.HasGap || old.GapKind != parent.GapKind
			old.Source, old.Origin, old.HasGap, old.GapKind = parent.Source, parent.Origin, parent.HasGap, parent.GapKind
			buf.SetAt(i, old)
			if gapChanged {
				callback = ce.notifyGapChange(key, old, step, parentTS, now, existingOnly)
			}
			return true
		}
		parent.CreatedAt, parent.LastUpdate = old.CreatedAt, now
		parent.LateTradeCount = old.LateTradeCount
		buf.SetAt(i, parent)
		callback = ce.notifyGapChange(key, parent, step, parentTS, now, existingOnly)
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
