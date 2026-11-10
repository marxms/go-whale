// Package health provides the per-symbol / per-partition freshness ledger used by the
// Binance FUTURES transport. It exists because the aggregate connection-level signal (one
// shared lastMessageAt / messagesIn) kept a dead partition invisible while a live sibling
// partition kept the aggregate warm.
//
// It is the liveness authority for that transport, NOT for every stream in the process —
// BingX does not use it.
//
// The ledger is keyed by symbol; each entry holds atomics so the ingest hot path
// (~50k trades/s) stamps without locking. The symbol set is stable after warmup,
// so a sync.Map (read-optimized) over atomic-valued entries gives O(1), lock-free
// stamps, while the periodic per-partition scan tolerates a full Range.
package health

import (
	"math"
	"sync"
	"sync/atomic"
)

// entry is the per-symbol freshness record.
type entry struct {
	lastSeenAtMs atomic.Int64 // wall-clock ms of the last ingested message
	partitionID  atomic.Int32 // owning connection partition; -1 until assigned
}

// Ledger is a concurrent symbol -> freshness map.
type Ledger struct {
	entries sync.Map // string -> *entry
}

// New returns an empty ledger.
func New() *Ledger { return &Ledger{} }

func (l *Ledger) get(symbol string) *entry {
	if v, ok := l.entries.Load(symbol); ok {
		return v.(*entry)
	}
	e := &entry{}
	e.partitionID.Store(-1)
	actual, _ := l.entries.LoadOrStore(symbol, e)
	return actual.(*entry)
}

// Stamp records that a message for symbol arrived at nowMs. Hot path: a single
// map read (fast path) plus one atomic store.
func (l *Ledger) Stamp(symbol string, nowMs int64) {
	l.get(symbol).lastSeenAtMs.Store(nowMs)
}

// Assign sets the owning partition for symbol (called when its handler attaches
// on the connect/reconnect path). A symbol with no assignment is excluded from
// per-partition staleness until assigned.
func (l *Ledger) Assign(symbol string, partitionID int) {
	if partitionID < math.MinInt32 {
		partitionID = math.MinInt32
	} else if partitionID > math.MaxInt32 {
		partitionID = math.MaxInt32
	}
	l.get(symbol).partitionID.Store(int32(partitionID))
}

// LastSeen returns the last-seen wall-clock ms for symbol (0,false if unknown).
func (l *Ledger) LastSeen(symbol string) (int64, bool) {
	if v, ok := l.entries.Load(symbol); ok {
		return v.(*entry).lastSeenAtMs.Load(), true
	}
	return 0, false
}

// Forget removes a symbol (on unsubscribe).
func (l *Ledger) Forget(symbol string) { l.entries.Delete(symbol) }

// PartitionStat summarizes one partition's freshness.
type PartitionStat struct {
	PartitionID int
	Total       int // assigned symbols
	Stale       int // assigned symbols with no message within thresholdMs
}

// StaleFraction returns Stale/Total (0 when Total==0).
func (s PartitionStat) StaleFraction() float64 {
	if s.Total == 0 {
		return 0
	}
	return float64(s.Stale) / float64(s.Total)
}

// PartitionStaleness reports per-partition staleness as of nowMs: a symbol is
// stale when it has never been stamped or its last message is older than
// thresholdMs. Off the hot path (periodic scan), so a full Range is acceptable.
func (l *Ledger) PartitionStaleness(nowMs, thresholdMs int64) map[int]PartitionStat {
	stats := make(map[int]PartitionStat)
	l.entries.Range(func(_, v any) bool {
		e := v.(*entry)
		pid := int(e.partitionID.Load())
		if pid < 0 {
			return true // not yet assigned to a partition
		}
		s := stats[pid]
		s.PartitionID = pid
		s.Total++
		last := e.lastSeenAtMs.Load()
		if last == 0 || nowMs-last > thresholdMs {
			s.Stale++
		}
		stats[pid] = s
		return true
	})
	return stats
}

// Size returns the number of tracked symbols.
func (l *Ledger) Size() int {
	n := 0
	l.entries.Range(func(_, _ any) bool { n++; return true })
	return n
}
