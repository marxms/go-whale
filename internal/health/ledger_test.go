package health

import (
	"fmt"
	"math"
	"sync"
	"testing"
)

func TestLedgerStampAndLastSeen(t *testing.T) {
	l := New()
	if _, ok := l.LastSeen("BTC/USDT"); ok {
		t.Fatal("unknown symbol should report not-seen")
	}
	l.Stamp("BTC/USDT", 1000)
	if got, ok := l.LastSeen("BTC/USDT"); !ok || got != 1000 {
		t.Fatalf("LastSeen = %d,%v want 1000,true", got, ok)
	}
	l.Stamp("BTC/USDT", 2000)
	if got, _ := l.LastSeen("BTC/USDT"); got != 2000 {
		t.Fatalf("LastSeen not updated: %d", got)
	}
}

// TestPartitionStalenessIsolatesDeadPartition proves per-partition isolation: when
// partition 0 goes silent while partition 1 keeps stamping, partition 0 reads fully
// stale and partition 1 fully fresh. A live sibling cannot mask a dead partition —
// the defect the aggregate signal could never distinguish.
func TestPartitionStalenessIsolatesDeadPartition(t *testing.T) {
	l := New()
	const now = 100_000
	const threshold = 30_000

	for _, s := range []string{"A", "B", "C"} { // partition 0: dead (stamped 60s ago)
		l.Assign(s, 0)
		l.Stamp(s, now-60_000)
	}
	for _, s := range []string{"D", "E"} { // partition 1: fresh (1s ago)
		l.Assign(s, 1)
		l.Stamp(s, now-1_000)
	}

	stats := l.PartitionStaleness(now, threshold)
	if p0 := stats[0]; p0.Total != 3 || p0.Stale != 3 || p0.StaleFraction() != 1.0 {
		t.Fatalf("partition 0 = %+v, want all 3 stale (fraction 1.0)", p0)
	}
	if p1 := stats[1]; p1.Total != 2 || p1.Stale != 0 || p1.StaleFraction() != 0.0 {
		t.Fatalf("partition 1 = %+v, want all fresh (fraction 0.0)", p1)
	}
}

func TestNeverStampedCountsStale(t *testing.T) {
	l := New()
	l.Assign("X", 0)
	if stats := l.PartitionStaleness(100_000, 30_000); stats[0].Stale != 1 {
		t.Fatalf("never-stamped assigned symbol should be stale: %+v", stats[0])
	}
}

func TestUnassignedExcludedFromPartitionStats(t *testing.T) {
	l := New()
	l.Stamp("Y", 50_000) // stamped but never Assign'd
	if stats := l.PartitionStaleness(100_000, 30_000); len(stats) != 0 {
		t.Fatalf("unassigned symbol must not appear in partition stats: %+v", stats)
	}
}

func TestForgetRemovesSymbol(t *testing.T) {
	l := New()
	l.Stamp("Z", 1)
	l.Forget("Z")
	if _, ok := l.LastSeen("Z"); ok {
		t.Fatal("forgotten symbol should be gone")
	}
}

// TestLedgerConcurrentStampAndScan exercises the lock-free hot path under -race:
// many goroutines stamp while a scanner ranges concurrently.
func TestLedgerConcurrentStampAndScan(_ *testing.T) {
	l := New()
	symbols := make([]string, 200)
	for i := range symbols {
		symbols[i] = fmt.Sprintf("S%03d", i)
		l.Assign(symbols[i], i%2)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			var ts int64 = 1
			for {
				select {
				case <-stop:
					return
				default:
				}
				l.Stamp(symbols[(seed+int(ts))%len(symbols)], ts)
				ts++
			}
		}(g)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			_ = l.PartitionStaleness(int64(i), 30_000)
		}
		close(stop)
	}()
	wg.Wait()
}

// TestLedgerAssignClampsPartitionID pins the int32 bound: a partition id outside the int32
// range must saturate instead of wrapping into a different partition.
func TestLedgerAssignClampsPartitionID(t *testing.T) {
	l := New()
	l.Assign("BTC/USDT", math.MaxInt64)
	if got := l.get("BTC/USDT").partitionID.Load(); got != math.MaxInt32 {
		t.Fatalf("partition id = %d, want %d", got, int32(math.MaxInt32))
	}
	l.Assign("ETH/USDT", math.MinInt64)
	if got := l.get("ETH/USDT").partitionID.Load(); got != math.MinInt32 {
		t.Fatalf("partition id = %d, want %d", got, int32(math.MinInt32))
	}
}
