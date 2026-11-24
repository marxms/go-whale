package health

import (
	"reflect"
	"testing"
)

// Membership is the authority a partition-scoped reconnect reads. It must reflect every path that
// assigns a symbol — the combined dial and the dynamic Subscribe — and be deterministic, because a
// redial built from a random order produces a different URL and a different stream split.
func TestSymbolsForPartition(t *testing.T) {
	l := New()
	l.Assign("SOL/USDT", 0)
	l.Assign("BTC/USDT", 0)
	l.Assign("ETH/USDT", 1)
	l.Assign("XRP/USDT", 2)
	l.Assign("ADA/USDT", 0)

	if got := l.SymbolsForPartition(0); !reflect.DeepEqual(got, []string{"ADA/USDT", "BTC/USDT", "SOL/USDT"}) {
		t.Fatalf("partition 0 = %v, want the sorted members", got)
	}
	if got := l.SymbolsForPartition(1); !reflect.DeepEqual(got, []string{"ETH/USDT"}) {
		t.Fatalf("partition 1 = %v", got)
	}
	if got := l.SymbolsForPartition(9); len(got) != 0 {
		t.Fatalf("unknown partition = %v, want empty", got)
	}
}

// Reassignment moves the symbol: a dynamic Subscribe attaches on partition 0, so the old partition
// must stop claiming it or a redial would carry a symbol the socket no longer serves.
func TestSymbolsForPartitionFollowsReassignment(t *testing.T) {
	l := New()
	l.Assign("BTC/USDT", 1)
	l.Assign("BTC/USDT", 0)

	if got := l.SymbolsForPartition(1); len(got) != 0 {
		t.Fatalf("partition 1 = %v, want empty after the move", got)
	}
	if got := l.SymbolsForPartition(0); !reflect.DeepEqual(got, []string{"BTC/USDT"}) {
		t.Fatalf("partition 0 = %v", got)
	}
}
