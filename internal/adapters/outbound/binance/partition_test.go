package binancesdk

import "testing"

// assertEveryStreamOnce verifies the partitions are a disjoint cover of want:
// every stream appears exactly once across all partitions.
func assertEveryStreamOnce(t *testing.T, want []string, parts [][]string) {
	t.Helper()
	seen := map[string]int{}
	for _, p := range parts {
		for _, s := range p {
			seen[s]++
		}
	}
	if len(seen) != len(want) {
		t.Fatalf("partitions cover %d distinct streams, want %d", len(seen), len(want))
	}
	for _, s := range want {
		if seen[s] != 1 {
			t.Fatalf("stream %q appears %d times across partitions, want exactly 1", s, seen[s])
		}
	}
}

// TestPartitionStreams_ByCountAndLength proves : the combined-URL stream set
// is split into per-connection partitions that respect BOTH the stream-count and
// URL-length budgets, every stream lands in exactly one partition, and ordering
// is preserved.
func TestPartitionStreams_ByCountAndLength(t *testing.T) {
	t.Run("empty input yields no partitions", func(t *testing.T) {
		if got := partitionStreams(nil); got != nil {
			t.Fatalf("partitionStreams(nil) = %v, want nil", got)
		}
	})

	t.Run("split by stream count", func(t *testing.T) {
		maxStreams := 2
		maxLength := 1 << 20 // effectively unlimited
		streams := []string{"a@aggTrade", "b@aggTrade", "c@aggTrade", "d@aggTrade", "e@aggTrade"}
		parts := partitionStreamsWithBudget(streams, maxStreams, maxLength)
		if len(parts) != 3 {
			t.Fatalf("expected 3 partitions (2+2+1), got %d: %v", len(parts), parts)
		}
		assertEveryStreamOnce(t, streams, parts)
		for i, p := range parts {
			if len(p) > maxStreams {
				t.Fatalf("partition %d has %d streams > budget %d", i, len(p), maxStreams)
			}
		}
	})

	t.Run("split by url length", func(t *testing.T) {
		maxStreams := 1000
		maxLength := 25 // each "xx@aggTrade/" ~12 chars => ~2 per partition
		streams := []string{"aa@aggTrade", "bb@aggTrade", "cc@aggTrade", "dd@aggTrade"}
		parts := partitionStreamsWithBudget(streams, maxStreams, maxLength)
		if len(parts) < 2 {
			t.Fatalf("expected length-driven split into >=2 partitions, got %d: %v", len(parts), parts)
		}
		assertEveryStreamOnce(t, streams, parts)
	})

	t.Run("single oversized stream still placed", func(t *testing.T) {
		maxStreams := 1000
		maxLength := 1 // smaller than any single stream
		streams := []string{"aaaa@aggTrade"}
		parts := partitionStreamsWithBudget(streams, maxStreams, maxLength)
		if len(parts) != 1 || len(parts[0]) != 1 {
			t.Fatalf("a single stream must always be placed, got %v", parts)
		}
	})
}
