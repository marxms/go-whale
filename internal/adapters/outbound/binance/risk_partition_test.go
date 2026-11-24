package binancesdk

import (
	"fmt"
	"testing"
)

// Measured on 2026-11-24: the trade partition carrying 币安人生USDT entered a reconnect flap
// (25+ remote 1006s in 35 min) while every ASCII-only partition stayed up. Isolating the
// risky names bounds a venue-side rejection to a handful of symbols instead of a full
// 200-332-stream partition.
func TestRiskyStreamsAreIsolated(t *testing.T) {
	t.Parallel()

	risky := "币安人生usdt@aggTrade"
	parts := partitionStreams([]string{"aaa@aggTrade", risky, "bbb@aggTrade", "ccc@aggTrade"})
	if len(parts) != 2 {
		t.Fatalf("partitions = %d, want 2 (one risky, one safe): %v", len(parts), parts)
	}
	if len(parts[0]) != 1 || parts[0][0] != risky {
		t.Fatalf("first partition = %v, want only the risky stream", parts[0])
	}
	if len(parts[1]) != 3 {
		t.Fatalf("safe partition = %d streams, want 3", len(parts[1]))
	}
}

func TestKlineRiskyStreamsAreIsolated(t *testing.T) {
	t.Parallel()

	risky := "币安人生usdt@kline_5m"
	parts := partitionKlineStreams([]string{"aaa@kline_5m", risky, "bbb@kline_5m"})
	if len(parts) != 2 || len(parts[0]) != 1 || parts[0][0] != risky {
		t.Fatalf("kline partitions = %v, want the risky stream alone first", parts)
	}
}

// Isolating the risky names must not duplicate or drop a subscription: every stream still
// lands in exactly one partition, and the per-class budget still applies.
func TestPartitioningKeepsEveryStreamOnce(t *testing.T) {
	t.Parallel()

	streams := make([]string, 0, 400)
	for i := 0; i < 399; i++ {
		streams = append(streams, fmt.Sprintf("sym%03d@aggTrade", i))
	}
	streams = append(streams, "币安人生usdt@aggTrade")

	parts := partitionStreams(streams)
	seen := map[string]int{}
	for _, part := range parts {
		for _, stream := range part {
			seen[stream]++
		}
	}
	if len(seen) != len(streams) {
		t.Fatalf("distinct streams = %d, want %d", len(seen), len(streams))
	}
	for stream, count := range seen {
		if count != 1 {
			t.Fatalf("stream %q appears %d times", stream, count)
		}
	}
}
