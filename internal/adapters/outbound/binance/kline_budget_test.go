package binancesdk

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// TestKlineBudgetIsSeparateFromAggTrade proves the kline path has its own budget. Behaviour
// alone cannot tell two equal numbers apart, so the reference is also asserted where it is
// written: the experiment must be able to change one budget without touching the other.
func TestKlineBudgetIsSeparateFromAggTrade(t *testing.T) {
	streams := make([]string, 0, maxKlineStreamsPerConnection+1)
	for index := 0; index <= maxKlineStreamsPerConnection; index++ {
		streams = append(streams, fmt.Sprintf("s%d", index))
	}

	// Exactly the budget fits on one connection; one extra forces a second.
	if parts := partitionKlineStreams(streams[:maxKlineStreamsPerConnection]); len(parts) != 1 {
		t.Fatalf("kline budget: %d streams produced %d partitions, want 1",
			maxKlineStreamsPerConnection, len(parts))
	}
	parts := partitionKlineStreams(streams)
	if len(parts) != 2 {
		t.Fatalf("%d streams produced %d partitions, want 2 (kline budget %d)",
			len(streams), len(parts), maxKlineStreamsPerConnection)
	}
	if len(parts[0]) != maxKlineStreamsPerConnection {
		t.Fatalf("first partition = %d streams, want the kline budget", len(parts[0]))
	}

	source, err := os.ReadFile("flags.go")
	if err != nil {
		t.Fatalf("reading flags.go: %v", err)
	}
	text := string(source)
	if !strings.Contains(text, "func partitionKlineStreams(") {
		t.Fatal("the kline partitioner no longer exists")
	}
	if !strings.Contains(text, "partitionStreamsWithBudget(streams, maxKlineStreamsPerConnection, maxCombinedURLLength)") {
		t.Fatal("the kline partitioner no longer reads maxKlineStreamsPerConnection")
	}
	if !strings.Contains(text, "maxKlineStreamsPerConnection = 200") {
		t.Fatal("the kline budget must carry the measured 200-stream value, declared separately from the aggTrade constant")
	}
}
