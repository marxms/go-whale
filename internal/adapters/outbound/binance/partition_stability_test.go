package binancesdk

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/ingestion/streaming"
)

// TestPartitionReconnectDelayUsesStabilityFloor pins the schedule: a stable socket dials at
// once; a short-lived one pays base, then doubles, then caps. Without this, the 2026-11-21
// storm (7 dials in 17 s) is reproduced by every flapping endpoint.
func TestPartitionReconnectDelayUsesStabilityFloor(t *testing.T) {
	if got := partitionReconnectDelay(2*time.Minute, 3); got != 0 {
		t.Fatalf("stable connection delay = %v, want 0", got)
	}
	if got := partitionReconnectDelay(2*time.Second, 0); got != 0 {
		t.Fatalf("unarmed streak delay = %v, want 0", got)
	}
	if got := partitionReconnectDelay(2*time.Second, 1); got != reconnectBackoffBase {
		t.Fatalf("first short-lived reconnect delay = %v, want %v", got, reconnectBackoffBase)
	}
	if got := partitionReconnectDelay(2*time.Second, 2); got != reconnectBackoffBase*2 {
		t.Fatalf("second short-lived reconnect delay = %v, want %v", got, reconnectBackoffBase*2)
	}
	if got := partitionReconnectDelay(2*time.Second, 10); got != reconnectBackoffMax {
		t.Fatalf("capped reconnect delay = %v, want %v", got, reconnectBackoffMax)
	}
}

// TestNotePartitionReconnectTracksShortLivedSockets is the effect test: the streak advances
// while sockets die young and resets when one lives past the floor.
func TestNotePartitionReconnectTracksShortLivedSockets(t *testing.T) {
	c := NewSDKClient(streaming.ClientConfig{TradeBufferSize: 10, MarketType: "swap"})
	t.Cleanup(func() { _ = c.Close() })

	c.markPartitionUp(0)
	delay, streak := c.notePartitionReconnect(0, time.Now())
	if delay != reconnectBackoffBase || streak != 1 {
		t.Fatalf("first short reconnect = (%v, %d), want (%v, 1)", delay, streak, reconnectBackoffBase)
	}
	c.markPartitionUp(0)
	delay, streak = c.notePartitionReconnect(0, time.Now())
	if delay != reconnectBackoffBase*2 || streak != 2 {
		t.Fatalf("second short reconnect = (%v, %d), want (%v, 2)", delay, streak, reconnectBackoffBase*2)
	}

	// A socket that lived past the floor resets the streak: the next reconnect dials at once.
	c.partitionStabilityMu.Lock()
	c.partitionUpSince[0] = time.Now().Add(-2 * reconnectStabilityFloor)
	c.partitionStabilityMu.Unlock()
	delay, streak = c.notePartitionReconnect(0, time.Now())
	if delay != 0 || streak != 0 {
		t.Fatalf("stable reconnect = (%v, %d), want (0, 0)", delay, streak)
	}
}

// TestPartitionReconnectPathUsesTheStabilityDelay keeps the helpers wired to the real path.
// A helper that no reconnect calls is a green test over the same storm.
func TestPartitionReconnectPathUsesTheStabilityDelay(t *testing.T) {
	source, err := os.ReadFile("adapter.go")
	if err != nil {
		t.Fatalf("reading adapter.go: %v", err)
	}
	text := string(source)
	for _, want := range []string{
		"notePartitionReconnect(req.partition, req.at)",
		"time.AfterFunc(delay",
		"markPartitionUp(pid)",
		"markPartitionUp(i)",
		"c.disconnectTime.Store(req.at)",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("the reconnect path lost a required piece: %s missing", want)
		}
	}
}

// A request older than the live socket is stale: the partition was already restored after the
// failure it names. Acting on it tears down a healthy connection — the pattern 77cdeab fought.
func TestPartitionAlreadyRestoredDetectsStaleRequests(t *testing.T) {
	c := NewSDKClient(streaming.ClientConfig{TradeBufferSize: 10, MarketType: "swap"})
	t.Cleanup(func() { _ = c.Close() })
	up := time.Now()
	c.partitionStabilityMu.Lock()
	c.partitionUpSince[0] = up
	c.partitionStabilityMu.Unlock()
	if !c.partitionAlreadyRestored(0, up.Add(-time.Second)) {
		t.Fatal("a request observed before the live socket was not stale")
	}
	if c.partitionAlreadyRestored(0, up.Add(time.Second)) {
		t.Fatal("a request observed after the live socket was treated as stale")
	}
	if c.partitionAlreadyRestored(1, up) {
		t.Fatal("a partition with no recorded socket was treated as restored")
	}
}
