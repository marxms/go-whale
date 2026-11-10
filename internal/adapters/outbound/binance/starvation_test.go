package binancesdk

import (
	"fmt"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/health"
)

func TestRecentlyStarved(t *testing.T) {
	h := &starvationHeartbeat{}
	now := time.Now().UnixMilli()
	if h.recentlyStarved(now) {
		t.Fatal("never-starved heartbeat must not report recently starved")
	}
	h.markStarved(now - 5_000) // 5s ago, within the suppress window
	if !h.recentlyStarved(now) {
		t.Fatal("a starvation 5s ago should be recent")
	}
	h.markStarved(now - starvationSuppressWindowMs - 1_000) // outside the window
	if h.recentlyStarved(now) {
		t.Fatal("a starvation outside the window must not report recent")
	}
}

func staleAllPartitions(c *SDKClient, partitions, perPartition int, ageMs int64) {
	now := time.Now().UnixMilli()
	for p := 0; p < partitions; p++ {
		for i := 0; i < perPartition; i++ {
			s := fmt.Sprintf("P%dS%d", p, i)
			c.ledger.Assign(s, p)
			c.ledger.Stamp(s, now-ageMs)
		}
	}
}

// TestStarvationInterlockSuppressesAllStaleReconnect: when every partition looks
// stale AND the process was just descheduled, the staleness is CPU starvation, not
// stream death — the reconnect must be suppressed (don't storm a starved box, O4).
func TestStarvationInterlockSuppressesAllStaleReconnect(t *testing.T) {
	c := &SDKClient{combinedURLConnectEnabled: true, livenessLedgerEnabled: true, ledger: health.New(), reconnectCh: make(chan string, 1), heartbeat: &starvationHeartbeat{}}
	staleAllPartitions(c, 2, 6, livenessStaleThresholdMs+10_000)
	c.heartbeat.markStarved(time.Now().UnixMilli())

	c.checkPartitionLiveness()

	select {
	case <-c.reconnectCh:
		t.Fatal("reconnect must be SUPPRESSED when all-partition staleness coincides with process starvation")
	default:
	}
}

// TestStarvationInterlockReconnectsWithFreshSibling: a fresh sibling partition
// proves the process IS scheduled, so a dead partition is genuine and reconnects
// even while the starvation flag is set.
func TestStarvationInterlockReconnectsWithFreshSibling(t *testing.T) {
	c := &SDKClient{combinedURLConnectEnabled: true, livenessLedgerEnabled: true, ledger: health.New(), reconnectCh: make(chan string, 1), heartbeat: &starvationHeartbeat{}}
	now := time.Now().UnixMilli()
	for i := 0; i < 6; i++ {
		s := fmt.Sprintf("D%d", i)
		c.ledger.Assign(s, 0)
		c.ledger.Stamp(s, now-livenessStaleThresholdMs-10_000)
	}
	for i := 0; i < 6; i++ {
		s := fmt.Sprintf("L%d", i)
		c.ledger.Assign(s, 1)
		c.ledger.Stamp(s, now)
	}
	c.heartbeat.markStarved(now)

	c.checkPartitionLiveness()

	select {
	case <-c.reconnectCh:
	default:
		t.Fatal("a dead partition with a fresh sibling must reconnect despite the starvation flag")
	}
}

// TestStarvationInterlockReconnectsAllStaleWhenNotStarved: all partitions dead with
// a scheduled process (e.g. a real network drop) is genuine — reconnect.
func TestStarvationInterlockReconnectsAllStaleWhenNotStarved(t *testing.T) {
	c := &SDKClient{combinedURLConnectEnabled: true, livenessLedgerEnabled: true, ledger: health.New(), reconnectCh: make(chan string, 1), heartbeat: &starvationHeartbeat{}}
	staleAllPartitions(c, 2, 6, livenessStaleThresholdMs+10_000)

	c.checkPartitionLiveness()

	select {
	case <-c.reconnectCh:
	default:
		t.Fatal("all partitions dead without starvation must reconnect")
	}
}
