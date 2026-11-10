package binancesdk

import (
	"fmt"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/health"
)

// TestGetHealthStateDegradesOnDeadPartition proves 2E: a dead partition degrades
// /health even when the aggregate last-message is fresh (a live sibling keeps that
// aggregate warm) — closing the O1 aggregation lie at the health surface.
func TestGetHealthStateDegradesOnDeadPartition(t *testing.T) {
	c := &SDKClient{combinedURLConnectEnabled: true, livenessLedgerEnabled: true, ledger: health.New()}
	c.connected.Store(true)
	c.lastMessageAt.Store(time.Now()) // aggregate FRESH

	if got := c.GetHealthState(); got != HealthHealthy {
		t.Fatalf("expected HealthHealthy with a fresh aggregate and no dead partition, got %v", got)
	}

	c.ledgerDeadPartition.Store(true)
	if got := c.GetHealthState(); got != HealthStalled {
		t.Fatalf("expected HealthStalled when a partition is dead despite the fresh aggregate, got %v", got)
	}
}

// TestCheckPartitionLivenessPublishesDeadPartitionFlag proves the watchdog publishes
// the dead-partition flag that GetHealthState reads, and clears it on recovery.
func TestCheckPartitionLivenessPublishesDeadPartitionFlag(t *testing.T) {
	c := &SDKClient{combinedURLConnectEnabled: true, livenessLedgerEnabled: true, ledger: health.New(), reconnectCh: make(chan string, 1), heartbeat: &starvationHeartbeat{}}
	now := time.Now().UnixMilli()
	for i := 0; i < 6; i++ {
		s := fmt.Sprintf("D%d", i)
		c.ledger.Assign(s, 0)
		c.ledger.Stamp(s, now-livenessStaleThresholdMs-10_000)
	}
	c.checkPartitionLiveness()
	if !c.ledgerDeadPartition.Load() {
		t.Fatal("watchdog must publish ledgerDeadPartition=true when a partition is dead")
	}

	for i := 0; i < 6; i++ {
		c.ledger.Stamp(fmt.Sprintf("D%d", i), time.Now().UnixMilli())
	}
	select {
	case <-c.reconnectCh:
	default:
	}
	c.checkPartitionLiveness()
	if c.ledgerDeadPartition.Load() {
		t.Fatal("watchdog must clear ledgerDeadPartition when all partitions are fresh again")
	}
}
