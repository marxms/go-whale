package binancesdk

import (
	"fmt"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/health"
)

// TestPartitionLikelyLive proves the O6 gate: a dynamic subscribe writes only to a
// partition that is live (fresh per the ledger and not mid-reconnect). A
// collectively-stale partition is treated as dead so the subscribe defers instead
// of writing to a dead socket; unknown/empty partitions are optimistically live.
func TestPartitionLikelyLive(t *testing.T) {
	c := &SDKClient{combinedURLConnectEnabled: true, livenessLedgerEnabled: true, ledger: health.New()}
	now := time.Now().UnixMilli()

	for i := 0; i < 6; i++ { // partition 0: fresh
		s := fmt.Sprintf("L%d", i)
		c.ledger.Assign(s, 0)
		c.ledger.Stamp(s, now)
	}
	if !c.partitionLikelyLive(0) {
		t.Fatal("fresh partition 0 should be live")
	}

	for i := 0; i < 6; i++ { // partition 1: collectively stale
		s := fmt.Sprintf("D%d", i)
		c.ledger.Assign(s, 1)
		c.ledger.Stamp(s, now-livenessStaleThresholdMs-10_000)
	}
	if c.partitionLikelyLive(1) {
		t.Fatal("collectively-stale partition 1 should not be live")
	}

	if !c.partitionLikelyLive(9) {
		t.Fatal("unknown/empty partition should be optimistically live")
	}

	c.reconnecting.Store(true)
	if c.partitionLikelyLive(0) {
		t.Fatal("no partition is live while a reconnect is in progress")
	}
}
