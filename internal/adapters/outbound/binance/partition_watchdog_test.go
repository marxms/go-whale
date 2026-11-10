package binancesdk

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/health"
)

// TestPartitionLivenessWatchdogTriggersOnDeadPartition is the in-action proof of
// the O1 fix: a partition whose symbols are collectively stale triggers a
// reconnect even while a sibling partition is fully live — the case the aggregate
// monitor is structurally blind to.
func TestPartitionLivenessWatchdogTriggersOnDeadPartition(t *testing.T) {
	c := &SDKClient{combinedURLConnectEnabled: true, livenessLedgerEnabled: true, ledger: health.New(), reconnectCh: make(chan string, 1)}
	now := time.Now().UnixMilli()
	for i := 0; i < 6; i++ { // partition 0: dead
		s := fmt.Sprintf("DEAD%d", i)
		c.ledger.Assign(s, 0)
		c.ledger.Stamp(s, now-livenessStaleThresholdMs-10_000)
	}
	for i := 0; i < 6; i++ { // partition 1: live sibling
		s := fmt.Sprintf("LIVE%d", i)
		c.ledger.Assign(s, 1)
		c.ledger.Stamp(s, now)
	}

	c.checkPartitionLiveness()

	select {
	case reason := <-c.reconnectCh:
		if !strings.Contains(reason, "partition 0") {
			t.Fatalf("reconnect reason should name the dead partition 0: %q", reason)
		}
	default:
		t.Fatal("dead partition did not trigger a reconnect — O1 would persist")
	}
}

// TestPartitionLivenessWatchdogIgnoresIlliquidSingleSymbol proves a single
// illiquid aggTrade symbol going quiet does NOT flap the partition (the lesson
// that got the per-symbol STEP2 watchdog discarded): the trigger is the stale
// FRACTION, not any one symbol's silence.
func TestPartitionLivenessWatchdogIgnoresIlliquidSingleSymbol(t *testing.T) {
	c := &SDKClient{combinedURLConnectEnabled: true, livenessLedgerEnabled: true, ledger: health.New(), reconnectCh: make(chan string, 1)}
	now := time.Now().UnixMilli()
	c.ledger.Assign("ILLIQUID", 0)
	c.ledger.Stamp("ILLIQUID", now-livenessStaleThresholdMs-10_000) // 1 stale
	for i := 0; i < 9; i++ {                                        // 9 fresh => 10% stale < 50%
		s := fmt.Sprintf("LIQUID%d", i)
		c.ledger.Assign(s, 0)
		c.ledger.Stamp(s, now)
	}

	c.checkPartitionLiveness()

	select {
	case <-c.reconnectCh:
		t.Fatal("a single illiquid stale symbol must NOT trigger a partition reconnect")
	default:
	}
}

// TestPartitionLivenessWatchdogHoldsDuringReconnect proves the watchdog does not
// pile on while a reconnect is already in progress.
func TestPartitionLivenessWatchdogHoldsDuringReconnect(t *testing.T) {
	c := &SDKClient{combinedURLConnectEnabled: true, livenessLedgerEnabled: true, ledger: health.New(), reconnectCh: make(chan string, 1)}
	c.reconnecting.Store(true)
	now := time.Now().UnixMilli()
	for i := 0; i < 6; i++ {
		s := fmt.Sprintf("DEAD%d", i)
		c.ledger.Assign(s, 0)
		c.ledger.Stamp(s, now-livenessStaleThresholdMs-10_000)
	}

	c.checkPartitionLiveness()

	select {
	case <-c.reconnectCh:
		t.Fatal("watchdog triggered a reconnect while one was already in progress")
	default:
	}
}
