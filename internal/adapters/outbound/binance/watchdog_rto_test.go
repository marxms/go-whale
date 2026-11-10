package binancesdk

import (
	"testing"
	"time"
)

// TestWatchdogTuningMeetsRTO is a deterministic guard tying the per-partition
// watchdog tuning to the owner SLO: the worst-case detection latency (a symbol
// must be silent for the staleness threshold, observed at most one watchdog
// interval later) plus a reconnect budget must stay under the 60s RTO for a dead
// partition. If someone widens the threshold/interval past the RTO, this fails.
func TestWatchdogTuningMeetsRTO(t *testing.T) {
	const rto = 60 * time.Second
	const reconnectBudget = 10 * time.Second // headroom for the combined-URL handshake + resubscribe

	detection := time.Duration(livenessStaleThresholdMs)*time.Millisecond + livenessWatchdogInterval
	if detection+reconnectBudget > rto {
		t.Fatalf("watchdog detection %v + reconnect budget %v exceeds the %v RTO — recalibrate livenessStaleThresholdMs / livenessWatchdogInterval",
			detection, reconnectBudget, rto)
	}
}

// TestStarvationSuppressCannotDelayGenuineDeath asserts the load-bearing invariant
// that the D2 starvation suppress window is SHORTER than the staleness threshold.
// This guarantees that a starvation flag set around the time of a genuine partition
// death has already expired by the time that death becomes detectable (threshold
// elapsed) — so the interlock can never suppress, and therefore never delay, a real
// reconnect past the RTO. If someone widens the suppress window past the threshold,
// this fails.
func TestStarvationSuppressCannotDelayGenuineDeath(t *testing.T) {
	suppress := time.Duration(starvationSuppressWindowMs) * time.Millisecond
	threshold := time.Duration(livenessStaleThresholdMs) * time.Millisecond
	if suppress >= threshold {
		t.Fatalf("starvation suppress window %v must be < staleness threshold %v, else the D2 interlock could delay a genuine partition death past the RTO",
			suppress, threshold)
	}
}
