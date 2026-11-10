package scheduler

import (
	"context"
	"time"
)

// AwaitOutcome says how a readiness wait ended.
type AwaitOutcome uint8

const (
	// AwaitReady means the predicate became true before the timeout.
	AwaitReady AwaitOutcome = iota
	// AwaitTimedOut means the deadline elapsed while the predicate was still false.
	AwaitTimedOut
	// AwaitCancelled means ctx ended before the predicate became true.
	AwaitCancelled
)

// Await polls ready until it returns true, the timeout elapses or ctx is canceled.
//
// The predicate is evaluated immediately before the first wait, so a fleet that is
// already published costs no ticker turn. The timers live here so application wiring only
// supplies the policy question ("is the fleet published?"), not the clock.
func Await(
	ctx context.Context,
	timeout, interval time.Duration,
	ready func(context.Context) bool,
) AwaitOutcome {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(interval)
	defer tick.Stop()

	for {
		if ready(ctx) {
			return AwaitReady
		}
		select {
		case <-ctx.Done():
			return AwaitCancelled
		case <-deadline.C:
			return AwaitTimedOut
		case <-tick.C:
		}
	}
}

// Every runs fn on every interval tick until ctx is canceled or fn returns true.
// There is no immediate call: the first invocation is one interval after start.
func Every(ctx context.Context, interval time.Duration, fn func(context.Context) bool) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if fn(ctx) {
				return
			}
		}
	}
}
