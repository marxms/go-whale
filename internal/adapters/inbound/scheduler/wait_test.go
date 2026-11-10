package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestAwait_ReadyImmediatelyChecksBeforeWaiting(t *testing.T) {
	var checks atomic.Int32
	outcome := Await(context.Background(), time.Second, time.Millisecond, func(context.Context) bool {
		checks.Add(1)
		return true
	})

	if outcome != AwaitReady {
		t.Fatalf("outcome = %v, want AwaitReady", outcome)
	}
	if got := checks.Load(); got != 1 {
		t.Errorf("readiness was checked %d times, want exactly once for an already-ready fleet", got)
	}
}

func TestAwait_TimesOut(t *testing.T) {
	start := time.Now()
	outcome := Await(context.Background(), 20*time.Millisecond, 5*time.Millisecond, func(context.Context) bool {
		return false
	})

	if outcome != AwaitTimedOut {
		t.Fatalf("outcome = %v, want AwaitTimedOut", outcome)
	}
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Errorf("wait returned after %s, before its own deadline", elapsed)
	}
}

func TestAwait_HonoursCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan AwaitOutcome, 1)
	go func() {
		done <- Await(ctx, time.Hour, time.Hour, func(context.Context) bool { return false })
	}()

	select {
	case outcome := <-done:
		if outcome != AwaitCancelled {
			t.Fatalf("outcome = %v, want AwaitCancelled", outcome)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the wait ignored cancellation")
	}
}

func TestEvery_StopsWhenFnReportsDone(t *testing.T) {
	var calls atomic.Int32
	done := make(chan struct{})

	go func() {
		Every(context.Background(), time.Millisecond, func(context.Context) bool {
			return calls.Add(1) == 1
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Every did not stop after fn reported done")
	}
}

func TestEvery_StopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		Every(ctx, time.Millisecond, func(context.Context) bool { return false })
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Every ignored cancellation")
	}
}
