package recovery

import (
	"context"
	"testing"
)

func TestMemoryRetrySchedulerStopCancelsTheLoop(t *testing.T) {
	s := NewMemoryRetryScheduler()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Start(ctx)
	s.mu.Lock()
	runCtx := s.ctx
	s.mu.Unlock()
	s.Stop()
	select {
	case <-runCtx.Done():
	default:
		t.Fatal("Stop did not cancel the context the retry loop reads")
	}

	// Idempotent: a second stop must not panic or require a new Start.
	s.Stop()
}
