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

func TestMemoryRetrySchedulerNeverRunsTheLoopWithoutAStartedContext(t *testing.T) {
	s := NewMemoryRetryScheduler()
	s.pending.Store(true)

	s.processLoop()

	if s.pending.Load() {
		t.Fatal("processLoop kept running with no context: the loop must be tracked or not run")
	}
}

func TestMemoryRetrySchedulerStartResumesAQueuedJob(t *testing.T) {
	s := NewMemoryRetryScheduler()
	s.Schedule(BackfillRetryJob{ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "5m"})

	s.Start(context.Background())
	defer s.Stop()

	if !s.pending.Load() {
		t.Fatal("Start did not resume the loop for a job queued before Start")
	}
}
