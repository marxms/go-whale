package recovery

import (
	"context"
	"testing"
	"time"
)

// TestManagerQueueFull verifies that when the queue is full, excess jobs are
// coalesced into the pending backlog.
func TestManagerQueueFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := NewManager(nil, 1, nil)
	// Replace the default queue with a tiny one for testing
	m.queue = NewCoalescingQueue(2, 8)

	now := time.Now()
	syms := []string{"BTC/USDT", "ETH/USDT", "SOL/USDT", "DOGE/USDT", "XRP/USDT"}

	// Override context so workers don't start processing unexpectedly
	m.ctx = ctx
	m.cancel = cancel

	m.EnqueueBatch("binance", "swap", syms, now.Add(-5*time.Second), now)

	// Queue size is 2, so 3 should be coalesced into pending backlog.
	stats := m.Stats()
	if stats.Queued != 2 {
		t.Errorf("expected 2 queued, got %d", stats.Queued)
	}
	if stats.Pending != 3 {
		t.Errorf("expected 3 pending, got %d", stats.Pending)
	}
	if stats.Coalesced != 3 {
		t.Errorf("expected 3 coalesced, got %d", stats.Coalesced)
	}
	if stats.Failed != 0 {
		t.Errorf("expected 0 failed, got %d", stats.Failed)
	}
}

// TestManagerQueueSaturationIncrementsFailed verifies that when both queue
// and pending backlog are full, jobs are counted as failed.
func TestManagerQueueSaturationIncrementsFailed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := NewManager(nil, 1, nil)
	m.queue = NewCoalescingQueue(1, 1)
	m.ctx = ctx
	m.cancel = cancel

	now := time.Now()
	syms := []string{"BTC/USDT", "ETH/USDT", "SOL/USDT"}
	m.EnqueueBatch("binance", "swap", syms, now.Add(-5*time.Second), now)

	stats := m.Stats()
	if stats.Queued != 1 {
		t.Fatalf("expected 1 queued, got %d", stats.Queued)
	}
	if stats.Pending != 1 {
		t.Fatalf("expected 1 pending, got %d", stats.Pending)
	}
	if stats.Coalesced != 1 {
		t.Fatalf("expected 1 coalesced, got %d", stats.Coalesced)
	}
	if stats.Failed != 1 {
		t.Fatalf("expected 1 failed, got %d", stats.Failed)
	}
}

// TestManagerPromotesPendingDeterministically verifies that promoted jobs
// move from pending back to the queue.
func TestManagerPromotesPendingDeterministically(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := NewManager(nil, 1, nil)
	m.queue = NewCoalescingQueue(1, 8)
	m.ctx = ctx
	m.cancel = cancel

	now := time.Now()
	m.EnqueueBatch("binance", "swap", []string{"BTC/USDT", "ETH/USDT", "SOL/USDT"}, now.Add(-5*time.Second), now)

	if got := m.queue.Len(); got != 1 {
		t.Fatalf("expected queue len 1, got %d", got)
	}
	if got := m.queue.PendingLen(); got != 2 {
		t.Fatalf("expected pending 2, got %d", got)
	}

	// Free one slot by consuming from the channel
	<-m.queue.Read()
	m.queue.Promote(1)

	if got := m.queue.Len(); got != 1 {
		t.Fatalf("expected queue len 1 after promotion, got %d", got)
	}
	if got := m.queue.PendingLen(); got != 1 {
		t.Fatalf("expected pending 1 after promotion, got %d", got)
	}

	stats := m.Stats()
	if stats.Retried == 0 {
		t.Fatalf("expected retried > 0 after promotion, got %d", stats.Retried)
	}
}

// TestManagerDelayCalculation verifies delay clamping logic.
func TestManagerDelayCalculation(t *testing.T) {
	m := NewManager(nil, 1, nil)

	now := time.Now()

	tests := []struct {
		name          string
		gapDuration   time.Duration
		expectedDelay time.Duration
	}{
		{"short gap clamps to min", 1 * time.Second, DefaultMinDelay},
		{"medium gap uses 80%", 10 * time.Second, 8 * time.Second},
		{"long gap clamps to max", 60 * time.Second, DefaultMaxDelay},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			disconnectTime := now.Add(-tt.gapDuration)
			m.EnqueueBatch("binance", "swap", []string{"BTC/USDT"}, disconnectTime, now)

			job := <-m.queue.Read()
			if job.Delay != tt.expectedDelay {
				t.Errorf("expected delay %v, got %v", tt.expectedDelay, job.Delay)
			}
		})
	}
}

// TestManagerStatsAccuracy verifies zero values on fresh manager.
func TestManagerStatsAccuracy(t *testing.T) {
	m := NewManager(nil, 2, nil)

	stats := m.Stats()
	if stats.Queued != 0 || stats.Pending != 0 || stats.Backlog != 0 ||
		stats.Processed != 0 || stats.Dropped != 0 || stats.Errors != 0 ||
		stats.Enqueued != 0 || stats.Coalesced != 0 || stats.Retried != 0 || stats.Failed != 0 {
		t.Errorf("expected all zeroes initially, got %+v", stats)
	}
	if stats.Workers != 2 {
		t.Errorf("expected 2 workers, got %d", stats.Workers)
	}
}

// TestManagerConcurrentEnqueue verifies no data races under concurrent load.
func TestManagerConcurrentEnqueue(t *testing.T) {
	m := NewManager(nil, 3, nil)
	ctx, cancel := context.WithCancel(context.Background())
	m.ctx = ctx
	m.cancel = cancel

	now := time.Now()
	syms := []string{"BTC/USDT", "ETH/USDT", "SOL/USDT", "DOGE/USDT", "XRP/USDT"}

	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func(exchange string) {
			m.EnqueueBatch(exchange, "swap", syms, now.Add(-5*time.Second), now)
		}("exchange-" + string(rune('a'+i)))
	}

	// Allow goroutines to finish
	time.Sleep(100 * time.Millisecond)
	close(done)

	stats := m.Stats()
	// No panics and no data races is the main assertion
	t.Logf("Concurrent test: enqueued=%d coalesced=%d retried=%d failed=%d processed=%d dropped=%d errors=%d queued=%d pending=%d",
		stats.Enqueued, stats.Coalesced, stats.Retried, stats.Failed,
		stats.Processed, stats.Dropped, stats.Errors, stats.Queued, stats.Pending)
}
