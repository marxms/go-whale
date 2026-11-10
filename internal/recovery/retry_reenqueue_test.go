package recovery

import (
	"testing"
	"time"
)

// TestRetrySchedulerReenqueuesReadyJobs proves the N1 fix: a deferred retry job
// whose NextRetryAt has elapsed is handed to the re-enqueue callback (re-entering
// the worker pipeline), and a job not yet due is left untouched. Previously
// processLoop only logged "ready to be re-enqueued" and dropped the job.
func TestRetrySchedulerReenqueuesReadyJobs(t *testing.T) {
	r := &MemoryRetryScheduler{}
	r.pending.Store(true) // suppress the background processLoop for a deterministic test

	var got []BackfillRetryJob
	r.SetReenqueue(func(j BackfillRetryJob) { got = append(got, j) })

	r.Schedule(BackfillRetryJob{ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "5m"})

	// Before NextRetryAt elapses: nothing re-enqueued.
	r.processReadyOnce(time.Now())
	if len(got) != 0 {
		t.Fatalf("job re-enqueued before its retry time: got %d", len(got))
	}

	// After it elapses: re-enqueued exactly once, Attempt incremented by Schedule.
	r.processReadyOnce(time.Now().Add(RetryMaxDelay + time.Second))
	if len(got) != 1 {
		t.Fatalf("ready job not re-enqueued: got %d, want 1", len(got))
	}
	if got[0].Symbol != "BTC/USDT" || got[0].Attempt != 1 || got[0].Timeframe != "5m" {
		t.Fatalf("re-enqueued job wrong: %+v", got[0])
	}

	// Queue is drained: a second pass re-enqueues nothing.
	r.processReadyOnce(time.Now().Add(RetryMaxDelay + time.Second))
	if len(got) != 1 {
		t.Fatalf("ready job re-enqueued twice: got %d", len(got))
	}
}

// TestRetrySchedulerCoalescesBySymbol proves a symbol with multiple deferred
// timeframes re-enqueues ONCE per cycle (not once per TF), keeping the highest
// attempt — preventing the N×TF amplification when re-entering the whole-symbol
// worker pipeline.
func TestRetrySchedulerCoalescesBySymbol(t *testing.T) {
	r := &MemoryRetryScheduler{}
	r.pending.Store(true)

	var got []BackfillRetryJob
	r.SetReenqueue(func(j BackfillRetryJob) { got = append(got, j) })

	for _, tf := range []string{"5m", "15m", "1h", "4h"} {
		r.Schedule(BackfillRetryJob{ExchangeID: "binance", MarketType: "future", Symbol: "ETH/USDT", Timeframe: tf})
	}
	r.processReadyOnce(time.Now().Add(RetryMaxDelay + time.Second))

	if len(got) != 1 {
		t.Fatalf("expected 1 coalesced re-enqueue for the symbol, got %d", len(got))
	}
	if got[0].Symbol != "ETH/USDT" {
		t.Fatalf("wrong symbol: %s", got[0].Symbol)
	}
}

// TestManagerReenqueuePreservesAttempt proves the attempt budget survives the
// BackfillRetryJob -> RecoveryJob round-trip, so re-enqueued retries still reach
// the RetryMaxAttempts cap instead of resetting to attempt 1 forever.
func TestManagerReenqueuePreservesAttempt(t *testing.T) {
	m := newTestManager(fakeFiller{}, newRecordingWriter(), nil)

	m.reenqueueRetryJob(BackfillRetryJob{
		ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT", Attempt: 5,
	})

	select {
	case job := <-m.queue.Read():
		if job.Symbol != "BTC/USDT" {
			t.Fatalf("wrong symbol: %s", job.Symbol)
		}
		if job.RetryAttempt != 5 {
			t.Fatalf("attempt not preserved across re-enqueue: got %d, want 5", job.RetryAttempt)
		}
	default:
		t.Fatal("re-enqueued job did not land in the worker queue")
	}
}
