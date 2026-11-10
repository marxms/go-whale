package recovery

import (
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// TestEmptyCycleDoesNotRetry pins REC-05/REC-06: a cycle whose fill returned no candles is
// counted as an empty cycle and feeds the breaker, never reported as success, and never
// re-scheduled. A retry here is what kept a silent venue in a permanent recovery loop.
func TestEmptyCycleDoesNotRetry(t *testing.T) {
	w := newRecordingWriter()
	stale := staleProvider{ts: time.Now().Add(-time.Hour).UnixMilli()}
	m := newTestManager(fakeFiller{}, w, stale)
	m.retry.(*MemoryRetryScheduler).pending.Store(true)

	var emptyCycles int
	m.SetMetrics(Metrics{EmptyCycleTotal: func(exchange, marketType string) {
		if exchange == "binance" && marketType == "future" {
			emptyCycles++
		}
	}})

	job := RecoveryJob{
		ExchangeID: "binance", MarketType: "swap", Symbol: "BTC/USDT",
		DisconnectTime: time.Now().Add(-time.Hour), ReconnectTime: time.Now(),
	}
	m.process(t.Context(), 0, job)

	if emptyCycles != 1 {
		t.Fatalf("empty cycles = %d, want 1", emptyCycles)
	}
	if w.seededBatch != 0 || len(w.seededCur) != 0 {
		t.Fatalf("empty cycle wrote candles: historical=%d current=%d", w.seededBatch, len(w.seededCur))
	}
	if len(m.retry.(*MemoryRetryScheduler).queue) != 0 {
		t.Fatalf("empty cycle scheduled a retry: %+v", m.retry.(*MemoryRetryScheduler).queue)
	}

	// Draining the scheduler must hand nothing back to the worker queue.
	m.retry.(*MemoryRetryScheduler).processReadyOnce(time.Now().Add(RetryMaxDelay + time.Second))
	if m.queue.Len() != 0 {
		t.Fatalf("empty cycle re-entered the queue: %d jobs", m.queue.Len())
	}

	// A cycle that does write is not an empty cycle.
	current := candle.Candle{
		Timestamp: candle.BucketTimestamp(time.Now().UnixMilli(), "1m"),
		Open:      1, High: 1, Low: 1, Close: 1, Volume: 1,
	}
	m.SetFiller(fakeFiller{current: &current})
	m.process(t.Context(), 0, RecoveryJob{
		ExchangeID: "binance", MarketType: "swap", Symbol: "ETH/USDT",
		DisconnectTime: time.Now().Add(-time.Hour), ReconnectTime: time.Now(),
	})
	if emptyCycles != 1 {
		t.Fatalf("a cycle that wrote candles counted as empty: total=%d", emptyCycles)
	}
}
