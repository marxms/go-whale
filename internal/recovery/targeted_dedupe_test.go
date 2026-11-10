package recovery

import "testing"

func TestOneTargetedSettlePerRestart(t *testing.T) {
	m := NewManager(nil, 0, nil)
	const bucket = int64(1_790_000_100_000)

	m.EnqueueTargetedBucket("binance", "future", "BTC/USDT", "5m", bucket)
	m.EnqueueTargetedBucket("binance", "future", "BTC/USDT", "5m", bucket)
	if got := m.Stats().Enqueued; got != 1 {
		t.Fatalf("same bucket enqueued %d times, want 1", got)
	}

	m.EnqueueTargetedBucket("binance", "future", "BTC/USDT", "5m", bucket+300_000)
	if got := m.Stats().Enqueued; got != 2 {
		t.Fatalf("a new bucket was suppressed: enqueued=%d, want 2", got)
	}
}
