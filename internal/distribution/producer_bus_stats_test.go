package distribution

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

type queueStatsPublisher struct {
	started chan struct{}
	release chan struct{}
}

func (p *queueStatsPublisher) Publish(ctx context.Context, _ string, _ []byte) error {
	select {
	case p.started <- struct{}{}:
	default:
	}
	select {
	case <-p.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// The bus reports how far behind it is, not only what it dropped. The first event is held in
// flight; the second and third are pending, so depth and the oldest age must both be visible.
func TestProducerExposesQueueDepthAndOldestPending(t *testing.T) {
	pub := &queueStatsPublisher{started: make(chan struct{}, 4), release: make(chan struct{})}
	var mu sync.Mutex
	depth := -1
	var age time.Duration
	producer := NewAsyncProducer(
		pub,
		"test",
		func() int64 { return time.Now().UnixMilli() },
		WithHooks(ProducerHooks{
			OnQueueDepth: func(_ string, current int) {
				mu.Lock()
				depth = current
				mu.Unlock()
			},
			OnOldestPendingAge: func(_ string, current time.Duration) {
				mu.Lock()
				age = current
				mu.Unlock()
			},
		}),
	)
	scope := Scope{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "5m"}

	producer.PublishClose(scope, candle.Candle{Timestamp: 1, Close: 1})
	select {
	case <-pub.started:
	case <-time.After(time.Second):
		t.Fatal("the owner did not start publishing")
	}
	producer.PublishClose(scope, candle.Candle{Timestamp: 2, Close: 2})
	time.Sleep(2 * time.Millisecond)
	producer.PublishClose(scope, candle.Candle{Timestamp: 3, Close: 3})
	time.Sleep(time.Millisecond)

	mu.Lock()
	gotDepth, gotAge := depth, age
	mu.Unlock()
	if gotDepth != 2 {
		t.Fatalf("queue depth = %d, want 2", gotDepth)
	}
	if gotAge <= 0 {
		t.Fatalf("oldest pending age = %v, want > 0", gotAge)
	}

	close(pub.release)
	producer.Stop()

	mu.Lock()
	gotDepth = depth
	mu.Unlock()
	if gotDepth != 0 {
		t.Fatalf("queue depth after drain = %d, want 0", gotDepth)
	}
}
