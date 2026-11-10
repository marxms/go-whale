package distribution

import (
	"context"
	"sync"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

type blackholePublisher struct{}

func (blackholePublisher) Publish(_ context.Context, _ string, _ []byte) error { return nil }

// TestStop_ConcurrentWithPublish reproduces a `send on closed channel` panic.
//
// # THE RACE
//
// enqueue checks p.stopped and then sends, and the send is NOT under p.mu:
//
//	if p.stopped.Load() { return }      // (1)
//	q := p.queueFor(typ)
//	select { case q <- ev:              // (3)
//
// Stop sets stopped, takes p.mu and closes every queue — between (1) and (3). A send on a
// closed channel is an unrecoverable panic on whatever goroutine happened to be publishing,
// which at a bucket boundary is the goroutine that processes trades.
//
// The field carrying the producer across the App literal must be set, so Stop runs on EVERY
// deploy and this race is armed on every deploy. Its ordering matters: the panic aborts
// Shutdown BEFORE qdbPipeline.Stop(), discarding the batch still buffered in the cold
// pipeline — the exact permanent cold hole this guard exists to close.
func TestStop_ConcurrentWithPublish(_ *testing.T) {
	for range 40 {
		p := NewAsyncProducer(blackholePublisher{}, "test", func() int64 { return 0 })
		scope := Scope{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "5m"}
		c := candle.Candle{Timestamp: 1_700_000_000_000, Close: 100, Volume: 1}

		// Warm the queues so the owners exist and Stop has something to close.
		p.PublishClose(scope, c)
		p.PublishRevised(scope, c)

		var wg sync.WaitGroup
		start := make(chan struct{})
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for range 200 {
					// A panic here fails the test by crashing the run, which is the point:
					// in production it crashes the trade-processing goroutine mid-shutdown.
					p.PublishClose(scope, c)
					p.PublishRevised(scope, c)
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			p.Stop()
		}()

		close(start)
		wg.Wait()
	}
}

// TestStop_IsIdempotentUnderConcurrency keeps the documented "safe to call more than once"
// property true once the locking changes — two Shutdown paths racing must not double-close.
func TestStop_IsIdempotentUnderConcurrency(_ *testing.T) {
	p := NewAsyncProducer(blackholePublisher{}, "test", func() int64 { return 0 })
	p.PublishClose(Scope{Symbol: "BTC/USDT"}, candle.Candle{Timestamp: 1})

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() { defer wg.Done(); p.Stop() }()
	}
	wg.Wait()
}

// TestEnqueueAfterStop_IsADropNotAPanic states the post-stop contract. A publisher that is
// still running when Stop completes — the seeder keeps writing after streamCancel, because
// TieredStore.AddStreamingCandle does not consult the context — must be dropped quietly, not
// crash the process.
func TestEnqueueAfterStop_IsADropNotAPanic(_ *testing.T) {
	p := NewAsyncProducer(blackholePublisher{}, "test", func() int64 { return 0 })
	scope := Scope{Symbol: "BTC/USDT"}
	p.PublishClose(scope, candle.Candle{Timestamp: 1})
	p.Stop()

	p.PublishClose(scope, candle.Candle{Timestamp: 2})
	p.PublishRevised(scope, candle.Candle{Timestamp: 3})

	// A brand-new event TYPE after Stop must not start an owner goroutine either: that
	// goroutine would outlive Shutdown with nothing left to close it.
	p.PublishRevised(Scope{Symbol: "ETH/USDT"}, candle.Candle{Timestamp: 4})
}
