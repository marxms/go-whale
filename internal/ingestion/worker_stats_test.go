package ingestion

import (
	"context"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/market"
	"github.com/marxms/go-whale/internal/trade"
)

type blockingSource struct {
	fillSource
	release chan struct{}
}

func (b *blockingSource) GetTrades(context.Context, string) ([]trade.Trade, error) {
	<-b.release
	return nil, nil
}

// Busy time, queue depth and in-flight are measured on the real worker path.
// Queue depth is sampled while a symbol waits; in-flight while a worker drains
// it; busy time advances only after the drain returns.
func TestWorkerBusyAndQueueMetrics(t *testing.T) {
	metrics := newTestMetrics()
	manager := NewHybridManager(nil, 1, nil)
	manager.SetMetrics(metrics.port())
	release := make(chan struct{})
	state := &HybridClientState{
		Client: &blockingSource{release: release}, Symbol: "BTC/USDT",
		Exchange: "binance", MarketType: market.Future, Key: "binance:future:BTC/USDT",
	}
	manager.clients[state.Key] = state

	done := make(chan struct{})
	go func() {
		manager.processClient(context.Background(), 0, state)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for manager.inFlightCount.Load() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := manager.inFlightCount.Load(); got != 1 {
		t.Fatalf("in-flight=%d while the handler is blocked, want 1", got)
	}
	manager.publishBufferStats()
	if metrics.workerInFlight != 1 {
		t.Fatalf("published in-flight=%d, want 1", metrics.workerInFlight)
	}

	close(release)
	<-done
	manager.publishBufferStats()
	if metrics.workerInFlight != 0 {
		t.Fatalf("published in-flight=%d after the drain, want 0", metrics.workerInFlight)
	}
	if metrics.workerBusy <= 0 {
		t.Fatalf("worker busy time=%v, want > 0 after a real drain", metrics.workerBusy)
	}

	// Queue depth is a separate signal: one idle symbol waiting, no worker
	// draining it yet.
	state.inFlight.Store(false)
	manager.scheduleClients()
	manager.publishBufferStats()
	if metrics.workerQueue != 1 {
		t.Fatalf("published queue depth=%d, want 1", metrics.workerQueue)
	}
	if metrics.workerInFlight != 0 {
		t.Fatalf("published in-flight=%d with an idle worker, want 0", metrics.workerInFlight)
	}
	<-manager.processQueue
}
