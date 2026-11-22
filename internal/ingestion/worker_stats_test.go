package ingestion

import (
	"context"
	"errors"
	"strconv"
	"sync"
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

type errorSource struct{ fillSource }

func (e *errorSource) GetTrades(context.Context, string) ([]trade.Trade, error) {
	return nil, errors.New("boom")
}

// An error on the drain path must release the in-flight claim and still count
// busy time; a leaked claim would stop the symbol from ever being drained.
func TestWorkerErrorReleasesInFlight(t *testing.T) {
	metrics := newTestMetrics()
	manager := NewHybridManager(nil, 1, nil)
	manager.SetMetrics(metrics.port())
	state := &HybridClientState{
		Client: &errorSource{}, Symbol: "BTC/USDT",
		Exchange: "binance", MarketType: market.Future, Key: "binance:future:BTC/USDT",
	}
	state.inFlight.Store(true)

	manager.processClient(context.Background(), 0, state)

	if state.inFlight.Load() {
		t.Fatal("error path leaked the in-flight claim")
	}
	if got := manager.inFlightCount.Load(); got != 0 {
		t.Fatalf("in-flight count=%d after an error, want 0", got)
	}
	if metrics.workerBusy <= 0 {
		t.Fatalf("worker busy=%v after an error path, want > 0", metrics.workerBusy)
	}
}

// Start with the claim FREE and let many goroutines race without any
// synchronization beyond the start gate. The CAS must elect exactly one
// enqueue; a Load+Store implementation lets every goroutine in.
func TestScheduleClientsConcurrentEnqueuesExactlyOnce(t *testing.T) {
	manager := NewHybridManager(nil, 8, nil)
	state := &HybridClientState{Key: "binance:future:BTC/USDT"}
	manager.clients[state.Key] = state

	const racers = 128
	start := make(chan struct{})
	var wg sync.WaitGroup
	for index := 0; index < racers; index += 1 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			manager.scheduleClients()
		}()
	}
	close(start)
	wg.Wait()

	if got := len(manager.processQueue); got != 1 {
		t.Fatalf("queue=%d, want exactly 1 — the CAS must elect a single winner", got)
	}
	<-manager.processQueue
}

// Isolated mutation control: the same scheduling algorithm with Load+Store
// instead of CompareAndSwap. It must enqueue more than once; if this stops
// reproducing, the production test above no longer guards the CAS.
func scheduleClientsMutant(manager *HybridManager, barrier *sync.WaitGroup) {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	for _, state := range manager.clients {
		if state.inFlight.Load() {
			continue
		}
		// Widen the Load→Store window deterministically so the mutation always
		// reproduces; the production test above uses no barrier.
		barrier.Done()
		barrier.Wait()
		state.inFlight.Store(true)
		select {
		case manager.processQueue <- state:
		default:
			state.inFlight.Store(false)
		}
	}
}

func TestScheduleClientsMutantLoadStoreDoubleEnqueues(t *testing.T) {
	manager := NewHybridManager(nil, 8, nil)
	state := &HybridClientState{Key: "binance:future:BTC/USDT"}
	manager.clients[state.Key] = state

	const racers = 128
	start := make(chan struct{})
	barrier := &sync.WaitGroup{}
	barrier.Add(racers)
	var wg sync.WaitGroup
	for index := 0; index < racers; index += 1 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			scheduleClientsMutant(manager, barrier)
		}()
	}
	close(start)
	wg.Wait()

	if got := len(manager.processQueue); got <= 1 {
		t.Fatalf("queue=%d: the Load+Store mutant should enqueue the same symbol more than once", got)
	}
	for len(manager.processQueue) > 0 {
		<-manager.processQueue
	}
}

type sequenceSource struct {
	fillSource
	mu      sync.Mutex
	batches [][]trade.Trade
	next    int
}

func (s *sequenceSource) GetTrades(context.Context, string) ([]trade.Trade, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.next >= len(s.batches) {
		return nil, nil
	}
	batch := s.batches[s.next]
	s.next++
	return batch, nil
}

type recordingHandler struct {
	mu   sync.Mutex
	seen []string
}

func (h *recordingHandler) HandleTrades(string, any) error { return nil }

func (h *recordingHandler) HandleTradesWithContext(_ context.Context, _, _, _ string, trades any) error {
	batch, _ := trades.([]trade.Trade)
	ids := ""
	for index, item := range batch {
		if index > 0 {
			ids += ","
		}
		ids += item.ID
	}
	h.mu.Lock()
	h.seen = append(h.seen, ids)
	h.mu.Unlock()
	return nil
}

// The worker drains one batch per claim and releases it; the next tick sees the
// next batch. Per-symbol order is preserved by construction, which is the
// precondition the watermark relies on.
func TestWorkerProcessesBatchInOrderPerSymbol(t *testing.T) {
	batch := func(from, to int64) []trade.Trade {
		base := time.Now().Add(-30 * time.Second)
		out := make([]trade.Trade, 0, to-from+1)
		for id := from; id <= to; id++ {
			out = append(out, trade.Trade{
				ID: strconv.FormatInt(id, 10), Symbol: "BTC/USDT", Price: 100, Amount: 1,
				Side: trade.Buy, Timestamp: base.Add(time.Duration(id) * time.Millisecond).UnixMilli(),
			})
		}
		return out
	}
	recorder := &recordingHandler{}
	manager := NewHybridManager(recorder, 1, nil)
	state := &HybridClientState{
		Client: &sequenceSource{batches: [][]trade.Trade{batch(1, 2), batch(3, 4)}},
		Symbol: "BTC/USDT", Exchange: "binance", MarketType: market.Future,
		Key: "binance:future:BTC/USDT",
	}

	manager.processClient(context.Background(), 0, state)
	manager.processClient(context.Background(), 0, state)

	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if len(recorder.seen) != 2 || recorder.seen[0] != "1,2" || recorder.seen[1] != "3,4" {
		t.Fatalf("handler saw %v, want [1,2] then [3,4] in order", recorder.seen)
	}
	if state.inFlight.Load() {
		t.Fatal("claim leaked between batches")
	}
}
