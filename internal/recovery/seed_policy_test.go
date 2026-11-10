package recovery

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// fakeProbe reports "not delivering" for the first silentCalls probes, then
// delivering. It counts probes so a test can assert the gate neither spins nor
// short-circuits.
type fakeProbe struct {
	silentCalls int32
	calls       atomic.Int32
	neverLive   bool
}

func (f *fakeProbe) Delivering(_, _, _ string) bool {
	n := f.calls.Add(1)
	if f.neverLive {
		return false
	}
	return n > f.silentCalls
}

func (f *fakeProbe) DeliveringCount(_, _ string, symbols []string) int {
	if f.neverLive {
		return 0
	}
	return len(symbols)
}

// TestAwaitSymbolDelivery_WaitsUntilTheStreamDelivers is the regression for the boot
// hole: the seed's REST snapshot and the live stream are stitched by a MAX, and a MAX
// can only stitch windows that overlap.
func TestAwaitSymbolDelivery_WaitsUntilTheStreamDelivers(t *testing.T) {
	t.Parallel()

	probe := &fakeProbe{silentCalls: 3}
	deadline := time.Now().Add(5 * time.Second)

	ok := AwaitSymbolDelivery(context.Background(), probe, "binance", "future", "ETH/USDT", deadline, time.Millisecond)

	if !ok {
		t.Fatal("delivery not confirmed — the seed would take its snapshot against a silent stream")
	}
	if got := probe.calls.Load(); got != 4 {
		t.Errorf("probed %d times, want 4 (3 silent + the one that saw delivery)", got)
	}
}

func TestAwaitSymbolDelivery_ReturnsImmediatelyWhenAlreadyDelivering(t *testing.T) {
	t.Parallel()

	probe := &fakeProbe{silentCalls: 0}
	start := time.Now()

	if !AwaitSymbolDelivery(context.Background(), probe, "binance", "future", "BTC/USDT",
		start.Add(time.Second), 500*time.Millisecond) {
		t.Fatal("a stream that is already delivering must not be waited on")
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("waited %v on an already-delivering stream — the gate must probe before it sleeps", elapsed)
	}
	if got := probe.calls.Load(); got != 1 {
		t.Errorf("probed %d times, want exactly 1", got)
	}
}

func TestAwaitSymbolDelivery_GivesUpAtTheSharedDeadline(t *testing.T) {
	t.Parallel()

	probe := &fakeProbe{neverLive: true}
	start := time.Now()

	if AwaitSymbolDelivery(context.Background(), probe, "binance", "future", "QUIET/USDT",
		start.Add(30*time.Millisecond), 5*time.Millisecond) {
		t.Fatal("a stream that never delivers must not be reported as confirmed")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("waited %v past a 30ms deadline", elapsed)
	}
}

func TestAwaitSymbolDelivery_ExpiredDeadlineProbesOnceAndProceeds(t *testing.T) {
	t.Parallel()

	probe := &fakeProbe{neverLive: true}
	start := time.Now()

	if AwaitSymbolDelivery(context.Background(), probe, "binance", "future", "LATE/USDT",
		start.Add(-time.Second), time.Second) {
		t.Fatal("expired deadline must not report confirmed delivery")
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("slept %v on an already-expired deadline", elapsed)
	}
	if got := probe.calls.Load(); got != 1 {
		t.Errorf("probed %d times, want exactly 1", got)
	}
}

func TestAwaitSymbolDelivery_HonoursCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	probe := &fakeProbe{neverLive: true}
	start := time.Now()

	if AwaitSymbolDelivery(ctx, probe, "binance", "future", "X/USDT", start.Add(time.Minute), 10*time.Millisecond) {
		t.Fatal("a canceled context must not report confirmed delivery")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("waited %v after cancellation", elapsed)
	}
}

func TestAwaitSymbolDelivery_NilProbeIsTheOldBehaviour(t *testing.T) {
	t.Parallel()

	start := time.Now()
	if AwaitSymbolDelivery(context.Background(), nil, "binance", "future", "X/USDT",
		start.Add(time.Minute), 10*time.Millisecond) {
		t.Fatal("no probe cannot mean confirmed delivery")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("blocked %v with no probe wired", elapsed)
	}
}

// TestSeedConcurrency_StaysWithinTheBinanceWeightBudget pins the number to the
// arithmetic that justifies it, so a future "let's make boot faster" change has to
// redo the budget instead of just raising a constant.
func TestSeedConcurrency_StaysWithinTheBinanceWeightBudget(t *testing.T) {
	t.Parallel()

	const (
		binanceWeightPerMinute = 2400.0
		klinesWeightAtLimit200 = 2.0
		symbols                = 200.0
		timeframes             = 8.0
		serialSeconds          = 702.0
		maxBudgetShare         = 0.60
	)

	totalWeight := symbols * timeframes * klinesWeightAtLimit200
	parallelSeconds := serialSeconds / float64(SeedConcurrency)
	weightPerMinute := totalWeight / parallelSeconds * 60

	if got, ceiling := weightPerMinute, binanceWeightPerMinute*maxBudgetShare; got > ceiling {
		t.Fatalf("SeedConcurrency=%d drives %.0f weight/min, above the %.0f/min self-imposed ceiling",
			SeedConcurrency, got, ceiling)
	}
}

func TestSeedConcurrency_IsActuallyParallel(t *testing.T) {
	t.Parallel()

	if SeedConcurrency < 2 {
		t.Fatalf("SeedConcurrency=%d is serial — the 11m42s boot is back", SeedConcurrency)
	}
}
