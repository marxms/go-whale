package scheduler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// clockOf advances by elapsed between the first and second read — exactly the two reads a
// measured cycle performs.
func clockOf(elapsed time.Duration) func() time.Time {
	base := time.Date(2026, 8, 25, 3, 0, 0, 0, time.UTC)
	reads := 0
	return func() time.Time {
		reads++
		if reads == 1 {
			return base
		}
		return base.Add(elapsed)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitDone(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("%s did not return", what)
	}
}

// The duration published for a cycle is the MEASURED one, against a real clock: the loop
// sleeps for a known span and the report has to contain at least that span.
func TestLoop_MeasuresTheRealCycleDuration(t *testing.T) {
	const work = 25 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reports := make(chan Report, 1)
	loop := NewLoop(Options{
		Interval: time.Hour,
		Cycle: func(context.Context) error {
			time.Sleep(work)
			return nil
		},
		OnCycle: func(_ context.Context, report Report) {
			select {
			case reports <- report:
			default:
			}
		},
	})

	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	var report Report
	select {
	case report = <-reports:
	case <-time.After(3 * time.Second):
		t.Fatal("the first cycle was never reported")
	}

	if report.Elapsed < work {
		t.Errorf("cycle elapsed = %s, want at least the %s it slept", report.Elapsed, work)
	}
	if report.Interval != time.Hour {
		t.Errorf("report interval = %s, want the scheduling interval 1h", report.Interval)
	}
	if report.Overran() {
		t.Errorf("a %s cycle against a 1h interval was called an overrun", report.Elapsed)
	}

	cancel()
	waitDone(t, done, "the loop")
}

// A cycle longer than its period is a LAP, and the measurement comes from the injected
// clock rather than from the wall clock. This is the accounting the overrun alarm reads.
func TestMeasure_UsesTheInjectedClockAndReportsALap(t *testing.T) {
	report := Measure(context.Background(), nil, time.Hour, clockOf(90*time.Minute))

	if report.Elapsed != 90*time.Minute {
		t.Errorf("measured elapsed = %s, want 90m", report.Elapsed)
	}
	if report.Interval != time.Hour {
		t.Errorf("interval = %s, want 1h", report.Interval)
	}
	if !report.Overran() {
		t.Error("a 90m cycle against a 1h interval was not reported as an overrun")
	}
	if got := report.LapFactor(); got != 1.5 {
		t.Errorf("lap factor = %v, want 1.5", got)
	}
}

// Tick-lapped accounting end to end: a real cycle longer than the scheduling period comes
// out of the loop as a Report whose Overran/LapFactor describe exactly that.
func TestLoop_ACycleLongerThanThePeriodIsReportedAsALap(t *testing.T) {
	const interval = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reports := make(chan Report, 1)
	loop := NewLoop(Options{
		Interval: interval,
		Cycle: func(context.Context) error {
			time.Sleep(20 * time.Millisecond)
			return nil
		},
		OnCycle: func(_ context.Context, report Report) {
			select {
			case reports <- report:
			default:
			}
		},
	})

	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	var report Report
	select {
	case report = <-reports:
	case <-time.After(3 * time.Second):
		t.Fatal("the long cycle was never reported")
	}

	if !report.Overran() {
		t.Fatalf("a cycle of %s against a %s interval was not reported as a lap", report.Elapsed, interval)
	}
	if report.LapFactor() <= 1 {
		t.Errorf("lap factor = %v, want greater than 1", report.LapFactor())
	}

	cancel()
	waitDone(t, done, "the loop")
}

func TestReport_WithoutAnIntervalCannotOverrun(t *testing.T) {
	report := Report{Elapsed: time.Hour}
	if report.Overran() {
		t.Error("a report with no configured interval claimed an overrun")
	}
	if got := report.LapFactor(); got != 0 {
		t.Errorf("lap factor = %v, want 0 when there is no cadence to compare against", got)
	}
}

// THE COALESCING CONTRACT. While one cycle runs, ticks that arrive are DROPPED by the
// ticker, never queued into a second concurrent cycle. This proves the loop never
// overlaps and never accumulates a backlog of cycles to run back-to-back.
func TestLoop_ATickDuringACycleIsCoalesced(t *testing.T) {
	const interval = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{}, 16)
	release := make(chan struct{})
	var inFlight, maxInFlight, cycles atomic.Int32

	loop := NewLoop(Options{
		Interval: interval,
		Cycle: func(ctx context.Context) error {
			n := inFlight.Add(1)
			defer inFlight.Add(-1)
			for {
				m := maxInFlight.Load()
				if n <= m || maxInFlight.CompareAndSwap(m, n) {
					break
				}
			}

			cycles.Add(1)
			started <- struct{}{}

			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil
		},
	})

	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("the first cycle never started")
	}

	// Several ticks fall inside the first cycle. None may start a second cycle.
	time.Sleep(5 * interval)
	if got := cycles.Load(); got != 1 {
		t.Fatalf("%d cycles started while the first was still running: ticks were queued instead of coalesced", got)
	}
	select {
	case <-started:
		t.Fatal("a second cycle started concurrently with the first")
	default:
	}

	// Release the first; the buffered tick (at most one) starts the next cycle immediately.
	release <- struct{}{}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("the loop did not resume after the cycle finished")
	}
	if got := maxInFlight.Load(); got > 1 {
		t.Fatalf("max concurrent cycles = %d, want 1", got)
	}

	cancel()
	waitDone(t, done, "the loop")
}

// AfterCycle is the per-cycle hook. The IMMEDIATE cycle counts too: a report that only
// ran on ticks would leave the first hour after a deploy unchecked, and a report that only
// ran at boot would call every later cycle a shortfall.
func TestLoop_AfterCycleRunsForEveryCycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var afterCycles atomic.Int32
	loop := NewLoop(Options{
		Interval: 5 * time.Millisecond,
		Cycle:    func(context.Context) error { return nil },
		AfterCycle: func(context.Context, Report) {
			afterCycles.Add(1)
		},
	})

	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	waitFor(t, "the immediate cycle and at least one tick", func() bool {
		return afterCycles.Load() >= 2
	})
	cancel()
	waitDone(t, done, "the loop")

	if got := afterCycles.Load(); got < 2 {
		t.Errorf("AfterCycle ran %d times, want the immediate cycle plus at least one tick", got)
	}
}

func TestLoop_CancellationStopsTheLoopAndRunsTeardown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var cycles, teardowns atomic.Int32
	loop := NewLoop(Options{
		Interval: time.Hour,
		Cycle: func(context.Context) error {
			cycles.Add(1)
			return nil
		},
		Teardown: func() { teardowns.Add(1) },
	})

	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	waitFor(t, "the first cycle", func() bool { return cycles.Load() == 1 })
	cancel()
	waitDone(t, done, "the loop")

	if got := teardowns.Load(); got != 1 {
		t.Errorf("teardown ran %d times, want exactly once", got)
	}
	if got := cycles.Load(); got != 1 {
		t.Errorf("%d cycles ran, want only the immediate first one", got)
	}
}

// A failed startup is retried with the scheduler's backoff; when the attempts run out the
// loop never runs a cycle.
func TestLoop_StartupRetriesUntilExhausted(t *testing.T) {
	var attempts, cycles atomic.Int32
	loop := NewLoop(Options{
		Interval:        time.Hour,
		Startup:         func(context.Context) (bool, error) { attempts.Add(1); return true, errors.New("boom") },
		StartupAttempts: 3,
		StartupBackoff:  time.Millisecond,
		Cycle: func(context.Context) error {
			cycles.Add(1)
			return nil
		},
	})

	loop.Run(context.Background())

	if got := attempts.Load(); got != 3 {
		t.Errorf("startup attempts = %d, want 3", got)
	}
	if got := cycles.Load(); got != 0 {
		t.Errorf("%d cycles ran after startup exhausted", got)
	}
}

// A shutdown during the backoff must not sit out the full wait.
func TestLoop_StartupCancellationInterruptsTheBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attempts atomic.Int32
	loop := NewLoop(Options{
		Interval:        time.Hour,
		Startup:         func(context.Context) (bool, error) { attempts.Add(1); return true, errors.New("boom") },
		StartupAttempts: 5,
		StartupBackoff:  time.Hour,
	})

	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	waitFor(t, "the first startup attempt", func() bool { return attempts.Load() == 1 })
	cancel()
	waitDone(t, done, "the loop")

	if got := attempts.Load(); got != 1 {
		t.Errorf("startup attempts = %d, want the one already in flight", got)
	}
}

// A declared refusal is not a failure: no retry, no cycle, teardown still runs.
func TestLoop_StartupRefusalIsNotRetried(t *testing.T) {
	var attempts, cycles, teardowns atomic.Int32
	loop := NewLoop(Options{
		Interval:        time.Hour,
		Startup:         func(context.Context) (bool, error) { attempts.Add(1); return false, nil },
		StartupAttempts: 5,
		StartupBackoff:  time.Millisecond,
		Cycle: func(context.Context) error {
			cycles.Add(1)
			return nil
		},
		Teardown: func() { teardowns.Add(1) },
	})

	loop.Run(context.Background())

	if got := attempts.Load(); got != 1 {
		t.Errorf("startup attempts = %d, want exactly one for a declared refusal", got)
	}
	if got := cycles.Load(); got != 0 {
		t.Errorf("%d cycles ran after the schedule declined", got)
	}
	if got := teardowns.Load(); got != 1 {
		t.Errorf("teardown ran %d times, want once", got)
	}
}

// The first cycle is skipped when shutdown arrives during the pre-cycle wait. This is the
// Coinalyze fleet wait: running a cycle against a canceled context produces only noise.
func TestLoop_BeforeFirstCancellationSkipsTheFirstCycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var cycles atomic.Int32
	loop := NewLoop(Options{
		Interval: time.Hour,
		BeforeFirst: func(ctx context.Context) bool {
			<-ctx.Done()
			return false
		},
		Cycle: func(context.Context) error {
			cycles.Add(1)
			return nil
		},
	})

	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	cancel()
	waitDone(t, done, "the loop")

	if got := cycles.Load(); got != 0 {
		t.Errorf("%d cycles ran against a canceled context before the first tick", got)
	}
}

func TestLoop_BootstrapRunsBeforeTheFirstCycle(t *testing.T) {
	var mu sync.Mutex
	var order []string
	record := func(step string) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, step)
	}
	steps := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), order...)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	loop := NewLoop(Options{
		Interval:  time.Hour,
		Bootstrap: func(context.Context) { record("bootstrap") },
		Cycle: func(context.Context) error {
			record("cycle")
			return nil
		},
	})

	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	waitFor(t, "the first cycle", func() bool { return len(steps()) == 2 })
	cancel()
	waitDone(t, done, "the loop")

	got := steps()
	if got[0] != "bootstrap" || got[1] != "cycle" {
		t.Fatalf("order = %v, want bootstrap then cycle", got)
	}
}
