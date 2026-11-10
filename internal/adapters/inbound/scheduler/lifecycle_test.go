package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type runnerFunc func(context.Context)

func (f runnerFunc) Run(ctx context.Context) { f(ctx) }

// The first runner plus one fresh runner per restart, with the attempts bounded as: start,
// restart while attempts remain.
func TestLifecycle_RestartsAndBuildsFreshRunners(t *testing.T) {
	var runs, builds atomic.Int32

	lifecycle := NewLifecycle(LifecycleOptions{
		Name:  "test schedule",
		First: runnerFunc(func(context.Context) { runs.Add(1) }),
		New: func() Runner {
			builds.Add(1)
			return runnerFunc(func(context.Context) { runs.Add(1) })
		},
		MaxRestarts:  3,
		RestartDelay: time.Millisecond,
	})

	lifecycle.Run(context.Background())

	if got := runs.Load(); got != 3 {
		t.Errorf("runner executions = %d, want 3 (the initial attempt plus two restarts before the attempt budget ends)", got)
	}
	if got := builds.Load(); got != 2 {
		t.Errorf("fresh runners built = %d, want 2", got)
	}
}

func TestLifecycle_CancellationDoesNotRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var runs, builds atomic.Int32

	lifecycle := NewLifecycle(LifecycleOptions{
		Name: "test schedule",
		First: runnerFunc(func(context.Context) {
			runs.Add(1)
			cancel()
		}),
		New: func() Runner {
			builds.Add(1)
			return runnerFunc(func(context.Context) { runs.Add(1) })
		},
		MaxRestarts:  3,
		RestartDelay: time.Millisecond,
	})

	lifecycle.Run(ctx)

	if got := runs.Load(); got != 1 {
		t.Errorf("runner executions = %d, want only the canceled attempt", got)
	}
	if got := builds.Load(); got != 0 {
		t.Errorf("a canceled lifecycle built %d replacement runners", got)
	}
}

// The health flag brackets EVERY attempt, including the final one.
func TestLifecycle_AliveBracketsEveryAttempt(t *testing.T) {
	var mu sync.Mutex
	var states []bool

	lifecycle := NewLifecycle(LifecycleOptions{
		Name:  "test schedule",
		First: runnerFunc(func(context.Context) {}),
		New:   func() Runner { return runnerFunc(func(context.Context) {}) },
		OnAlive: func(alive bool) {
			mu.Lock()
			defer mu.Unlock()
			states = append(states, alive)
		},
		MaxRestarts:  3,
		RestartDelay: time.Millisecond,
	})

	lifecycle.Run(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if len(states) != 6 {
		t.Fatalf("alive states = %v, want a true/false pair per attempt", states)
	}
	for i, state := range states {
		want := i%2 == 0
		if state != want {
			t.Fatalf("alive states = %v, want alternating true/false starting with true", states)
		}
	}
}
