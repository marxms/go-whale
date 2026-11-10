package scheduler

import (
	"context"
	"log/slog"
	"time"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// Runner is one runnable schedule. Loop satisfies it.
type Runner interface {
	Run(ctx context.Context)
}

// LifecycleOptions configures a restarting lifecycle.
type LifecycleOptions struct {
	// Name labels this lifecycle in operational logs.
	Name string

	// First is the runner used for the first attempt. Nil builds one through New.
	First Runner

	// New builds a fresh runner for each restart. Nil means the same runner is reused,
	// which is only correct for a stateless runner.
	New func() Runner

	// MaxRestarts is the total number of attempts: start, then restart while attempts
	// remain.
	MaxRestarts int

	// RestartDelay is the wait after the first unexpected exit; it doubles after each
	// subsequent exit.
	RestartDelay time.Duration

	// OnAlive reports whether a runner is currently executing. It brackets every attempt,
	// including the final one.
	OnAlive func(alive bool)

	// Log is the injected logger. Nil is a silent sink.
	Log *slog.Logger
}

// Lifecycle runs a schedule and restarts it with exponential backoff after an unexpected
// exit. Cancellation is not an unexpected exit: when ctx is done the lifecycle returns
// without restarting.
type Lifecycle struct {
	opts LifecycleOptions
	log  *slog.Logger
}

// NewLifecycle builds a lifecycle from explicit options.
func NewLifecycle(opts LifecycleOptions) *Lifecycle {
	return &Lifecycle{opts: opts, log: logger.OrDiscard(opts.Log)}
}

// Run blocks until the final attempt exits or ctx is canceled.
func (l *Lifecycle) Run(ctx context.Context) {
	runner := l.opts.First
	if runner == nil && l.opts.New != nil {
		runner = l.opts.New()
	}

	attempts := l.opts.MaxRestarts
	if attempts < 1 {
		attempts = 1
	}
	delay := l.opts.RestartDelay

	for attempt := 1; attempt <= attempts; attempt++ {
		if runner == nil {
			return
		}

		if l.opts.OnAlive != nil {
			l.opts.OnAlive(true)
		}
		runner.Run(ctx)
		if l.opts.OnAlive != nil {
			l.opts.OnAlive(false)
		}

		// Cancellation or an explicit stop is a clean exit, not a crash to restart.
		if ctx.Err() != nil {
			return
		}

		if attempt == attempts {
			logger.OrDiscard(l.log).Error("lifecycle failed after restarts; giving up",
				"lifecycle", l.lifecycleName(), "attempts", attempts)
			return
		}

		logger.OrDiscard(l.log).Warn("lifecycle exited unexpectedly; restarting",
			"lifecycle", l.lifecycleName(), "restart", attempt, "max_restarts", attempts, "delay", delay)

		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}

		delay *= 2
		if l.opts.New != nil {
			runner = l.opts.New()
		}
	}
}

func (l *Lifecycle) lifecycleName() string {
	if l.opts.Name == "" {
		return "scheduler"
	}
	return l.opts.Name
}
