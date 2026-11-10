// Package scheduler owns the clocks and lifecycle of periodic work.
//
// A collector answers ONE question: what does a cycle do. This package answers when the
// cycle runs, how long it took, whether a scheduled tick was lapped, and when the loop
// stops. Keeping the two apart is what lets a cycle be tested without waiting on a
// ticker, and a ticker be tested without a market.
//
// It imports no exchange or vendor adapter and contains no market rule: the cycle is a
// callback supplied by the composition root.
package scheduler

import (
	"context"
	"log/slog"
	"time"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// Report is what one scheduled cycle did.
type Report struct {
	// Interval is the cadence the cycle was scheduled on.
	Interval time.Duration
	// Elapsed is the MEASURED wall-clock duration of the cycle.
	Elapsed time.Duration
	// Err is the cycle's own error. A cycle that fails is still measured.
	Err error
}

// Overran reports that the cycle outlasted the period it is scheduled on.
//
// That is the exact condition under which time.Ticker starts losing ticks: the runtime
// does not queue them, so every tick that falls inside a running cycle is dropped and
// the real spacing between cycles becomes the CYCLE's duration instead of the configured
// one.
func (r Report) Overran() bool { return r.Interval > 0 && r.Elapsed > r.Interval }

// LapFactor is how many configured periods the cycle actually consumed.
func (r Report) LapFactor() float64 {
	if r.Interval <= 0 {
		return 0
	}
	return float64(r.Elapsed) / float64(r.Interval)
}

// Measure runs ONE cycle, measures it against the supplied clock and returns the report.
//
// The clock is a parameter so the measurement can be tested without sleeping; production
// passes nil and gets time.Now. A failed cycle is measured exactly like a successful one —
// measuring only the clean ones would hide the cycle that took long because it was
// failing.
func Measure(
	ctx context.Context,
	cycle func(context.Context) error,
	interval time.Duration,
	now func() time.Time,
) Report {
	if now == nil {
		now = time.Now
	}
	start := now()
	var err error
	if cycle != nil {
		err = cycle(ctx)
	}
	return Report{Interval: interval, Elapsed: now().Sub(start), Err: err}
}

// Options configures a Loop.
type Options struct {
	// Name labels this schedule in operational logs.
	Name string

	// Interval is the cadence between cycles. The first cycle is immediate.
	Interval time.Duration

	// Startup runs once before the loop, retried with StartupAttempts and
	// StartupBackoff. A nil Startup skips the phase.
	//
	// proceed=false with a nil error means "do not run this schedule" (disabled by
	// configuration, empty fleet). That is a legitimate outcome, not a failure, and it is
	// not retried.
	Startup         func(ctx context.Context) (proceed bool, err error)
	StartupAttempts int
	// StartupBackoff is the wait after the FIRST failure. It doubles after each retry.
	StartupBackoff time.Duration

	// Bootstrap runs once after a successful Startup and before the first cycle.
	Bootstrap func(ctx context.Context)

	// BeforeFirst runs after Bootstrap and before the first cycle. Returning false
	// cancels the schedule without running a cycle — used to skip a cycle when shutdown
	// arrived during a startup wait.
	BeforeFirst func(ctx context.Context) bool

	// Cycle is ONE synchronous cycle. A tick that arrives while it runs is dropped by the
	// ticker, never queued: cycles never overlap, and a long cycle shows up as a lap in
	// the Report rather than as concurrent work.
	Cycle func(ctx context.Context) error

	// OnCycle receives every measured Report, before AfterCycle. Nil disables it.
	OnCycle func(ctx context.Context, report Report)

	// AfterCycle runs after OnCycle for every cycle, including failed ones.
	AfterCycle func(ctx context.Context, report Report)

	// Teardown releases resources when the loop exits, on every path.
	Teardown func()

	// OnStop overrides the default stop log line. Nil logs a generic line naming the
	// schedule.
	OnStop func()

	// Now is the measurement clock. Nil means time.Now.
	Now func() time.Time

	// Log is the injected logger. Nil is a silent sink.
	Log *slog.Logger
}

// Loop is one periodic schedule. It is a value; it owns no goroutine until Run is called.
type Loop struct {
	opts Options
	log  *slog.Logger
}

// NewLoop builds a schedule from explicit options.
func NewLoop(opts Options) *Loop {
	return &Loop{opts: opts, log: logger.OrDiscard(opts.Log)}
}

// Run blocks until ctx is canceled, running the first cycle immediately and then one
// cycle per Interval tick.
//
// The cycle executes in the select body, which is the coalescing contract: a tick that
// falls inside a running cycle is DROPPED by time.Ticker, so two cycles never overlap.
// When a cycle outlasts its period, the ticker hands over at most one buffered tick
// immediately afterwards and the schedule laps — the Report says so, and no second
// scheduling path is created to hide it.
func (l *Loop) Run(ctx context.Context) {
	if l.opts.Teardown != nil {
		defer l.opts.Teardown()
	}

	if l.opts.Startup != nil && !l.runStartup(ctx) {
		return
	}
	if l.opts.Bootstrap != nil {
		l.opts.Bootstrap(ctx)
	}
	if l.opts.BeforeFirst != nil && !l.opts.BeforeFirst(ctx) {
		return
	}

	l.runCycle(ctx)

	ticker := time.NewTicker(l.opts.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if l.opts.OnStop != nil {
				l.opts.OnStop()
			} else if l.opts.Name != "" {
				logger.OrDiscard(l.log).Info("schedule stopped", "schedule", l.opts.Name)
			}
			return
		case <-ticker.C:
			l.runCycle(ctx)
		}
	}
}

func (l *Loop) runCycle(ctx context.Context) {
	report := Measure(ctx, l.opts.Cycle, l.opts.Interval, l.opts.Now)
	if l.opts.OnCycle != nil {
		l.opts.OnCycle(ctx, report)
	}
	if l.opts.AfterCycle != nil {
		l.opts.AfterCycle(ctx, report)
	}
}

// runStartup runs the startup phase with exponential backoff: the first wait is
// StartupBackoff and each retry doubles it. Cancellation is observed during the wait, so a
// shutdown does not sit out a 160s backoff.
func (l *Loop) runStartup(ctx context.Context) bool {
	attempts := l.opts.StartupAttempts
	if attempts < 1 {
		attempts = 1
	}
	backoff := l.opts.StartupBackoff

	for attempt := 1; attempt <= attempts; attempt++ {
		proceed, err := l.opts.Startup(ctx)
		if err == nil {
			return proceed
		}

		logger.OrDiscard(l.log).Error("schedule startup attempt failed",
			"schedule", l.scheduleName(), "attempt", attempt, "max_attempts", attempts, "error", err)
		if attempt == attempts {
			logger.OrDiscard(l.log).Error("schedule startup attempts exhausted; giving up",
				"schedule", l.scheduleName(), "attempts", attempts)
			return false
		}

		select {
		case <-ctx.Done():
			logger.OrDiscard(l.log).Info("schedule: context canceled during startup retry", "schedule", l.scheduleName())
			return false
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return false
}

func (l *Loop) scheduleName() string {
	if l.opts.Name == "" {
		return "scheduler"
	}
	return l.opts.Name
}
