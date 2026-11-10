package app

// redis_budget_wiring.go — the CONTINUOUS observer of the two occupancies, tied to the lifecycle.
//
// The sizing sheet (sizing_sheet.go) speaks ONCE, at startup. That answers "does the plan fit?" and
// not "does the plan keep fitting?". The incident that motivated this observer was only noticed at
// 99.92% occupancy, with 77,842 keys already evicted — two numbers that were available all along
// and that nobody sampled.

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/marxms/go-whale/internal/health"
)

// budgetInfoReader is the slice of the client the observer needs.
type budgetInfoReader interface {
	Info(ctx context.Context, sections ...string) *redis.StringCmd
}

// startRedisBudgetObserver samples the server on the declared cadence until the context falls.
//
// UNDECLARED CADENCE ⇒ THE LOOP DOES NOT RUN, and says why. Inventing an interval here would be the
// same sin as the literal coefficient, with the aggravation that nobody would notice: an observer
// that samples at the wrong frequency still looks like an observer.
func (a *App) startRedisBudgetObserver(ctx context.Context) {
	seconds := a.cfg.Retention.BudgetSampleIntervalSeconds
	if seconds <= 0 {
		a.logger().Warn("redis budget observer not started; retention.budget_sample_interval_seconds is absent")
		return
	}
	if a.redisStore == nil || a.redisStore.Client() == nil {
		return
	}

	client := a.redisStore.Client()
	monitor := health.NewRedisBudgetMonitor(a.cfg.Retention.OccupancyAlertRatio)
	monitor.SetMetrics(a.healthRedisMetrics)
	containerName := health.ContainerNameFromAddr(a.cfg.RedisAddr)
	interval := time.Duration(seconds) * time.Second

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		signal := monitor.Observe(a.sampleRedisBudget(ctx, client, containerName))
		if signal.Alert {
			a.logger().Warn("redis budget signal", "signal", signal.String())
		} else {
			a.logger().Info("redis budget signal", "signal", signal.String())
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// sampleRedisBudget reads the three INFO sections and delegates the ceiling
// preference to internal/health: the LIVE daemon value first, the YAML declaration
// second. The declaration exists because the Docker socket is not mounted in this
// service; mounting it would trade a signal for a privilege escalation.
func (a *App) sampleRedisBudget(ctx context.Context, client budgetInfoReader, containerName string) health.RedisBudgetSample {
	read := func(ctx context.Context, section string) (string, error) {
		return client.Info(ctx, section).Result()
	}
	return health.SampleRedisBudget(ctx, read, containerName,
		a.cfg.Retention.RedisContainerMemoryLimitMiB)
}
