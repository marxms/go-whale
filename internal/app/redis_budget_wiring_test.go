package app

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/marxms/go-whale/internal/platform/config"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// THE SAMPLING (live ceiling → YAML declaration) lives in internal/health
// (budget_sample_test.go). This case covers the continuous-loop wiring.

// TestStartRedisBudgetObserver_DoesNotRunWithoutADeclaredCadence — inventing an interval would be
// the same sin as the literal coefficient, with the aggravation that nobody would notice: an
// observer that samples at the wrong frequency still looks like an observer.
func TestStartRedisBudgetObserver_DoesNotRunWithoutADeclaredCadence(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	mr := miniredis.RunT(t)
	app := &App{log: log, cfg: config.Config{RedisAddr: mr.Addr()}}

	done := make(chan struct{})
	go func() {
		defer close(done)
		app.startRedisBudgetObserver(context.Background())
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("the observer kept running without a declared cadence")
	}

	var declared bool
	for _, record := range sink.Records() {
		if strings.Contains(record.Text(), "budget_sample_interval_seconds is absent") {
			declared = true
		}
	}
	if !declared {
		t.Fatalf("the absence of the cadence was not declared: %v", sink.Records())
	}
}
