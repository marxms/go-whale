package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// TestKlineBackoffIsBoundedAndNotFixed30s is the named SPC-11 proof: the redial interval grows
// from 1s and stops at 30s with full jitter, and a dial that always fails stops after the
// declared number of attempts, publishing the terminal state instead of looping forever.
func TestKlineBackoffIsBoundedAndNotFixed30s(t *testing.T) {
	app := &App{}
	policy := app.klineBackoffPolicy()
	if policy.Base != time.Second || policy.Max != klineReconnectCap {
		t.Fatalf("policy = %+v, want base 1s and cap %v", policy, klineReconnectCap)
	}

	previous := time.Duration(0)
	for attempt := 0; attempt < klineMaxReconnectAttempts; attempt++ {
		delay := policy.Delay(attempt, 1)
		if delay < previous {
			t.Fatalf("attempt %d delay %v shrank from %v", attempt, delay, previous)
		}
		if delay > klineReconnectCap {
			t.Fatalf("attempt %d delay %v exceeds the cap", attempt, delay)
		}
		previous = delay
	}
	if previous != klineReconnectCap {
		t.Fatalf("delays stop at %v, want the declared cap %v", previous, klineReconnectCap)
	}
	if policy.Delay(0, 1) >= klineReconnectCap {
		t.Fatalf("first delay = %v: a fixed cap is the 30s loop the bound removes", policy.Delay(0, 1))
	}

	// A dial that always fails stops after the bounded attempts.
	starts := 0
	app = &App{
		streamCtx:    context.Background(),
		klineFeed:    &controlledKlineFeed{have: true, silent: 600},
		klineSymbols: []string{"BTC/USDT"},
		newKlineFeed: func(context.Context, string, string, string, func(string, candle.Candle)) klineFeed {
			starts++
			return &controlledKlineFeed{
				have:   true,
				silent: 600,
				start:  func([]string) error { return errors.New("dial refused") },
			}
		},
	}
	for tick := 0; tick < klineMaxReconnectAttempts+5; tick++ {
		app.restartKlineFeedIfSilent(context.Background(), "binance", "future")
		// The scheduler tick is the observation cadence; the test does not wait the backoff.
		app.klineNextAttempt = time.Time{}
	}
	if starts != klineMaxReconnectAttempts {
		t.Fatalf("redials = %d, want the declared bound %d", starts, klineMaxReconnectAttempts)
	}
	if exhausted, configured := app.klineReconnectExhaustedProbe(); !configured || !exhausted {
		t.Fatalf("probe = %v/%v, want the terminal state published", exhausted, configured)
	}
}
