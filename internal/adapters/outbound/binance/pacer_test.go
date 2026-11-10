package binancesdk

import (
	"context"
	"testing"
	"time"
)

// TestSubscribePacerEnforcesInterval verifies that sequential wait() calls are
// spaced by at least the configured interval. With interval 50ms, four calls
// must take at least 3×interval (the first is immediate; calls 2..4 each wait).
func TestSubscribePacerEnforcesInterval(t *testing.T) {
	const interval = 50 * time.Millisecond
	p := newSubscribePacer(interval)

	start := time.Now()
	for i := 0; i < 4; i++ {
		if err := p.wait(context.Background()); err != nil {
			t.Fatalf("wait %d returned error: %v", i, err)
		}
	}
	elapsed := time.Since(start)

	if minimum := 3 * interval; elapsed < minimum {
		t.Fatalf("4 paced waits took %v, expected >= %v (3×interval)", elapsed, minimum)
	}
}

// TestSubscribePacerContextCancel verifies wait() honors context cancellation
// instead of blocking for the full (long) interval. The first call returns
// immediately and sets last; the second is asked to wait ~10s but is given an
// already-canceled context, so it must return a non-nil error promptly.
func TestSubscribePacerContextCancel(t *testing.T) {
	p := newSubscribePacer(10 * time.Second)

	if err := p.wait(context.Background()); err != nil {
		t.Fatalf("first wait returned error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- p.wait(ctx) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("wait returned nil error for canceled context, expected ctx.Err()")
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("wait took %v to honor cancellation, expected prompt return", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("wait blocked on canceled context (did not return promptly)")
	}
}

// TestSubscribePacerFirstCallImmediate verifies a fresh pacer's first wait()
// does not block — there is no prior frame to pace against.
func TestSubscribePacerFirstCallImmediate(t *testing.T) {
	p := newSubscribePacer(10 * time.Second)

	start := time.Now()
	if err := p.wait(context.Background()); err != nil {
		t.Fatalf("first wait returned error: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("first wait took %v, expected near-immediate return", elapsed)
	}
}
