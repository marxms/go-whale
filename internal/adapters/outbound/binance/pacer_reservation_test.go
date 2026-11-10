package binancesdk

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

// TestPacerRateLimitPreserved is a characterization test (N5 refactor guard):
// the pacer must still bound frames to one per interval. Deterministic via
// synctest synthetic time. N sequential waits take exactly (N-1)*interval.
func TestPacerRateLimitPreserved(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const interval = 100 * time.Millisecond
		p := newSubscribePacer(interval)

		start := time.Now()
		for i := 0; i < 4; i++ {
			if err := p.wait(context.Background()); err != nil {
				t.Fatalf("wait %d: %v", i, err)
			}
		}
		synctest.Wait()

		if got, want := time.Since(start), 3*interval; got != want {
			t.Fatalf("paced wall-time = %v, want %v (one frame per interval)", got, want)
		}
	})
}

// TestPacerRespectsContextCancel proves cancellation is honored while waiting on
// a reserved slot (and, with the slot-reservation refactor, the mutex is not held
// across the wait).
func TestPacerRespectsContextCancel(t *testing.T) {
	p := newSubscribePacer(time.Hour) // long interval forces the 2nd call to wait
	if err := p.wait(context.Background()); err != nil {
		t.Fatalf("first wait: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.wait(ctx); err == nil {
		t.Fatal("expected context cancellation error while waiting on the reserved slot")
	}
}
