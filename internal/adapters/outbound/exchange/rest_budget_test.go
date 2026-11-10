package exchange

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestKlinesWeight_TiersAreHalfOpen is the guard for the arithmetic error that caused a
// production incident.
//
// Binance bills klines by limit TIER: [1,100) -> 1, [100,500) -> 2, [500,1000] -> 5. The
// boundary at 500 is EXCLUSIVE of the weight-2 tier. Reading it as inclusive is what made a
// seed of 801 bars look like weight 2 when it costs 5, which under-counted the seed's spend
// by 2.5x and produced 4,602 HTTP 429s on the 500-symbol deploy.
func TestKlinesWeight_TiersAreHalfOpen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		limit int
		want  int
		why   string
	}{
		{limit: 1, want: 1, why: "bottom of the cheapest tier"},
		{limit: 99, want: 1, why: "last limit before the weight-2 tier"},
		{limit: 100, want: 2, why: "first limit in the weight-2 tier"},
		{limit: 499, want: 2, why: "LAST limit that still costs 2 — the boundary we got wrong"},
		{limit: 500, want: 5, why: "500 is NOT in [100,500): it costs 5, not 2"},
		{limit: 801, want: 5, why: "what the seed actually requests (buffer 800 + 1)"},
		{limit: 1000, want: 5, why: "top of the weight-5 tier"},
		{limit: 1001, want: 10, why: "past 1000 the weight doubles again"},
	}

	for _, tt := range tests {
		if got := KlinesWeight(tt.limit); got != tt.want {
			t.Errorf("KlinesWeight(%d) = %d, want %d — %s", tt.limit, got, tt.want, tt.why)
		}
	}
}

// TestRestBudget_StartsEmptySoBootCannotBurst pins the anti-burst property.
//
// A bucket that starts full lets a booting process spend a whole minute's allowance
// instantly — which is precisely the collision that got us rate-limited: the boot-gap batch
// fired ~30s after boot while the seed was still running, and both drew from a full bucket.
func TestRestBudget_StartsEmptySoBootCannotBurst(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{t: time.Unix(1785000000, 0)}
	b := newRestBudgetWithClock(600, clock.Now)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already-canceled: Acquire must NOT succeed from a pre-filled bucket

	if err := b.Acquire(ctx, 5); err == nil {
		t.Fatal("Acquire succeeded on a fresh budget with a canceled context — the bucket " +
			"started pre-filled, so a booting process can still burst a full minute's weight")
	}
}

// TestRestBudget_RefillsAtTheConfiguredRate pins that spend is bounded by the documented
// ceiling and not by whatever each component guessed for itself.
func TestRestBudget_RefillsAtTheConfiguredRate(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{t: time.Unix(1785000000, 0)}
	b := newRestBudgetWithClock(600, clock.Now) // 600 weight/min = 10 weight/s

	clock.advance(10 * time.Second) // earns 100 weight

	ctx := context.Background()
	for i := 0; i < 20; i++ { // 20 x 5 = 100 weight, exactly what was earned
		if err := b.Acquire(ctx, 5); err != nil {
			t.Fatalf("Acquire #%d failed with %v — 10s at 600/min should fund 100 weight", i+1, err)
		}
	}

	// The 21st must not be fundable: the bucket is spent and the clock has not moved.
	spent, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Acquire(spent, 5); err == nil {
		t.Fatal("Acquire succeeded past the earned allowance — the bucket is not enforcing the rate")
	}
}

// TestRestBudget_PenaltySuspendsEveryConsumer is the guard for ESCALATION.
//
// Binance answers 429 first and then a 418 IP ban that outlives the minute window and
// escalates on repeat. If each consumer discovers the refusal on its own and keeps retrying
// at its own pace, the process walks itself from a warning into a ban — which is what left
// the fleet at 34 symbols after the outage. A penalty has to be a property of the process.
func TestRestBudget_PenaltySuspendsEveryConsumer(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{t: time.Unix(1785000000, 0)}
	b := newRestBudgetWithClock(600, clock.Now)
	clock.advance(time.Minute) // fully funded

	b.Penalize(30 * time.Second)

	if !b.UnderPenalty() {
		t.Fatal("UnderPenalty() = false right after Penalize — consumers cannot tell they are banned")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Acquire(ctx, 1); err == nil {
		t.Fatal("Acquire succeeded while under penalty — a funded bucket let a consumer keep " +
			"hitting an exchange that is already refusing us, which is how 429 escalates to a 418 ban")
	}

	clock.advance(31 * time.Second)
	if b.UnderPenalty() {
		t.Fatal("still under penalty after it should have expired — the process would stall forever")
	}
	if err := b.Acquire(context.Background(), 1); err != nil {
		t.Fatalf("Acquire failed after the penalty expired: %v", err)
	}
}

// TestRestBudget_ConcurrentAcquireRespectsTheCeiling is the property that matters most:
// the bound must hold across goroutines, because the whole reason this type exists is that
// independent components were each individually sane and collectively unbounded.
func TestRestBudget_ConcurrentAcquireRespectsTheCeiling(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{t: time.Unix(1785000000, 0)}
	b := newRestBudgetWithClock(600, clock.Now)
	clock.advance(10 * time.Second) // funds exactly 100 weight

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // nobody may wait: only already-funded weight can be taken

	var mu sync.Mutex
	granted := 0
	var wg sync.WaitGroup
	for i := 0; i < 60; i++ { // 60 goroutines x 5 weight = 300 requested vs 100 funded
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := b.Acquire(ctx, 5); err == nil {
				mu.Lock()
				granted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if granted != 20 {
		t.Errorf("granted %d requests of 5 weight from a 100-weight bucket, want exactly 20 — "+
			"concurrent consumers are racing past the shared ceiling, which is the exact defect "+
			"this budget exists to prevent", granted)
	}
}

// fakeClock is a manually advanced clock so budget tests are deterministic instead of
// sleeping. Safe for the concurrent test: advance() is only called before goroutines start.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}
