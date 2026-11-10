package exchange

import (
	"context"
	"sync"
	"time"
)

// Binance USDⓈ-M futures allows 2.400 request-weight per minute PER IP, counted across
// every endpoint. The klines weight depends on the limit TIER, and the tiers are half-open
// — a detail that already cost us one production incident by being read as inclusive:
//
//	[1,100) -> 1     [100,500) -> 2     [500,1000] -> 5     (1000,1500] -> 10
//
// So limit=499 costs 2 and limit=500 costs 5.
const (
	// BinanceIPWeightPerMinute is the documented per-IP ceiling.
	BinanceIPWeightPerMinute = 2400

	// budgetSafetyFraction is the share of the ceiling this process will spend. The
	// remainder absorbs what we cannot meter: the websocket control plane, retries inside
	// the venue adapter, and the fact that Binance counts a rejected request too. 0.75 leaves 600
	// weight/min of slack.
	budgetSafetyFraction = 0.75
)

// KlinesWeight returns the request weight Binance bills for a klines call of `limit` bars.
//
// The half-open tiers are the whole point of this function existing: reading [100,500) as
// inclusive of 500 is what made a seed of 801 bars look like weight 2 when it costs 5.
func KlinesWeight(limit int) int {
	switch {
	case limit <= 0:
		return 1
	case limit < 100:
		return 1
	case limit < 500:
		return 2
	case limit <= 1000:
		return 5
	default:
		return 10
	}
}

// RestBudget is a process-wide token bucket over Binance request weight.
//
// A shared bucket makes the ceiling a property of the PROCESS instead of a property each
// component has to guess. Consumers ask for what a call costs and wait their turn; when
// the fleet grows, everyone slows proportionally instead of the fleet exceeding the cap.
//
// Binance escalates: 429 first, then a 418 IP ban that outlives the minute window. Penalize
// makes that visible to every consumer at once, so a rate-limited process backs off as a
// whole rather than having each component discover the ban independently and keep retrying.
//
// See docs/decisions/0030-process-wide-rest-budget.md.
type RestBudget struct {
	mu sync.Mutex

	perMinute  int
	available  float64
	lastRefill time.Time

	// penaltyUntil suspends ALL spending. Set by Penalize when the exchange refuses us.
	penaltyUntil time.Time

	now func() time.Time // injectable clock for deterministic tests
}

// NewRestBudget builds a bucket sized at budgetSafetyFraction of the per-IP ceiling.
// It starts EMPTY on purpose: a process that boots and immediately spends a full bucket is
// exactly the burst that gets rate-limited, so the first callers wait for refill.
func NewRestBudget() *RestBudget {
	return newRestBudgetWithClock(int(float64(BinanceIPWeightPerMinute)*budgetSafetyFraction), time.Now)
}

func newRestBudgetWithClock(perMinute int, now func() time.Time) *RestBudget {
	if perMinute < 1 {
		perMinute = 1
	}
	return &RestBudget{
		perMinute:  perMinute,
		available:  0,
		lastRefill: now(),
		now:        now,
	}
}

// PerMinute reports the configured spend rate, for logging and for tests that assert the
// budget is derived from the documented ceiling rather than hardcoded.
func (b *RestBudget) PerMinute() int {
	if b == nil {
		return 0
	}
	return b.perMinute
}

// refillLocked adds the weight earned since the last refill. Capped at one minute's worth
// so an idle process cannot bank a burst — banking is what a token bucket is for, but here
// the thing we are protecting against IS the burst.
func (b *RestBudget) refillLocked() {
	now := b.now()
	elapsed := now.Sub(b.lastRefill)
	if elapsed <= 0 {
		return
	}
	b.lastRefill = now
	b.available += float64(b.perMinute) * elapsed.Seconds() / 60.0
	if ceiling := float64(b.perMinute); b.available > ceiling {
		b.available = ceiling
	}
}

// Acquire blocks until `weight` is available, the context is done, or the process is under
// penalty and the context expires first. It returns ctx.Err() on cancellation so callers
// propagate shutdown instead of spending after streamCtx closed.
//
// A weight larger than one minute's allowance would never be satisfiable, so it is clamped
// rather than deadlocking the caller.
func (b *RestBudget) Acquire(ctx context.Context, weight int) error {
	if b == nil {
		return nil // nil budget = unmetered, for tests and for exchanges we do not meter
	}
	if weight < 1 {
		weight = 1
	}

	for {
		b.mu.Lock()
		if weight > b.perMinute {
			weight = b.perMinute
		}
		b.refillLocked()

		wait := time.Duration(0)
		if penalty := b.penaltyUntil.Sub(b.now()); penalty > 0 {
			wait = penalty
		} else if b.available >= float64(weight) {
			b.available -= float64(weight)
			b.mu.Unlock()
			return nil
		} else {
			deficit := float64(weight) - b.available
			wait = time.Duration(deficit / float64(b.perMinute) * float64(time.Minute))
		}
		b.mu.Unlock()

		if wait < 10*time.Millisecond {
			wait = 10 * time.Millisecond
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// Penalize suspends all spending for d, and drains the bucket so nothing rushes out the
// moment the penalty lifts.
//
// Call this when the exchange refuses us (429/418). Binance's own penalty outlives the
// minute window and escalates on repeat, so treating a refusal as "wait and retry
// immediately" is what turns a warning into a ban.
func (b *RestBudget) Penalize(d time.Duration) {
	if b == nil || d <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	until := b.now().Add(d)
	if until.After(b.penaltyUntil) {
		b.penaltyUntil = until
	}
	b.available = 0
	b.lastRefill = b.now()
}

// UnderPenalty reports whether spending is currently suspended, so callers can log the
// reason for a stall instead of appearing hung.
func (b *RestBudget) UnderPenalty() bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.penaltyUntil.After(b.now())
}

// binancePenaltyWindow is how long the process stops spending after the exchange refuses us.
//
// Binance's documented minimum IP ban is 2 minutes and it escalates on repeat, so backing
// off for less than that guarantees walking into the next tier. 2 minutes is the floor, not
// a tuned value: a shorter backoff has produced bans that outlived a whole deploy window.
const binancePenaltyWindow = 2 * time.Minute

// AcquireKlines reserves the weight a klines call of limit bars will cost. Consumers call
// it immediately BEFORE the request so the reservation and the spend stay adjacent —
// reserving early and requesting late lets two consumers interleave past the cap.
func (b *RestBudget) AcquireKlines(ctx context.Context, limit int) error {
	return b.Acquire(ctx, KlinesWeight(limit))
}

// PenalizeForRefusal suspends all spending after a refusal. It is a method on the shared
// budget because the limit is per-IP: the only backoff that works brakes every consumer at
// once. The composition root injects the same instance into seed and gap recovery.
func (b *RestBudget) PenalizeForRefusal() {
	b.Penalize(binancePenaltyWindow)
}
