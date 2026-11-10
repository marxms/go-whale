package recovery

import (
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/market"
)

// TestNoProgressBreakerSuspendsAfterK verifies : a symbol gap-recovered K
// consecutive cycles WITHOUT its persisted freshness advancing is suspended and
// the no-progress metric is emitted. A freshness advance resets the counter so a
// genuinely-recovering symbol is never suspended.
func TestNoProgressBreakerSuspendsAfterK(t *testing.T) {
	const (
		k       = 3
		backoff = time.Minute
	)
	now := time.Unix(1_700_000_000, 0)
	clock := now
	b := newNoProgressBreaker(k, backoff, DefaultFreshnessGraceMs)
	b.now = func() time.Time { return clock }

	const key = "binance:swap|BTC/USDT"

	// First observation establishes the watermark; it is never "no progress".
	if got := b.observe(key, 1000); got != progressAdvanced {
		t.Fatalf("first observe = %v, want progressAdvanced", got)
	}
	if b.isSuspended(key) {
		t.Fatal("must not be suspended after first observation")
	}

	// K consecutive non-advancing observations (same or lower freshness).
	for i := 0; i < k-1; i++ {
		if got := b.observe(key, 1000); got != progressStalled {
			t.Fatalf("observe %d = %v, want progressStalled", i, got)
		}
		if b.isSuspended(key) {
			t.Fatalf("must not be suspended before reaching K (i=%d)", i)
		}
	}
	// The K-th stall trips suspension.
	if got := b.observe(key, 1000); got != progressSuspended {
		t.Fatalf("K-th observe = %v, want progressSuspended", got)
	}
	if !b.isSuspended(key) {
		t.Fatal("must be suspended after K consecutive stalls")
	}
	if got := b.suspendedCount(); got != 1 {
		t.Fatalf("suspendedCount = %d, want 1", got)
	}

	// Suspension lifts after the backoff window.
	clock = clock.Add(backoff + time.Second)
	if b.isSuspended(key) {
		t.Fatal("suspension must lift after backoff window")
	}
	if got := b.suspendedCount(); got != 0 {
		t.Fatalf("suspendedCount after expiry = %d, want 0", got)
	}
}

// TestBreakerSuspendsAfterK pins REC-06 at the production threshold: the manager's breaker
// uses DefaultNoProgressK, and a symbol that writes nothing for that many consecutive cycles
// is suspended exactly then, not earlier and not later.
func TestBreakerSuspendsAfterK(t *testing.T) {
	m := newTestManager(fakeFiller{}, newRecordingWriter(), staleProvider{ts: time.Now().Add(-time.Hour).UnixMilli()})
	job := RecoveryJob{
		ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT",
		DisconnectTime: time.Now().Add(-2 * time.Hour), ReconnectTime: time.Now(),
	}
	key := "binance:future|" + market.NormalizeSymbol(job.Symbol)

	// The first observation establishes the watermark and is never "no progress".
	m.process(t.Context(), 0, job)
	if m.breaker.isSuspended(key) {
		t.Fatal("baseline cycle must not suspend the symbol")
	}
	for i := 1; i < DefaultNoProgressK; i++ {
		m.process(t.Context(), 0, job)
		if m.breaker.isSuspended(key) {
			t.Fatalf("suspended after %d of %d no-progress cycles", i, DefaultNoProgressK)
		}
	}
	m.process(t.Context(), 0, job)
	if !m.breaker.isSuspended(key) {
		t.Fatalf("not suspended after %d consecutive no-progress cycles", DefaultNoProgressK)
	}
}

// TestNoProgressBreakerResetsOnAdvance verifies that real freshness progress
// clears the stall counter, so a recovering symbol is never suspended.
func TestNoProgressBreakerResetsOnAdvance(t *testing.T) {
	b := newNoProgressBreaker(2, time.Minute, DefaultFreshnessGraceMs)
	const key = "binance:swap|ETH/USDT"

	if got := b.observe(key, 100); got != progressAdvanced {
		t.Fatalf("seed = %v, want progressAdvanced", got)
	}
	if got := b.observe(key, 100); got != progressStalled {
		t.Fatalf("stall 1 = %v, want progressStalled", got)
	}
	// Freshness advances — counter must reset.
	if got := b.observe(key, 200); got != progressAdvanced {
		t.Fatalf("advance = %v, want progressAdvanced", got)
	}
	// One more stall must NOT suspend (counter was reset to 0, K=2 not reached).
	if got := b.observe(key, 200); got != progressStalled {
		t.Fatalf("stall after reset = %v, want progressStalled", got)
	}
	if b.isSuspended(key) {
		t.Fatal("must not be suspended: advance reset the stall counter")
	}
}

// TestNoProgressBreakerPlateauGuard verifies the cold-start fix: a symbol whose
// freshness is recent (within the grace window of now) is healthy and is NEVER
// suspended, even across many non-advancing cycles — freshness legitimately
// plateaus within a bucket period. Only genuinely-stale symbols accrue stalls.
func TestNoProgressBreakerPlateauGuard(t *testing.T) {
	clock := time.Unix(1_700_000_000, 0)
	b := newNoProgressBreaker(3, time.Minute, DefaultFreshnessGraceMs)
	b.now = func() time.Time { return clock }

	// Recently-fresh symbol (freshness = now-1min, within the 30min grace).
	const key = "binance:swap|JTO/USDT"
	fresh := clock.Add(-time.Minute).UnixMilli()
	if got := b.observe(key, fresh); got != progressAdvanced {
		t.Fatalf("seed = %v, want progressAdvanced", got)
	}
	// Many non-advancing cycles MUST NOT suspend a recently-fresh symbol.
	for i := 0; i < 10; i++ {
		if got := b.observe(key, fresh); got != progressAdvanced {
			t.Fatalf("recent-freshness cycle %d = %v, want progressAdvanced (plateau guard)", i, got)
		}
		if b.isSuspended(key) {
			t.Fatalf("must NOT suspend a recently-fresh symbol (i=%d)", i)
		}
	}

	// Contrast: a genuinely-stale (days-old) symbol still suspends after K stalls.
	const dead = "binance:swap|DELISTED/USDT"
	old := clock.Add(-48 * time.Hour).UnixMilli()
	b.observe(dead, old) // seed watermark
	suspended := false
	for i := 0; i < 3; i++ {
		if b.observe(dead, old) == progressSuspended {
			suspended = true
		}
	}
	if !suspended {
		t.Fatal("a genuinely-stale (days-old) symbol must still suspend after K stalls")
	}
}

// TestRestBudgetBoundsCallRate verifies : aggregate gap-recovery REST calls
// are capped within a configurable budget (token bucket). Burst is honored, then
// further calls are denied until tokens refill.
func TestRestBudgetBoundsCallRate(t *testing.T) {
	clock := time.Unix(1_700_000_000, 0)
	// 10 calls/sec, burst 5.
	b := newRESTBudget(10, 5)
	b.now = func() time.Time { return clock }

	// Burst of 5 is allowed immediately.
	for i := 0; i < 5; i++ {
		if !b.allow() {
			t.Fatalf("burst call %d should be allowed", i)
		}
	}
	// 6th call exceeds the burst with no refill yet — denied.
	if b.allow() {
		t.Fatal("call beyond burst must be denied before refill")
	}

	// After 0.2s at 10/sec, 2 tokens refill.
	clock = clock.Add(200 * time.Millisecond)
	if !b.allow() {
		t.Fatal("call after partial refill should be allowed (token 1)")
	}
	if !b.allow() {
		t.Fatal("call after partial refill should be allowed (token 2)")
	}
	if b.allow() {
		t.Fatal("third call after 0.2s refill must be denied")
	}

	// Refill is capped at burst (no unbounded accumulation).
	clock = clock.Add(10 * time.Second)
	allowed := 0
	for i := 0; i < 100; i++ {
		if b.allow() {
			allowed++
		}
	}
	if allowed != 5 {
		t.Fatalf("refill must cap at burst=5, got %d allowed", allowed)
	}
}

// TestRestBudgetDisabled verifies a non-positive rate disables throttling
// entirely (always allow), so the budget is opt-in.
func TestRestBudgetDisabled(t *testing.T) {
	b := newRESTBudget(0, 0)
	for i := 0; i < 1000; i++ {
		if !b.allow() {
			t.Fatalf("disabled budget must always allow (i=%d)", i)
		}
	}
}
