package recovery

import (
	"sync"
	"time"
)

// Defaults for the no-progress breaker and aggregate REST budget.
// All are tunable so recovery cannot itself trip Binance rate limits and a
// genuinely-stuck symbol is throttled instead of spinning every cycle.
const (
	// DefaultNoProgressK is the number of consecutive recovery cycles a symbol
	// may run WITHOUT its persisted freshness advancing before it is suspended.
	DefaultNoProgressK = 5
	// DefaultNoProgressBackoff is how long a no-progress symbol stays suspended
	// before it is eligible for recovery again.
	DefaultNoProgressBackoff = 15 * time.Minute
	// DefaultRESTBudgetPerSec caps aggregate gap-recovery REST calls per second
	// across all workers so recovery stays within a safe fraction of Binance's
	// request-weight budget.
	DefaultRESTBudgetPerSec = 20.0
	// DefaultRESTBudgetBurst is the maximum burst of REST calls allowed before
	// the per-second rate caps further calls.
	DefaultRESTBudgetBurst = 40.0
	// DefaultFreshnessGraceMs is the plateau guard: a symbol whose persisted
	// freshness is within this window of now is healthy and NEVER accrues a
	// no-progress stall, even if it did not advance this cycle. Freshness
	// legitimately plateaus within a bucket period (the latest candle is the open
	// bucket and cannot advance until it closes), and cold-start recovery cycles
	// run faster than buckets close — without this guard, valid current symbols
	// falsely trip K stalls and are suspended.
	// 30 min cleanly separates live symbols (seconds-fresh) from genuinely-stuck
	// ones (delisted tickers are days stale), erring toward NOT suspending.
	DefaultFreshnessGraceMs = int64(30 * 60 * 1000)
)

// progressOutcome reports what observe() decided for a single recovery cycle.
type progressOutcome uint8

const (
	// progressAdvanced means persisted freshness moved forward; counter reset.
	progressAdvanced progressOutcome = iota
	// progressStalled means freshness did not advance but K is not yet reached.
	progressStalled
	// progressSuspended means this stall reached K and the symbol is now suspended.
	progressSuspended
)

// progressState tracks a single scope|symbol's freshness watermark, its
// consecutive-stall counter, and when (if) it is suspended.
type progressState struct {
	watermark     int64
	stalls        int
	suspendedTill time.Time
}

// noProgressBreaker throttles symbols that are gap-recovered repeatedly without
// their persisted freshness advancing. After k consecutive non-advancing cycles
// a symbol is suspended for backoff; the no-progress metric is emitted and a
// gauge of currently-suspended symbols is maintained. It is concurrency-safe.
type noProgressBreaker struct {
	k       int
	backoff time.Duration
	graceMs int64 // plateau guard: freshness within this window of now ⇒ healthy
	now     func() time.Time

	mu    sync.Mutex
	state map[string]*progressState
}

// newNoProgressBreaker creates a breaker. A k <= 0 disables suspension (every
// cycle reports advanced/stalled but never suspends). graceMs <= 0 disables the
// plateau guard (legacy behavior — any non-advancing cycle counts as a stall).
func newNoProgressBreaker(k int, backoff time.Duration, graceMs int64) *noProgressBreaker {
	return &noProgressBreaker{
		k:       k,
		backoff: backoff,
		graceMs: graceMs,
		now:     time.Now,
		state:   make(map[string]*progressState),
	}
}

// observe records the freshness watermark for key after a recovery cycle and
// returns the resulting outcome. freshness is the max persisted latest-timestamp
// observed across timeframes for the symbol. A strictly higher value than the
// stored watermark counts as progress and resets the stall counter.
func (b *noProgressBreaker) observe(key string, freshness int64) progressOutcome {
	b.mu.Lock()
	defer b.mu.Unlock()

	st, ok := b.state[key]
	if !ok {
		b.state[key] = &progressState{watermark: freshness}
		return progressAdvanced
	}

	if freshness > st.watermark {
		st.watermark = freshness
		st.stalls = 0
		st.suspendedTill = time.Time{}
		return progressAdvanced
	}

	// PLATEAU GUARD: a symbol whose freshness is recent is healthy — its freshness
	// legitimately plateaus within a bucket period (the open bucket cannot advance
	// until it closes) and cold-start cycles run faster than buckets close. Such a
	// non-advance MUST NOT count as a stall (the stage false-positive that suspended
	// valid current symbols). Only a genuinely-stale symbol (freshness far behind
	// now, e.g. a delisted ticker days old) accrues no-progress stalls.
	if b.graceMs > 0 && freshness > 0 && b.now().UnixMilli()-freshness < b.graceMs {
		st.stalls = 0
		return progressAdvanced
	}

	st.stalls++
	if b.k > 0 && st.stalls >= b.k {
		st.suspendedTill = b.now().Add(b.backoff)
		st.stalls = 0
		return progressSuspended
	}
	return progressStalled
}

// isSuspended reports whether key is currently throttled. An expired suspension
// is cleared lazily so the symbol becomes eligible again.
func (b *noProgressBreaker) isSuspended(key string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	st, ok := b.state[key]
	if !ok || st.suspendedTill.IsZero() {
		return false
	}
	if b.now().Before(st.suspendedTill) {
		return true
	}
	st.suspendedTill = time.Time{}
	return false
}

// suspendedCount returns the number of symbols currently suspended, lazily
// clearing expired suspensions as it counts.
func (b *noProgressBreaker) suspendedCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()
	n := 0
	for _, st := range b.state {
		if st.suspendedTill.IsZero() {
			continue
		}
		if now.Before(st.suspendedTill) {
			n++
		} else {
			st.suspendedTill = time.Time{}
		}
	}
	return n
}

// restBudget is a token-bucket rate limiter that bounds aggregate gap-recovery
// REST call rate across all workers so recovery cannot itself trip Binance's
// request-weight limits. A non-positive rate disables throttling. It is
// concurrency-safe.
type restBudget struct {
	ratePerSec float64
	burst      float64
	now        func() time.Time

	mu     sync.Mutex
	tokens float64
	last   time.Time
}

// newRESTBudget creates a token bucket allowing ratePerSec calls/second with the
// given burst capacity. ratePerSec <= 0 disables throttling (allow always true).
func newRESTBudget(ratePerSec, burst float64) *restBudget {
	return &restBudget{
		ratePerSec: ratePerSec,
		burst:      burst,
		now:        time.Now,
		tokens:     burst,
	}
}

// allow reports whether one REST call may proceed now, consuming a token if so.
// It refills tokens based on elapsed wall-clock time, capped at burst.
func (b *restBudget) allow() bool {
	if b.ratePerSec <= 0 {
		return true
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()
	if b.last.IsZero() {
		b.last = now
	}
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * b.ratePerSec
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
		b.last = now
	}

	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}
