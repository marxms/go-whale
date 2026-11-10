package binancews

import "time"

// Backoff is the reconnect cadence after a transport failure. Base doubles per
// consecutive failure up to Max; StableAfter is how long a connection must stay
// healthy before the failure count resets. The caller supplies the jitter in
// [0,1) so a fleet does not reconnect in lockstep.
type Backoff struct {
	Base        time.Duration
	Max         time.Duration
	StableAfter time.Duration
}

// DefaultBackoff is used when the caller declares none.
func DefaultBackoff() Backoff {
	return Backoff{Base: time.Second, Max: 60 * time.Second, StableAfter: 30 * time.Second}
}

// Delay returns the full-jitter delay for a zero-based attempt. Jitter is
// clamped to [0,1]; 1 returns the full exponential delay.
func (b Backoff) Delay(attempt int, jitter float64) time.Duration {
	if b.Base <= 0 {
		return 0
	}
	delay := b.Base
	for i := 0; i < attempt; i++ {
		if b.Max > 0 && delay >= b.Max/2 {
			delay = b.Max
			break
		}
		delay *= 2
	}
	if b.Max > 0 && delay > b.Max {
		delay = b.Max
	}
	if jitter < 0 {
		jitter = 0
	}
	if jitter > 1 {
		jitter = 1
	}
	return time.Duration(float64(delay) * jitter)
}

// Stable reports whether the connection has been healthy long enough for the
// failure count to reset.
func (b Backoff) Stable(healthyFor time.Duration) bool {
	return b.StableAfter <= 0 || healthyFor >= b.StableAfter
}
