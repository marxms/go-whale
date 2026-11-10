package recovery

import "testing"

// TestFillStartMs_HonoursTheJobWindow guards a field that was constructed on every code path
// and read on none.
//
// The manager sets FillRequest.GapStart unconditionally — from the ring's latest timestamp
// when the provider knows one, from the job's DisconnectTime when it does not. The filler
// used SinceMs and ignored it, which on the fallback branches means starting from ZERO.
//
// A start of zero is not "no filter": the pagination loop runs `for currentStart < endMs`
// and advances by the last open time of each page, so it walks from the Unix epoch to now in
// pages of FetchLimit — a value the same fallback branch sizes to the gap, so it can be two.
// Recovering a short disconnect therefore issued tens of thousands of weighted REST calls,
// all billed to the per-IP budget shared with the seeder, to reach the handful of buckets
// that were missing.
func TestFillStartMs_HonoursTheJobWindow(t *testing.T) {
	t.Parallel()

	const disconnect = int64(1_700_000_000_000)

	got := fillStartMs(FillRequest{GapStart: disconnect, SinceMs: 0})
	if got != disconnect {
		t.Errorf("start = %d, want %d — the job carries the window it means in GapStart. "+
			"Falling back to SinceMs=0 starts the pagination at the Unix epoch and walks "+
			"forward to now, spending thousands of weighted REST calls on history nobody "+
			"asked for", got, disconnect)
	}
}

// TestFillStartMs_FallsBackToSince keeps the older contract working. A request that carries
// no explicit window still starts where the series left off, which is what the provider
// branch has always relied on.
func TestFillStartMs_FallsBackToSince(t *testing.T) {
	t.Parallel()

	const since = int64(1_700_000_600_000)
	if got := fillStartMs(FillRequest{GapStart: 0, SinceMs: since}); got != since {
		t.Errorf("start = %d, want %d — with no explicit window the head of the series is "+
			"the right place to resume", got, since)
	}
}

// TestFillStartMs_BothZeroStaysZero states the remaining edge honestly rather than hiding it
// behind a default. With neither field set there is no window to honor, and inventing one
// would silently change which buckets a caller gets back. The manager always sets at least
// one, so this is a contract statement, not a live path.
func TestFillStartMs_BothZeroStaysZero(t *testing.T) {
	t.Parallel()

	if got := fillStartMs(FillRequest{}); got != 0 {
		t.Errorf("start = %d, want 0 — with no window on the request there is nothing to "+
			"honor, and fabricating a bound would change the answer without saying so", got)
	}
}
