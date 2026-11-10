package ingestion

import (
	"math/rand"
	"testing"
)

// TestSeenWindow_AdmitsLateArrivalInsideWindow is the property the monotonic watermark
// could not provide, and whose absence was silently costing volume.
//
// Measured in production: binance:future:REZ/USDT arrived with id=44822238 after the
// watermark had already reached 44822239 — a gap of ONE — and was discarded as "already
// seen" despite never having been seen. The Binance SDK dispatches every message on its own
// goroutine (common/websocket.go executeCallbacks), so adjacent trades race and swap.
func TestSeenWindow_AdmitsLateArrivalInsideWindow(t *testing.T) {
	t.Parallel()

	var w seenWindow
	const base = 44822238

	if !w.admit(base + 1) {
		t.Fatal("first id must be admitted")
	}
	if !w.admit(base) {
		t.Fatal("an id one BELOW the highest, never seen, must be admitted — rejecting it is the volume loss")
	}
	if w.admit(base) {
		t.Fatal("the same id must not be admitted twice — that would inflate volume instead")
	}
	if w.admit(base + 1) {
		t.Fatal("re-admitting the highest id would double-count it")
	}
}

// TestSeenWindow_RejectsReplayAndTooOld pins the reason deduplication exists at all.
func TestSeenWindow_RejectsReplayAndTooOld(t *testing.T) {
	t.Parallel()

	var w seenWindow
	for id := int64(1000); id < 1100; id++ {
		if !w.admit(id) {
			t.Fatalf("id %d should be new", id)
		}
	}
	for id := int64(1000); id < 1100; id++ {
		if w.admit(id) {
			t.Fatalf("id %d replayed and was admitted — this inflates volume", id)
		}
	}
	// Far below the window: a genuine stale replay, not reordering.
	if w.admit(1099 - seenWindowSize) {
		t.Fatal("an id older than the whole window must be rejected")
	}
}

// TestSeenWindow_ShuffledStreamLosesNothing is the end-to-end property: a stream delivered
// in ANY local order must be admitted exactly once per id. This is what makes the dedup
// match the transport's real contract instead of an assumed one.
func TestSeenWindow_ShuffledStreamLosesNothing(t *testing.T) {
	t.Parallel()

	const (
		start  = int64(10_000_000)
		n      = 5000
		jitter = 64 // local reordering far wider than the observed gap=1
	)

	ids := make([]int64, 0, n)
	for i := int64(0); i < n; i++ {
		ids = append(ids, start+i)
	}
	// Shuffle within a sliding neighborhood to model out-of-order arrival.
	rng := rand.New(rand.NewSource(42)) //nolint:gosec // G404: deterministic test sampling, not cryptographic randomness.
	for i := range ids {
		j := i + rng.Intn(jitter)
		if j < len(ids) {
			ids[i], ids[j] = ids[j], ids[i]
		}
	}

	var w seenWindow
	admitted := 0
	for _, id := range ids {
		if w.admit(id) {
			admitted++
		}
	}
	if admitted != n {
		t.Fatalf("admitted %d of %d shuffled ids — %d trades would have been lost", admitted, n, n-admitted)
	}

	// And a full replay of the same stream must add nothing.
	for _, id := range ids {
		if w.admit(id) {
			t.Fatal("a replayed id was admitted — volume would be inflated")
		}
	}
}

// TestSeenWindow_LargeJumpDoesNotResurrectStaleBits guards the ring's reuse. When the id
// jumps by more than the window, every slot wraps onto bits from a previous lap; if they
// were not cleared, brand-new trades would be read as "already seen" and dropped — turning
// a reconnect into silent data loss.
func TestSeenWindow_LargeJumpDoesNotResurrectStaleBits(t *testing.T) {
	t.Parallel()

	var w seenWindow
	for id := int64(1); id <= 100; id++ {
		w.admit(id)
	}

	jumped := int64(100 + seenWindowSize*3)
	if !w.admit(jumped) {
		t.Fatal("the jumped id must be admitted")
	}
	// Ids congruent to the old ones modulo the window must NOT read as seen.
	for id := jumped + 1; id <= jumped+100; id++ {
		if !w.admit(id) {
			t.Fatalf("id %d was rejected — a stale bit from a previous lap of the ring survived", id)
		}
	}
}

// TestSeenWindow_SmallGapsAreCleared covers the incremental advance: ids skipped between
// the old and new highest (a real gap in the exchange stream) must not be left marked, or a
// later in-window arrival of those very ids would be dropped.
func TestSeenWindow_SmallGapsAreCleared(t *testing.T) {
	t.Parallel()

	var w seenWindow
	w.admit(1)
	// Lap the ring once so the slots for 2..10 carry stale bits.
	for id := int64(2); id <= seenWindowSize+10; id++ {
		w.admit(id)
	}
	// Now jump forward, skipping ids, then deliver the skipped ones late.
	base := int64(seenWindowSize + 10)
	w.admit(base + 50)
	for id := base + 1; id < base+50; id++ {
		if !w.admit(id) {
			t.Fatalf("skipped id %d was rejected on late arrival — it had never been seen", id)
		}
	}
}

// TestSeenWindow_NegativeIDIsClamped proves the guard in slot: a malformed negative id lands
// in slot 0 instead of wrapping to an arbitrary slot, and still deduplicates as one id.
func TestSeenWindow_NegativeIDIsClamped(t *testing.T) {
	t.Parallel()

	var w seenWindow
	if !w.admit(-1) {
		t.Fatal("the first id must be admitted")
	}
	if w.admit(-1) {
		t.Fatal("a repeated negative id must be a duplicate, not a second admission")
	}
}
