package recovery

import "testing"

// TestPriorityDrainsBeforeBulk pins the ordering that makes a targeted repair useful rather
// than merely eventual.
//
// # THE MEASUREMENT THAT FORCED THIS
//
// The bulk channel is FIFO and the boot-gap sweep puts the whole fleet on it at start-up.
// Measured in production: ~10 jobs a minute drained, ~500 sweep jobs ahead of the repairs
// enqueued when the boot buckets closed. The repairs were therefore ~50 minutes from being
// attempted — on a 5-minute bucket, long after every consumer downstream has read the wrong
// value. The mechanism was not broken; it was arriving after it could matter, which for a
// repair is the same thing.
//
// The two kinds of work are not comparable: a targeted repair is one bucket and one page, a
// sweep job walks every timeframe. Letting the cheap, time-critical one overtake the bulk one
// costs nothing.
func TestPriorityDrainsBeforeBulk(t *testing.T) {
	q := NewCoalescingQueue(16, 16)

	// Bulk first, so FIFO alone would return it first.
	if got := q.Enqueue(RecoveryJob{Symbol: "BULK"}); got != enqueueResultQueued {
		t.Fatalf("bulk enqueue = %v, want queued", got)
	}
	if got := q.EnqueuePriority(RecoveryJob{Symbol: "TARGETED", TargetedBucket: 1}); got != enqueueResultQueued {
		t.Fatalf("priority enqueue = %v, want queued", got)
	}

	select {
	case job := <-q.ReadPriority():
		if job.Symbol != "TARGETED" {
			t.Fatalf("priority channel yielded %q", job.Symbol)
		}
	default:
		t.Fatal("the priority channel is empty — a targeted repair went onto the bulk channel " +
			"and will wait behind the boot-gap sweep")
	}
	select {
	case job := <-q.Read():
		if job.Symbol != "BULK" {
			t.Fatalf("bulk channel yielded %q — the two channels are crossed", job.Symbol)
		}
	default:
		t.Fatal("the bulk job vanished")
	}
}

// TestPriorityDropsRatherThanBuffers pins the bound. One targeted repair per key per boot is
// the contract; a backlog here would mean the trigger is firing repeatedly, which is a defect
// in the caller. Dropping surfaces it — buffering hides it behind growing memory until
// something else fails first.
func TestPriorityDropsRatherThanBuffers(t *testing.T) {
	q := NewCoalescingQueue(4, 4)
	// The priority channel is sized independently of chanSize; fill it and confirm the
	// overflow is reported rather than absorbed.
	for range cap(q.priority) {
		if got := q.EnqueuePriority(RecoveryJob{TargetedBucket: 1}); got != enqueueResultQueued {
			t.Fatalf("premature refusal at a non-full channel: %v", got)
		}
	}
	if got := q.EnqueuePriority(RecoveryJob{TargetedBucket: 1}); got != enqueueResultFailed {
		t.Errorf("overflow = %v, want failed — an unbounded priority path would let a "+
			"misfiring trigger consume memory instead of reporting itself", got)
	}
}
