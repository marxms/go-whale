package candle

import (
	"slices"
	"testing"
)

const healStep = int64(5 * 60 * 1000)

// bufferTimestamps returns the buffer contents oldest -> newest, for assertions about which
// buckets survived a fold and in what order.
func bufferTimestamps(buf *RingBuffer[Candle]) []int64 {
	out := make([]int64, 0, buf.Len())
	for _, c := range buf.Slice() {
		out = append(out, c.Timestamp)
	}
	return out
}

func filledBuffer(capacity int, timestamps ...int64) *RingBuffer[Candle] {
	buf := NewRingBuffer[Candle](capacity)
	for _, ts := range timestamps {
		buf.Push(Candle{Timestamp: ts, Close: 100, Volume: 1})
	}
	return buf
}

// TestMergeBatchIntoBuffer_KeepsNewestWhenUnionExceedsCapacity pins the truncation rule.
//
// The union of a buffer and a batch can be longer than the ring. Which end gets dropped is
// not a detail: consumers read the RECENT window, so dropping the newest buckets would hide
// live data behind a repair — the fold would heal an old hole by opening a new one at the
// live edge.
func TestMergeBatchIntoBuffer_KeepsNewestWhenUnionExceedsCapacity(t *testing.T) {
	t.Parallel()

	// Ring holds 4; the union below is 6 buckets wide.
	buf := filledBuffer(4, 10, 20, 30, 40)
	batch := []Candle{
		{Timestamp: 50, Close: 101, Volume: 2},
		{Timestamp: 60, Close: 102, Volume: 2},
	}

	stats := MergeBatchIntoBuffer(buf, batch, healStep, 1_000)

	if stats.Merged != 0 || stats.Inserted != 2 {
		t.Errorf("merged/inserted = %d/%d, want 0/2", stats.Merged, stats.Inserted)
	}
	if got, want := bufferTimestamps(buf), []int64{30, 40, 50, 60}; !slices.Equal(got, want) {
		t.Errorf("buffer = %v, want %v — truncation must drop the OLDEST buckets; dropping the "+
			"newest would hide live data behind a repair", got, want)
	}
	if buf.Len() != 4 {
		t.Errorf("Len = %d, want 4 — the ring must never report more than its capacity", buf.Len())
	}
}

// TestMergeBatchIntoBuffer_UnsortedBatchFoldsInOrder guards the merge-join's precondition.
//
// "oldest-first" is a contract the callers document, not one the type enforces. A merge-join
// over unsorted input does not fail loudly — it silently emits an out-of-order buffer, and
// every consumer downstream (indicators, contiguity checks, cold writes) would then read a
// series whose timestamps go backwards.
func TestMergeBatchIntoBuffer_UnsortedBatchFoldsInOrder(t *testing.T) {
	t.Parallel()

	buf := filledBuffer(16, 10, 30)
	batch := []Candle{
		{Timestamp: 40, Close: 104, Volume: 2},
		{Timestamp: 20, Close: 102, Volume: 2},
	}

	stats := MergeBatchIntoBuffer(buf, batch, healStep, 1_000)

	if got, want := bufferTimestamps(buf), []int64{10, 20, 30, 40}; !slices.Equal(got, want) {
		t.Errorf("buffer = %v, want %v — an unsorted batch produced an out-of-order buffer", got, want)
	}
	// Repairing the order is right; repairing it QUIETLY is not. The producer that built the
	// batch has a bug, and the same slice goes on to cold storage and Redis where nothing
	// re-sorts it — so the violation has to reach an operator.
	if !stats.Unsorted {
		t.Error("unsorted = false — a contract violation was silently repaired, hiding the defect " +
			"in whoever produced the batch")
	}
}

// TestMergeBatchIntoBuffer_ReportsWhenStoredTotalExceedsTheExchange makes the RATCHET
// observable.
//
// The union takes the MAX of two whole-bucket totals, so a stored value that is too high
// survives every later repair — and the hot tier wins over cold on read, so nothing
// downstream corrects it either. That trade is deliberate (the measured, recurring failure
// is missing buckets, not inflated ones), but a self-sealing divergence from the exchange
// must not also be invisible: without this counter, an operator running a repair would see
// it succeed while the wrong number kept being served.
func TestMergeBatchIntoBuffer_ReportsWhenStoredTotalExceedsTheExchange(t *testing.T) {
	t.Parallel()

	buf := NewRingBuffer[Candle](16)
	buf.Push(Candle{Timestamp: 10, Close: 100, Volume: 5})
	buf.Push(Candle{Timestamp: 20, Close: 100, Volume: 999}) // stored too high

	batch := []Candle{
		{Timestamp: 10, Close: 100, Volume: 6},
		{Timestamp: 20, Close: 100, Volume: 7}, // the exchange's true total, lower than stored
	}

	stats := MergeBatchIntoBuffer(buf, batch, healStep, 1_000)

	if stats.Exceeded != 1 {
		t.Errorf("exceeded = %d, want 1 — the union kept a stored total larger than the "+
			"exchange's without reporting it", stats.Exceeded)
	}
	if got := buf.Slice()[1].Volume; got != 999 {
		t.Errorf("Volume = %v, want 999 — the MAX rule itself changed; this test pins that the "+
			"ratchet is REPORTED, not that it was removed", got)
	}
}

// TestMergeBatchIntoBuffer_DoesNotReorderTheCallersBatch is the other half of the same
// concern. The caller writes the SAME slice on to cold storage and Redis (tiered_store), so
// sorting it in place to satisfy this function would silently reorder someone else's payload
// — trading a local convenience for a defect in two other tiers.
func TestMergeBatchIntoBuffer_DoesNotReorderTheCallersBatch(t *testing.T) {
	t.Parallel()

	buf := filledBuffer(16, 10)
	batch := []Candle{
		{Timestamp: 40, Close: 104, Volume: 2},
		{Timestamp: 20, Close: 102, Volume: 2},
	}

	MergeBatchIntoBuffer(buf, batch, healStep, 1_000)

	if got, want := []int64{batch[0].Timestamp, batch[1].Timestamp}, []int64{40, 20}; !slices.Equal(got, want) {
		t.Errorf("caller batch = %v, want %v — the fold reordered a slice its caller also writes "+
			"to cold storage and Redis", got, want)
	}
}

// TestMergeBatchIntoBuffer_DuplicateBucketsInBatchFoldIntoOne keeps the one-candle-per-bucket
// invariant when an upstream response repeats a kline. Emitting both would put two rows for
// the same timestamp in the ring, which every downstream reader treats as two bars.
func TestMergeBatchIntoBuffer_DuplicateBucketsInBatchFoldIntoOne(t *testing.T) {
	t.Parallel()

	buf := filledBuffer(16, 10)
	batch := []Candle{
		{Timestamp: 20, Close: 102, Volume: 5, TakerBuyVolume: 3, TakerSellVolume: 2},
		{Timestamp: 20, Close: 103, Volume: 7, TakerBuyVolume: 4, TakerSellVolume: 3},
	}

	stats := MergeBatchIntoBuffer(buf, batch, healStep, 1_000)

	if got, want := bufferTimestamps(buf), []int64{10, 20}; !slices.Equal(got, want) {
		t.Fatalf("buffer = %v, want %v — a repeated kline became two bars", got, want)
	}
	if stats.Inserted != 1 {
		t.Errorf("inserted = %d, want 1 — a repeated kline was counted as two repairs", stats.Inserted)
	}
	dup := buf.Slice()[1]
	if dup.Volume != 7 {
		t.Errorf("Volume = %v, want 7 — duplicates must union (MAX), not overwrite or sum", dup.Volume)
	}
}

// TestMergeBatchIntoBuffer_EmptyBufferAdmitsBatchVerbatim covers the boot shape: nothing to
// union against, so the batch must land untouched. Anything else would re-derive values on
// the warm path, which is the one place the persisted level is the only truth available.
func TestMergeBatchIntoBuffer_EmptyBufferAdmitsBatchVerbatim(t *testing.T) {
	t.Parallel()

	buf := NewRingBuffer[Candle](16)
	batch := []Candle{
		{Timestamp: 10, Close: 100, Volume: 3, TakerBuyVolume: 2, TakerSellVolume: 1},
		{Timestamp: 20, Close: 101, Volume: 4, TakerBuyVolume: 0, TakerSellVolume: 4},
	}

	stats := MergeBatchIntoBuffer(buf, batch, healStep, 1_000)

	if stats.Merged != 0 || stats.Inserted != 2 {
		t.Errorf("merged/inserted = %d/%d, want 0/2", stats.Merged, stats.Inserted)
	}
	// Verbatim includes the ZEROS. The second bar is fully sell-side; admitting it as
	// anything other than 0/4 would erase a real measurement.
	got := buf.Slice()
	for i, want := range [][2]float64{{2, 1}, {0, 4}} {
		if got[i].TakerBuyVolume != want[0] || got[i].TakerSellVolume != want[1] {
			t.Errorf("bar %d taker pair = %v/%v, want %v/%v — the batch was not admitted verbatim",
				i, got[i].TakerBuyVolume, got[i].TakerSellVolume, want[0], want[1])
		}
	}
}
