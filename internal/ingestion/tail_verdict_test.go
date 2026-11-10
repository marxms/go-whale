package ingestion

import (
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// TestSeenWindow_TailIsUnjudgedUntilTheBoundary is the defect, stated as an invariant.
//
// # A BUCKET DAMAGED IN ITS LAST TRADES CLOSED DECLARING ITSELF INTACT
//
// confirmMissing withholds judgement on the newest holeConfirmIds ids so that reordering is not
// reported as loss — without that margin a complete tape produced 103 false positives. That is
// the right call and it has a cost nobody had priced: the verdict on a bucket's FINAL ids lands
// only once the sequence has advanced 8 further, which is AFTER the bucket rolled and after
// checkOpenTimeContinuity has already read HasGap off the closing candle.
//
// So the mark could never reach the bucket it described. Measured in production over 11h and
// 21,3M trades: trades_missing_total 1558, candle_late_update_total ZERO on every reason — the
// ids genuinely never arrive, and nothing arrives late enough to be re-judged, so a verdict that
// misses the close misses forever.
//
// This test asserts both halves: the margin still protects a fresh tail, AND the boundary
// crossing forces the verdict out before the bucket is gone.
func TestSeenWindow_TailIsUnjudgedUntilTheBoundary(t *testing.T) {
	t.Parallel()

	// contiguous is what makes any verdict possible at all — a venue whose ids are not a dense
	// sequence can never have a "hole". Omitting it made the first assertion below pass
	// VACUOUSLY, for the wrong reason, which is the shape of a test that proves nothing.
	w := seenWindow{contiguous: true}
	const base = int64(85_816_515)

	// A clean run, then a hole, then only THREE further ids — fewer than holeConfirmIds, so the
	// hole is still inside the reordering margin. This is exactly the state a bucket is in when
	// it loses one of its last trades.
	for i := int64(0); i < 20; i++ {
		w.admit(base + i)
	}
	const holeID = base + 20
	for i := int64(21); i <= 23; i++ {
		w.admit(base + i)
	}

	if n := w.confirmMissing(); n != 0 {
		t.Fatalf("confirmMissing reported %d with the hole still inside the reorder margin, want 0 "+
			"— judging this early is what produced 103 false positives on a complete tape", n)
	}

	// The bucket is about to roll. This is the last instant the mark can land on it.
	got := w.confirmTail()
	if got != 1 {
		t.Fatalf("confirmTail reported %d, want 1 (id %d never arrived) — without this verdict the "+
			"bucket closes with HasGap false, no targeted re-read is enqueued, and the short bar "+
			"is permanent", got, holeID)
	}

	// Judged exactly once: a second call must not re-count the same id, or every subsequent
	// boundary would re-report the same loss and inflate trades_missing_total without bound.
	if again := w.confirmTail(); again != 0 {
		t.Errorf("confirmTail re-reported %d ids already judged — the verdict line must advance", again)
	}
	if after := w.confirmMissing(); after != 0 {
		t.Errorf("confirmMissing re-reported %d ids the tail pass already judged — the two callers "+
			"share one verdict line precisely so they cannot double-count", after)
	}
}

// TestSeenWindow_TailVerdictStillRefusesToJudgeAnIntactTape keeps the fix from becoming the
// defect it replaces. Dropping the reorder margin at the boundary must not manufacture losses on
// a tape that is merely out of order — that is the failure mode the margin exists for, and this
// change deliberately trades it away only at the one instant where waiting costs more.
func TestSeenWindow_TailVerdictStillRefusesToJudgeAnIntactTape(t *testing.T) {
	t.Parallel()

	w := seenWindow{contiguous: true}
	const base = int64(1_000_000)

	// Every id present, delivered in a shuffled order that would trip a naive per-jump detector.
	for _, off := range []int64{0, 3, 1, 4, 2, 7, 5, 9, 6, 8} {
		w.admit(base + off)
	}

	if n := w.confirmMissing() + w.confirmTail(); n != 0 {
		t.Errorf("reported %d missing on a COMPLETE tape delivered out of order — a detector that "+
			"cries wolf gets muted, and a muted detector is how the real loss walks past", n)
	}
}

// TestCrossedBucketBoundary_UsesTheFinestTimeframe pins the trigger.
//
// Timeframe boundaries are aligned, so crossing any coarser one necessarily crosses the finest.
// Asking the finest therefore catches EVERY crossing; asking a coarser one would silently skip
// the 5m buckets that make up most of the series — the fix would exist and do nothing for the
// timeframe where the loss was measured.
func TestCrossedBucketBoundary_UsesTheFinestTimeframe(t *testing.T) {
	t.Parallel()

	h := &StreamTradeHandler{tfReg: candle.NewTimeframeRegistry([]string{"1h", "5m", "15m"})}

	if got := h.finestTimeframe(); got != "5m" {
		t.Fatalf("finestTimeframe = %q, want %q — the registry order must not decide this", got, "5m")
	}

	const bucket = int64(1_785_909_600_000) // a 5m boundary
	for _, tc := range []struct {
		name       string
		prev, next int64
		want       bool
		why        string
	}{
		{
			"inside one bucket", bucket + 1_000, bucket + 200_000, false,
			"no bucket rolled, so nothing is about to become unreachable",
		},
		{
			"crosses a 5m boundary", bucket + 290_000, bucket + 310_000, true,
			"this batch is what rolls the bucket — the last instant the mark can land on it",
		},
		{
			"first batch for the key", 0, bucket + 1_000, false,
			"no previous position to compare against",
		},
		{
			"non-monotonic watermark", bucket + 310_000, bucket + 1_000, false,
			"a watermark that went backwards is not a roll",
		},
	} {
		if got := h.crossedBucketBoundary(tc.prev, tc.next); got != tc.want {
			t.Errorf("%s: crossedBucketBoundary = %v, want %v — %s", tc.name, got, tc.want, tc.why)
		}
	}
}

// TestTape_LossInTheFINALTradesOfABucketStillMarksIt proves the WIRING, not just the unit.
//
// The seen-window tests above show confirmTail computes the right verdict. They would keep
// passing if the handler stopped calling it — and "the fix exists in the source and is absent
// from the path production runs" is the defect this activity has hit three separate times
// (a type assertion that silently failed, handlers attached after the partitions were
// connected, a counter incremented on the branch instead of after the work).
//
// So this drives the real handler through the real store.
//
// Batch 1 establishes a watermark inside bucket A. Batch 2 carries the LAST trades of A — one of
// them missing — plus the first of B, which is what rolls A. The hole sits within holeConfirmIds
// of the highest id, so confirmMissing alone withholds judgement and A closes claiming to be
// complete. Only the boundary-triggered tail verdict can still reach it.
func TestTape_LossInTheFINALTradesOfABucketStillMarksIt(t *testing.T) {
	store := &captureStore{}
	h := NewStreamTradeHandler(store, "binance", "future", candle.NewTimeframeRegistry([]string{"1m"}))

	bucketA := time.UnixMilli(1_785_909_600_000)
	bucketB := bucketA.Add(time.Minute)

	// Batch 1: a clean run inside A. Establishes the watermark the crossing is measured against.
	if err := h.HandleTradesWithContext("binance", "future", "XRP/USDT",
		tapeAt(bucketA, contiguousIDs(500_000, 500_030)...)); err != nil {
		t.Fatalf("batch 1: %v", err)
	}
	store.all() // drain, so the assertion below reads only the crossing batch

	// Batch 2: the tail of A with id 500_034 never delivered, then A rolls into B. Only THREE ids
	// follow the hole — inside the reorder margin, which is exactly why a tail-blind verdict
	// stays quiet.
	tail := withoutID(contiguousIDs(500_031, 500_037), 500_034)
	batch := tapeAt(bucketA.Add(58*time.Second), tail...)
	batch = append(batch, tapeAt(bucketB, contiguousIDs(500_038, 500_040)...)...)
	if err := h.HandleTradesWithContext("binance", "future", "XRP/USDT", batch); err != nil {
		t.Fatalf("batch 2: %v", err)
	}

	var markedA bool
	var sawA bool
	for _, u := range store.all() {
		if u.Candle.Timestamp == bucketA.UnixMilli() {
			sawA = true
			if u.Candle.HasGap {
				markedA = true
			}
		}
	}
	if !sawA {
		t.Fatal("bucket A never reached persistence — the fixture does not exercise the path")
	}
	if !markedA {
		t.Error("bucket A rolled unmarked after losing one of its FINAL trades. The verdict on a " +
			"bucket's last ids lands only once the sequence advances holeConfirmIds further, which " +
			"is after checkOpenTimeContinuity has already read HasGap off the closing candle — so " +
			"no targeted re-read is enqueued and the short bar is permanent. Measured in " +
			"production: trades_missing_total 1558 over 11h with candle_late_update_total at zero, " +
			"meaning a verdict that misses the close misses forever")
	}
}

// TestTape_TailLossIsAttributedSeparately makes the boundary verdict FALSIFIABLE.
//
// After the fix shipped, the ratio of repairs to lost trades read 0,138 against 0,143 before it —
// indistinguishable. One number cannot separate "the fix works and tail losses are rare" from
// "the fix never fires", and those call for opposite responses. A fix without an observable is a
// claim, not a change; this activity spent its whole length correcting that defect elsewhere and
// then committed it in the correction itself.
//
// trades_missing_tail_total isolates the losses whose verdict only the boundary could reach.
func TestTape_TailLossIsAttributedSeparately(t *testing.T) {
	const ex, mk = "binance", "future"
	store := &captureStore{}
	h, m := handlerWithMetrics(store, ex, mk, []string{"1m"})
	h.PreregisterDiscardReasons(ex, mk)
	before := m.missingTail[labels(ex, mk)]
	bucketA := time.UnixMilli(1_785_996_000_000)

	if err := h.HandleTradesWithContext(ex, mk, "XRP/USDT",
		tapeAt(bucketA, contiguousIDs(900_000, 900_030)...)); err != nil {
		t.Fatalf("batch 1: %v", err)
	}

	// One id lost among the LAST trades of A, then A rolls. Only three ids follow the hole, so
	// the reordering frontier still withholds judgement — the boundary is the only thing that
	// can force it out in time.
	tail := withoutID(contiguousIDs(900_031, 900_037), 900_034)
	batch := tapeAt(bucketA.Add(58*time.Second), tail...)
	batch = append(batch, tapeAt(bucketA.Add(time.Minute), contiguousIDs(900_038, 900_040)...)...)
	if err := h.HandleTradesWithContext(ex, mk, "XRP/USDT", batch); err != nil {
		t.Fatalf("batch 2: %v", err)
	}

	if got := m.missingTail[labels(ex, mk)] - before; got != 1 {
		t.Errorf("trades_missing_tail_total rose by %v, want 1 — without this attribution the "+
			"boundary verdict is unfalsifiable: repairs-per-loss reads the same whether it fires "+
			"on every tail or on none", got)
	}
}
