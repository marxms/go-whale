package recovery

import (
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// TestTargetedSettleDelay_OutlastsTheKlineSettleGrace pins the coupling that made the repair
// into the defect.
//
// # THE REPAIR WAS FETCHING A RECORD THE EXCHANGE HAD NOT FINISHED WRITING
//
// A targeted repair exists to adopt the exchange's FINAL record for a bucket that just closed.
// It was enqueued with DefaultMinDelay — 2 seconds. Measured from the production host against
// the aggTrade tape, a Binance kline for a just-closed bucket is still settling well past that:
// BNB -1,68% at +1,59s, DOGE -1,30% at +1,14s, SOL still -0,32% at +3,04s, LINK -0,195% at
// +4,07s, zero divergence only from +5s on.
//
// So the repair fetched a short kline, the merge REPLACED (that branch does not take a maximum),
// and the counter recorded a heal. The mechanism meant to fix short buckets was manufacturing
// them — with no trade dropped, no id missed, and every detector correctly silent.
//
// The two constants are ONE decision expressed in two packages. If the delay ever drops below
// the grace, every record this job fetches is refused authority and the settle becomes a
// permanent no-op — the repair would stop damaging data and stop working, which is a quieter
// failure and therefore a worse one. This test is the only place that says so.
func TestTargetedSettleDelay_OutlastsTheKlineSettleGrace(t *testing.T) {
	t.Parallel()

	grace := time.Duration(candle.KlineSettleGraceMs) * time.Millisecond

	if TargetedSettleDelay < grace {
		t.Fatalf("TargetedSettleDelay=%v is shorter than candle.KlineSettleGraceMs=%v — every "+
			"record this job fetches would be refused authority and the targeted settle would "+
			"become a permanent no-op", TargetedSettleDelay, grace)
	}
	if TargetedSettleDelay <= DefaultMinDelay {
		t.Errorf("TargetedSettleDelay=%v is not longer than DefaultMinDelay=%v — the whole point "+
			"is that a targeted settle must wait longer than an ordinary reconnect backfill, "+
			"because it ADOPTS a record instead of filling a hole",
			TargetedSettleDelay, DefaultMinDelay)
	}
}

// TestEnqueueTargetedBucket_UsesTheSettleDelay checks the constant is actually the one the job
// carries. A well-named constant nobody wires up is the defect this activity has hit repeatedly:
// the fix present in the source and absent from the path production runs.
func TestEnqueueTargetedBucket_UsesTheSettleDelay(t *testing.T) {
	t.Parallel()

	m := &Manager{queue: NewCoalescingQueue(16, 16)}
	m.EnqueueTargetedBucket("binance", "future", "SKHYNIX/USDT", "5m", 1_785_909_600_000)

	var job RecoveryJob
	select {
	case job = <-m.queue.ReadPriority():
	default:
		t.Fatal("no job on the priority queue — a targeted settle must be enqueued ahead of the " +
			"boot-gap backlog, or it waits behind ~500 jobs at ~10/min")
	}
	if job.Delay != TargetedSettleDelay {
		t.Errorf("job.Delay = %v, want TargetedSettleDelay = %v — the constant exists but the "+
			"enqueue does not use it, so the repair still fetches inside the settle window",
			job.Delay, TargetedSettleDelay)
	}
	if job.TargetedBucket != 1_785_909_600_000 || job.TargetedTimeframe != "5m" {
		t.Errorf("job targets bucket=%d tf=%q, want 1785909600000/5m — a settle that re-derives "+
			"its window from the ring head can never reach a bucket that has already closed",
			job.TargetedBucket, job.TargetedTimeframe)
	}
}
