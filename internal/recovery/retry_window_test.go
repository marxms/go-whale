package recovery

import (
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// TestRetryPreservesAdmittedWindow pins REC-04: a deferred retry round-trips through the
// scheduler carrying window.startMs, so the re-enqueued job still fetches from the admitted
// lower bound even after the live head has advanced. Losing it would turn the retry into a
// head-derived fetch and leave the hole the first attempt was asked to close.
func TestRetryPreservesAdmittedWindow(t *testing.T) {
	f := &windowFiller{failNext: 1}
	m, start, head := admittedWindowFixture(t, f)

	job := admittedJob(t, m, start, head)
	job.RetryAttempt = 4
	m.process(t.Context(), 0, job)

	m.retry.(*MemoryRetryScheduler).processReadyOnce(time.Now().Add(RetryMaxDelay + time.Second))
	var retry RecoveryJob
	select {
	case retry = <-m.queue.Read():
	default:
		t.Fatal("failed attempt did not return to the worker queue")
	}
	if retry.window.startMs != start {
		t.Fatalf("retry window.startMs = %d, want the admitted start %d", retry.window.startMs, start)
	}
	if retry.DisconnectTime.UnixMilli() != start {
		t.Fatalf("retry disconnect = %d, want the admitted start %d", retry.DisconnectTime.UnixMilli(), start)
	}

	// The head has moved to the current bucket; the retry must still start at the admitted
	// lower bound, not re-derive from the newer head.
	m.SetTimestampProvider(staleProvider{ts: candle.BucketTimestamp(time.Now().UnixMilli(), "5m")})
	retry.Delay = 0
	m.process(t.Context(), 0, retry)

	req := f.requests[len(f.requests)-1]
	if req.SinceMs != start || req.GapStart != start {
		t.Fatalf("retry fetched (%d,%d), want the admitted start %d", req.SinceMs, req.GapStart, start)
	}
	if req.FetchLimit <= 2 {
		t.Fatalf("retry narrowed the admitted window to FetchLimit=%d", req.FetchLimit)
	}
	assertWindowRepaired(t, m, f, start, head)
}
