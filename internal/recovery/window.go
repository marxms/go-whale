package recovery

import (
	"github.com/marxms/go-whale/internal/candle"
)

// deriveWindow plans one timeframe's fetch for a recovery job. It is pure: the caller
// supplies the persisted head and the wall clock, so every branch is testable without a
// store.
//
// A zero fetchLimit means the timeframe is skipped: the silence crossed no bucket boundary
// of this timeframe and covers less than a bucket fraction of it, so the live stream still
// completes the current bucket on its own.
//
// The admitted window (job.window.startMs > 0) is a floor, never a narrower interval: the
// fetch starts at min(head, window.startMs). The straddle branch, which repairs only the
// bucket whose close the silence crossed, applies only when no window was admitted —
// otherwise the window would be silently replaced by a one-bucket repair.
func deriveWindow(job RecoveryJob, tf string, head, now int64) (sinceMs, gapStart int64, fetchLimit int) {
	tfSeconds := candle.ParseTimeframeToSeconds(tf)
	if tfSeconds == 0 {
		return 0, 0, 0
	}
	tfMs := int64(tfSeconds) * 1000

	// A targeted interior repair keeps the window it asked for: one named bucket.
	if job.TargetedBucket > 0 && job.TargetedTimeframe == tf {
		gapStart = candle.BucketTimestamp(job.TargetedBucket, tf)
		return gapStart, gapStart, 2
	}

	window := job.window
	// A lower bound in the future is not a window: fetching it would ask for an empty
	// interval. Derive from the head instead.
	if window.startMs > now {
		window.startMs = 0
	}
	gapDuration := job.ReconnectTime.Sub(job.DisconnectTime)
	if window.startMs > 0 {
		gapDuration = window.gapDuration
	}

	if head > 0 {
		sinceMs = head
		currentBucket := candle.BucketTimestamp(now, tf)
		// Materiality gate: recover a stale timeframe always, and a fresh one only when
		// the disconnect covers at least reconnectGapMaterialityFraction of its bucket, or
		// when the silence crossed a bucket boundary. Frames carry a running total, so a
		// gap inside the open bucket heals; one spanning its close can never be completed.
		// Both instants come from the job, and a retry preserves them.
		crossedBoundary := candle.BucketTimestamp(job.DisconnectTime.UnixMilli(), tf) !=
			candle.BucketTimestamp(job.ReconnectTime.UnixMilli(), tf)
		if head >= currentBucket-tfMs &&
			gapDuration.Milliseconds() < int64(float64(tfMs)*reconnectGapMaterialityFraction) &&
			!crossedBoundary {
			return 0, 0, 0
		}
		// A crossed boundary with no admitted window becomes a one-bucket repair: the head
		// may already have advanced past the shortened bucket, and dragging the window back
		// to the disconnect would re-fetch everything in between.
		straddled := candle.BucketTimestamp(job.DisconnectTime.UnixMilli(), tf)
		if window.startMs == 0 && crossedBoundary && straddled < candle.BucketTimestamp(sinceMs, tf) {
			return straddled, straddled, 2
		}
	}

	// The admitted window always wins as the lower bound: a head older than it already
	// covers more history, a head newer than it must still be dragged back to the floor.
	if window.startMs > 0 && (sinceMs == 0 || window.startMs < sinceMs) {
		sinceMs = window.startMs
	}
	if sinceMs <= 0 {
		// No persisted head and no admitted window: size the fetch from the silence.
		gapStart = candle.BucketTimestamp(job.DisconnectTime.UnixMilli(), tf)
		fetchLimit = int(gapDuration.Milliseconds()/tfMs) + 2
	} else {
		fetchLimit = int((now-sinceMs)/tfMs) + 2
		gapStart = candle.BucketTimestamp(sinceMs, tf)
	}
	if fetchLimit < 2 {
		fetchLimit = 2
	}
	if fetchLimit > 1000 {
		fetchLimit = 1000
	}
	return sinceMs, gapStart, fetchLimit
}
