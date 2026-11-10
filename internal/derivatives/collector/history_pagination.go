package collector

import (
	"context"
	"log/slog"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// ─────────────────────────────────────────────────────────────────────────────
// BACKWARD-WALKING HISTORY PAGINATION
//
// Three measured facts shape every line below.
//
//  1. A request carrying `startTime` alone returns the TAIL of [startTime, now] —
//     present-day data. Measured: `period=5m&limit=5&startTime=d-29` answered with
//     02:45→03:05 of the current day. So the window must be explicit at BOTH ends.
//
//  2. Even with both ends, the response carries the LAST `limit` points of the
//     window. A cursor that advances `startTime` therefore re-reads the same recent
//     page forever, and the series it assembles is truncated in its interior while
//     looking contiguous from its endpoints. Pagination has to move `endTime`
//     BACKWARD.
//
//  3. `openInterestHist` and `globalLongShortAccountRatio` retain thirty days:
//     measured HTTP 400 (-1130) at 35 days, OK at 25. Crossing that boundary is a
//     hard error, not a short answer. `/fapi/v1/fundingRate` carries NO such
//     retention and is therefore NOT clamped — the funding leg and the positioning
//     legs do not share a horizon.
// ─────────────────────────────────────────────────────────────────────────────

const (
	// historyPageLimit is the provider's maximum page size. It is a property of the
	// endpoint, not a tuning knob, which is why it is not read from config.
	historyPageLimit = 500

	// derivativesRetention is the query horizon of the positioning feeds. The
	// measurement brackets it: 25 days answers, 35 days returns HTTP 400 (-1130).
	// 30 days is the declared value inside that bracket.
	derivativesRetention = 30 * 24 * time.Hour

	// retentionGuardBand keeps the clamped `startTime` strictly inside the boundary.
	// It covers the skew between this process's clock and the exchange's — a
	// boundary computed from a local clock that runs a few seconds fast lands
	// outside the window and takes the hard error. It is a safety margin on a
	// REQUEST, never a substitute for a missing measurement.
	retentionGuardBand = time.Hour

	// declaredBaselinePoints is the baseline length the change declares: 288 points
	// of the pair's OWN timeframe, truncated by retention. Measured: 288 beats 48
	// (AUC +0,0283, IC95 [+0,0117; +0,0454], p = 0,001). Lengths above 288 were not
	// tested, so 288 is a declaration and not the top of a sweep.
	declaredBaselinePoints = 288
)

// historyPage is one explicit request window, in inclusive milliseconds.
type historyPage struct {
	StartMs int64
	EndMs   int64
}

// historyDepthPoints returns how many points of `timeframe` the bootstrap should
// assemble, and whether the timeframe is recognized at all.
//
// It is the smaller of what the baseline declares it needs and what retention can
// physically serve — which reproduces the declared table exactly: 5m→288, 1h→288,
// 4h→180, 1d→30. The daily timeframe is therefore born AT the declared minimum and
// carries the immature seal by construction, not by accident.
//
// `seriesCap` raises the target when the store is configured to keep more than the
// baseline needs: collecting deeper than the store keeps would pay REST pages for
// points that the ZSET cap deletes on arrival, and collecting shallower than the
// store keeps would leave declared capacity unused.
func historyDepthPoints(timeframe string, seriesCap int) (int, bool) {
	periodMs, ok := candle.ParseTimeframeMs(timeframe)
	if !ok || periodMs <= 0 {
		return 0, false
	}

	target := declaredBaselinePoints
	if seriesCap > target {
		target = seriesCap
	}

	retentionPoints := int(derivativesRetention.Milliseconds() / periodMs)
	if retentionPoints < target {
		target = retentionPoints
	}
	if target <= 0 {
		return 0, false
	}
	return target, true
}

// historyWalker emits successive windows, each one strictly older than the last.
//
// It never fabricates progress: the cursor moves back by the span it just requested,
// and moves back FURTHER when the response proves the provider had nothing newer
// than some earlier instant. It never moves forward, so the sequence terminates.
type historyWalker struct {
	periodMs   int64
	boundaryMs int64 // oldest instant a request may carry; never crossed
	cursorMs   int64 // `endTime` of the next page
	remaining  int   // points still wanted
	pages      int
	maxPages   int
	exhausted  bool
}

// newHistoryWalker builds the walk for one (symbol, feed, timeframe).
//
// `nowMs` is injected rather than read from the clock so the walk is reproducible in
// a test — a pagination bug that only appears at a particular wall-clock offset is
// exactly the kind that ships.
func newHistoryWalker(nowMs int64, timeframe string, targetPoints int) (*historyWalker, bool) {
	periodMs, ok := candle.ParseTimeframeMs(timeframe)
	if !ok || periodMs <= 0 || targetPoints <= 0 || nowMs <= 0 {
		return nil, false
	}

	boundaryMs := nowMs - (derivativesRetention - retentionGuardBand).Milliseconds()
	if boundaryMs < 0 {
		boundaryMs = 0
	}

	// One page covers at most `historyPageLimit` buckets, so the number of pages that
	// can fit between the cursor and the boundary is bounded. The +1 absorbs the
	// partial page at the far end. This is a belt on top of the strictly decreasing
	// cursor, not the mechanism that terminates the walk.
	maxPages := targetPoints/historyPageLimit + 1

	return &historyWalker{
		periodMs:   periodMs,
		boundaryMs: boundaryMs,
		cursorMs:   nowMs,
		remaining:  targetPoints,
		maxPages:   maxPages,
	}, true
}

// next returns the window to request, or false when the walk is over.
func (w *historyWalker) next() (historyPage, bool) {
	if w == nil || w.exhausted || w.remaining <= 0 || w.pages >= w.maxPages {
		return historyPage{}, false
	}
	if w.cursorMs <= w.boundaryMs {
		return historyPage{}, false
	}

	span := int64(w.remaining)
	if span > historyPageLimit {
		span = historyPageLimit
	}

	start := w.cursorMs - (span-1)*w.periodMs
	if start < w.boundaryMs {
		start = w.boundaryMs
	}

	w.pages++
	return historyPage{StartMs: start, EndMs: w.cursorMs}, true
}

// observe feeds the response back into the cursor.
//
// `oldestMs` is the earliest timestamp the page actually returned. When the provider
// hands back less than the requested window — which is the normal case, since it
// answers with the LAST `limit` points — the cursor must land just before what was
// returned, otherwise the next request repeats the part already collected.
//
// An empty page ends the walk. Inside retention, a window with nothing in it means
// the series does not reach that far back (a recent listing), and every older window
// is emptier still.
func (w *historyWalker) observe(page historyPage, oldestMs int64, count int) {
	if w == nil {
		return
	}

	w.remaining -= count

	nextEnd := page.StartMs - 1
	if count > 0 && oldestMs > 0 && oldestMs-1 < nextEnd {
		nextEnd = oldestMs - 1
	}
	w.cursorMs = nextEnd

	if count == 0 || nextEnd <= w.boundaryMs {
		w.exhausted = true
	}
}

// collectPaginatedHistory runs the backward walk for one (symbol, feed, timeframe),
// persists every point it collects, and reports the depth actually reached.
//
// It is a free function with a type parameter rather than a method because the two
// feeds return different row types and Go methods cannot be generic. The callbacks are
// the only feed-specific part: how to issue the page, where the row keeps its
// timestamp, and how the row becomes a snapshot.
//
// `nowMs` is a parameter, not a call to the clock, so the whole walk is reproducible.
func collectPaginatedHistory[T any](
	ctx context.Context,
	metrics Metrics,
	store derivatives.Writer,
	scope SymbolScope,
	feed derivativesFeed,
	timeframe string,
	seriesCap int,
	nowMs int64,
	fetch func(historyPage) ([]T, error),
	timestampOf func(T) int64,
	snapshotOf func(T) *derivatives.DerivativesSnapshot,
	logs ...*slog.Logger,
) int {
	log := logger.OrDiscard(logs...)
	// A period the provider does not serve answers HTTP 200 with an empty array, so
	// asking is indistinguishable from asking about a symbol with no data. The matrix
	// answers it without spending a request.
	if supportFor(feed, timeframe) == periodUnavailable {
		recordFeedRequest(metrics, scope.ExchangeID, feed, timeframe, outcomeUnsupported)
		return 0
	}

	depth, depthOK := historyDepthPoints(timeframe, seriesCap)
	if !depthOK {
		recordFeedRequest(metrics, scope.ExchangeID, feed, timeframe, outcomeError)
		log.Warn("derivatives bootstrap: unrecognized timeframe; no depth to request",
			"feed", feed, "timeframe", timeframe)
		return 0
	}

	walker, walkOK := newHistoryWalker(nowMs, timeframe, depth)
	if !walkOK {
		recordFeedRequest(metrics, scope.ExchangeID, feed, timeframe, outcomeError)
		log.Warn("derivatives bootstrap: could not plan the history walk",
			"feed", feed, "timeframe", timeframe)
		return 0
	}

	saved := 0
	dropped := 0
	var lastPersistErr error
	var oldest, newest int64

	for ctx.Err() == nil {
		page, more := walker.next()
		if !more {
			break
		}

		points, err := fetch(page)
		recordHistoryPage(metrics, scope.ExchangeID, feed, timeframe)
		if err != nil {
			recordFeedRequest(metrics, scope.ExchangeID, feed, timeframe, outcomeError)
			log.Debug("derivatives bootstrap window failed",
				"feed", feed,
				"symbol", scope.Symbol,
				"timeframe", timeframe,
				"window_start_ms", page.StartMs,
				"window_end_ms", page.EndMs,
				"error", err)
			break
		}

		if len(points) == 0 {
			recordFeedRequest(metrics, scope.ExchangeID, feed, timeframe, classifyEmptyCollection(feed, timeframe))
		} else {
			recordFeedRequest(metrics, scope.ExchangeID, feed, timeframe, outcomePoints)
		}

		var pageOldest int64
		for _, point := range points {
			ts := timestampOf(point)
			if ts > 0 && (pageOldest == 0 || ts < pageOldest) {
				pageOldest = ts
			}
		}
		walker.observe(page, pageOldest, len(points))

		for _, point := range points {
			snapshot := snapshotOf(point)
			if snapshot == nil {
				continue
			}
			if err := store.SaveSeriesOnly(ctx, snapshot, timeframe); err != nil {
				// Counted, not logged per point: a Redis outage would otherwise print
				// one line per bucket per symbol. The count is what matters, because
				// a hole in the series is invisible from its endpoints and the depth
				// gauge below would still report a plausible number.
				dropped++
				lastPersistErr = err
				continue
			}
			saved++
			if ts := snapshot.Timestamp; ts > 0 {
				if oldest == 0 || ts < oldest {
					oldest = ts
				}
				if ts > newest {
					newest = ts
				}
			}
		}
	}

	if dropped > 0 {
		log.Warn("derivatives bootstrap: points not persisted",
			"feed", feed,
			"symbol", scope.Symbol,
			"timeframe", timeframe,
			"dropped", dropped,
			"points", dropped+saved,
			"error", lastPersistErr)
	}

	recordCollectedDepth(metrics, scope.ExchangeID, feed, scope.Symbol, timeframe, saved, oldest, newest)
	return saved
}
