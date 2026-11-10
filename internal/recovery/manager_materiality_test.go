package recovery

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// recordingFiller records which timeframes Fill was invoked for, so a test can
// assert that the materiality gate skipped the immaterial ones.
type recordingFiller struct {
	mu     sync.Mutex
	filled map[string]int
	reqs   map[string]FillRequest
}

func (f *recordingFiller) Name() string { return "recording" }

func (f *recordingFiller) Fill(_ context.Context, _ HistorySource, req FillRequest) (FillResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.filled == nil {
		f.filled = make(map[string]int)
		f.reqs = make(map[string]FillRequest)
	}
	f.filled[req.Timeframe]++
	f.reqs[req.Timeframe] = req
	return FillResult{}, nil
}

func (f *recordingFiller) calledFor(tf string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.filled[tf] > 0
}

// requestFor returns the FillRequest the gate allowed through for a timeframe.
func (f *recordingFiller) requestFor(tf string) FillRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reqs[tf]
}

// nowProvider reports every timeframe as fresh (latest persisted candle = now),
// so the only thing deciding recovery is the disconnect-gap materiality gate.
type nowProvider struct{}

func (nowProvider) GetLatestTimestamp(context.Context, string, string, string, string) (int64, bool) {
	return time.Now().UnixMilli(), true
}

func newMaterialityManager(f GapFiller, p LatestTimestampProvider, tfs []string) *Manager {
	m := NewManager(newRecordingWriter(), 1, candle.NewTimeframeRegistry(tfs))
	m.clientPool = fakeClientPool{}
	m.validator = passValidator{}
	m.SetFiller(f)
	m.SetTimestampProvider(p)
	return m
}

// TestProcessSkipsImmaterialTimeframesOnShortReconnect proves the materiality gate: a 33s
// reconnect gap recovers only the fast timeframe it materially affects (5m, where
// 33s is 11% of the bucket) and skips the slow ones (1h/1d/1w, where 33s is a
// negligible fraction). This collapses the 8-TF fan-out that, with deferred jobs
// retrying, turns into a ~19k-event deferral/retry storm on every fleet reconnect.
func TestProcessSkipsImmaterialTimeframesOnShortReconnect(t *testing.T) {
	f := &recordingFiller{}
	m := newMaterialityManager(f, nowProvider{}, []string{"5m", "1h", "1d", "1w"})

	now := time.Now()
	m.process(t.Context(), 0, RecoveryJob{
		ExchangeID: "binance", MarketType: "future", Symbol: "MANTA/USDT",
		DisconnectTime: now.Add(-33 * time.Second), ReconnectTime: now,
	})

	if !f.calledFor("5m") {
		t.Error("5m must be recovered — a 33s gap is 11% of a 5m bucket (material)")
	}
	// The anti-storm property is asserted as a PROPERTY, not as "these three timeframes are
	// skipped". A 33s silence that happens to straddle the close of an hour must now be repaired
	// — that is the defect G1 caught — so whether 1h is fetched depends on the wall-clock phase
	// and a fixed expectation here would flake once an hour. What must hold at every phase is
	// that a coarse timeframe is never SWEPT: either it is skipped, or it asks for exactly the
	// one bucket whose close the silence spanned. A mutation that drags the window back to the
	// disconnect instant fails this on the sweep, which is the harm the gate exists to prevent.
	for _, tf := range []string{"1h", "1d", "1w"} {
		if !f.calledFor(tf) {
			continue // skipped: immaterial and no close was spanned
		}
		if got := f.requestFor(tf).FetchLimit; got > 2 {
			t.Errorf("%s was swept with FetchLimit %d — a short silence may repair ONE straddled bucket, never a range (wasteful REST that feeds the retry storm)", tf, got)
		}
	}
}

// TestProcessNeverSkipsGenuinelyStaleTimeframes proves the safety invariant: a
// timeframe whose latest persisted candle is 2+ buckets old (a closed bucket is
// genuinely missing) is ALWAYS recovered, even with a zero-length disconnect gap —
// which is exactly the shape of a re-enqueued retry job (DisconnectTime ==
// ReconnectTime). The materiality gate must never suppress a real gap.
func TestProcessNeverSkipsGenuinelyStaleTimeframes(t *testing.T) {
	f := &recordingFiller{}
	// 10 days behind on every timeframe: a closed bucket is missing in all of them.
	stale := staleProvider{ts: time.Now().Add(-10 * 24 * time.Hour).UnixMilli()}
	m := newMaterialityManager(f, stale, []string{"5m", "1h", "1d"})

	now := time.Now()
	m.process(t.Context(), 0, RecoveryJob{
		ExchangeID: "binance", MarketType: "future", Symbol: "OLD/USDT",
		DisconnectTime: now, ReconnectTime: now, // zero gap — would skip fresh TFs
	})

	for _, tf := range []string{"5m", "1h", "1d"} {
		if !f.calledFor(tf) {
			t.Errorf("%s is genuinely stale (10d behind) — must be recovered regardless of disconnect-gap materiality", tf)
		}
	}
}

// TestProcessGateScalesWithGapSize proves the gate is proportional, not a fixed
// allow-list: a 2h outage materially affects more timeframes than a 33s blip, so
// it recovers the faster ones (5m, 1h — 2h is >10% of their buckets) while still
// sparing the slow ones (1d, where 2h is 8.3% of the bucket, and 1w, where it is
// ~1.2%).
func TestProcessGateScalesWithGapSize(t *testing.T) {
	f := &recordingFiller{}
	m := newMaterialityManager(f, nowProvider{}, []string{"5m", "1h", "1d", "1w"})

	now := time.Now()
	m.process(t.Context(), 0, RecoveryJob{
		ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT",
		DisconnectTime: now.Add(-2 * time.Hour), ReconnectTime: now,
	})

	for _, tf := range []string{"5m", "1h"} {
		if !f.calledFor(tf) {
			t.Errorf("%s must be recovered — a 2h gap is >10%% of its bucket (material)", tf)
		}
	}
	// 2h is below 10% of both buckets (8.3% of 1d, 1.2% of 1w), so the fraction alone
	// would skip them. A silence that happens to straddle a bucket close must still be
	// repaired — its close was truncated — so at some wall-clock phases the coarse
	// timeframe is fetched. The invariant that holds at EVERY phase is that it is never
	// swept: either skipped, or a targeted repair of the one straddled bucket.
	for _, tf := range []string{"1d", "1w"} {
		if !f.calledFor(tf) {
			continue
		}
		if got := f.requestFor(tf).FetchLimit; got > 2 {
			t.Errorf("%s was swept with FetchLimit %d — an immaterial gap may repair one straddled bucket, never a range", tf, got)
		}
	}
}

// TestProcessRecoversAClosedBucketStraddledByTheGap pins the boundary the materiality
// fraction cannot see: the SAME short hole is immaterial to a coarse bucket and fatal to
// it, because a gap that spans the bucket's close truncates it permanently (the venue
// never resends a finished bucket) while a gap inside the open bucket is healed by the
// next running total. The fraction is only a valid proxy for "the live stream completes
// the current bucket" when the bucket IS still current.
//
// Measured on G1 with the gate as it was: exactly one pair failed, 2h only — its bucket
// straddled a silence that every faster timeframe had repaired.
func TestProcessRecoversAClosedBucketStraddledByTheGap(t *testing.T) {
	const tf = "1d"
	msPerDay := int64((24 * time.Hour) / time.Millisecond)

	// The discriminator is whether the SILENCE crossed a bucket boundary, so the cases vary
	// that — not the persisted head. Varying the head names a case "spans the close" while
	// moving the one term that has nothing to do with it.
	for _, tc := range []struct {
		name       string
		disconnect time.Duration // offset from now, i.e. how long the silence lasted
		headBack   int64         // buckets behind the open one the persisted head sits
		wantFetch  bool
		straddle   bool // pin the silence across the CURRENT bucket's open, clock-independent
	}{
		{
			// The silence began inside the PREVIOUS bucket: it was still being written when the
			// frames stopped, so its close was spanned and its total is short.
			name:       "silence spans the close of the previous bucket",
			disconnect: -25 * time.Hour, headBack: 1, wantFetch: true,
		},
		{
			// The RACE that made a head-based test wrong: the silence straddles the close AND
			// the head has ALREADY advanced into the new bucket, because the first frame after
			// reconnect folded it. Testing the head instead of the silence skips this one — and
			// that is precisely the bar G1 caught short.
			name:       "silence straddles the close and the head already advanced",
			disconnect: -25 * time.Hour, headBack: 0, wantFetch: true,
		},
		{
			// The silence is wholly inside the OPEN bucket, which the venue's cumulative frames
			// complete on their own — this is the REST saving the gate exists for.
			name:       "silence lies inside the open bucket",
			disconnect: -33 * time.Second, headBack: 0, wantFetch: false,
		},
		{
			// A SHORT silence that straddles the close — the exact shape G1 caught. It is far
			// below the materiality fraction, so the fraction alone can never flag it; only the
			// boundary can. This is the case a fraction-only gate silently skips, and the bar
			// then stays short forever because the venue never resends a finished bucket.
			name:       "short silence straddles the close and is under the fraction",
			disconnect: -10 * time.Second, headBack: 0, wantFetch: true, straddle: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			openBucket := candle.BucketTimestamp(now.UnixMilli(), tf)
			head := openBucket - tc.headBack*msPerDay

			// The straddle case pins BOTH instants around the current bucket's open, so the
			// silence is always 20s long and always crosses the close regardless of when the
			// suite runs. Deriving it from `now` made the case's shape depend on the clock.
			disconnectAt, reconnectAt := now.Add(tc.disconnect), now
			if tc.straddle {
				disconnectAt = time.UnixMilli(openBucket).Add(-10 * time.Second)
				reconnectAt = time.UnixMilli(openBucket).Add(10 * time.Second)
			}

			f := &recordingFiller{}
			m := newMaterialityManager(f, staleProvider{ts: head}, []string{"5m", tf})
			m.process(t.Context(), 0, RecoveryJob{
				ExchangeID: "binance", MarketType: "future", Symbol: "ASTER/USDT",
				DisconnectTime: disconnectAt, ReconnectTime: reconnectAt,
			})

			if got := f.calledFor(tf); got != tc.wantFetch {
				t.Fatalf("%s recovered = %v, want %v (head=%d openBucket=%d disconnect=%v)",
					tf, got, tc.wantFetch, head, openBucket, tc.disconnect)
			}
			if !tc.wantFetch {
				return
			}
			// The repair must NAME the bucket the silence began in. A fetch that starts at the
			// head itself would ask the exchange for a window beginning inside a bucket and come
			// back with the open one only — the no-op class that made the boot settle return
			// success having fetched something else.
			got := f.requestFor(tf)
			wantStart := candle.BucketTimestamp(disconnectAt.UnixMilli(), tf)
			if got.GapStart != wantStart {
				t.Errorf("GapStart = %d, want the straddled bucket %d — the closed bucket must be the first one fetched", got.GapStart, wantStart)
			}
			if got.CurrentBucket != openBucket {
				t.Errorf("CurrentBucket = %d, want %d — the open bucket must be classified, not settled as history", got.CurrentBucket, openBucket)
			}
		})
	}
}
