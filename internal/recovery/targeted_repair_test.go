package recovery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/market"
)

// capturingFiller records the FillRequest it was handed, which is the only place the decision
// under test becomes observable.
type capturingFiller struct{ got []FillRequest }

func (f *capturingFiller) Name() string { return "capturing" }
func (f *capturingFiller) Fill(_ context.Context, _ HistorySource, req FillRequest) (FillResult, error) {
	f.got = append(f.got, req)
	return FillResult{}, nil
}

type targetedRetryRecorder struct{ jobs []BackfillRetryJob }

func (r *targetedRetryRecorder) Schedule(job BackfillRetryJob)     { r.jobs = append(r.jobs, job) }
func (*targetedRetryRecorder) SetReenqueue(func(BackfillRetryJob)) {}
func (*targetedRetryRecorder) Start(context.Context)               {}
func (*targetedRetryRecorder) Stop()                               {}

type targetedWriter struct {
	*recordingWriter
	target    int64
	persisted bool
	dropWrite bool
	keepOld   bool
	stored    candle.Candle
	writeErr  error
	queries   [][2]int64
}

type boundedSeedWriter struct {
	*recordingWriter
	wrote []candle.Candle
}

func (w *boundedSeedWriter) SeedCandlesBatch(_ context.Context, _, _, _, _ string, cs []candle.Candle) error {
	w.wrote = append(w.wrote, cs...)
	return nil
}

func (w *targetedWriter) SeedCandlesBatch(_ context.Context, _, _, _, _ string, cs []candle.Candle) error {
	if len(cs) > 0 && w.writeErr == nil && !w.dropWrite {
		w.persisted = true
		if !w.keepOld {
			w.stored = cs[len(cs)-1]
		}
	}
	return w.writeErr
}

func (w *targetedWriter) GetCandlesByScoreRange(_ context.Context, _, _, _, _ string, from, to int64) ([]candle.Candle, error) {
	w.queries = append(w.queries, [2]int64{from, to})
	if w.persisted && from <= w.target && w.target <= to {
		return []candle.Candle{w.stored}, nil
	}
	return nil, nil
}

func TestProcess_TargetedRepairOnlyRequestsNamedTimeframeAndBucket(t *testing.T) {
	const tf = "5m"
	now := time.Now()
	target := candle.BucketTimestamp(now.Add(-80*time.Minute).UnixMilli(), tf)
	f := &capturingFiller{}
	m := newTestManager(f, newRecordingWriter(), staleProvider{ts: candle.BucketTimestamp(now.UnixMilli(), tf)})
	m.tfReg = candle.NewTimeframeRegistry([]string{"1m", tf, "1h"})
	m.process(t.Context(), 0, RecoveryJob{
		ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT",
		TargetedTimeframe: tf, TargetedBucket: target,
		DisconnectTime: time.UnixMilli(target), ReconnectTime: time.UnixMilli(target),
	})
	if len(f.got) != 1 {
		t.Fatalf("targeted repair issued %d fills, want only %s", len(f.got), tf)
	}
	req := f.got[0]
	if req.Timeframe != tf || req.GapStart != target || req.UntilMs != target {
		t.Errorf("targeted request = %+v, want timeframe %s and exact interval [%d,%d]", req, tf, target, target)
	}
	if req.CurrentBucket != candle.BucketTimestamp(time.Now().UnixMilli(), tf) {
		t.Errorf("CurrentBucket = %d; open-bucket classification must still use the real current bucket", req.CurrentBucket)
	}
}

func TestProcess_TargetedHistoryDoesNotTripFreshnessBreaker(t *testing.T) {
	const tf = "1m"
	target := candle.BucketTimestamp(time.Now().Add(-time.Hour).UnixMilli(), tf)
	c := candle.Candle{Timestamp: target, Open: 1, High: 1, Low: 1, Close: 1, Volume: 1}
	m := newTestManager(fakeFiller{historical: []candle.Candle{c}}, newRecordingWriter(), nil)
	m.retry = &targetedRetryRecorder{}
	m.breaker = newNoProgressBreaker(2, time.Hour, 0)
	job := RecoveryJob{
		ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT",
		TargetedTimeframe: tf, TargetedBucket: target,
	}
	for i := 0; i < 4; i++ {
		m.process(t.Context(), 0, job)
	}
	key := "binance:future|" + market.NormalizeSymbol(job.Symbol)
	if m.breaker.isSuspended(key) {
		t.Fatal("valid historical repairs cannot suspend the symbol for lack of live freshness")
	}
	if got := m.Stats().Processed; got != 4 {
		t.Errorf("processed %d targeted jobs, want 4; the freshness breaker may have skipped later targets", got)
	}
}

func TestProcess_TargetedDoesNotSeedOpenBucketFromFiller(t *testing.T) {
	const tf = "1m"
	open := candle.BucketTimestamp(time.Now().UnixMilli(), tf)
	target := open - 60_000
	historical := candle.Candle{Timestamp: target, Open: 1, High: 1, Low: 1, Close: 1, Volume: 1}
	previous := candle.Candle{Timestamp: target - 60_000, Open: 3, High: 3, Low: 3, Close: 3, Volume: 3}
	later := candle.Candle{Timestamp: open + 60_000, Open: 4, High: 4, Low: 4, Close: 4, Volume: 4}
	current := candle.Candle{Timestamp: open, Open: 2, High: 2, Low: 2, Close: 2, Volume: 2}
	w := &boundedSeedWriter{recordingWriter: newRecordingWriter()}
	m := newTestManager(fakeFiller{historical: []candle.Candle{previous, historical, later}, current: &current}, w, nil)
	m.retry = &targetedRetryRecorder{}
	m.process(t.Context(), 0, RecoveryJob{
		ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT",
		TargetedTimeframe: tf, TargetedBucket: target,
	})
	if len(w.wrote) != 1 || w.wrote[0].Timestamp != target {
		t.Errorf("historical writes = %+v, want only the named bucket %d", w.wrote, target)
	}
	if len(w.seededCur) != 0 {
		t.Errorf("targeted repair wrote open buckets: %+v", w.seededCur)
	}
}

func TestProcess_TargetedValidationOnlyChecksTarget(t *testing.T) {
	target := candle.BucketTimestamp(time.Now().Add(-time.Hour).UnixMilli(), "1m")
	c := candle.Candle{Timestamp: target, Open: 1, High: 1, Low: 1, Close: 1, Volume: 1}
	for _, tc := range []struct {
		name        string
		persisted   bool
		dropWrite   bool
		emptyResult bool
		writeErr    error
		wantRetry   bool
	}{
		{name: "target stored despite unrelated suffix hole", persisted: true},
		{name: "target missing after successful fetch", dropWrite: true, wantRetry: true},
		{name: "exchange returned no target", emptyResult: true, wantRetry: true},
		{name: "old Redis value alone is not a completed fetch", persisted: true, emptyResult: true, wantRetry: true},
		{name: "cold write failed although Redis exposes target", persisted: true, writeErr: errors.New("cold write failed"), wantRetry: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := &targetedWriter{recordingWriter: newRecordingWriter(), target: target, persisted: tc.persisted, stored: c, dropWrite: tc.dropWrite, writeErr: tc.writeErr}
			var historical []candle.Candle
			if !tc.emptyResult {
				historical = []candle.Candle{c}
			}
			m := newTestManager(fakeFiller{historical: historical}, w, nil)
			m.validator = suffixHoleValidator{}
			retries := &targetedRetryRecorder{}
			m.retry = retries
			m.process(t.Context(), 0, RecoveryJob{ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT", TargetedTimeframe: "1m", TargetedBucket: target})
			if got := len(retries.jobs); (got != 0) != tc.wantRetry {
				t.Errorf("retry count = %d, want retry=%v", got, tc.wantRetry)
			}
			if len(w.queries) != 1 || w.queries[0] != [2]int64{target, target} {
				t.Errorf("targeted validation queries = %v, want exact range [%d,%d]", w.queries, target, target)
			}
		})
	}
}

type suffixHoleValidator struct{}

func (suffixHoleValidator) Validate(context.Context, string, string, string, string, int64, int64) (bool, int) {
	return false, 1
}

type targetedResultFiller struct{ result FillResult }

func (targetedResultFiller) Name() string { return "targeted-result" }
func (f targetedResultFiller) Fill(context.Context, HistorySource, FillRequest) (FillResult, error) {
	return f.result, nil
}

func TestProcess_TargetedReadbackMatchesAuthoritativeValues(t *testing.T) {
	const tf = "1m"
	target := candle.BucketTimestamp(time.Now().Add(-time.Hour).UnixMilli(), tf)
	incoming := candle.Candle{
		Timestamp: target, Open: 10, High: 12, Low: 9, Close: 11, Volume: 8,
		TakerBuyVolume: 3, TakerSellVolume: 2,
	}
	oldVolume := incoming
	oldVolume.Volume = 7
	oldTaker := incoming
	oldTaker.TakerSellVolume = 1
	oldHigh := incoming
	oldHigh.High = 13
	basicIncoming := incoming
	basicIncoming.TakerBuyVolume, basicIncoming.TakerSellVolume = 0, 0
	zeroBuySDK := incoming
	zeroBuySDK.TakerBuyVolume, zeroBuySDK.TakerSellVolume = 0, incoming.Volume
	oldNonzeroTaker := zeroBuySDK
	oldNonzeroTaker.TakerBuyVolume = 1
	oldNonzeroTaker.TakerSellVolume = incoming.Volume - 1

	for _, tc := range []struct {
		name          string
		incoming      candle.Candle
		stored        candle.Candle
		takerMeasured bool
		wantRetry     bool
	}{
		{name: "cold old volume differs", incoming: incoming, stored: oldVolume, takerMeasured: true, wantRetry: true},
		{name: "cold already has exact truth", incoming: incoming, stored: incoming, takerMeasured: true},
		{name: "SDK measured zero taker buy", incoming: zeroBuySDK, stored: oldNonzeroTaker, takerMeasured: true, wantRetry: true},
		{name: "Basic preserves richer taker", incoming: basicIncoming, stored: oldNonzeroTaker},
		{name: "Basic ratcheted high still differs", incoming: basicIncoming, stored: oldHigh, wantRetry: true},
		{name: "SDK taker differs", incoming: incoming, stored: oldTaker, takerMeasured: true, wantRetry: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := &targetedWriter{recordingWriter: newRecordingWriter(), target: target, persisted: true, keepOld: true, stored: tc.stored}
			f := targetedResultFiller{result: FillResult{
				Historical: []candle.Candle{tc.incoming}, TakerVolumesAuthoritative: tc.takerMeasured,
			}}
			m := newTestManager(f, w, nil)
			m.validator = suffixHoleValidator{}
			retries := &targetedRetryRecorder{}
			m.retry = retries
			m.process(t.Context(), 0, RecoveryJob{
				ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT",
				TargetedTimeframe: tf, TargetedBucket: target,
			})
			if got := len(retries.jobs); (got != 0) != tc.wantRetry {
				t.Errorf("retry count = %d, want retry=%v for readback %+v and incoming %+v", got, tc.wantRetry, tc.stored, tc.incoming)
			}
		})
	}
}

// TestProcess_TargetedBucketReachesTheFiller is the end-to-end guard for a repair that
// reported success while fetching something else entirely.
//
// # THE DEFECT
//
// The boot-bucket settle enqueued a job naming ONE closed bucket. The worker ignored it: with
// a timestamp provider present it derives the window from the ring HEAD, which is at or past
// the present, so the fetch covered [head, now] and the named bucket was never requested. The
// job completed, reported success, and the bucket stayed 6,02% short half an hour after it
// closed — above the documented cap.
//
// Nothing caught it because no test drove a job through `process` and inspected the window
// the filler actually received. The settle had a unit test proving it ENQUEUED, and that was
// mistaken for proof that it REPAIRED.
func TestProcess_TargetedBucketReachesTheFiller(t *testing.T) {
	const tf = "1m"
	step := int64(60_000)
	// The ring head is NOW; the bucket we want repaired is well behind it. This is exactly the
	// shape the derived window cannot express.
	head := time.Now().UnixMilli() / step * step
	target := head - 10*step

	f := &capturingFiller{}
	m := newTestManager(f, newRecordingWriter(), staleProvider{ts: head})

	m.process(t.Context(), 0, RecoveryJob{
		ExchangeID: "binance", MarketType: "future", Symbol: "DOGE/USDT",
		TargetedTimeframe: tf, TargetedBucket: target,
		DisconnectTime: time.UnixMilli(target), ReconnectTime: time.UnixMilli(target),
	})

	if len(f.got) == 0 {
		t.Fatal("the filler was never called — a targeted repair that issues no request is the " +
			"no-op this test exists to prevent")
	}
	req := f.got[0]
	if req.GapStart != candle.BucketTimestamp(target, tf) {
		t.Errorf("GapStart = %d, want %d (the bucket that was NAMED). The window was re-derived "+
			"from the ring head instead, which is at or past the present and can never reach a "+
			"bucket that already closed — the job then fetches something else and reports "+
			"success", req.GapStart, candle.BucketTimestamp(target, tf))
	}
	if req.SinceMs != candle.BucketTimestamp(target, tf) {
		t.Errorf("SinceMs = %d, want %d", req.SinceMs, candle.BucketTimestamp(target, tf))
	}
}

// TestProcess_OrdinaryJobNeverSweepsHistory is the other half: a catch-up job must never turn
// into a range fetch. Making every job targeted would re-fetch history on every reconnect, which
// is the REST storm the materiality gate exists to prevent.
//
// A silence that CROSSES a bucket close makes that bucket suspect: such a job requests EXACTLY that
// bucket, not the ring head, which is already past it and would only return the open bucket. The
// property this test protects is not "every job derives from the head" but "no job sweeps history".
// It is asserted here without depending on the clock phase — a job that straddled requests one bucket
// (fetchLimit 2); one that did not derive from the head as before.
func TestProcess_OrdinaryJobNeverSweepsHistory(t *testing.T) {
	const tf = "1m"
	step := int64(60_000)
	head := time.Now().UnixMilli()/step*step - 5*step
	disconnect := time.Now().Add(-time.Hour)
	straddled := candle.BucketTimestamp(disconnect.UnixMilli(), tf)
	headBucket := candle.BucketTimestamp(head, tf)

	f := &capturingFiller{}
	m := newTestManager(f, newRecordingWriter(), staleProvider{ts: head})

	m.process(t.Context(), 0, RecoveryJob{
		ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT",
		DisconnectTime: disconnect, ReconnectTime: time.Now(),
	})

	if len(f.got) == 0 {
		t.Fatal("ordinary catch-up job issued no request")
	}
	req := f.got[0]
	if req.UntilMs != 0 {
		t.Errorf("ordinary recovery UntilMs = %d, want zero so it covers the live horizon", req.UntilMs)
	}
	if req.FetchLimit > 2 && req.GapStart != headBucket {
		t.Fatalf("FetchLimit = %d and GapStart = %d — a job that is not repairing one straddled "+
			"bucket must derive its window from the ring head %d; anything else re-fetches history",
			req.FetchLimit, req.GapStart, headBucket)
	}
	// Straddling is repaired where the silence began; it never reaches back further than that.
	if req.FetchLimit == 2 && req.GapStart != straddled {
		t.Errorf("GapStart = %d, want the bucket the silence began in %d — the widened window "+
			"must be the damaged bucket, not an arbitrary earlier one", req.GapStart, straddled)
	}
}
