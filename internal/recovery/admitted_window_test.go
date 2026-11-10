package recovery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// windowFiller models the venue returning exactly the requested window.
type windowFiller struct {
	requests    []FillRequest
	failNext    int
	partialNext int
}

func (f *windowFiller) Name() string { return "window" }
func (f *windowFiller) Fill(_ context.Context, _ HistorySource, req FillRequest) (FillResult, error) {
	f.requests = append(f.requests, req)
	if f.failNext > 0 {
		f.failNext--
		return FillResult{}, errors.New("temporary venue failure")
	}
	current := windowCandle(req.CurrentBucket)
	if f.partialNext > 0 {
		f.partialNext--
		return FillResult{Current: &current}, nil
	}
	var history []candle.Candle
	step := int64(candle.ParseTimeframeToSeconds(req.Timeframe)) * 1000
	for at := req.GapStart; at < req.CurrentBucket; at += step {
		history = append(history, windowCandle(at))
	}
	return FillResult{Historical: history, Current: &current}, nil
}

func windowCandle(at int64) candle.Candle {
	return candle.Candle{Timestamp: at, Open: 10, High: 12, Low: 9, Close: 11, Volume: 7, TakerBuyVolume: 3}
}

func admittedWindowFixture(t *testing.T, f *windowFiller) (*Manager, int64, int64) {
	t.Helper()
	store, _ := newValidatorTestStore(t)
	writer := DefaultStorageWriter{Store: store}
	head := candle.BucketTimestamp(time.Now().UnixMilli(), "5m")
	start := head - 3*300_000
	if err := writer.SeedCandlesBatch(context.Background(), "binance", "future", "BTC/USDT", "5m", []candle.Candle{windowCandle(start), windowCandle(head)}); err != nil {
		t.Fatal(err)
	}
	m := NewManager(writer, 1, candle.NewTimeframeRegistry([]string{"5m"}))
	t.Cleanup(m.Stop)
	m.clientPool = fakeClientPool{}
	m.SetFiller(f)
	m.SetTimestampProvider(staleProvider{ts: head})
	// The test drives due jobs synchronously; no scheduler goroutine is needed.
	m.retry.(*MemoryRetryScheduler).pending.Store(true)
	if ok, missing := m.validator.Validate(context.Background(), "binance", "future", "BTC/USDT", "5m", start, candle.BucketTimestamp(time.Now().UnixMilli(), "5m")); ok || missing != 2 {
		t.Fatalf("fixture must contain two interior holes: ok=%v missing=%d", ok, missing)
	}
	return m, start, head
}

func admittedJob(t *testing.T, m *Manager, start, end int64) RecoveryJob {
	t.Helper()
	m.EnqueueBatch("binance", "future", []string{"BTC/USDT"}, time.UnixMilli(start), time.UnixMilli(end))
	select {
	case job := <-m.queue.Read():
		// Scheduling delay has its own existing tests. Run the now-due worker here.
		job.Delay = 0
		return job
	default:
		t.Fatal("admitted job missing")
		return RecoveryJob{}
	}
}

func assertWindowRepaired(t *testing.T, m *Manager, f *windowFiller, start, head int64) {
	t.Helper()
	if len(f.requests) == 0 {
		t.Fatal("no request reached the filler")
	}
	req := f.requests[len(f.requests)-1]
	completedBucket := candle.BucketTimestamp(time.Now().UnixMilli(), "5m")
	wantLimit := int((req.CurrentBucket-start)/300_000) + 2
	if req.ExchangeID != "binance" || req.MarketType != "future" || req.Symbol != "BTC/USDT" || req.Timeframe != "5m" || req.GapStart != start || req.SinceMs != start || req.CurrentBucket < head || req.CurrentBucket > completedBucket || req.CurrentBucket%300_000 != 0 || req.FetchLimit != wantLimit {
		t.Fatalf("admitted window was narrowed: %+v, want start=%d current in [%d,%d] limit=%d", req, start, head, completedBucket, wantLimit)
	}
	got, err := m.writer.GetCandlesByScoreRange(context.Background(), "binance", "future", "BTC/USDT", "5m", start+300_000, head-300_000)
	if err != nil || len(got) != 2 {
		t.Fatalf("interior buckets not persisted: %+v err=%v", got, err)
	}
	for i, c := range got {
		if c.Timestamp != start+int64(i+1)*300_000 || c.Open != 10 || c.High != 12 || c.Low != 9 || c.Close != 11 || c.Volume != 7 || c.TakerBuyVolume != 3 || c.Source != "gap-recovery" || c.FetchedAt <= 0 || c.RequestedAt <= 0 {
			t.Fatalf("wrong repaired bucket: %+v", c)
		}
	}
	if ok, missing := m.validator.Validate(context.Background(), "binance", "future", "BTC/USDT", "5m", start, candle.BucketTimestamp(time.Now().UnixMilli(), "5m")); !ok || missing != 0 {
		t.Fatalf("window still has holes: ok=%v missing=%d", ok, missing)
	}
}

func TestAdmittedWindowRepairsInteriorAfterLiveHeadAdvances(t *testing.T) {
	f := &windowFiller{}
	m, start, head := admittedWindowFixture(t, f)
	m.process(t.Context(), 0, admittedJob(t, m, start, head))
	assertWindowRepaired(t, m, f, start, head)
}

func TestAdmittedWindowSurvivesDeferredRetry(t *testing.T) {
	for _, cause := range []string{"fill", "validation", "budget", "circuit"} {
		t.Run(cause, func(t *testing.T) {
			f := &windowFiller{}
			m, start, head := admittedWindowFixture(t, f)
			job := admittedJob(t, m, start, head)
			job.RetryAttempt = 4
			switch cause {
			case "fill":
				f.failNext = 1
			case "validation":
				f.partialNext = 1
			case "budget":
				m.budget = newRESTBudget(1, 1)
				m.budget.tokens = 0
			case "circuit":
				cb := m.cb.(*SimpleCircuitBreaker)
				cb.openUntil["binance:future"] = time.Now().Add(time.Hour)
			}
			m.process(t.Context(), 0, job)
			if ok, missing := m.validator.Validate(context.Background(), "binance", "future", "BTC/USDT", "5m", start, candle.BucketTimestamp(time.Now().UnixMilli(), "5m")); ok || missing != 2 {
				t.Fatalf("failed attempt unexpectedly repaired window: ok=%v missing=%d", ok, missing)
			}
			m.budget.tokens = 1
			m.cb.RecordSuccess("binance:future")
			m.retry.(*MemoryRetryScheduler).processReadyOnce(time.Now().Add(RetryMaxDelay + time.Second))
			select {
			case retry := <-m.queue.Read():
				if retry.RetryAttempt != 5 {
					t.Fatalf("retry budget reset: %+v", retry)
				}
				m.process(t.Context(), 0, retry)
			default:
				t.Fatal("failed attempt did not return to worker queue")
			}
			assertWindowRepaired(t, m, f, start, head)
		})
	}
}

func TestAdmittedWindowPreservesMateriality(t *testing.T) {
	f := &recordingFiller{}
	m := newMaterialityManager(f, nowProvider{}, []string{"5m", "1h", "1d", "1w"})
	t.Cleanup(m.Stop)
	// Mid-hour: a 33 s window must not straddle an hour/day/week close. If it does,
	// the gate correctly repairs that one bucket, so a fixed "only 5m" expectation
	// would flake once an hour.
	now := time.Now().Truncate(time.Hour).Add(30 * time.Minute)
	m.EnqueueBatch("binance", "future", []string{"BTC/USDT"}, now.Add(-33*time.Second), now)
	job := <-m.queue.Read()
	job.Delay = 0
	m.process(t.Context(), 0, job)
	if !f.calledFor("5m") || f.calledFor("1h") || f.calledFor("1d") || f.calledFor("1w") {
		t.Fatalf("admitted 33s gap must recover only 5m: %+v", f.filled)
	}
}

func TestAdmittedWindowSurvivesPendingReplacement(t *testing.T) {
	f := &windowFiller{}
	m, start, head := admittedWindowFixture(t, f)
	m.queue = NewCoalescingQueue(1, 2)
	m.queue.Enqueue(RecoveryJob{Symbol: "occupies-channel"})
	m.EnqueueBatch("binance", "future", []string{"BTC/USDT"}, time.UnixMilli(start), time.UnixMilli(head))
	m.EnqueueBatch("binance", "future", []string{"BTC/USDT"}, time.UnixMilli(start+300_000), time.UnixMilli(head))
	job, ok := m.queue.Pop()
	if !ok || job.Symbol != "BTC/USDT" {
		t.Fatalf("pending recovery missing: %+v", job)
	}
	// A failed Promote can put the older job back after newer admission.
	m.queue.Enqueue(job)
	newer, ok := m.queue.Pop()
	if !ok {
		t.Fatal("pending job missing before push front")
	}
	m.EnqueueBatch("binance", "future", []string{"BTC/USDT"}, time.UnixMilli(start+600_000), time.UnixMilli(head))
	m.queue.PushFront(newer)
	job, ok = m.queue.Pop()
	if !ok || m.queue.PendingLen() != 0 {
		t.Fatal("coalescing must leave exactly one pending job")
	}
	job.Delay = 0
	m.process(t.Context(), 0, job)
	assertWindowRepaired(t, m, f, start, head)
}

func TestAdmittedWindowRetryCoalescesWithoutLosingEarliestWindow(t *testing.T) {
	f := &windowFiller{failNext: 2}
	m, start, head := admittedWindowFixture(t, f)
	first := admittedJob(t, m, start, head)
	first.RetryAttempt = 3
	m.process(t.Context(), 0, first)
	second := admittedJob(t, m, start+300_000, head)
	second.RetryAttempt = 5
	m.process(t.Context(), 0, second)
	m.retry.(*MemoryRetryScheduler).processReadyOnce(time.Now().Add(RetryMaxDelay + time.Second))
	select {
	case job := <-m.queue.Read():
		if job.RetryAttempt != 6 || m.queue.Len() != 0 {
			t.Fatalf("expected one bulk replay preserving max attempt: %+v queue=%d", job, m.queue.Len())
		}
		m.process(t.Context(), 0, job)
	default:
		t.Fatal("retry missing")
	}
	assertWindowRepaired(t, m, f, start, head)
}

func TestAdmittedWindowUsesEarlierHeadAndMissingProvider(t *testing.T) {
	for _, providerPresent := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing", true: "earlier"}[providerPresent], func(t *testing.T) {
			m, start, head := admittedWindowFixture(t, &windowFiller{})
			f := &capturingFiller{}
			m.SetFiller(f)
			wantStart := start
			m.SetTimestampProvider(nil)
			if providerPresent {
				wantStart -= 300_000
				m.SetTimestampProvider(staleProvider{ts: wantStart})
			}
			m.process(t.Context(), 0, admittedJob(t, m, start, head))
			if len(f.got) != 1 || f.got[0].SinceMs != wantStart || f.got[0].GapStart != wantStart || f.got[0].CurrentBucket < head || f.got[0].CurrentBucket > candle.BucketTimestamp(time.Now().UnixMilli(), "5m") || f.got[0].CurrentBucket%300_000 != 0 || f.got[0].FetchLimit != int((f.got[0].CurrentBucket-wantStart)/300_000)+2 {
				t.Fatalf("window must cover both admitted and older persisted history: %+v want=%d", f.got, wantStart)
			}
		})
	}
}

func TestAdmittedWindowRetryKeepsTargetedIdentity(t *testing.T) {
	m, start, head := admittedWindowFixture(t, &windowFiller{})
	f := &capturingFiller{}
	m.SetFiller(f)
	bulk := admittedJob(t, m, start, head)
	m.scheduleRetry(bulk, "5m", start, 5)
	for _, target := range []int64{head - 5*300_000, head - 6*300_000} {
		m.retry.Schedule(BackfillRetryJob{
			ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT",
			TargetedTimeframe: "5m", TargetedBucket: target,
		})
	}
	m.retry.(*MemoryRetryScheduler).processReadyOnce(time.Now().Add(RetryMaxDelay + time.Second))
	if m.queue.Len() != 1 || len(m.queue.ReadPriority()) != 2 {
		t.Fatalf("bulk and two distinct targeted intents must survive: bulk=%d targeted=%d", m.queue.Len(), len(m.queue.ReadPriority()))
	}
	for i := 0; i < 2; i++ {
		m.process(t.Context(), 0, <-m.queue.ReadPriority())
	}
	m.process(t.Context(), 0, <-m.queue.Read())
	if len(f.got) != 3 {
		t.Fatalf("three named windows must reach filler: %+v", f.got)
	}
	for i, target := range []int64{head - 5*300_000, head - 6*300_000} {
		if f.got[i].GapStart != target || f.got[i].SinceMs != target || f.got[i].FetchLimit != 2 || f.got[i].Timeframe != "5m" {
			t.Fatalf("targeted identity lost: %+v want=%d", f.got[i], target)
		}
	}
	if f.got[2].GapStart != start || f.got[2].SinceMs != start || f.got[2].CurrentBucket < head || f.got[2].CurrentBucket > candle.BucketTimestamp(time.Now().UnixMilli(), "5m") || f.got[2].CurrentBucket%300_000 != 0 || f.got[2].FetchLimit != int((f.got[2].CurrentBucket-start)/300_000)+2 {
		t.Fatalf("bulk window lost: %+v", f.got[2])
	}
}

func TestAdmittedWindowCoalescingPreservesOriginalMateriality(t *testing.T) {
	f := &capturingFiller{}
	m := newMaterialityManager(f, nowProvider{}, []string{"5m", "1h", "1d", "1w"})
	t.Cleanup(m.Stop)
	m.retry.(*MemoryRetryScheduler).pending.Store(true)
	// Mid-hour: see TestAdmittedWindowPreservesMateriality.
	now := time.Now().Truncate(time.Hour).Add(30 * time.Minute)
	for _, end := range []time.Time{now.Add(-12 * time.Hour), now} {
		m.EnqueueBatch("binance", "future", []string{"BTC/USDT"}, end.Add(-33*time.Second), end)
		job := <-m.queue.Read()
		m.scheduleRetry(job, "5m", 0, 0)
	}
	// Schedule stamps NextRetryAt from the real clock, so due-ness must be measured
	// from it too; `now` is the mid-hour anchor and can already be in the past.
	m.retry.(*MemoryRetryScheduler).processReadyOnce(time.Now().Add(RetryMaxDelay + time.Second))
	m.process(t.Context(), 0, <-m.queue.Read())
	if len(f.got) != 1 || f.got[0].Timeframe != "5m" || f.got[0].SinceMs != now.Add(-12*time.Hour-33*time.Second).UnixMilli() {
		t.Fatalf("waiting/coalescing must not make 33s gaps material to slow frames: %+v", f.got)
	}
}

func TestAdmittedWindowCoalescingPreservesManualBackfill(t *testing.T) {
	for _, manualFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "manual-replaces-batch", true: "batch-replaces-manual"}[manualFirst], func(t *testing.T) {
			f := &capturingFiller{}
			m := newMaterialityManager(f, nowProvider{}, []string{"5m", "1h", "1d"})
			t.Cleanup(m.Stop)
			m.queue = NewCoalescingQueue(1, 2)
			m.queue.Enqueue(RecoveryJob{Symbol: "occupies-channel"})
			now := time.Now()
			start := now.Add(-24 * time.Hour).UnixMilli()
			manual := func() {
				m.ForceBackfill(BackfillRequest{ExchangeID: "binance", MarketType: "future", Symbols: []string{"BTC/USDT"}, SinceMs: start})
			}
			batch := func() {
				m.EnqueueBatch("binance", "future", []string{"BTC/USDT"}, now.Add(-33*time.Second), now)
			}
			if manualFirst {
				manual()
				batch()
			} else {
				batch()
				manual()
			}
			job, ok := m.queue.Pop()
			if !ok || m.queue.PendingLen() != 0 {
				t.Fatal("expected one coalesced recovery")
			}
			job.Delay = 0
			m.process(t.Context(), 0, job)
			if len(f.got) != 3 {
				t.Fatalf("manual day-long window must still recover 5m/1h/1d: %+v", f.got)
			}
			for _, req := range f.got {
				if req.SinceMs != start || req.GapStart != candle.BucketTimestamp(start, req.Timeframe) || req.FetchLimit < 2 || req.FetchLimit > 1000 {
					t.Fatalf("manual intent lost or cap bypassed: %+v want=%d", req, start)
				}
			}
		})
	}
}
