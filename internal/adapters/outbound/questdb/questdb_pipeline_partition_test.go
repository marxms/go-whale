package questdb

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// flushBatches gives synchronous sweep fixtures one shared shutdown-sized budget.
// Production worker turns and shutdown pass their contexts explicitly.
func (p *Pipeline) flushBatches(entries []candleEntry) {
	ctx, cancel := context.WithTimeout(p.ctx, 30*time.Second)
	defer cancel()
	p.flushBatchesWithContext(ctx, entries)
}

type partitionAuditStore struct {
	countingColdStore
	days       [][]int64
	target     int64
	applied    int64
	captureErr error
	waitErr    error
	calls      []string
}

func (s *partitionAuditStore) WriteBatches(_ context.Context, groups map[candle.SeriesKey][]candle.Candle) error {
	s.calls = append(s.calls, "submit")
	var timestamps []int64
	for _, cs := range groups {
		for _, c := range cs {
			timestamps = append(timestamps, c.Timestamp)
		}
	}
	s.days = append(s.days, timestamps)
	return nil
}

func (s *partitionAuditStore) CaptureApplyTarget(context.Context) (int64, error) {
	s.calls = append(s.calls, "capture")
	return s.target, s.captureErr
}

func (s *partitionAuditStore) WaitApplied(_ context.Context, target int64) error {
	s.calls = append(s.calls, "apply")
	if s.waitErr != nil {
		return s.waitErr
	}
	s.applied = target
	return nil
}

func TestPipelinePartition_RealSweepNeverSubmitsMultipleUTCDays(t *testing.T) {
	s := &partitionAuditStore{target: 4}
	p := NewPipeline(s, 16, time.Hour)
	day := int64(24 * time.Hour / time.Millisecond)
	entries := []candleEntry{
		{Exchange: "binance", MarketType: "future", Symbol: "A/USDT", Timeframe: "1d", Candle: candle.Candle{Timestamp: day + 1, Close: 1}},
		{Exchange: "binance", MarketType: "future", Symbol: "B/USDT", Timeframe: "5m", Candle: candle.Candle{Timestamp: -1, Close: 2}},
		{Exchange: "binance", MarketType: "future", Symbol: "A/USDT", Timeframe: "1d", Candle: candle.Candle{Timestamp: day + 1, Close: 3}},
		{Exchange: "binance", MarketType: "future", Symbol: "C/USDT", Timeframe: "5m", Candle: candle.Candle{Timestamp: 0, Close: 4}},
	}
	p.flushBatches(entries)
	if len(s.days) != 3 {
		t.Fatalf("after real pipeline recombination: got %d submissions for 3 UTC days, want 3; calls=%v", len(s.days), s.calls)
	}
	for _, batch := range s.days {
		for _, ts := range batch {
			if !time.UnixMilli(ts).UTC().Truncate(24 * time.Hour).Equal(time.UnixMilli(batch[0]).UTC().Truncate(24 * time.Hour)) {
				t.Fatalf("multi-DAY transaction: %v", batch)
			}
		}
	}
	want := []string{"submit", "capture", "apply", "submit", "capture", "apply", "submit", "capture", "apply"}
	if len(s.calls) != len(want) {
		t.Fatalf("barrier missing between days: %v", s.calls)
	}
	for i, c := range want {
		if s.calls[i] != c {
			t.Fatalf("operation %d=%q want %q", i, s.calls[i], c)
		}
	}
}

type dayScriptStore struct {
	countingColdStore
	submissions   []map[candle.SeriesKey][]candle.Candle
	submitErrors  []error
	captureErrors []error
	waitErrors    []error
	captures      int
	waits         []int64
}

func (s *dayScriptStore) WriteBatches(_ context.Context, groups map[candle.SeriesKey][]candle.Candle) error {
	s.submissions = append(s.submissions, groups)
	i := len(s.submissions) - 1
	if i < len(s.submitErrors) {
		return s.submitErrors[i]
	}
	return nil
}

func (s *dayScriptStore) CaptureApplyTarget(context.Context) (int64, error) {
	s.captures++
	if s.captures <= len(s.captureErrors) && s.captureErrors[s.captures-1] != nil {
		return 0, s.captureErrors[s.captures-1]
	}
	return int64(100 + s.captures), nil
}

func (s *dayScriptStore) WaitApplied(_ context.Context, target int64) error {
	s.waits = append(s.waits, target)
	i := len(s.waits) - 1
	if i < len(s.waitErrors) {
		return s.waitErrors[i]
	}
	return nil
}

func scriptedEntries() []candleEntry {
	day := int64(24 * time.Hour / time.Millisecond)
	return []candleEntry{
		{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "5m", Candle: candle.Candle{Timestamp: 0, Close: 1}},
		{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "5m", Candle: candle.Candle{Timestamp: day, Close: 2}},
		{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "5m", Candle: candle.Candle{Timestamp: day, Close: 3}},
		{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "5m", Candle: candle.Candle{Timestamp: 2 * day, Close: 4}},
	}
}

func TestPipelinePartition_CaptureFailureResumesWithoutReplay(t *testing.T) {
	s := &dayScriptStore{captureErrors: []error{nil, errors.New("metadata down")}}
	p := NewPipeline(s, 16, time.Hour)
	blocked, pending := 0.0, 0.0
	p.SetMetrics(Metrics{
		ColdPipelineBlocked:     func(v float64) { blocked = v },
		ColdPipelinePendingRows: func(v float64) { pending = v },
	})
	_, _ = blocked, pending
	p.flushBatches(scriptedEntries())
	if len(s.submissions) != 2 || p.Stats().Flushed != 1 || p.Stats().RetryLen != 3 || p.Stats().BlockedPhase != "CAPTURE" {
		t.Fatalf("lost prefix/tail: calls=%d stats=%+v", len(s.submissions), p.Stats())
	}
	p.flushBatches(nil)
	if len(s.submissions) != 3 || p.Stats().Flushed != 4 || p.Stats().Blocked {
		t.Fatalf("acknowledged day replayed: calls=%d stats=%+v", len(s.submissions), p.Stats())
	}
	key := mustSeriesKey("binance", "future", "BTC/USDT", "5m")
	if got := s.submissions[1][key]; len(got) != 2 || got[0].Close != 2 || got[1].Close != 3 {
		t.Fatalf("revision order changed: %+v", got)
	}
}

func TestPipelinePartition_ApplyFailureKeepsFixedTarget(t *testing.T) {
	s := &dayScriptStore{waitErrors: []error{context.DeadlineExceeded, nil}}
	p := NewPipeline(s, 16, time.Hour)
	blocked, pending := 0.0, 0.0
	p.SetMetrics(Metrics{
		ColdPipelineBlocked:     func(v float64) { blocked = v },
		ColdPipelinePendingRows: func(v float64) { pending = v },
	})
	_, _ = blocked, pending
	p.flushBatches(scriptedEntries())
	if len(s.submissions) != 1 || p.Stats().Flushed != 0 || p.Stats().BlockedPhase != "APPLY" {
		t.Fatalf("advanced before apply: calls=%d stats=%+v", len(s.submissions), p.Stats())
	}
	p.flushBatches(nil)
	if !reflect.DeepEqual(s.waits, []int64{101, 101, 102, 103}) || s.captures != 3 || len(s.submissions) != 3 {
		t.Fatalf("moving target or replay: targets=%v captures=%d submits=%d", s.waits, s.captures, len(s.submissions))
	}
}

func TestPipelinePartition_UnsentFailureCanRetryOnlyUnconfirmedDay(t *testing.T) {
	s := &dayScriptStore{submitErrors: []error{nil, errors.New("not sent"), nil}}
	p := NewPipeline(s, 16, time.Hour)
	blocked, pending := 0.0, 0.0
	p.SetMetrics(Metrics{
		ColdPipelineBlocked:     func(v float64) { blocked = v },
		ColdPipelinePendingRows: func(v float64) { pending = v },
	})
	_, _ = blocked, pending
	p.flushBatches(scriptedEntries())
	p.flushBatches(nil)
	if len(s.submissions) != 4 || p.Stats().Flushed != 4 || p.Stats().RetryLen != 0 {
		t.Fatalf("prefix replay/loss: calls=%d stats=%+v", len(s.submissions), p.Stats())
	}
	key := mustSeriesKey("binance", "future", "BTC/USDT", "5m")
	if s.submissions[2][key][0].Timestamp != int64(24*time.Hour/time.Millisecond) {
		t.Fatal("replayed confirmed prefix")
	}
}

func TestPipelinePartition_LostACKFencesWithoutReplay(t *testing.T) {
	s := &dayScriptStore{submitErrors: []error{&candle.ColdWriteUncertainError{Err: errors.New("lost ACK")}}}
	p := NewPipeline(s, 16, time.Hour)
	blocked, pending := 0.0, 0.0
	p.SetMetrics(Metrics{
		ColdPipelineBlocked:     func(v float64) { blocked = v },
		ColdPipelinePendingRows: func(v float64) { pending = v },
	})
	_, _ = blocked, pending
	p.flushBatches(scriptedEntries())
	for range 3 {
		p.flushBatches(nil)
	}
	if len(s.submissions) != 1 || s.captures != 0 || p.Stats().Flushed != 0 || p.Stats().RetryLen != 4 || p.Stats().BlockedPhase != "UNCERTAIN" {
		t.Fatalf("uncertain write replayed/marked successful: calls=%d stats=%+v", len(s.submissions), p.Stats())
	}
}

type blockedApplyStore struct {
	countingColdStore
	entered chan struct{}
	release chan struct{}
}

func (s *blockedApplyStore) CaptureApplyTarget(context.Context) (int64, error) { return 1, nil }
func (s *blockedApplyStore) WaitApplied(ctx context.Context, _ int64) error {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestPipelinePartition_BlockedApplyNeverBlocksLiveEnqueue(t *testing.T) {
	s := &blockedApplyStore{entered: make(chan struct{}, 1), release: make(chan struct{})}
	p := NewPipeline(s, 1, time.Hour)
	p.batchSize = 1
	p.Start()
	first := scriptedEntries()[0]
	p.Enqueue(first.Exchange, first.MarketType, first.Symbol, first.Timeframe, first.Candle)
	select {
	case <-s.entered:
	case <-time.After(time.Second):
		t.Fatal("apply barrier was not entered")
	}
	returned := make(chan struct{})
	go func() {
		for range 3 {
			p.Enqueue(first.Exchange, first.MarketType, first.Symbol, first.Timeframe, first.Candle)
		}
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(time.Second):
		close(s.release)
		p.Stop(t.Context())
		t.Fatal("live enqueue waited for historical WAL")
	}
	if p.Stats().Dropped != 2 {
		close(s.release)
		p.Stop(t.Context())
		t.Fatalf("bounded live overflow changed: %+v", p.Stats())
	}
	close(s.release)
	p.Stop(t.Context())
}

type sweepBudgetStore struct {
	countingColdStore
	cancel    context.CancelFunc
	submits   int
	waits     int
	deadlines []time.Time
}

// The virtual budget expires after each successful apply. Reusing its context
// for another submission simulates an SDK flush started after the sweep deadline.
type workerBudgetStore struct {
	countingColdStore
	used    context.Context
	days    []int64
	applied chan struct{}
}

func (s *workerBudgetStore) WriteBatches(ctx context.Context, groups map[candle.SeriesKey][]candle.Candle) error {
	if ctx == s.used {
		return &candle.ColdWriteUncertainError{Err: context.DeadlineExceeded}
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) < 29*time.Second {
		return errors.New("worker did not start with a fresh budget")
	}
	for _, group := range groups {
		for _, candle := range group {
			s.days = append(s.days, candle.Timestamp)
		}
	}
	return nil
}

func (s *workerBudgetStore) CaptureApplyTarget(context.Context) (int64, error) {
	return int64(len(s.days)), nil
}

func (s *workerBudgetStore) WaitApplied(ctx context.Context, _ int64) error {
	s.used = ctx
	s.applied <- struct{}{}
	return nil
}

func TestPipelinePartition_WorkerRenewsBudgetAndContinuesImmediately(t *testing.T) {
	const count = 100
	s := &workerBudgetStore{applied: make(chan struct{}, count)}
	p := NewPipeline(s, count, time.Hour)
	p.batchSize = count
	for i := range count {
		entry := scriptedEntries()[0]
		entry.Candle.Timestamp = int64(i) * int64(24*time.Hour/time.Millisecond)
		p.Enqueue(entry.Exchange, entry.MarketType, entry.Symbol, entry.Timeframe, entry.Candle)
	}
	p.Start()
	for range count {
		select {
		case <-s.applied:
		case <-time.After(2 * time.Second):
			p.Stop(t.Context())
			t.Fatalf("worker failed to drain healthy DAYs immediately: stats=%+v", p.Stats())
		}
	}
	p.Stop(t.Context())
	if len(s.days) != count || p.Stats().Flushed != count || p.Stats().Blocked {
		t.Fatalf("sweep lost/replayed rows: days=%v stats=%+v", s.days, p.Stats())
	}
	for i, day := range s.days {
		if day != int64(i)*int64(24*time.Hour/time.Millisecond) {
			t.Fatalf("DAY %d out of order or replayed: %v", i, s.days)
		}
	}
}

func (s *sweepBudgetStore) WriteBatches(context.Context, map[candle.SeriesKey][]candle.Candle) error {
	s.submits++
	return nil
}

func (s *sweepBudgetStore) CaptureApplyTarget(context.Context) (int64, error) {
	return int64(s.submits), nil
}

func (s *sweepBudgetStore) WaitApplied(ctx context.Context, _ int64) error {
	s.waits++
	deadline, _ := ctx.Deadline()
	s.deadlines = append(s.deadlines, deadline)
	if s.waits == 2 {
		s.cancel()
		return ctx.Err()
	}
	return nil
}

func TestPipelinePartition_WholeSweepUsesOneCancellableBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s := &sweepBudgetStore{cancel: cancel}
	p := NewPipeline(s, 128, time.Hour)
	entries := []candleEntry{}
	for i := range 100 {
		entry := scriptedEntries()[0]
		entry.Candle.Timestamp = int64(i) * int64(24*time.Hour/time.Millisecond)
		entries = append(entries, entry)
	}
	p.flushBatchesWithContext(ctx, entries)
	if s.submits != 2 || s.waits != 2 || p.Stats().Flushed != 1 || p.Stats().RetryLen != 99 {
		t.Fatalf("cancellation advanced/lost sweep: submits=%d waits=%d stats=%+v", s.submits, s.waits, p.Stats())
	}
	if !s.deadlines[0].Equal(s.deadlines[1]) {
		t.Fatalf("per-DAY deadline was renewed: %v", s.deadlines)
	}
	p.flushBatchesWithContext(ctx, nil)
	if s.submits != 2 || s.waits != 2 {
		t.Fatal("expired total budget submitted or replayed another DAY")
	}
}

type stopVisibilityStore struct {
	countingColdStore
	normalEntered   chan struct{}
	normalCancelled chan struct{}
	waits           int
	submits         int
}

func (s *stopVisibilityStore) WriteBatches(context.Context, map[candle.SeriesKey][]candle.Candle) error {
	s.submits++
	return nil
}
func (s *stopVisibilityStore) CaptureApplyTarget(context.Context) (int64, error) { return 1, nil }
func (s *stopVisibilityStore) WaitApplied(ctx context.Context, _ int64) error {
	s.waits++
	if s.waits == 1 {
		close(s.normalEntered)
		<-ctx.Done()
		close(s.normalCancelled)
		return ctx.Err()
	}
	return errors.New("shutdown metadata unavailable")
}

func TestPipelinePartition_StopCancelsNormalApplyAndDoesNotReplayACK(t *testing.T) {
	s := &stopVisibilityStore{normalEntered: make(chan struct{}), normalCancelled: make(chan struct{})}
	p := NewPipeline(s, 128, time.Hour)
	p.batchSize = 100
	p.Start()
	for i := range 100 {
		entry := scriptedEntries()[0]
		entry.Candle.Timestamp = int64(i) * int64(24*time.Hour/time.Millisecond)
		p.Enqueue(entry.Exchange, entry.MarketType, entry.Symbol, entry.Timeframe, entry.Candle)
	}
	select {
	case <-s.normalEntered:
	case <-time.After(time.Second):
		t.Fatal("normal apply not entered")
	}
	stopped := make(chan struct{})
	go func() { p.Stop(t.Context()); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel normal apply")
	}
	select {
	case <-s.normalCancelled:
	default:
		t.Fatal("normal apply context was not canceled")
	}
	if s.submits != 1 || s.waits != 2 || p.Stats().RetryLen != 100 || p.Stats().BlockedPhase != "APPLY" {
		t.Fatalf("shutdown replay/continued sweep: submits=%d waits=%d stats=%+v", s.submits, s.waits, p.Stats())
	}
}

func TestPipelinePartition_NewRevisionDoesNotMutateACKedDay(t *testing.T) {
	s := &dayScriptStore{captureErrors: []error{errors.New("metadata down")}}
	p := NewPipeline(s, 16, time.Hour)
	blocked, pending := 0.0, 0.0
	p.SetMetrics(Metrics{
		ColdPipelineBlocked:     func(v float64) { blocked = v },
		ColdPipelinePendingRows: func(v float64) { pending = v },
	})
	_, _ = blocked, pending
	first := scriptedEntries()[0]
	p.flushBatches([]candleEntry{first})
	revision := first
	revision.Candle.Close = 9
	p.flushBatches([]candleEntry{revision})
	key := mustSeriesKey(first.Exchange, first.MarketType, first.Symbol, first.Timeframe)
	if len(s.submissions) != 2 || len(s.submissions[0][key]) != 1 || s.submissions[0][key][0].Close != 1 || s.submissions[1][key][0].Close != 9 || p.Stats().Flushed != 2 {
		t.Fatalf("ACKed snapshot mutated or revision reordered: submissions=%v stats=%+v", s.submissions, p.Stats())
	}
}

func TestPipelinePartition_MetricsDistinguishNormalWaitAndFence(t *testing.T) {
	s := &blockedApplyStore{entered: make(chan struct{}, 1), release: make(chan struct{})}
	p := NewPipeline(s, 16, time.Hour)
	blocked, pending := 0.0, 0.0
	p.SetMetrics(Metrics{
		ColdPipelineBlocked:     func(v float64) { blocked = v },
		ColdPipelinePendingRows: func(v float64) { pending = v },
	})
	_, _ = blocked, pending
	done := make(chan struct{})
	go func() { p.flushBatches([]candleEntry{scriptedEntries()[0]}); close(done) }()
	select {
	case <-s.entered:
	case <-time.After(time.Second):
		t.Fatal("normal apply not entered")
	}
	if got := blocked; got != 0 {
		close(s.release)
		<-done
		t.Fatalf("normal wait marked failed: %v", got)
	}
	if got := pending; got != 1 {
		close(s.release)
		<-done
		t.Fatalf("normal pending rows=%v want1", got)
	}
	close(s.release)
	<-done
	failed := &dayScriptStore{captureErrors: []error{errors.New("metadata down")}}
	p2 := NewPipeline(failed, 16, time.Hour)
	blocked2, pending2 := 0.0, 0.0
	p2.SetMetrics(Metrics{
		ColdPipelineBlocked:     func(v float64) { blocked2 = v },
		ColdPipelinePendingRows: func(v float64) { pending2 = v },
	})
	p2.flushBatches([]candleEntry{scriptedEntries()[0]})
	if got := blocked2; got != 1 {
		t.Fatalf("fence hidden from metrics: %v", got)
	}
	if got := pending2; got != 1 {
		t.Fatalf("fenced rows hidden: %v", got)
	}
	p2.flushBatches(nil)
	if got := blocked2; got != 0 {
		t.Fatalf("resolved fence remained failed: %v", got)
	}
	if got := pending2; got != 0 {
		t.Fatalf("confirmed rows still pending: %v", got)
	}
}
