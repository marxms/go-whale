package questdb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// candleEntry holds a closed candle with its routing metadata.
type candleEntry struct {
	Exchange   string
	MarketType string
	Symbol     string
	Timeframe  string
	Candle     candle.Candle
	// Only blocking historical writes may be compared with an applied cold row.
	coldNoopCandidate bool
	// The original candle remains here until its DAY is confirmed. This flag is
	// reversible if another writer advances the candle WAL before submission.
	coldNoopSkip bool
}

type admissionTimer interface {
	C() <-chan time.Time
	Stop() bool
}

type admissionClock interface {
	Now() time.Time
	NewTimer(time.Duration) admissionTimer
}

type (
	realAdmissionClock struct{}
	realAdmissionTimer struct{ *time.Timer }
)

func (realAdmissionClock) Now() time.Time { return time.Now() }
func (realAdmissionClock) NewTimer(d time.Duration) admissionTimer {
	return realAdmissionTimer{time.NewTimer(d)}
}
func (t realAdmissionTimer) C() <-chan time.Time { return t.Timer.C }

// PipelineStats exposes operational metrics for the QuestDB pipeline.
type PipelineStats struct {
	Flushed      int64  `json:"flushed"`
	Dropped      int64  `json:"dropped"`
	Pending      int    `json:"pending"`
	Errors       int64  `json:"errors"`
	Retried      int64  `json:"retried"`
	RetryLen     int    `json:"retry_queue_len"`
	Blocked      bool   `json:"blocked"`
	BlockedPhase string `json:"blocked_phase,omitempty"`
	PendingDays  int    `json:"pending_days"`
}

// Pipeline accumulates closed candles and flushes them to QuestDB in batches.
// Enqueue is non-blocking: if the buffer is full, candles are dropped (never block trade processing).
type Pipeline struct {
	log       *slog.Logger
	store     candle.ColdStore
	buffer    chan candleEntry
	batchSize int
	interval  time.Duration
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup

	// Pending work remains owned by the single flush loop until its DAY is applied.
	// RAM ownership is unchanged: this is not a durable outbox.
	pendingDays      []*coldDayWrite
	stateMu          sync.Mutex
	pendingRows      int
	pendingCount     int
	blockedPhase     string
	changed          chan struct{}
	waiters          int
	stallSince       time.Time
	admissionTimeout time.Duration
	admissionClock   admissionClock
	// WAL target at the last protected no-op decision or confirmed DAY.
	noopExpectedTarget int64
	noopExpected       bool

	flushed atomic.Int64
	dropped atomic.Int64
	errors  atomic.Int64
	retried atomic.Int64

	// metrics is the injected observability port. Zero value = safe no-op.
	metrics Metrics
}

// SetMetrics injects the observability port. The composition root calls it before Start.
func (p *Pipeline) SetMetrics(m Metrics) { p.metrics = m }

// NewPipeline creates a pipeline that batches closed candles for cold-store writes.
// bufferCap controls the channel capacity; flushInterval is the maximum delay between flushes.
func NewPipeline(store candle.ColdStore, bufferCap int, flushInterval time.Duration, logs ...*slog.Logger) *Pipeline {
	if bufferCap <= 0 {
		// NOT RETENTION: the capacity of the in-memory channel between the producer and the
		// flush, not the depth of a stored series.
		bufferCap = 16384
	}
	if flushInterval <= 0 {
		flushInterval = 5 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Pipeline{
		log:    logger.OrDiscard(logs...),
		store:  store,
		buffer: make(chan candleEntry, bufferCap),
		// Keep series aggregated while the DAY boundary limits non-preemptible WAL work.
		// The interval bounds collection latency, not the database's apply duration.
		batchSize:        8192,
		interval:         flushInterval,
		ctx:              ctx,
		cancel:           cancel,
		changed:          make(chan struct{}),
		admissionTimeout: 30 * time.Second,
		admissionClock:   realAdmissionClock{},
	}
}

// Enqueue adds a closed candle to the pipeline buffer.
// Non-blocking: if the buffer is full, the candle is dropped and a warning is logged.
func (p *Pipeline) Enqueue(exchange, marketType, symbol, timeframe string, c candle.Candle) {
	entry := candleEntry{
		Exchange:   exchange,
		MarketType: marketType,
		Symbol:     symbol,
		Timeframe:  timeframe,
		Candle:     c,
	}

	select {
	case p.buffer <- entry:
	default:
		dropped := p.dropped.Add(1)
		if dropped%100 == 1 {
			logger.OrDiscard(p.log).Warn("questdb pipeline: buffer full; dropped candle",
				"exchange", exchange,
				"market_type", marketType,
				"symbol", symbol,
				"timeframe", timeframe,
				"total_dropped", dropped)
		}
	}
}

// EnqueueClosedBlocking adds a CLOSED candle to the pipeline buffer, BLOCKING with
// backpressure until there is room (or ctx / pipeline shutdown). Used by the seed /
// backfill path — which runs in a worker and may block — so a mass re-add cannot
// silently DROP the authoritative closed candles the non-blocking Enqueue would. All
// cold writes still serialize through the single flush loop (single-writer invariant).
func (p *Pipeline) EnqueueClosedBlocking(ctx context.Context, exchange, marketType, symbol, timeframe string, c candle.Candle) error {
	entry := candleEntry{
		Exchange:          exchange,
		MarketType:        marketType,
		Symbol:            symbol,
		Timeframe:         timeframe,
		Candle:            c,
		coldNoopCandidate: true,
	}
	registered := false
	defer func() {
		if !registered {
			return
		}
		p.stateMu.Lock()
		p.waiters--
		if p.waiters == 0 {
			p.stallSince = time.Time{}
		}
		p.stateMu.Unlock()
	}()
	for {
		p.stateMu.Lock()
		if err := ctx.Err(); err != nil {
			p.stateMu.Unlock()
			return err
		}
		if err := p.ctx.Err(); err != nil {
			p.stateMu.Unlock()
			return err
		}
		if p.blockedPhase != "" {
			phase := p.blockedPhase
			p.stateMu.Unlock()
			return fmt.Errorf("cold pipeline fenced: %s", phase)
		}
		now := p.admissionClock.Now()
		if !p.stallSince.IsZero() && now.Sub(p.stallSince) >= p.admissionTimeout {
			p.stateMu.Unlock()
			return fmt.Errorf("cold pipeline admission made no confirmed progress: %w", context.DeadlineExceeded)
		}
		select {
		case p.buffer <- entry:
			p.stateMu.Unlock()
			return nil
		default:
		}
		if p.stallSince.IsZero() {
			p.stallSince = now
		}
		if !registered {
			p.waiters++
			registered = true
		}
		remaining := p.admissionTimeout - now.Sub(p.stallSince)
		changed := p.changed
		timer := p.admissionClock.NewTimer(remaining)
		p.stateMu.Unlock()
		select {
		case <-ctx.Done():
		case <-p.ctx.Done():
		case <-changed:
		case <-timer.C():
		}
		if !timer.Stop() {
			select {
			case <-timer.C():
			default:
			}
		}
	}
}

// signalLocked wakes admission waiters without storing candle payloads.
func (p *Pipeline) signalLocked() {
	if p.waiters == 0 {
		return
	}
	close(p.changed)
	p.changed = make(chan struct{})
}

func (p *Pipeline) signalCapacity() {
	p.stateMu.Lock()
	p.signalLocked()
	p.stateMu.Unlock()
}

func (p *Pipeline) confirmedProgressLocked() {
	if !p.stallSince.IsZero() {
		now := p.admissionClock.Now()
		// A confirmation cannot revive a wait that has already expired.
		if now.Sub(p.stallSince) < p.admissionTimeout {
			if len(p.pendingDays) == 0 && len(p.buffer) == 0 {
				p.stallSince = time.Time{}
			} else {
				p.stallSince = now
			}
		}
	}
	p.signalLocked()
}

// Start launches the background goroutine that drains the buffer and flushes batches.
func (p *Pipeline) Start() {
	p.wg.Add(1)
	go p.flushLoop()
	logger.OrDiscard(p.log).Info("questdb pipeline started",
		"batch_size", p.batchSize, "flush_interval", p.interval, "buffer_cap", cap(p.buffer))
}

// Stop cancels the pipeline context, waits for the flush goroutine, and flushes remaining
// entries. ctx bounds that final flush; shutdown passes a detached context so the drain gets
// its full deadline even though the stream context is already canceled.
func (p *Pipeline) Stop(ctx context.Context) {
	p.stateMu.Lock()
	p.cancel()
	p.signalLocked()
	p.stateMu.Unlock()
	p.wg.Wait()

	// The loop owns all unconfirmed work. Stop may make one bounded visibility
	// attempt, but must not replay an acknowledged or outcome-uncertain DAY.
	remaining := p.drain()
	if len(remaining) > 0 || p.retryLen() > 0 {
		flushCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		p.flushBatchesWithContext(flushCtx, remaining)
		cancel()
	}
	retryQueued := p.retryLen()
	logger.OrDiscard(p.log).Info("questdb pipeline stopped",
		"flushed", p.flushed.Load(),
		"dropped", p.dropped.Load(),
		"errors", p.errors.Load(),
		"retried", p.retried.Load(),
		"retry_queued", retryQueued)
	if retryQueued > 0 {
		logger.OrDiscard(p.log).Error("questdb pipeline shutdown left candles unconfirmed in the cold tier",
			"unconfirmed", retryQueued, "phase", p.Stats().BlockedPhase)
	}
}

// retryLen includes the unconfirmed DAY and its unsent tail, never a confirmed prefix.
func (p *Pipeline) retryLen() int {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	return p.pendingRows
}

// Stats returns a snapshot of pipeline metrics.
func (p *Pipeline) Stats() PipelineStats {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	return PipelineStats{Flushed: p.flushed.Load(), Dropped: p.dropped.Load(), Pending: len(p.buffer), Errors: p.errors.Load(), Retried: p.retried.Load(), RetryLen: p.pendingRows, Blocked: p.blockedPhase != "", BlockedPhase: p.blockedPhase, PendingDays: p.pendingCount}
}

// flushLoop runs in a goroutine, accumulating entries and flushing on batchSize or timer.
func (p *Pipeline) flushLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	batch := make([]candleEntry, 0, p.batchSize)
	continueImmediately := false

	for {
		// Do not consume more channel capacity while an unconfirmed DAY is fenced.
		// In particular a timer resumes CAPTURE/APPLY, never replays an acknowledged DAY.
		if len(p.pendingDays) > 0 {
			if continueImmediately {
				select {
				case <-p.ctx.Done():
					return
				default:
				}
				continueImmediately = p.flushWorkerTurn(nil)
				continue
			}
			select {
			case <-p.ctx.Done():
				p.pendingDays = appendDayWrites(p.pendingDays, batch)
				return
			case <-ticker.C:
				continueImmediately = p.flushWorkerTurn(nil)
			}
			continue
		}
		select {
		case <-p.ctx.Done():
			// Stop owns the single final bounded flush after this goroutine exits.
			p.pendingDays = appendDayWrites(p.pendingDays, batch)
			p.publishPending("")
			return

		case entry := <-p.buffer:
			p.signalCapacity()
			batch = append(batch, entry)
			if len(batch) >= p.batchSize {
				continueImmediately = p.flushWorkerTurn(batch)
				batch = batch[:0]
				ticker.Reset(p.interval)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				continueImmediately = p.flushWorkerTurn(batch)
				batch = batch[:0]
			}
		}
	}
}

// drain reads all remaining entries from the buffer channel without blocking.
func (p *Pipeline) drain() []candleEntry {
	var entries []candleEntry
	for {
		select {
		case e := <-p.buffer:
			entries = append(entries, e)
		default:
			return entries
		}
	}
}

// coldDayWrite is a submission and its fixed apply target. A lost ACK has no
// automatic replay: DEDUP cannot prevent an old retry overwriting a newer revision.
type coldDayWrite struct {
	entries      []candleEntry
	acknowledged bool
	target       int64
	targetKnown  bool
	uncertain    bool
}

// appendDayWrites partitions AFTER the sweep has recombined keys. Equal row identities
// keep admission order. Dates before the Unix epoch use UTC calendar days as well.
func appendDayWrites(existing []*coldDayWrite, entries []candleEntry) []*coldDayWrite {
	byDay := make(map[int64][]candleEntry)
	for _, entry := range entries {
		day := time.UnixMilli(entry.Candle.Timestamp).UTC().Truncate(24 * time.Hour).UnixMilli()
		byDay[day] = append(byDay[day], entry)
	}
	days := make([]int64, 0, len(byDay))
	for day := range byDay {
		days = append(days, day)
	}
	sort.Slice(days, func(i, j int) bool { return days[i] < days[j] })
	// Shutdown can hand off the collected batch before draining the channel.
	// Consolidate only unsent DAYs; an ACKed or uncertain request is immutable.
	mergeable := make(map[int64]int)
	for i, write := range existing {
		if !write.acknowledged && !write.uncertain {
			day := time.UnixMilli(write.entries[0].Candle.Timestamp).UTC().Truncate(24 * time.Hour).UnixMilli()
			mergeable[day] = i
		}
	}
	for _, day := range days {
		if i, ok := mergeable[day]; ok {
			existing[i].entries = append(existing[i].entries, byDay[day]...)
		} else {
			existing = append(existing, &coldDayWrite{entries: byDay[day]})
		}
	}
	return existing
}

func (p *Pipeline) publishPending(phase string) {
	p.publishPendingWithProgress(phase, false)
}

func (p *Pipeline) publishPendingWithProgress(phase string, confirmed bool) {
	rows := 0
	for _, day := range p.pendingDays {
		rows += len(day.entries)
	}
	p.stateMu.Lock()
	p.pendingRows = rows
	p.pendingCount = len(p.pendingDays)
	p.blockedPhase = phase
	if confirmed {
		p.confirmedProgressLocked()
	} else {
		p.signalLocked()
	}
	p.stateMu.Unlock()
	blocked := float64(0)
	if phase != "" {
		blocked = 1
	}
	p.metrics.setColdPipelineBlocked(blocked)
	p.metrics.setColdPipelinePendingRows(float64(rows))
}

// A worker turn confirms at most one DAY, including a retained predecessor.
// Only successful progress permits an immediate next turn with a fresh budget.
func (p *Pipeline) flushWorkerTurn(entries []candleEntry) bool {
	ctx, cancel := context.WithTimeout(p.ctx, 30*time.Second)
	defer cancel()
	var held *Store
	if len(entries) > 0 && len(p.pendingDays) == 0 {
		p.noopExpected = false
		if q, ok := p.store.(*Store); ok {
			var release func()
			var marked int
			entries, release, p.noopExpectedTarget, marked = q.filterNewColdBatch(ctx, entries)
			if release != nil {
				held = q
				defer release() // the same permit covers comparison and first DAY apply
				p.noopExpected = marked > 0
			}
		}
	}
	p.pendingDays = appendDayWrites(p.pendingDays, entries)
	p.publishPending(p.Stats().BlockedPhase)
	if len(p.pendingDays) == 0 {
		return false
	}
	day := p.pendingDays[0]
	if day.uncertain {
		p.publishPending("UNCERTAIN")
		return false
	}
	if held == nil && p.noopExpected {
		if q, ok := p.store.(*Store); ok {
			release, err := q.acquireCandleDayPermit(ctx)
			if err != nil {
				p.restoreUnsentNoops()
			} else {
				held = q
				defer release()
				// An ACKed DAY is immutable: its omitted rows were already
				// linearized under the prior permit. A pre-send failure is not.
				if q.pendingPlan != nil && (q.pendingPlan.current() != day || !day.acknowledged) {
					p.restoreUnsentNoops()
				} else if !day.acknowledged {
					position, err := q.readColdWALPosition(ctx)
					if err != nil || position.writer != p.noopExpectedTarget || position.sequencer != p.noopExpectedTarget || ctx.Err() != nil {
						p.restoreUnsentNoops()
					}
				}
			}
		} else {
			p.restoreUnsentNoops()
		}
	}
	noopRows := countColdNoopRows(day)
	if noopRows == len(day.entries) {
		if held == nil || ctx.Err() != nil || day.acknowledged {
			p.restoreUnsentNoops()
		} else {
			// A fully identical DAY has no ILP submission or false ACK. Its
			// applied-row proof was read under the same permit, and migration
			// cannot interleave before this confirmation.
			p.metrics.addColdSeedNoopRows(float64(noopRows))
			p.pendingDays[0] = nil
			p.pendingDays = p.pendingDays[1:]
			if !p.hasPendingNoops() {
				p.noopExpected = false
			}
			p.publishPendingWithProgress("", true)
			return true
		}
		noopRows = 0
	}
	plan := &coldDayPlan{days: []*coldDayWrite{day}}
	var phase string
	var err error
	if held != nil {
		phase, err = held.applyColdPlanLimitHeld(ctx, plan, true)
	} else {
		phase, err = applyColdPlanStep(ctx, p.store, plan)
	}
	if err != nil {
		p.errors.Add(1)
		p.retried.Add(int64(len(day.entries)))
		p.publishPending(phase)
		logger.OrDiscard(p.log).Error("questdb pipeline: DAY fenced",
			"phase", phase,
			"day", time.UnixMilli(day.entries[0].Candle.Timestamp).UTC().Format("2006-01-02"),
			"rows", len(day.entries),
			"remaining_days", len(p.pendingDays),
			"target_known", day.targetKnown,
			"target", day.target,
			"error", err)
		return false
	}
	if plan.current() == nil {
		p.flushed.Add(int64(len(day.entries) - noopRows))
		if noopRows > 0 {
			p.metrics.addColdSeedNoopRows(float64(noopRows))
		}
		p.pendingDays[0] = nil
		p.pendingDays = p.pendingDays[1:]
		if p.hasPendingNoops() {
			p.noopExpectedTarget = day.target
			p.noopExpected = day.targetKnown
			if !day.targetKnown {
				p.restoreUnsentNoops()
			}
		} else {
			p.noopExpected = false
		}
	}
	p.publishPendingWithProgress("", true)
	return true
}

func countColdNoopRows(day *coldDayWrite) int {
	count := 0
	for _, entry := range day.entries {
		if entry.coldNoopSkip {
			count++
		}
	}
	return count
}

func (p *Pipeline) hasPendingNoops() bool {
	for _, day := range p.pendingDays {
		if countColdNoopRows(day) > 0 {
			return true
		}
	}
	return false
}

// Keep the original candles in every unsent DAY. An ACKed or uncertain DAY
// cannot be replayed; its no-op decision was already linearized under a permit.
func (p *Pipeline) restoreUnsentNoops() {
	for _, day := range p.pendingDays {
		if day.acknowledged || day.uncertain {
			continue
		}
		for i := range day.entries {
			day.entries[i].coldNoopSkip = false
		}
	}
	p.noopExpected = false
}

// One budget covers the WHOLE sweep. Shutdown supplies its own single bounded
// attempt after canceling the normal one; acknowledgement and target survive both.
func (p *Pipeline) flushBatchesWithContext(ctx context.Context, entries []candleEntry) {
	// This path is used by shutdown and migration tests, outside the protected
	// new-batch turn. Preserve every unsubmitted candle for its final attempt.
	p.restoreUnsentNoops()
	for i := range entries {
		entries[i].coldNoopSkip = false
	}
	p.pendingDays = appendDayWrites(p.pendingDays, entries)
	// A retry in progress retains the previous error until visibility is confirmed.
	p.publishPending(p.Stats().BlockedPhase)
	for len(p.pendingDays) > 0 {
		day := p.pendingDays[0]
		if day.uncertain {
			p.publishPending("UNCERTAIN")
			return
		}
		phase, err := applyColdDay(ctx, p.store, day)
		if err != nil {
			p.errors.Add(1)
			p.retried.Add(int64(len(day.entries)))
			p.publishPending(phase)
			logger.OrDiscard(p.log).Error("questdb pipeline: DAY fenced",
				"phase", phase,
				"day", time.UnixMilli(day.entries[0].Candle.Timestamp).UTC().Format("2006-01-02"),
				"rows", len(day.entries),
				"remaining_days", len(p.pendingDays),
				"target_known", day.targetKnown,
				"target", day.target,
				"error", err)
			return
		}
		noopRows := countColdNoopRows(day)
		p.flushed.Add(int64(len(day.entries) - noopRows))
		if noopRows > 0 {
			p.metrics.addColdSeedNoopRows(float64(noopRows))
		}
		// Release only confirmed rows. A later failure must not replay this prefix.
		p.pendingDays[0] = nil
		p.pendingDays = p.pendingDays[1:]
		p.publishPendingWithProgress("", true)
	}
}

// coldBatchWriter is the common submission seam of runtime and sole-copy migration.
type coldBatchWriter interface {
	WriteBatches(context.Context, map[candle.SeriesKey][]candle.Candle) error
}

// coldDayPlan owns an existing bounded sweep and its confirmed cursor. Keeping
// the unsent tail is essential for migration's MIN(timestamp) checkpoint.
type coldDayPlan struct {
	days []*coldDayWrite
	next int
}

func (plan *coldDayPlan) current() *coldDayWrite {
	if plan == nil || plan.next >= len(plan.days) {
		return nil
	}
	return plan.days[plan.next]
}

type coldPlanCoordinator interface {
	applyColdPlan(context.Context, *coldDayPlan) (string, error)
}

type coldPlanStepCoordinator interface {
	applyColdPlanStep(context.Context, *coldDayPlan) (string, error)
}

func applyColdPlanStep(ctx context.Context, store coldBatchWriter, plan *coldDayPlan) (string, error) {
	if coordinator, ok := store.(coldPlanStepCoordinator); ok {
		return coordinator.applyColdPlanStep(ctx, plan)
	}
	return applyColdPlanUncoordinatedLimit(ctx, store, plan, true)
}

func applyColdDay(ctx context.Context, store coldBatchWriter, day *coldDayWrite) (string, error) {
	return applyColdPlan(ctx, store, &coldDayPlan{days: []*coldDayWrite{day}})
}

func applyColdPlan(ctx context.Context, store coldBatchWriter, plan *coldDayPlan) (string, error) {
	if coordinator, ok := store.(coldPlanCoordinator); ok {
		return coordinator.applyColdPlan(ctx, plan)
	}
	return applyColdPlanUncoordinated(ctx, store, plan)
}

func applyColdPlanUncoordinated(ctx context.Context, store coldBatchWriter, plan *coldDayPlan) (string, error) {
	return applyColdPlanUncoordinatedLimit(ctx, store, plan, false)
}

func applyColdPlanUncoordinatedLimit(ctx context.Context, store coldBatchWriter, plan *coldDayPlan, oneDay bool) (string, error) {
	for plan.current() != nil {
		phase, err := applyColdDayUncoordinated(ctx, store, plan.current())
		if err != nil {
			return phase, err
		}
		plan.days[plan.next] = nil
		plan.next++
		if oneDay {
			break
		}
	}
	return "", nil
}

// applyColdDayUncoordinated is the single DAY/ACK/fixed-target state machine.
// QuestDB adds a shared admission guard around it; synchronous adapters need none.
func applyColdDayUncoordinated(ctx context.Context, store coldBatchWriter, day *coldDayWrite) (string, error) {
	if day.uncertain {
		return "UNCERTAIN", &candle.ColdWriteUncertainError{Err: errors.New("reconciliation required before replay")}
	}
	barrier, hasBarrier := store.(candle.ColdStoreApplyBarrier)
	phase := "SUBMIT"
	if day.acknowledged {
		phase = "CAPTURE"
	}
	if day.targetKnown {
		phase = "APPLY"
	}
	err := ctx.Err()
	if err == nil && !day.acknowledged {
		groups := make(map[candle.SeriesKey][]candle.Candle)
		for _, entry := range day.entries {
			if entry.coldNoopSkip {
				continue
			}
			key, keyErr := candle.SeriesKeyOf(entry.Exchange, entry.MarketType, entry.Symbol, entry.Timeframe)
			if keyErr != nil {
				return "SUBMIT", fmt.Errorf("cold batch identity %s:%s:%s:%s: %w",
					entry.Exchange, entry.MarketType, entry.Symbol, entry.Timeframe, keyErr)
			}
			groups[key] = append(groups[key], entry.Candle)
		}
		if len(groups) == 0 {
			return "NOOP_UNGUARDED", fmt.Errorf("cold no-op DAY requires a held WAL proof")
		}
		err = store.WriteBatches(ctx, groups)
		if err == nil {
			day.acknowledged = true
		} else {
			var uncertain *candle.ColdWriteUncertainError
			if errors.As(err, &uncertain) {
				day.uncertain = true
				phase = "UNCERTAIN"
			}
		}
	}
	if err == nil && hasBarrier {
		phase = "CAPTURE"
		if !day.targetKnown {
			day.target, err = barrier.CaptureApplyTarget(ctx)
			day.targetKnown = err == nil
		}
		if err == nil {
			phase = "APPLY"
			err = barrier.WaitApplied(ctx, day.target)
		}
	}
	return phase, err
}
