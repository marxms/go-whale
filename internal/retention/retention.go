// Package retention owns the durable-tier retention POLICY and the loop that
// applies it. It lives outside candle/engine because retention is not candle
// behavior: it decides which durable partitions may be dropped, and it must be
// injected with the database executor instead of reaching for one.
package retention

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// COLD-TIER RETENTION.
//
// QuestDB 8.2.3 has no row-level DELETE, so `DROP PARTITION` by time is the ONLY mechanism
// that returns space. Retention therefore covers every table with durable history, is
// bounded by the depth a deliberate write reaches, and does not act before the startup
// gates.

// Executor is the minimum retention needs from the database.
//
// An interface, not the concrete `*QuestDBStore`, because the DECISION is what needs testing:
// which cutoff, against which table, and under which license. Standing up QuestDB to assert
// "the cutoff does not cross the deliberate boundary" would measure the driver.
type Executor interface {
	// TableExists answers whether the table exists. It separates "the table does not exist"
	// from "no partition was due" BEFORE the `ALTER`, instead of trying to read the difference
	// from its error message.
	TableExists(ctx context.Context, table string) (bool, error)
	// OldestTimestamp returns the table's oldest timestamp. `false` means an empty table. It is
	// a cheap read — QuestDB keeps the minimum per partition — and it is what allows emitting
	// no `ALTER` at all when there is nothing to drop.
	OldestTimestamp(ctx context.Context, table string) (time.Time, bool, error)
	// DropPartitionsBefore drops partitions before the cutoff.
	DropPartitionsBefore(ctx context.Context, table string, cutoff time.Time) error
}

// Policy declares the retention of ONE table.
type Policy struct {
	// Table is the target table.
	Table string
	// RetentionDays is the declared window, in days.
	RetentionDays int
	// DeliberateHorizon is the DEPTH a deliberate write reaches — deep seeding or migration —
	// and which retention must not reach.
	//
	// It is a DURATION, not an instant, on purpose: an instant computed at construction would
	// age inside a process that stays up for months, and the boundary would move backwards
	// without anyone touching it.
	//
	// Zero means NO declared boundary, and then the window alone decides.
	DeliberateHorizon time.Duration
	// HorizonReason names where the boundary comes from. It appears in the log when the cutoff
	// recedes, so an operator who sees a table larger than the window knows why.
	HorizonReason string
}

// Outcome is the verdict of ONE policy in a pass.
type Outcome string

const (
	// Dropped means partitions before the cutoff were dropped.
	Dropped Outcome = "dropped"
	// NothingDue means the table exists and nothing in it precedes the cutoff.
	NothingDue Outcome = "nothing_due"
	// TableAbsent means the table does not exist. This case must not be confused with
	// NothingDue — one is a configuration error, the other is normal operation.
	TableAbsent Outcome = "table_absent"
	// Failed means the read or the `ALTER` failed.
	Failed Outcome = "failed"
	// Deferred means the startup gates have not approved yet.
	Deferred Outcome = "deferred_until_gates"
)

// Result is one policy's verdict, with the number that produced it.
type Result struct {
	Table   string
	Outcome Outcome
	Cutoff  time.Time
	// FloorApplied says the deliberate boundary PUSHED the cutoff back. Without this field, a
	// table larger than the declared window would look like broken retention.
	FloorApplied bool
	Detail       string
}

// ManagerOptions assembles the manager.
type ManagerOptions struct {
	Executor Executor
	Policies []Policy
	// Interval is the period between passes. Zero falls back to 24h.
	Interval time.Duration
	// Now is the clock. Nil falls back to time.Now.
	Now func() time.Time
	// GatesApproved is THE LICENSE TO ACT, and it is how the "not before startup gates" rule is
	// applied instead of remembered.
	//
	// Moving the `Start()` call after the gates fixes the order once and does not prevent
	// anyone from moving it back. The predicate is consulted before EVERY pass, so retention
	// started early drops nothing: it defers and says it deferred. Nil means "no declared
	// license" — and then it never acts, because the safe default of a destructive mechanism is
	// not to act.
	GatesApproved func() bool
	// Log is the injected logger. A nil logger is a silent sink.
	Log *slog.Logger
}

// Manager applies the declared policies.
type Manager struct {
	exec     Executor
	policies []Policy
	interval time.Duration
	now      func() time.Time
	// gatesApproved is the license to act. See ManagerOptions.
	gatesApproved func() bool
	log           *slog.Logger
	stopCh        chan struct{}
	stopOnce      sync.Once
	wg            sync.WaitGroup
	// passes counts the passes that actually EXECUTED a policy. It exists so that "did not run
	// before the gates" is an assertion, not a code reading.
	passes atomic.Int64
}

// NewManager assembles the manager from the declared policies.
//
// The license starts DENIED. A nil executor is also a declared state — and then no policy acts:
// the destructive path is refused instead of panicking.
func NewManager(opts ManagerOptions) *Manager {
	interval := opts.Interval
	if interval <= 0 {
		interval = 24 * time.Hour
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	gates := opts.GatesApproved
	if gates == nil {
		gates = func() bool { return false }
	}
	return &Manager{
		exec:          opts.Executor,
		policies:      opts.Policies,
		interval:      interval,
		now:           now,
		gatesApproved: gates,
		log:           logger.OrDiscard(opts.Log),
		stopCh:        make(chan struct{}),
	}
}

// Start launches the background retention loop.
func (rm *Manager) Start(ctx context.Context) {
	rm.wg.Add(1)
	go rm.loop(ctx)
	for _, p := range rm.policies {
		logger.OrDiscard(rm.log).Info("retention policy declared",
			"table", p.Table, "retention_days", p.RetentionDays, "horizon", horizonSuffix(p))
	}
}

func horizonSuffix(p Policy) string {
	if p.DeliberateHorizon <= 0 {
		return ""
	}
	return fmt.Sprintf(", with a deliberate boundary of %d days (%s)",
		int(p.DeliberateHorizon.Hours()/24), p.HorizonReason)
}

// Stop signals the loop to exit and waits for completion.
func (rm *Manager) Stop() {
	rm.stopOnce.Do(func() { close(rm.stopCh) })
	rm.wg.Wait()
	logger.OrDiscard(rm.log).Info("retention stopped")
}

// Passes is how many passes actually EXECUTED a policy.
func (rm *Manager) Passes() int64 { return rm.passes.Load() }

func (rm *Manager) loop(ctx context.Context) {
	defer rm.wg.Done()

	rm.report(rm.RunOnce(ctx))

	ticker := time.NewTicker(rm.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rm.report(rm.RunOnce(ctx))
		case <-rm.stopCh:
			return
		}
	}
}

// RunOnce runs ONE pass over all policies and returns one verdict per table.
//
// Exported so a caller that owns the lifecycle can apply the policy once without
// starting a goroutine. The gates are still consulted on every call: being exported is
// not authority to skip them.
func (rm *Manager) RunOnce(ctx context.Context) []Result {
	if !rm.gatesApproved() {
		// THE LICENSE COMES FIRST. An `ALTER TABLE … DROP PARTITION` fired before the gates
		// approve is destruction executed by a process that perhaps should not even be up.
		return rm.deferred("the startup gates have not approved yet; no partition was touched")
	}
	if rm.exec == nil {
		// WITHOUT AN EXECUTOR THERE IS NO DESTRUCTIVE PATH. Refusing is the safe default;
		// calling a method on a nil interface would panic and take down the whole process.
		return rm.failed("no retention executor was injected; no partition was touched")
	}
	rm.passes.Add(1)

	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	out := make([]Result, 0, len(rm.policies))
	for _, policy := range rm.policies {
		out = append(out, rm.applyPolicy(runCtx, policy))
	}
	return out
}

func (rm *Manager) deferred(detail string) []Result {
	out := make([]Result, 0, len(rm.policies))
	for _, p := range rm.policies {
		out = append(out, Result{Table: p.Table, Outcome: Deferred, Detail: detail})
	}
	return out
}

func (rm *Manager) failed(detail string) []Result {
	out := make([]Result, 0, len(rm.policies))
	for _, p := range rm.policies {
		out = append(out, Result{Table: p.Table, Outcome: Failed, Detail: detail})
	}
	return out
}

func (rm *Manager) applyPolicy(ctx context.Context, policy Policy) Result {
	cutoff, floorApplied := policy.Cutoff(rm.now())
	result := Result{Table: policy.Table, Cutoff: cutoff, FloorApplied: floorApplied}

	exists, err := rm.exec.TableExists(ctx, policy.Table)
	if err != nil {
		result.Outcome = Failed
		result.Detail = fmt.Sprintf("the table existence could not be read: %v", err)
		return result
	}
	if !exists {
		// THIS CASE IS NOT "NOTHING WAS DUE". A missing table means the policy points at a
		// name the database does not have — wrong configuration, not normal operation — so it
		// comes out as a named failure instead of a note.
		result.Outcome = TableAbsent
		result.Detail = "the table declared in the policy does NOT EXIST in the database — its retention will never run"
		return result
	}

	oldest, hasRows, err := rm.exec.OldestTimestamp(ctx, policy.Table)
	if err != nil {
		result.Outcome = Failed
		result.Detail = fmt.Sprintf("the oldest timestamp could not be read: %v", err)
		return result
	}
	if !hasRows || !oldest.Before(cutoff) {
		// NO `ALTER` IS EMITTED. Asking first removes the ambiguity a fire-and-read-the-error
		// approach has: "there was nothing to drop" would otherwise share a log level with a
		// real defect.
		result.Outcome = NothingDue
		result.Detail = fmt.Sprintf("the oldest is %s, not older than the cutoff", timestampOrEmpty(oldest, hasRows))
		return result
	}

	if err := rm.exec.DropPartitionsBefore(ctx, policy.Table, cutoff); err != nil {
		result.Outcome = Failed
		result.Detail = fmt.Sprintf("o DROP PARTITION falhou: %v", err)
		return result
	}
	result.Outcome = Dropped
	result.Detail = fmt.Sprintf("partitions older than %s dropped; the oldest was %s",
		cutoff.Format("2006-01-02"), oldest.Format("2006-01-02"))
	return result
}

func timestampOrEmpty(ts time.Time, hasRows bool) string {
	if !hasRows {
		return "(empty table)"
	}
	return ts.Format("2006-01-02")
}

// Cutoff decides WHERE retention cuts, and this is where the deliberate boundary acts.
//
// The cutoff is never NEWER than the depth a deliberate write reaches. Without that, the
// window erases exactly what deep seeding or a migration just wrote, and the damage repeats on
// every boot because the window is recomputed each time.
//
// The window's GRACE DAY stays where it was: it exists so the active partition is not dropped.
func (p Policy) Cutoff(now time.Time) (time.Time, bool) {
	window := now.UTC().AddDate(0, 0, -(p.RetentionDays + 1))
	if p.DeliberateHorizon <= 0 {
		return window, false
	}
	boundary := now.UTC().Add(-p.DeliberateHorizon)
	if boundary.Before(window) {
		return boundary, true
	}
	return window, false
}

// WriteFloor is the SAME boundary as `Cutoff`, offered to whoever WRITES.
//
// It is not a second calculation: it delegates to `Cutoff`, which remains the only one. A grace
// day added here and not there would reopen the band, which is why this function does no
// arithmetic.
//
// WITHOUT A DECLARED WINDOW THERE IS NO CLAMP. `false` means write everything. A clamp without
// retention would refuse writes with no counterpart: nobody would ever drop the refused band,
// so refusing it only destroys data.
func (p Policy) WriteFloor(now time.Time) (time.Time, bool) {
	if p.RetentionDays <= 0 {
		return time.Time{}, false
	}
	floor, _ := p.Cutoff(now)
	return floor, true
}

// report prints ONE verdict per table, at its severity.
//
// THE LEVEL IS THE DISTINCTION. Sending everything — including a missing table — to `Info` with
// a note that an error there "is normal" leaves an operator reading the log unable to separate
// the two.
func (rm *Manager) report(results []Result) {
	for _, r := range results {
		// THE PASS NUMBER GOES ON THE LINE, and it is not decoration: it answers "has retention
		// ever run?" without anyone correlating timestamps. Retention deferred forever by the
		// gates prints `pass 0` on every tick, visibly.
		line := fmt.Sprintf("[retention] passada %d · %s: %s — corte em %s%s · %s",
			rm.Passes(), r.Table, r.Outcome, r.Cutoff.Format("2006-01-02"), floorSuffix(r), r.Detail)
		switch r.Outcome {
		case TableAbsent, Failed:
			logger.OrDiscard(rm.log).Error(line, "table", r.Table, "outcome", r.Outcome)
		case Deferred:
			logger.OrDiscard(rm.log).Warn(line, "table", r.Table, "outcome", r.Outcome)
		default:
			logger.OrDiscard(rm.log).Info(line, "table", r.Table, "outcome", r.Outcome)
		}
	}
}

func floorSuffix(r Result) string {
	if !r.FloorApplied {
		return ""
	}
	return " (PULLED BACK by the deliberate boundary — the table keeps more than the declared window, on purpose)"
}
