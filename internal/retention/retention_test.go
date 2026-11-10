package retention

import (
	"context"
	"errors"
	"testing"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// THE RETENTION OF `derivatives`.
//
// Three mutants, three measured defects:
//   (a) the table this path grows with NO retention at all;
//   (b) the cutoff reaching rows a deliberate write just produced — measured in production:
//       130,284 candles written at 00:07 and deleted at 00:29;
//   (c) retention starting at construction, before the gates speak, with the failure buried at
//       Info level next to "nothing was due".
// ─────────────────────────────────────────────────────────────────────────────

// fakeExecutor is the doubled database. It records what was REQUESTED, which is what decides.
type fakeExecutor struct {
	existing map[string]bool
	oldest   map[string]time.Time
	drops    []dropCall
	dropErr  error
	existErr error
}

type dropCall struct {
	table  string
	cutoff time.Time
}

func (f *fakeExecutor) TableExists(_ context.Context, table string) (bool, error) {
	if f.existErr != nil {
		return false, f.existErr
	}
	return f.existing[table], nil
}

func (f *fakeExecutor) OldestTimestamp(_ context.Context, table string) (time.Time, bool, error) {
	ts, ok := f.oldest[table]
	return ts, ok, nil
}

func (f *fakeExecutor) DropPartitionsBefore(_ context.Context, table string, cutoff time.Time) error {
	f.drops = append(f.drops, dropCall{table: table, cutoff: cutoff})
	return f.dropErr
}

func fixedClock(t time.Time) func() time.Time { return func() time.Time { return t } }

func resultFor(results []Result, table string) (Result, bool) {
	for _, r := range results {
		if r.Table == table {
			return r, true
		}
	}
	return Result{}, false
}

// MUTANT (a) — `derivatives` WITHOUT RETENTION. It is one policy pointing at `candles`, leaving the
// table this path grows untouched.
func TestRetention_DerivativesIsCoveredAlongsideCandles(t *testing.T) {
	agora := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	exec := &fakeExecutor{
		existing: map[string]bool{"candles": true, "derivatives": true},
		oldest: map[string]time.Time{
			"candles":     time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
			"derivatives": time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}
	rm := NewManager(ManagerOptions{
		Executor: exec,
		Now:      fixedClock(agora),
		Policies: []Policy{
			{Table: "candles", RetentionDays: 365},
			{Table: "derivatives", RetentionDays: 2000},
		},
		GatesApproved: func() bool { return true },
	})

	results := rm.RunOnce(t.Context())
	if len(results) != 2 {
		t.Fatalf("the pass returned %d verdicts, want 2 — a table without a verdict is a table without retention", len(results))
	}
	r, ok := resultFor(results, "derivatives")
	if !ok {
		t.Fatal("derivatives received no verdict at all")
	}
	if r.Outcome != Dropped {
		t.Errorf("derivatives: verdict %s, want dropped", r.Outcome)
	}

	var alcancadas []string
	for _, d := range exec.drops {
		alcancadas = append(alcancadas, d.table)
	}
	if len(alcancadas) != 2 {
		t.Fatalf("the DROP reached %v — both tables have declared retention", alcancadas)
	}
}

// MUTANT (b) — THE CUTOFF REACHING WHAT THE DELIBERATE WRITE WROTE.
//
// The real table: `min(timestamp)` of `derivatives` is exactly the ~2,000 daily buckets seeding
// requests on purpose. A 365-day window would cut years off it on every boot.
func TestRetention_TheCutNeverReachesWhatADeliberateWriteReached(t *testing.T) {
	agora := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	deliberado := time.Date(2021, 3, 3, 0, 0, 0, 0, time.UTC)
	horizonte := agora.Sub(deliberado)

	exec := &fakeExecutor{
		existing: map[string]bool{"derivatives": true},
		oldest:   map[string]time.Time{"derivatives": deliberado},
	}
	rm := NewManager(ManagerOptions{
		Executor: exec,
		Now:      fixedClock(agora),
		Policies: []Policy{{
			// A SHORT WINDOW ON PURPOSE: it is the operator lowering the number, and it is
			// exactly against that the boundary exists.
			Table:             "derivatives",
			RetentionDays:     365,
			DeliberateHorizon: horizonte,
			HorizonReason:     "seeding of ~2,000 daily buckets",
		}},
		GatesApproved: func() bool { return true },
	})

	results := rm.RunOnce(t.Context())
	r, _ := resultFor(results, "derivatives")

	// THE CUTOFF CANNOT BE NEWER THAN THE DELIBERATE ROW. `DROP PARTITION WHERE timestamp <
	// cutoff` with a cutoff at 2025-08-24 would delete the 2021-03-03 one.
	if r.Cutoff.After(deliberado) {
		t.Fatalf("the cutoff landed at %s, AFTER the deliberate row of %s — it would delete it",
			r.Cutoff.Format("2006-01-02"), deliberado.Format("2006-01-02"))
	}
	if !r.FloorApplied {
		t.Error("the cutoff receded and the verdict did not say so — the table would look like broken retention")
	}
	if r.Outcome != NothingDue {
		t.Errorf("verdict %s: with the cutoff pushed back to the boundary, nothing precedes it", r.Outcome)
	}
	if len(exec.drops) != 0 {
		t.Errorf("a DROP was emitted in %v — there was nothing to drop", exec.drops)
	}
}

// AND THE BOUNDARY IS NOT AN OFF SWITCH: when it is NEWER than the window, the window decides.
// Without this case, `DeliberateHorizon` could become permanently disabled retention.
func TestRetention_TheFloorDoesNotOverrideAWindowThatIsAlreadyDeeper(t *testing.T) {
	agora := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	policy := Policy{
		Table:             "derivatives",
		RetentionDays:     2000,
		DeliberateHorizon: 30 * 24 * time.Hour,
	}
	cutoff, floorApplied := policy.Cutoff(agora)

	janela := agora.AddDate(0, 0, -2001)
	if !cutoff.Equal(janela) {
		t.Errorf("cutoff at %s, want %s — the boundary is newer than the window and must not decide",
			cutoff.Format("2006-01-02"), janela.Format("2006-01-02"))
	}
	if floorApplied {
		t.Error("the verdict says the boundary pushed the cutoff back, and it did not")
	}
}

// MUTANT (c), FIRST HALF — RETENTION STARTING BEFORE THE GATES.
//
// Retention must not run during `App` CONSTRUCTION, while `preflightGates` only runs at the first
// instruction of `Run`. A `DROP PARTITION` fired there is destruction executed by a process the
// gates might have refused.
func TestRetention_NothingIsDroppedBeforeTheGatesApprove(t *testing.T) {
	agora := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	exec := &fakeExecutor{
		existing: map[string]bool{"derivatives": true},
		oldest:   map[string]time.Time{"derivatives": time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC)},
	}
	aprovado := false
	rm := NewManager(ManagerOptions{
		Executor:      exec,
		Now:           fixedClock(agora),
		Policies:      []Policy{{Table: "derivatives", RetentionDays: 365}},
		GatesApproved: func() bool { return aprovado },
	})

	results := rm.RunOnce(t.Context())
	if len(exec.drops) != 0 {
		t.Fatalf("retention dropped a partition before the gates: %v", exec.drops)
	}
	r, _ := resultFor(results, "derivatives")
	if r.Outcome != Deferred {
		t.Errorf("verdict %s, want deferred_until_gates — and deferral must be SAID, not silent", r.Outcome)
	}
	if rm.Passes() != 0 {
		t.Errorf("the pass counted as executed (%d) without a license", rm.Passes())
	}

	// AND AFTER APPROVAL IT ACTS. Without this half, retention that never runs would pass.
	aprovado = true
	rm.report(rm.RunOnce(t.Context()))
	if len(exec.drops) != 1 {
		t.Fatalf("after the gates retention did not act: %v", exec.drops)
	}
	if rm.Passes() != 1 {
		t.Errorf("passes = %d, want 1", rm.Passes())
	}
}

// AN UNDECLARED LICENSE IS A DENIED LICENSE. The safe default for a destructive mechanism is not to
// act — the opposite would let a forgotten caller delete partitions by omission.
func TestRetention_AnUndeclaredLicenceNeverActs(t *testing.T) {
	exec := &fakeExecutor{
		existing: map[string]bool{"derivatives": true},
		oldest:   map[string]time.Time{"derivatives": time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC)},
	}
	rm := NewManager(ManagerOptions{
		Executor: exec,
		Policies: []Policy{{Table: "derivatives", RetentionDays: 365}},
	})
	if r, _ := resultFor(rm.RunOnce(t.Context()), "derivatives"); r.Outcome != Deferred {
		t.Errorf("without a declared license the verdict was %s", r.Outcome)
	}
	if len(exec.drops) != 0 {
		t.Errorf("without a declared license retention dropped %v", exec.drops)
	}
}

// WITHOUT AN EXECUTOR THE DESTRUCTIVE PATH IS REFUSED, not turned into a panic.
//
// An approved license does not authorize calling a method on a nil interface: an `App` assembled
// without a durable tier must not take down the process — it must say it has nowhere to act.
func TestRetention_ANilExecutorRefusesTheDestructivePath(t *testing.T) {
	rm := NewManager(ManagerOptions{
		Policies:      []Policy{{Table: "derivatives", RetentionDays: 365}},
		GatesApproved: func() bool { return true },
	})
	r, _ := resultFor(rm.RunOnce(t.Context()), "derivatives")
	if r.Outcome != Failed {
		t.Errorf("verdict %s, want failed — without an executor there is no legitimate destructive path", r.Outcome)
	}
	if rm.Passes() != 0 {
		t.Errorf("a pass without an executor counted as executed (%d)", rm.Passes())
	}
}

// MUTANT (c), SECOND HALF — A MISSING TABLE INDISTINGUISHABLE FROM "NOTHING WAS DUE".
//
// Firing the `ALTER` and reading its error at Info level, with a note that an error there "is
// normal", makes a policy pointing at a name the database does not have print the same line as a
// healthy table with no partition due.
func TestRetention_AMissingTableIsNotTheSameAsNothingDue(t *testing.T) {
	agora := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	exec := &fakeExecutor{
		existing: map[string]bool{"derivatives": true},
		oldest:   map[string]time.Time{"derivatives": agora.Add(-24 * time.Hour)},
	}
	rm := NewManager(ManagerOptions{
		Executor: exec,
		Now:      fixedClock(agora),
		Policies: []Policy{
			{Table: "derivativos", RetentionDays: 365}, // wrong name — does not exist
			{Table: "derivatives", RetentionDays: 365}, // exists, nothing due
		},
		GatesApproved: func() bool { return true },
	})

	results := rm.RunOnce(t.Context())
	ausente, _ := resultFor(results, "derivativos")
	vazia, _ := resultFor(results, "derivatives")

	if ausente.Outcome != TableAbsent {
		t.Errorf("a missing table came out as %s", ausente.Outcome)
	}
	if vazia.Outcome != NothingDue {
		t.Errorf("a table with no partition due came out as %s", vazia.Outcome)
	}
	if ausente.Outcome == vazia.Outcome {
		t.Fatal("the two cases collapsed into one verdict — that is exactly the measured defect")
	}
	if len(exec.drops) != 0 {
		t.Errorf("an ALTER was emitted with nothing to drop: %v", exec.drops)
	}
}

// A READ FAILURE IS A FAILURE, not "nothing was due". An existence that could not be read does not
// authorize concluding the table is healthy.
func TestRetention_AnUnreadableTableIsAFailureNotAnApproval(t *testing.T) {
	exec := &fakeExecutor{existErr: errors.New("connection refused")}
	rm := NewManager(ManagerOptions{
		Executor:      exec,
		Policies:      []Policy{{Table: "derivatives", RetentionDays: 365}},
		GatesApproved: func() bool { return true },
	})
	r, _ := resultFor(rm.RunOnce(t.Context()), "derivatives")
	if r.Outcome != Failed {
		t.Errorf("verdict %s, want failed", r.Outcome)
	}
}

// THE WRITE FLOOR IS THE SAME CUTOFF, not a second arithmetic.
func TestRetention_TheWriteFloorIsTheCutoff(t *testing.T) {
	agora := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	policy := Policy{Table: "candles", RetentionDays: 365, DeliberateHorizon: 800 * 24 * time.Hour}

	cutoff, _ := policy.Cutoff(agora)
	floor, clamped := policy.WriteFloor(agora)
	if !clamped {
		t.Fatal("the policy declared a window and the write clamp was left off")
	}
	if !floor.Equal(cutoff) {
		t.Fatalf("write floor = %s, cutoff = %s — the write would refuse what retention preserves",
			floor, cutoff)
	}
}

// WITHOUT A DECLARED WINDOW THERE IS NO CLAMP: refusing writes nobody would ever drop only destroys.
func TestRetention_NoWindowMeansNoWriteFloor(t *testing.T) {
	if _, clamped := (Policy{Table: "candles"}).WriteFloor(time.Now()); clamped {
		t.Error("a policy without a window turned the write clamp on")
	}
}

// ctxCaptureExecutor records the context the database call actually receives.
type ctxCaptureExecutor struct {
	tableCtx context.Context
}

func (e *ctxCaptureExecutor) TableExists(ctx context.Context, _ string) (bool, error) {
	e.tableCtx = ctx
	return false, nil
}

func (e *ctxCaptureExecutor) OldestTimestamp(context.Context, string) (time.Time, bool, error) {
	return time.Time{}, false, nil
}

func (e *ctxCaptureExecutor) DropPartitionsBefore(context.Context, string, time.Time) error {
	return nil
}

// TestRetention_RunOncePropagatesCallerCancellation proves a canceled caller reaches the
// destructive executor. The previous implementation built its own context.Background()
// deadline, so the database call kept running after the caller had gone away.
func TestRetention_RunOncePropagatesCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	exec := &ctxCaptureExecutor{}
	rm := NewManager(ManagerOptions{
		Executor:      exec,
		Now:           fixedClock(time.Now()),
		Policies:      []Policy{{Table: "candles", RetentionDays: 365}},
		GatesApproved: func() bool { return true },
	})
	rm.RunOnce(ctx)

	if exec.tableCtx == nil {
		t.Fatal("the executor was never called")
	}
	if !errors.Is(exec.tableCtx.Err(), context.Canceled) {
		t.Fatalf("executor context err = %v, want context.Canceled — RunOnce did not pass the caller's context",
			exec.tableCtx.Err())
	}
}
