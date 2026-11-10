package questdb

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// STAGE 1 OF BOOT — the sole-copy migration.
//
// The gate `sole-copy-vs-evict-everything-policy` is RIGHT: refusing is the feature
// working. What was missing was the work that removes the condition it refuses — and
// that work has to happen BEFORE the gate, i.e. during construction, because
// `preflightGates` returns on its second instruction when sole copy remains.
//
// The other reason it cannot move after retention: an incident in which retention erased
// exactly what the migration had just written. See
// docs/decisions/0032-migrate-sole-copy-before-retention.md.
//
// The rule lives in the QuestDB adapter because the migration writes the durable
// tier; the composition root only decides WHEN the stage runs and translates the
// outcome into the retention license.

// BootMigrationOutcome is the outcome of stage 1, consulted by retention.
type BootMigrationOutcome int

const (
	// BootMigrationNotRun is the zero value: the stage did not happen in this process.
	// It does NOT block retention — a binary without a durable tier has no retention
	// to block.
	BootMigrationNotRun BootMigrationOutcome = iota
	// BootMigrationNotApplicable is the deliberately skipped stage: without a durable
	// tier there is nowhere to migrate, and without a declared cell there is nothing
	// to migrate.
	BootMigrationNotApplicable
	// BootMigrationComplete is the pass that reached the end.
	BootMigrationComplete
	// BootMigrationIncomplete is the pass that ABORTED before the end. It is the only
	// outcome that withholds the retention license.
	BootMigrationIncomplete
)

// String renders the outcome for the boot log.
func (o BootMigrationOutcome) String() string {
	switch o {
	case BootMigrationNotApplicable:
		return "not applicable"
	case BootMigrationComplete:
		return "complete"
	case BootMigrationIncomplete:
		return "INCOMPLETE"
	default:
		return "not run"
	}
}

// SoleCopyMigrationRun is ONE migration pass. It is a seam, not a direct call to
// SoleCopyMigrator, for the same reason serverProbe exists: the property under test is
// WHEN the stage runs and what its outcome authorizes, and both are invisible behind a
// live Redis and a live QuestDB.
type SoleCopyMigrationRun func(ctx context.Context) (SoleCopyMigrationReport, error)

// BootMigrationBudget is the DECLARED ceiling of the migration; the context budget is
// derived from it.
//
// The number is not new: it is the same `-timeout` that cmd/candles-sole-copy-migrate
// already declares as the default for this exact pass. The budget of the stage derives
// from the ceiling of the thing being budgeted.
const BootMigrationBudget = 30 * time.Minute

// ColdTierBudget is the ceiling of each synchronous cold-tier startup step. It exists
// so a slow QuestDB (WAL recovery) cannot hang NewApp.
const ColdTierBudget = 10 * time.Second

// BootMigrationMaxPasses is the WORK CEILING PER BOOT, counted in passes.
//
// Each pass is limited to SoleCopyFlushThreshold candles — the same unit in which the
// durable tier commits. The measured sole copy is 95,811 candles in 1d plus 33,379 in
// 1w = 129,190, which at 20,000 per pass is SEVEN passes. The eighth is the RE-READ
// that declares completion. The per-boot ceiling is therefore 8 × 20,000 = 160,000
// candles. A binary in crashloop runs this once per restart, and an unbounded pass
// would multiply the work by the number of restarts without ever finishing any.
const BootMigrationMaxPasses = 8

// BootMigrationContext assembles the stage-1 budget with its own deadline.
//
// `context.Background()` alone is not canceled by anything. Under
// RestartPolicy=unless-stopped Docker sends SIGTERM and kills with SIGKILL 10s later;
// a migration hung on Background() ignores the first signal, spends the 10s and dies
// by the second, mid-batch. With the signal context it stops where it is, the outcome
// becomes INCOMPLETE, and the boundary re-read finds the rest on the next boot.
func BootMigrationContext() (context.Context, context.CancelFunc) {
	signalCtx, stopSignal := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	budgetCtx, expire := context.WithTimeout(signalCtx, BootMigrationBudget)
	return budgetCtx, func() {
		expire()
		stopSignal()
	}
}

// RunBootSoleCopyMigration executes stage 1 and translates the outcome.
//
// The completion criterion is read FROM THE DATA: the pass ends when ONE RE-READ of the
// durable boundary finds nothing more below it. There is no config flag, persisted marker
// or boot counter — and their absence is the property. The re-read boundary is the state
// itself, measured now.
//
// The report is published on EVERY pass and on both outcomes: a pass that aborts at
// symbol 300 of 500 migrated 299 symbols, and a reader who sees "candles = 0" takes it
// for "nothing to do".
func RunBootSoleCopyMigration(ctx context.Context, run SoleCopyMigrationRun, logs ...*slog.Logger) BootMigrationOutcome {
	log := logger.OrDiscard(logs...)
	if run == nil {
		log.Info("sole-copy migration: not applicable (no durable tier or no declared cell)")
		return BootMigrationNotApplicable
	}

	var candles, commits int
	for pass := 1; pass <= BootMigrationMaxPasses; pass++ {
		// THE SIGNAL AND THE DEADLINE ARE CONSULTED BETWEEN PASSES, not only inside them.
		if err := ctx.Err(); err != nil {
			log.Error("sole-copy migration interrupted before the pass; retention gets no license at this boot",
				"pass", pass, "candles", candles, "commits", commits, "error", err)
			return BootMigrationIncomplete
		}

		report, err := run(ctx)
		candles += report.Candles()
		commits += report.Commits
		for _, cell := range report.Cells {
			log.Info("sole-copy migration pass",
				"pass", pass,
				"timeframe", cell.Timeframe,
				"keys", cell.Keys,
				"keys_with_sole_copy", cell.KeysWithSoleCopy,
				"keys_without_cold_rows", cell.KeysWithoutColdRows,
				"candles", cell.Candles)
		}

		if err != nil {
			log.Error("sole-copy migration aborted; retention gets no license at this boot",
				"pass", pass, "candles", candles, "commits", commits, "error", err)
			return BootMigrationIncomplete
		}

		// THE DATA SAYS IT IS OVER. The re-read boundary found nothing below it.
		// Truncated prevents confusion: a pass that stopped at the CEILING may also have
		// migrated zero in the next cell, and it did NOT finish.
		if report.Candles() == 0 && !report.Truncated {
			log.Info("sole-copy migration complete",
				"passes", pass, "candles", candles, "commits", commits)
			return BootMigrationComplete
		}
	}

	log.Error("sole-copy migration stopped at the pass/row ceiling; the next boot resumes from the re-read boundary and retention gets no license at this boot",
		"max_passes", BootMigrationMaxPasses,
		"candle_limit", SoleCopyFlushThreshold,
		"candles", candles,
		"commits", commits)
	return BootMigrationIncomplete
}
