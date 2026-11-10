package app

import (
	"context"

	"github.com/marxms/go-whale/internal/adapters/outbound/questdb"
	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
)

// STARTUP STAGE 1 — wiring of the sole-copy migration.
//
// THE RULE lives in the QuestDB adapter (internal/adapters/outbound/questdb/boot_migration.go):
// the termination criterion, the per-start ceiling, the signal-cancellable budget and the
// translation of the outcome. Here there is only the decision of WHEN the stage runs and the
// assembly of the production pass from the two tiers.
//
// # WHY AT CONSTRUCTION
//
// With `SoleCopyKeysAtRisk > 0`, `preflightGates` returns an error at the SECOND instruction of
// `Run`; nothing after the refusal executes. A migration placed there would sit behind the refusal
// it exists to remove. And after retention, retention would erase exactly what the migration just
// wrote.
//
// MUTUAL EXCLUSION IS BY CONSTRUCTION, NOT BY LEASE: the compose serializes by `container_name`
// and has no replicas. A lease in Redis would fail because preflight approves the boot when Redis
// is absent, because `SET NX PX` is a key with a TTL — the cohort `volatile-*` evicts first — and
// because there is no safe, alive TTL under `RestartPolicy=unless-stopped` with SIGKILL 10 s after
// SIGTERM.

// soleCopyMigrationFor assembles the production pass, or returns nil when there is nothing to do.
// nil is a legitimate, declared state, not an error.
//
// THE CELLS ARE THE SAME AS THE GUARD'S, and come from the same derivation
// (`soleCopyGuardTimeframes`). A second list here would be the divergence the gate exists to catch,
// written inside the work it demands.
func (a *App) soleCopyMigrationFor(hot *redisstore.RedisStore, cold *questdb.Store) questdb.SoleCopyMigrationRun {
	if hot == nil || cold == nil {
		return nil
	}
	cells := a.soleCopyCellsFromConfig()
	if len(cells) == 0 {
		return nil
	}
	migrator := questdb.NewSoleCopyMigrator(hot, cold, a.log)
	return func(ctx context.Context) (questdb.SoleCopyMigrationReport, error) {
		return migrator.Run(ctx, questdb.SoleCopyMigrationOptions{
			Timeframes: cells,
			// APPLY. A dry pass at startup would count the problem and leave the gate
			// refusing forever.
			Apply: true,
			// PER-PASS CEILING in the durable tier's commit unit. The standalone tool remains
			// without a ceiling: it runs a single pass under supervision.
			MaxCandles: questdb.SoleCopyFlushThreshold,
		})
	}
}
