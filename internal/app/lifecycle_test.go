package app

import (
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/questdb"
	logger "github.com/marxms/go-whale/internal/platform/logging"
	"github.com/marxms/go-whale/internal/retention"
)

// appComRetencao assembles the license + manager pair that stage 3 grants, with the same shape as
// the license test in `retention_wiring_test.go`.
func appComRetencao(t *testing.T, log *slog.Logger, desfecho questdb.BootMigrationOutcome) (*App, *atomic.Bool, *retention.Manager) {
	t.Helper()
	licenca := &atomic.Bool{}
	mgr := retention.NewManager(retention.ManagerOptions{
		Executor:      countingRetentionExecutor{},
		Policies:      []retention.Policy{{Table: questdb.DerivativesTableName(), RetentionDays: 365}},
		GatesApproved: licenca.Load,
		Interval:      time.Hour,
		Log:           log,
	})
	t.Cleanup(mgr.Stop)
	return &App{log: log, gatesApproved: licenca, retentionMgr: mgr, bootMigration: desfecho}, licenca, mgr
}

// TestReleaseRetention_IncompleteMigrationWithholdsTheLicence ⭐.
//
// THE MUTANT IT KILLS: an unconditional `releaseRetention`. The migration wrote 130,284 candles
// and retention erased them on the next start, because the window is global and `DROP PARTITION`
// does not filter by timeframe. Aborting and releasing in the SAME start is the same scene
// compressed: retention drops exactly the partitions the next pass needs to find again below the
// re-read boundary.
func TestReleaseRetention_IncompleteMigrationWithholdsTheLicence(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	app, licenca, mgr := appComRetencao(t, log, questdb.BootMigrationIncomplete)

	app.releaseRetentionUnderMigrationLicence(t.Context())

	// A generous window: the defect would be the license appearing, not taking long.
	deadline := time.Now().Add(300 * time.Millisecond)
	for mgr.Passes() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if licenca.Load() {
		t.Fatal("the license was granted with the migration INCOMPLETE — retention would erase what the next pass needs")
	}
	if mgr.Passes() != 0 {
		t.Fatalf("%d retention pass(es) with the migration INCOMPLETE — the loop started", mgr.Passes())
	}

	var linhas string
	for _, record := range sink.Records() {
		linhas += record.Text() + "\n"
	}
	if !strings.Contains(linhas, "no license") {
		t.Errorf("withheld retention was not declared: %s", linhas)
	}
}

// TestReleaseRetention_OutcomesThatDidNotAbortRelease — the other half, and it is what stops the
// fix from becoming "retention never runs again".
//
// The rule is about a migration that ABORTS. `complete` did the work; `not applicable` had nothing
// to do; `not run` is the binary without a durable tier, which does not even have a retention
// manager. Withholding the license in any of the three would disable the declared retention
// forever.
func TestReleaseRetention_OutcomesThatDidNotAbortRelease(t *testing.T) {
	casos := []struct {
		nome     string
		desfecho questdb.BootMigrationOutcome
	}{
		{"complete", questdb.BootMigrationComplete},
		{"not applicable", questdb.BootMigrationNotApplicable},
		{"not run", questdb.BootMigrationNotRun},
	}

	for _, caso := range casos {
		t.Run(caso.nome, func(t *testing.T) {
			app, licenca, mgr := appComRetencao(t, nil, caso.desfecho)

			app.releaseRetentionUnderMigrationLicence(t.Context())

			deadline := time.Now().Add(2 * time.Second)
			for mgr.Passes() == 0 && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			if !licenca.Load() {
				t.Errorf("the license was not granted with migration %s", caso.desfecho)
			}
			if mgr.Passes() == 0 {
				t.Errorf("the retention loop did not start with migration %s", caso.desfecho)
			}
		})
	}
}
