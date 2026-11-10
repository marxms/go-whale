package app

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/questdb"
	"github.com/marxms/go-whale/internal/retention"
)

// THE POLICY CONSTRUCTION lives in internal/retention
// (internal/retention/policy_builders_test.go). These cases cover the LICENSE: who grants it, when
// and why.

// TestReleaseRetention_TheLicenceIsDeniedUntilTheGatesHaveSpoken — a constructed `App` that is
// never run drops no partition.
func TestReleaseRetention_TheLicenceIsDeniedUntilTheGatesHaveSpoken(t *testing.T) {
	license := &atomic.Bool{}
	exec := &countingRetentionExecutor{}
	mgr := retention.NewManager(retention.ManagerOptions{
		Executor:      exec,
		Policies:      []retention.Policy{{Table: questdb.DerivativesTableName(), RetentionDays: 365}},
		GatesApproved: license.Load,
		Interval:      time.Hour,
	})
	app := &App{gatesApproved: license, retentionMgr: mgr}

	if license.Load() {
		t.Fatal("the license was born granted — retention would run during App construction")
	}

	app.releaseRetention(t.Context())
	defer mgr.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for mgr.Passes() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !license.Load() {
		t.Error("releaseRetention did not grant the license")
	}
	if mgr.Passes() == 0 {
		t.Error("releaseRetention did not start the loop — the declared retention would never run")
	}
}

// countingRetentionExecutor is the license test's stub database: missing table, so the pass ends
// without touching anything.
type countingRetentionExecutor struct{}

func (countingRetentionExecutor) TableExists(_ context.Context, _ string) (bool, error) {
	return false, nil
}

func (countingRetentionExecutor) OldestTimestamp(_ context.Context, _ string) (time.Time, bool, error) {
	return time.Time{}, false, nil
}

func (countingRetentionExecutor) DropPartitionsBefore(_ context.Context, _ string, _ time.Time) error {
	return nil
}
