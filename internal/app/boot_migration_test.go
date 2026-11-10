package app

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/marxms/go-whale/internal/adapters/outbound/questdb"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"
)

// THE RULE of the sole-copy migration lives in the QuestDB adapter
// (internal/adapters/outbound/questdb/boot_migration_test.go). These cases cover what stays in
// `app`: when the stage runs, how the pass is assembled and what happens after a refusal.

// TestSoleCopyMigrationFor_WithoutDurableTierReturnsNil — assembly refuses before trying, and the
// cells it would declare are the SAME as the guard's.
func TestSoleCopyMigrationFor_WithoutDurableTierReturnsNil(t *testing.T) {
	app := &App{cfg: config.Config{CandleEngine: config.CandleEngineConfig{Timeframes: []string{"1d", "1w"}}}}
	if run := app.soleCopyMigrationFor(nil, nil); run != nil {
		t.Fatal("the pass was assembled without a hot or durable tier")
	}
}

// TestNewApp_TheMigrationStageHappensAtConstruction ⭐ is the positive half: stage 1 is decided
// INSIDE `NewApp`, before `Run` exists to be called.
//
// THE MUTANT IT KILLS: removing the call from `NewApp` (or moving it to `Run`). Without it the
// field stays at the zero value — "not executed" —, and the zero value is exactly what this test
// refuses. With QuestDB disabled the correct outcome is "not applicable", which is a DECIDED fact,
// not the absence of a decision.
func TestNewApp_TheMigrationStageHappensAtConstruction(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := config.Config{
		RedisAddr:          mr.Addr(),
		RedisPoolSize:      10,
		RedisDialTimeoutS:  1,
		RedisReadTimeoutS:  1,
		RedisWriteTimeoutS: 1,
		RedisPoolTimeoutS:  1,
		RedisMaxRetries:    3,
		CandleEngine: config.CandleEngineConfig{
			Enabled:     true,
			Timeframes:  []string{"1d", "1w"},
			BufferSizes: map[string]int{"default": 800},
		},
	}

	app, err := NewApp(cfg, candle.NewTimeframeRegistry(cfg.CandleEngine.Timeframes), nil)
	if err != nil {
		t.Fatalf("NewApp: %v", err)
	}
	t.Cleanup(func() { _ = app.Shutdown(context.Background()) })

	if app.bootMigration == questdb.BootMigrationNotRun {
		t.Fatal("`NewApp` returned without deciding stage 1 — the migration does not run at construction")
	}
	if app.bootMigration != questdb.BootMigrationNotApplicable {
		t.Fatalf("outcome = %v — without a durable tier the stage is `not applicable`", app.bootMigration)
	}
}

// TestRun_NothingAfterTheRefusalExecutes ⭐ is the NEGATIVE half: the proof that `Run` is not the
// place for the migration.
//
// With a remaining sole copy under a policy that evicts everything, `preflightGates` returns an
// error at the SECOND instruction of `Run` and the return happens there. The immediately following
// step — `releaseRetention`, which grants the license — does not execute.
func TestRun_NothingAfterTheRefusalExecutes(t *testing.T) {
	app, client := appOnMiniredis(t, staticServerProbe{policy: "volatile-ttl"})
	app.gatesApproved = &atomic.Bool{}

	// `candles` resident and WITHOUT a TTL: immortal under a policy that only evicts keys with a
	// TTL.
	if err := client.ZAdd(context.Background(), "candles:binance:future:BTC/USDT:1d",
		redis.Z{Score: 1, Member: "a"}).Err(); err != nil {
		t.Fatalf("seed candles: %v", err)
	}

	if err := app.Run(context.Background()); err == nil {
		t.Fatal("the gate did not refuse — the test scenario did not arm itself")
	}
	if app.gatesApproved.Load() {
		t.Fatal("a step AFTER `preflightGates` executed after the refusal — the migration's position premise falls")
	}
}
