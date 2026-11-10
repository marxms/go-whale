package app

// app_test.go — THE LAST LINK of the refusal chain, the only one without an assertion.
//
// The refusal chain has four links and three are checked: `EnsureTables` returns
// `SchemaContractError` (`questdb_table_test.go`), `degradeOrRefuse` translates the contract class
// into an error (`startup_gates_test.go`), and `buildColdTier` returns `(nil, err)` for that class.
// The fourth — the `NewApp` line that translates that error into `return nil, qdbErr` — was not
// asserted anywhere, and replacing it with `qdbStore = nil` passes `go vet ./...` and
// `go test -race -shuffle=on -count=1 ./...`.
//
// And `qdbStore = nil` is NOT an innocuous state. It is literally the disarm described in `app.go`:
// `probeSoleCopy` returns `Evaluated: false`, the sole-copy gate falls from `GateFatal` to
// `GateError`, and a DDL error disables the guard over the 129,190 candles.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/marxms/go-whale/internal/adapters/outbound/questdb"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// configParaNewApp returns the minimal configuration that takes `NewApp` to the durable-tier
// branch: candle engine enabled (the branch lives inside it) and QuestDB enabled.
func configParaNewApp(t *testing.T) config.Config {
	t.Helper()
	mr := miniredis.RunT(t)
	return config.Config{
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
		QuestDB: config.QuestDBConfig{Enabled: true},
	}
}

// TestNewApp_ContractDivergenceDoesNotProceedWithANilStore ⭐ is the missing link:
// *"the system SHALL NOT proceed with a nil durable store"*.
//
// THE MUTANT IT KILLS: replacing `return nil, qdbErr` with `qdbStore = nil` and letting the start
// continue. No other case in the suite observes it — the `buildColdTier` cases stop one level
// below, and the gate cases build the `App` directly, without passing through this constructor.
func TestNewApp_ContractDivergenceDoesNotProceedWithANilStore(t *testing.T) {
	divergencia := fmt.Errorf("ensure: %w", &questdb.SchemaContractError{
		Table: "derivatives", Field: "partitionBy", Declared: "DAY", Read: "NONE",
	})

	app, err := NewApp(
		configParaNewApp(t),
		candle.NewTimeframeRegistry([]string{"1d", "1w"}),
		nil,
		withColdTierConstructor(func(config.QuestDBConfig) (*questdb.Store, error) {
			return nil, divergencia
		}),
	)

	if err == nil {
		t.Fatal("`NewApp` did NOT refuse the start under contract divergence — that is exactly " +
			"the state that disarms the sole-copy guard: without a durable tier, `probeSoleCopy` returns " +
			"Evaluated=false and the gate falls from FATAL to ERROR")
	}
	// THE APPLICATION DOES NOT EXIST. Returning the error is not enough: a version that returned
	// `(app, err)` would leave a constructed `App` within reach of whoever ignored the error — and
	// ignoring the error is exactly what `main` does NOT do, but the contract must hold without
	// depending on that.
	if app != nil {
		t.Fatalf("`NewApp` returned an `App` alongside the refusal — the process cannot start in "+
			"any state under contract divergence; qdbStore=%v", app.qdbStore)
	}
	if !errors.Is(err, questdb.ErrSchemaContract) {
		t.Fatalf("the refusal lost the contract chain — `errors.Is` is what separates this class "+
			"from unavailability: %v", err)
	}
	// The error crosses `NewApp` READABLE: the four read facts remain in it.
	for _, esperado := range []string{"derivatives", "partitionBy", "DAY", "NONE"} {
		if !strings.Contains(err.Error(), esperado) {
			t.Errorf("the refusal does not carry %q — an error that does not say where to look is not actionable: %v", esperado, err)
		}
	}
}

// TestNewApp_UnavailabilityStartsWithoutDurableTier is the SIBLING case, and it exists because
// without that difference the refusal above would be satisfied by a `NewApp` that refuses any
// QuestDB failure — trading a degradation for a crash, which is worse.
//
// The two inputs traverse the SAME `NewApp` line and produce OPPOSITE outcomes.
func TestNewApp_UnavailabilityStartsWithoutDurableTier(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	app, err := NewApp(
		configParaNewApp(t),
		candle.NewTimeframeRegistry([]string{"1d", "1w"}),
		log,
		withColdTierConstructor(func(config.QuestDBConfig) (*questdb.Store, error) {
			return nil, errors.New("dial tcp 127.0.0.1:8812: connection refused")
		}),
	)
	if err != nil {
		t.Fatalf("unavailability became a startup refusal — a streaming service that crashes "+
			"because the cold tier blinked is worse than one that degrades loudly: %v", err)
	}
	if app == nil {
		t.Fatal("`NewApp` returned `(nil, nil)`: the degradation outcome is the application running")
	}
	t.Cleanup(func() { _ = app.Shutdown(context.Background()) })
	if app.qdbStore != nil {
		t.Fatal("the constructor failed and the `App` kept a durable store")
	}

	var alto bool
	for _, record := range sink.Records() {
		if strings.Contains(record.Text(), "cold storage disabled") {
			alto = true
		}
	}
	if !alto {
		t.Fatalf("the degradation crossed `NewApp` in silence: %v", sink.Records())
	}
}
