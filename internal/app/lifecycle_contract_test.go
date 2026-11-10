package app

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"
)

// TestNewApp_StorageBuilderReturnsErrorInsteadOfPanicking proves the builder contract:
// a dependency that cannot be constructed is returned as an error, not raised as a panic
// and not hidden behind a half-built graph.
func TestNewApp_StorageBuilderReturnsErrorInsteadOfPanicking(t *testing.T) {
	cfg := config.Config{
		RedisAddr: "127.0.0.1:1",
		// PoolSize > 1 avoids go-redis's pool-level tryDial sleeper, which is only
		// spawned when dial errors reach the pool size. The assertion is the builder's
		// error, not a library retry cadence.
		RedisPoolSize:      10,
		RedisDialTimeoutS:  1,
		RedisReadTimeoutS:  1,
		RedisWriteTimeoutS: 1,
		RedisPoolTimeoutS:  1,
		RedisMaxRetries:    0,
		CandleEngine: config.CandleEngineConfig{
			Enabled:     true,
			Timeframes:  []string{"5m"},
			BufferSizes: map[string]int{"default": 10},
		},
	}

	app, err := NewApp(cfg, candle.NewTimeframeRegistry(cfg.CandleEngine.Timeframes), nil)
	if err == nil {
		t.Fatal("NewApp accepted an unreachable Redis — the builder swallowed the error")
	}
	if app != nil {
		t.Fatal("NewApp returned an App alongside the error — the partial graph would be within reach of whoever ignored the error")
	}
}

// TestShutdown_OrderIsObservableAndIdempotent fixes the shutdown sequence and proves a
// second call does not run it again.
//
// The order is the one that loses the least work: intake, timers, producers, stores, HTTP.
// It is observable through the test seam rather than by replacing subsystems, so a future
// reorder fails here even when every individual Stop is nil-safe.
func TestShutdown_OrderIsObservableAndIdempotent(t *testing.T) {
	var trace []string
	app := &App{shutdownTrace: &trace}

	if err := app.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown of a partial graph: %v", err)
	}
	if got, want := strings.Join(trace, ","), "intake,timers,producers,stores,http"; got != want {
		t.Fatalf("shutdown order = %q, want %q", got, want)
	}

	// A SECOND CALL IS A NO-OP. It must not stop the same stores twice.
	if err := app.Shutdown(t.Context()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
	if got := len(trace); got != 5 {
		t.Fatalf("the second Shutdown repeated %d phase(s) — shutdown is not idempotent", got-5)
	}
}

// TestShutdown_PartiallyConstructedGraphIsSafe covers the graph a failed builder leaves
// behind: every subsystem is nil, and closing it must still be a clean, ordered shutdown.
func TestShutdown_PartiallyConstructedGraphIsSafe(t *testing.T) {
	app := &App{}
	if err := app.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown of an empty App: %v", err)
	}
}

// TestAppProductionFilesContainNoDomainRule is the mechanical half of the composition-root rule set.
//
// RULE SET:
//
//	R1. app must not DEFINE a domain rule. The names below are the rules moved to their
//	    owning packages; a new definition here means the rule came back.
//	R2. app production code must not spell Redis key layout, SQL or vendor vocabulary.
//	    Those literals live in the Redis adapter, the QuestDB adapter and the exchange
//	    adapter respectively. Only string literals are inspected, so comments are not
//	    constrained by this scan.
//
// Together with the file split (build_storage/build_ingestion/build_recovery/
// build_derivatives/build_http/build_observability/run/shutdown), these are the rules
// this repository uses to claim "app is composition root and lifecycle".
func TestAppProductionFilesContainNoDomainRule(t *testing.T) {
	forbiddenFuncs := map[string]string{
		"discoveryHealthy":                "market",
		"enforceMaxAssets":                "market",
		"autoSubscribeInitBudget":         "ingestion",
		"waitWithTimeout":                 "ingestion",
		"reconcileGapsOnceWithDisconnect": "recovery",
		"scopeDisconnectPredicate":        "recovery",
		"isStale":                         "recovery",
		"bootGapMarkTimeframes":           "candle",
		"enqueueBootGap":                  "recovery",
		"awaitSymbolDelivery":             "recovery",
		"checkOpenTimeContinuity":         "candle/engine",
		"pruneClosedOpenTime":             "candle/engine",
		"liveKeepSet":                     "redis adapter",
		"sampleFleetLiveness":             "health",
		"verifiedListingFloors":           "market",
		"validSymbolsFiltered":            "market",
		"configuredSymbolCount":           "market",
		"decideCoinalyze":                 "derivatives/collector",
		"coinalyzeDeclaredCadence":        "derivatives/collector",
		"coinalyzeSymbolsOf":              "derivatives/collector",
		"reportCoinalyzeCadence":          "derivatives/collector",
		"fleetReady":                      "derivatives/collector",
		"candlesRetentionPolicy":          "retention",
		"candlesDeliberateHorizon":        "retention",
		"derivativesRetentionDays":        "retention",
		"residentReach":                   "candle",
		"newDerivativesDiskBudget":        "derivatives/collector",
		"derivativesBudgetLines":          "platform/ops",
		"buildSizingSheetFrom":            "platform/ops",
		"publishSizingSheet":              "platform/ops",
		"aggregateSizingMiB":              "platform/ops",
		"sampleImmortalFamilies":          "platform/ops",
		"isImmortalTTL":                   "platform/ops",
		"runBootSoleCopyMigration":        "questdb adapter",
		"bootMigrationContext":            "questdb adapter",
		"derivativesRetainedWindow":       "retention",
	}

	// R2. Exact vendor vocabulary and key-pattern prefixes. Log format strings may mention
	// the word as prose, so exact equality (not substring) is used for the vendor names.
	exactVendor := map[string]bool{
		"binance": true,
		"bingx":   true,
		"future":  true,
		"swap":    true,
		"spot":    true,
	}
	keyPatterns := []string{
		"active_symbols:",
		"candles:",
		"derivatives:series:",
	}
	sqlTokens := []string{
		"SELECT ",
		"INSERT INTO ",
		"ALTER TABLE ",
		"DROP PARTITION ",
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	checked := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		checked++

		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}

		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.FuncDecl:
				if owner, forbidden := forbiddenFuncs[n.Name.Name]; forbidden {
					t.Errorf("%s: app defines %q — that rule belongs to %s", name, n.Name.Name, owner)
				}
			case *ast.BasicLit:
				if n.Kind != token.STRING {
					return true
				}
				value, unquoteErr := strconv.Unquote(n.Value)
				if unquoteErr != nil {
					return true
				}
				if exactVendor[value] {
					t.Errorf("%s: vendor vocabulary %q in app — use market/adapter", name, value)
				}
				for _, pattern := range keyPatterns {
					if strings.Contains(value, pattern) {
						t.Errorf("%s: Redis key layout %q in app — belongs to the Redis adapter", name, pattern)
					}
				}
				for _, token := range sqlTokens {
					if strings.Contains(value, token) {
						t.Errorf("%s: SQL %q in app — belongs to the QuestDB adapter", name, token)
					}
				}
			}
			return true
		})
	}

	if checked < 10 {
		t.Fatalf("only %d production file(s) inspected — the scan did not cover app", checked)
	}
}
