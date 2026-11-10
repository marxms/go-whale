package app

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/marxms/go-whale/internal/adapters/outbound/questdb"
	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/candle/engine"
	"github.com/marxms/go-whale/internal/derivatives/collector"
	"github.com/marxms/go-whale/internal/platform/config"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// staticServerProbe serves synthetic server facts: miniredis does not speak `INFO memory` or
// `CONFIG GET zset-max-listpack-entries`, and the discrimination that matters here is "with this
// current policy, does the start fail?" — which only two controlled snapshots answer.
type staticServerProbe struct {
	policy   string
	listpack string
}

func (p staticServerProbe) ConfigGet(_ context.Context, parameter string) (map[string]string, error) {
	if parameter == "zset-max-listpack-entries" && p.listpack != "" {
		return map[string]string{parameter: p.listpack}, nil
	}
	return nil, errors.New("not declared in this double")
}

func (p staticServerProbe) InfoSection(_ context.Context, _ string) (string, error) {
	return "# Memory\r\nused_memory:1000\r\nmaxmemory:2000\r\nmaxmemory_policy:" + p.policy + "\r\n", nil
}

// soleCopyRemaining is the state measured in production: 297 of 500 `1d` keys with a candle older
// than the durable coverage, and the `cmd/candles-sole-copy-migrate` migration not yet run.
func soleCopyRemaining() engine.SoleCopyRisk {
	return engine.SoleCopyRisk{
		Evaluated: true,
		Cells: []engine.SoleCopyCellRisk{{
			Timeframe:        "1d",
			Keys:             500,
			KeysWithSoleCopy: 297,
			Sample:           []string{"BTC/USDT", "ETH/USDT"},
		}},
	}
}

// appOnMiniredis assembles the minimal App the preflight needs: a live hot tier and the scope.
func appOnMiniredis(t *testing.T, probe redisstore.RedisServerProbe) (*App, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	store, err := redisstore.NewRedisStore(t.Context(), config.Config{
		RedisAddr:          mr.Addr(),
		RedisPoolSize:      10,
		RedisDialTimeoutS:  1,
		RedisReadTimeoutS:  1,
		RedisWriteTimeoutS: 1,
		RedisPoolTimeoutS:  1,
		RedisMaxRetries:    3,
		CandleEngine:       config.CandleEngineConfig{Timeframes: []string{"1d"}, BufferSizes: map[string]int{"default": 800}},
	}, config.DefaultResidentDepth, candle.NewTimeframeRegistry([]string{"1d"}))
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	return &App{
		redisStore:        store,
		serverProbe:       probe,
		defaultExchange:   "binance",
		defaultMarketType: "future",
	}, store.Client()
}

// TestPreflightGates_SoleCopyUnderAllkeysLruRefusesTheBoot ⭐ is the exact scenario of a deploy
// that is authorized: the compose already carries `allkeys-lru` and the 129,190 sole-copy candles
// remain in Redis.
//
// THE MUTANT IT KILLS: removing the consumption of `config.FatalGateError` at the call site. Without
// it the verdict becomes an `a.log.Error` line and the service starts with the LRU free over
// `candles:1d`/`1w` — the least touched in the keyspace, therefore the FIRST to leave. Here the
// verdict is RETURNED, and `Run` propagates it to `main`.
func TestPreflightGates_SoleCopyUnderAllkeysLruRefusesTheBoot(t *testing.T) {
	tests := []struct {
		name       string
		policy     string
		soleCopy   engine.SoleCopyRisk
		wantRefusa bool
	}{
		{
			name:       "allkeys-lru with a remaining sole copy: the start FAILS",
			policy:     "allkeys-lru",
			soleCopy:   soleCopyRemaining(),
			wantRefusa: true,
		},
		{
			// `allkeys-lfu` is a legitimate alternative, and it puts the SAME keys in the queue: a
			// guard tied to the `allkeys-lru` string would be turned off by the recommended
			// alternative.
			name:       "allkeys-lfu with a remaining sole copy: fails the same",
			policy:     "allkeys-lfu",
			soleCopy:   soleCopyRemaining(),
			wantRefusa: true,
		},
		{
			name:       "allkeys-lru after the migration: starts",
			policy:     "allkeys-lru",
			soleCopy:   engine.SoleCopyRisk{Evaluated: true, Cells: []engine.SoleCopyCellRisk{{Timeframe: "1d", Keys: 500}}},
			wantRefusa: false,
		},
		{
			// NOT MEASURING IS NOT BEING CLEAN — and it does not stop the start either: a
			// momentarily down QuestDB would become a service that does not start, with no way back.
			name:       "allkeys-lru with the sole copy NOT MEASURED: loud alarm, the start proceeds",
			policy:     "allkeys-lru",
			soleCopy:   engine.SoleCopyRisk{Reason: "NENHUM tier durável configurado"},
			wantRefusa: false,
		},
		{
			// The policy that does not evict keys without a TTL does not reach the two cells.
			name:       "volatile-ttl with a remaining sole copy: the guard does not apply",
			policy:     "volatile-ttl",
			soleCopy:   soleCopyRemaining(),
			wantRefusa: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app, _ := appOnMiniredis(t, staticServerProbe{policy: tt.policy})

			err := app.preflightVerdict(context.Background(), app.serverFactsProbe(), app.redisStore.Client(), tt.soleCopy)

			if !tt.wantRefusa {
				if err != nil {
					t.Fatalf("the start was refused by a state that is not fatal: %v", err)
				}
				return
			}
			if !errors.Is(err, config.ErrStartupGate) {
				t.Fatalf("preflightVerdict = %v — the gate measured and did not act; want an ErrStartupGate chain", err)
			}
			for _, esperado := range []string{"sole-copy-vs-evict-everything-policy", tt.policy, "297", "RET-7", "BTC/USDT"} {
				if !strings.Contains(err.Error(), esperado) {
					t.Errorf("the refusal does not carry %q — an error that does not say where to look is not readable: %v", esperado, err)
				}
			}
		})
	}
}

// TestRun_RefusesToServeAnythingWhenTheStartupGateRefuses is the other half: the `PolicyIsFatal`
// switch on AND the error propagated by `Run`.
//
// The scene is `volatile-ttl` back in the compose with resident `candles` without a TTL — the
// regression the ratchet exists to prevent. `Run` must return the error BEFORE starting anything:
// the App in this test has no manager, no metrics collector and no HTTP server, so proceeding is not
// a silent alternative.
func TestRun_RefusesToServeAnythingWhenTheStartupGateRefuses(t *testing.T) {
	app, client := appOnMiniredis(t, staticServerProbe{policy: "volatile-ttl"})

	// `candles` resident and WITHOUT a TTL: immortal under a policy that only evicts keys with a
	// TTL.
	if err := client.ZAdd(context.Background(), "candles:binance:future:BTC/USDT:1d",
		redis.Z{Score: 1, Member: "a"}).Err(); err != nil {
		t.Fatalf("seed candles: %v", err)
	}

	err := app.Run(context.Background())

	if !errors.Is(err, config.ErrStartupGate) {
		t.Fatalf("Run = %v — the refused configuration did not stop the service from operating", err)
	}
	if !strings.Contains(err.Error(), "eviction-policy-vs-immortal-families") {
		t.Fatalf("the startup error does not name the rule that refused: %v", err)
	}
	if !strings.Contains(err.Error(), "candles") {
		t.Fatalf("the startup error does not name the unrecoverable family: %v", err)
	}
}

// TestPreflightGates_UnreadableHotTierIsNotAVerdict — without a client there are no two read
// numbers, and refusing against the absence of measurement would be a sentence without evidence.
func TestPreflightGates_UnreadableHotTierIsNotAVerdict(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	app := &App{log: log}
	if err := app.preflightGates(context.Background()); err != nil {
		t.Fatalf("preflightGates = %v — the absence of measurement became a verdict", err)
	}

	var declared bool
	for _, record := range sink.Records() {
		if strings.Contains(record.Text(), "preflight gate not evaluated") {
			declared = true
		}
	}
	if !declared {
		t.Fatalf("the non-evaluation was not declared: %v", sink.Records())
	}
}

// TestPreflightGates_FatalIsReturnedNotLogged — an error is logged OR propagated, never both. The
// rule is not aesthetic: "log and continue" made this guard print instead of stopping the process,
// and a `[GATE] REPROVA` line in the log is exactly what would make the fix look applied.
func TestPreflightGates_FatalIsReturnedNotLogged(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	app, _ := appOnMiniredis(t, staticServerProbe{policy: "allkeys-lru"})
	app.log = log
	err := app.preflightVerdict(context.Background(), app.serverFactsProbe(), app.redisStore.Client(), soleCopyRemaining())
	if !errors.Is(err, config.ErrStartupGate) {
		t.Fatalf("preflightVerdict = %v, want an ErrStartupGate chain", err)
	}

	for _, record := range sink.Records() {
		if strings.Contains(record.Text(), "sole-copy-vs-evict-everything-policy") {
			t.Fatalf("the fatal line was PRINTED in addition to being returned: %q", record.Text())
		}
	}
}

// TestPreflight_ADeclaredColumnOutsideTheGuardRefusesTheBoot is the WIRING half of the column
// order.
//
// The gate compares two sets, and a gate fed with nil never refuses: it becomes the sensor with the
// cable unplugged. Here the refusal crosses the whole path — `preflightVerdict` returns an error,
// and `Run` propagates it to `main`.
func TestPreflight_ADeclaredColumnOutsideTheGuardRefusesTheBoot(t *testing.T) {
	app, client := appOnMiniredis(t, staticServerProbe{policy: "allkeys-lru"})
	app.durableOrder = func() ([]string, []string) {
		return []string{"funding_rate", "n_step"}, []string{"funding_rate"}
	}

	err := app.preflightVerdict(context.Background(), app.serverFactsProbe(), client, engine.SoleCopyRisk{})
	if !errors.Is(err, config.ErrStartupGate) {
		t.Fatalf("preflightVerdict = %v, wanted the startup refusal — a column written outside "+
			"the protection is erased by dedup on the first degraded cycle", err)
	}
	if !strings.Contains(err.Error(), "n_step") {
		t.Fatalf("the refusal does not name the unprotected column: %v", err)
	}
}

// TestPreflight_TheRealSetsOfThisBuildDoNotRefuseTheBoot is the other side, and it runs on the REAL
// values: the coverage declaration and what the protection actually carries.
//
// Without it, the case above would pass in a build whose gate always refuses.
func TestPreflight_TheRealSetsOfThisBuildDoNotRefuseTheBoot(t *testing.T) {
	app, client := appOnMiniredis(t, staticServerProbe{policy: "allkeys-lru"})

	declaradas, protegidas := app.durableOrderSets()
	if len(declaradas) == 0 || len(protegidas) == 0 {
		t.Fatalf("the wiring read %d declared and %d protected — an empty set makes the gate "+
			"pass vacuously", len(declaradas), len(protegidas))
	}

	if err := app.preflightVerdict(context.Background(), app.serverFactsProbe(), client,
		engine.SoleCopyRisk{}); err != nil {
		t.Fatalf("the real sets of this build refuse the start: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// THE CONJUNCTION BETWEEN THE REAL MATRIX AND THE GATE.
//
// The gate has its own test in `internal/platform/config` over hand-built matrices, and the matrix
// has its own test in `internal/derivatives/collector`. Each half being right is EXACTLY how `1w`
// entered the durable coverage and produced a WARN every cycle for a whole deploy. This case is the
// conjunction: the matrix the binary actually publishes, against the registry the configuration
// actually delivers.
// ─────────────────────────────────────────────────────────────────────────────

func TestPreflight_TheRealSupportMatrixDoesNotRefuseTheProductionRegistry(t *testing.T) {
	registro := productionTimeframeRegistry()
	in := config.GateInputs{
		RegisteredTimeframes: registro,
		TimeframeSupport:     collector.TimeframeSupportRows(registro),
	}

	if err := config.FatalGateError(config.EvaluatePreflightGates(in)); err != nil {
		t.Fatalf("the real matrix refuses the production registry: %v", err)
	}

	// AND THE VERDICT IS INFO, not a warning. A WARN here would mean a registered timeframe nobody
	// measured — the category the gate exists to make visible, and one that should not exist in an
	// uneventful production registry.
	found := false
	for _, r := range config.EvaluatePreflightGates(in) {
		if r.Name != "timeframe-vs-any-source-support" {
			continue
		}
		found = true
		if r.Severity != config.GateInfo {
			t.Errorf("verdict %s over the production registry: %s", r.Severity, r.Message)
		}
	}
	if !found {
		t.Fatal("the per-timeframe coverage gate does not run in the preflight — the refusal would be a function value nobody reads")
	}
}

// AND THE WEEKLY, BY NAME. It is the timeframe that motivated the gate and the one it must NOT
// refuse: no durable tier, but served by the venue's settlement feed.
func TestPreflight_TheWeeklySurvivesTheRealMatrix(t *testing.T) {
	rows := collector.TimeframeSupportRows(productionTimeframeRegistry())

	servido := false
	for _, row := range rows {
		for _, tf := range row.Served {
			if tf == "1w" {
				servido = true
			}
		}
	}
	if !servido {
		t.Error("no cell of the real matrix serves 1w — the gate would refuse it, and it has 30 weekly points measured by the route")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// THE VERDICT OF THE RULE AGAINST THE REAL DECLARATION.
//
// The cases in `internal/platform/config` exercise the rule over synthetic coverages. This one
// exercises the SAME rule over the three production sources — the coverage declaration, the column
// list the read asks for and the durable-timeframe derivation —, because this is where the three
// meet. Without it, the question "are today's cells cappable?" would remain answered by human
// reading.
// ─────────────────────────────────────────────────────────────────────────────

// TestCapCoverage_TheRealDeclarationLeavesTheShortTimeframesUncappable ⭐
//
// The durable tier was widened from three to SEVEN timeframes, and `5m`/`15m`/`30m` entered. That
// looks like it authorizes their cap, and it does NOT: the widening is on the TIMEFRAME axis, and
// the quantity no column carries is on the FIELD axis. The member the cap removes takes it along.
//
// Six of the seven fields on the FIELD axis got columns, and the answer did NOT change: one is
// enough for the cell to stay uncappable.
func TestCapCoverage_TheRealDeclarationLeavesTheShortTimeframesUncappable(t *testing.T) {
	app := appWithRegistry(productionTimeframeRegistry())
	cobertura := app.durableCellCoverage()

	// They were SEVEN, it became ONE — and this case refused at the turn, which is exactly what the
	// previous comment promised: "a field that gained a column would leave here refusing this case,
	// which is the right moment to re-evaluate the cap".
	//
	// ⭐ AND THE RE-EVALUATION GIVES THE SAME VERDICT. `UncoveredMemberFields` iterates ALL declared
	// fields, so ONE field without a column is enough for the cell to stay uncappable. Six new
	// columns, written and readable, and the cap is still refused for `oiChangePct` — the quantity
	// that `dropPollToPollOIChange` zeroes on the series path and that, therefore, has nothing to
	// write and nothing to read. **Twelve of thirteen is not a released cap**, and it is this case
	// that stops someone from reading the new coverage as permission.
	semColuna := []string{"oiChangePct"}

	for _, tf := range []string{"5m", "15m", "30m"} {
		t.Run(tf, func(t *testing.T) {
			findings := cobertura.UncoveredMemberFields(tf)
			if len(findings) != len(semColuna) {
				t.Fatalf("%s: %d uncovered field(s), wanted %d — %v", tf, len(findings), len(semColuna), findings)
			}
			for i, field := range semColuna {
				if !strings.HasPrefix(findings[i], field+" (no durable column") {
					t.Fatalf("%s: finding %d = %q, wanted %q without a durable column", tf, i, findings[i], field)
				}
			}
		})
	}

	// And the gate, with an entry that SHORTENS any of the three, refuses the start naming the three
	// cells.
	gate := resultFor(t, config.EvaluateGates(config.GateInputs{
		CapEntriesByTimeframe: map[string]int64{"5m": 291, "15m": 291, "30m": 291},
		DefaultSeriesCap:      int64(config.DefaultResidentDepth),
		DurableCellCoverage:   cobertura,
	}), "cap-entry-vs-durable-cell-coverage")

	if gate.Severity != config.GateFatal {
		t.Fatalf("severity = %s against the REAL declaration, wanted GateFatal: %s", gate.Severity, gate.Message)
	}
	for _, tf := range []string{"5m", "15m", "30m"} {
		if !strings.Contains(gate.Message, tf+":") {
			t.Errorf("the refusal does not name cell %s: %s", tf, gate.Message)
		}
	}
	if !strings.Contains(gate.Message, "oiChangePct ") {
		t.Errorf("the refusal does not name any of the fields without a column: %s", gate.Message)
	}
}

// resultFor finds the row of one gate by name.
func resultFor(t *testing.T, results []config.GateResult, name string) config.GateResult {
	t.Helper()
	for _, r := range results {
		if r.Name == name {
			return r
		}
	}
	t.Fatalf("gate %q did not appear in the verdict: %v", name, results)
	return config.GateResult{}
}

// ─────────────────────────────────────────────────────────────────────────────
// THE SOLE-COPY GUARD STOPS BEING TWO TYPED STRINGS.
// ─────────────────────────────────────────────────────────────────────────────

// TestSoleCopyGuardTimeframes_AreDerivedFromTheReachAgainstTheDurableWindow ⭐
//
// TWO MUTANTS, PULLING IN OPPOSITE DIRECTIONS.
//
// (1) The return of the literal `[]string{"1d","1w"}`. A typed list answers the SAME thing for any
// configuration, and it is that invariance the window and raised-cap cases attack: different
// configurations must produce different guards.
//
// (2) The PREVIOUS derivation, which built the guarded set as *timeframes without a cap entry*. It
// satisfied (1) and was still disarmable by one config command: `candleCapFor` falls back to the
// inherited cap exactly when there is no entry, so the two sets were the same by construction, and
// adding `1w: 800` — the SAME number the cell already inherits — removed `1w` from the guard
// without erasing a candle. The three cases below are what kill that mutant.
//
// The question still comes from one place, but now it is the REASON and not the proxy: a cell is
// guarded BECAUSE its ZSET reaches deeper than the durable window — literally what the YAML comment
// next to the cap key says.
func TestSoleCopyGuardTimeframes_AreDerivedFromTheReachAgainstTheDurableWindow(t *testing.T) {
	const janelaDeployada = 365

	tests := []struct {
		name          string
		ce            config.CandleEngineConfig
		retentionDays int
		want          []string
	}{
		{
			name: "the deployed declaration guards 1d and 1w",
			ce: config.CandleEngineConfig{
				Timeframes:          productionTimeframeRegistry(),
				BufferSizes:         map[string]int{"default": 800},
				RedisCapByTimeframe: map[string]int{"5m": 521, "15m": 521, "30m": 521, "1h": 521, "2h": 521, "4h": 521},
			},
			retentionDays: janelaDeployada,
			want:          []string{"1d", "1w"},
		},
		{
			// THE MEASURED DISARM. `1w: 800` is the SAME number the cell inherits from
			// `buffer_sizes.default`: it erases no candle, changes no log, and under the previous
			// derivation it removed `1w` from the guard. It is the silent twin of `CONFIG SET`.
			name: "declaring for 1w the cap it already inherits does NOT remove it from the guard",
			ce: config.CandleEngineConfig{
				Timeframes:          productionTimeframeRegistry(),
				BufferSizes:         map[string]int{"default": 800},
				RedisCapByTimeframe: map[string]int{"5m": 521, "15m": 521, "30m": 521, "1h": 521, "2h": 521, "4h": 521, "1w": 800},
			},
			retentionDays: janelaDeployada,
			want:          []string{"1d", "1w"},
		},
		{
			// THE ENTRY THAT SHORTENS. Worse than the previous one: here the entry DESTROYS
			// members. If it also erased the question, the disarm and the damage would come on the
			// same YAML line.
			name: "a cap entry that SHORTENS 1w also does not remove it from the guard",
			ce: config.CandleEngineConfig{
				Timeframes:          productionTimeframeRegistry(),
				BufferSizes:         map[string]int{"default": 800},
				RedisCapByTimeframe: map[string]int{"5m": 521, "15m": 521, "30m": 521, "1h": 521, "2h": 521, "4h": 521, "1w": 52},
			},
			retentionDays: janelaDeployada,
			want:          []string{"1d", "1w"},
		},
		{
			// The same rule on the other guarded cell.
			name: "capping 1d does not remove it from the guard",
			ce: config.CandleEngineConfig{
				Timeframes:          productionTimeframeRegistry(),
				BufferSizes:         map[string]int{"default": 800},
				RedisCapByTimeframe: map[string]int{"5m": 521, "15m": 521, "30m": 521, "1h": 521, "2h": 521, "4h": 521, "1d": 521},
			},
			retentionDays: janelaDeployada,
			want:          []string{"1d", "1w"},
		},
		{
			// THE GUARD IS NOT A TYPED {1d,1w}, and this case proves it. A cap entry can ADD a
			// cell: 2,500 4h buckets reach 416 days, beyond the 365-day window — and the cell can
			// now guard what exists only in Redis.
			name: "raising the 4h cap beyond the durable window ADDS it to the guard",
			ce: config.CandleEngineConfig{
				Timeframes:          productionTimeframeRegistry(),
				BufferSizes:         map[string]int{"default": 800},
				RedisCapByTimeframe: map[string]int{"5m": 521, "15m": 521, "30m": 521, "1h": 521, "2h": 521, "4h": 2500},
			},
			retentionDays: janelaDeployada,
			want:          []string{"4h", "1d", "1w"},
		},
		{
			// ABSENCE OF MEASUREMENT IS NOT APPROVAL. Without a declared durable window there is
			// nothing to compare against, and declaring a cell covered would be approving for lack
			// of a probe.
			name:          "with no declared durable window, EVERY cell stays in the guard",
			ce:            config.CandleEngineConfig{Timeframes: []string{"1h", "1d"}},
			retentionDays: 0,
			want:          []string{"1h", "1d"},
		},
		{
			// A zeroed entry is not a declaration — and, under the current derivation, no entry
			// changes nothing either: `1h` stays out because 800 1h buckets reach 33 days, inside
			// the window, and not because someone declared a number for it.
			name: "an entry with value zero does not change the guard",
			ce: config.CandleEngineConfig{
				Timeframes:          []string{"1h", "1d"},
				RedisCapByTimeframe: map[string]int{"1h": 0},
			},
			retentionDays: janelaDeployada,
			want:          []string{"1d"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &App{cfg: config.Config{
				CandleEngine: tt.ce,
				QuestDB:      config.QuestDBConfig{RetentionDays: tt.retentionDays},
			}}
			got := app.soleCopyCellsFromConfig()
			if strings.Join(got, ",") != strings.Join(tt.want, ",") {
				t.Fatalf("soleCopyGuardTimeframes() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// CONTRACT DIVERGENCE REFUSES THE START; UNAVAILABILITY DOES NOT
// ─────────────────────────────────────────────────────────────────────────────

// TestBuildColdTier_ContractDivergenceRefusesTheStart ⭐.
//
// THE MUTANT IT KILLS: treating divergence as unavailability — returning `(nil, nil)` and letting
// `NewApp` continue. That is the exact state that disarms the guard: with a nil durable tier the
// sole copy is not MEASURED, and the case "sole copy NOT MEASURED: loud alarm, the start proceeds"
// in `TestPreflightGates_SoleCopyUnderAllkeysLruRefusesTheBoot` proves the gate falls from FATAL to
// ERROR. A DDL error would disable the protection over the 129,190 candles.
func TestBuildColdTier_ContractDivergenceRefusesTheStart(t *testing.T) {
	divergencia := fmt.Errorf("ensure: %w", &questdb.SchemaContractError{
		Table: "derivatives", Field: "partitionBy", Declared: "DAY", Read: "NONE",
	})

	store, err := buildColdTier(config.QuestDBConfig{}, func(config.QuestDBConfig) (*questdb.Store, error) {
		return nil, divergencia
	}, nil)

	if err == nil {
		t.Fatal("the contract divergence did NOT refuse the start — the nil durable tier disarms the sole-copy guard")
	}
	if store != nil {
		t.Fatal("the refusal returned a store: the start cannot proceed with a durable tier in any state")
	}
	if !errors.Is(err, questdb.ErrSchemaContract) {
		t.Fatalf("the refusal lost the contract chain: %v", err)
	}
	for _, esperado := range []string{"derivatives", "partitionBy", "DAY", "NONE"} {
		if !strings.Contains(err.Error(), esperado) {
			t.Errorf("the refusal does not carry %q — an error that does not say where to look is not readable: %v", esperado, err)
		}
	}
}

// TestBuildColdTier_UnavailabilityKeepsDegrading is the other half of the separation: a QuestDB that
// is down at startup must NOT become a service that does not start, because that has no way back. It
// degrades loudly, exactly as it always has.
func TestBuildColdTier_UnavailabilityKeepsDegrading(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	store, err := buildColdTier(config.QuestDBConfig{}, func(config.QuestDBConfig) (*questdb.Store, error) {
		return nil, errors.New("dial tcp 127.0.0.1:8812: connection refused")
	}, log)
	if err != nil {
		t.Fatalf("unavailability became a startup refusal: %v", err)
	}
	if store != nil {
		t.Fatal("the constructor failed and returned a store")
	}

	var alto bool
	for _, record := range sink.Records() {
		if strings.Contains(record.Text(), "cold storage disabled") {
			alto = true
		}
	}
	if !alto {
		t.Fatalf("the degradation was not declared loudly: %v", sink.Records())
	}
}

// TestBuildColdTier_OnlyTheTwoClassesAreSeparated fixes the property that names the fix: the TWO
// inputs traverse the same path and produce OPPOSITE outcomes. An implementation that lost the
// classification would pass the two tests above if it always degraded — this one requires the
// difference.
func TestBuildColdTier_OnlyTheTwoClassesAreSeparated(t *testing.T) {
	construtor := func(err error) questDBConstructor {
		return func(config.QuestDBConfig) (*questdb.Store, error) { return nil, err }
	}

	_, contratoErr := buildColdTier(config.QuestDBConfig{},
		construtor(&questdb.SchemaContractError{Table: "candles", Field: "dedup", Declared: "true", Read: "false"}), nil)
	_, indispErr := buildColdTier(config.QuestDBConfig{},
		construtor(errors.New("i/o timeout")), nil)

	if contratoErr == nil || indispErr != nil {
		t.Fatalf("the two classes did not separate: contract=%v unavailability=%v", contratoErr, indispErr)
	}
}
