package app

// startup_gates.go — THE STARTUP REFUSAL, and why it could not live on the sheet.
//
// # THE DEFECT THIS FILE REMOVES
//
// The sole-copy guard measured correctly and did not act: the only consumer of
// `config.FatalGateError` in production was an `a.logger().Error` inside `reportStartupGates`, which
// does not return an error, does not change the exit code and runs on a detached goroutine — after
// discovery, the derivatives poller and Coinalyze were already up. The verdict existed as a function
// value and the process started anyway. A ratchet that prints is a comment with a gate accent.
//
// # THE SEPARATION
//
// The sheet is deferred for a legitimate reason: `A` comes from `SCARD active_symbols:*`, which only
// exists after discovery. But the gates that REFUSE do not depend on `A` — they compare two numbers
// READ from the server and the keyspace, and all are available before anything starts. So they run
// HERE, SYNCHRONOUSLY, at the first gesture of `Run`, and the fatal verdict is RETURNED: `main`
// terminates in `log.Fatalf`, the process serves nothing, the exit code is non-zero.
//
// The projection remains deferred and NEVER fatal: without hot reload, a fatal gate over an
// estimate turns "wrong config" into "service that does not start, with no way back".
//
// # WHAT HAPPENS
//
// `docker-compose.base.yml` carries `--maxmemory-policy allkeys-lru` and the migration of
// `cmd/candles-sole-copy-migrate` has not run: the 129,190 candles of `candles:*:1d`/`:1w` that exist
// only in Redis remain there. With this file, that deploy does NOT start the service — it stops at
// startup naming the rule, the current policy and the count of keys at risk.

import (
	"context"
	"fmt"
	"log/slog"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle/engine"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/derivatives/collector"
	"github.com/marxms/go-whale/internal/platform/config"
	logger "github.com/marxms/go-whale/internal/platform/logging"
	"github.com/marxms/go-whale/internal/platform/ops"
)

// preflightGates measures what can be measured before the fleet and returns the refusal.
//
// An unavailable hot tier does not become a verdict: without a client there are no two read
// numbers, and refusing against the absence of measurement would be a sentence without evidence —
// the same rule `ServerFacts` applies to every fact it could not read.
func (a *App) preflightGates(ctx context.Context) error {
	if a.redisStore == nil || a.redisStore.Client() == nil {
		a.logger().Warn("preflight gate not evaluated; hot tier unavailable at boot")
		return nil
	}
	client := a.redisStore.Client()
	return a.preflightVerdict(ctx, a.serverFactsProbe(), client, a.probeSoleCopy(ctx))
}

// preflightVerdict is the decision half, separated from measurement so the wiring is testable
// without a live server — the wiring is exactly what had no test.
func (a *App) preflightVerdict(
	ctx context.Context,
	probe redisstore.RedisServerProbe,
	client ops.Redis,
	soleCopy engine.SoleCopyRisk,
) error {
	facts, err := redisstore.ReadServerFacts(ctx, probe, redisstore.DefaultCgroupRoot)
	if err != nil {
		a.logger().Warn("preflight: no server fact could be read", "error", err)
	}

	scope := fmt.Sprintf("%s:%s", a.defaultExchange, a.defaultMarketType)
	declaradas, protegidas := a.durableOrderSets()
	registro := a.cfg.CandleEngine.Timeframes

	results := config.EvaluatePreflightGates(config.GateInputs{
		// NOT READ here, and declared as such: `MAX_INDICATOR_VALUES` is the analyzer's env var —
		// the service that writes that family and refuses its own start for it
		// (`IndicatorStateService.onModuleInit`).
		IndicatorCap:           0,
		ZSetMaxListpackEntries: facts.ZSetMaxListpackEntries,
		MaxMemoryPolicy:        facts.MaxMemoryPolicy,
		ImmortalFamilies:       ops.SampleImmortalFamilies(ctx, client, scope),
		// ON. It does not refuse the current state — under `allkeys-lru` the gate's condition
		// does not even fire. It is a ratchet against returning `volatile-ttl` to the compose.
		PolicyIsFatal:      true,
		SoleCopyEvaluated:  soleCopy.Evaluated,
		SoleCopyKeysAtRisk: soleCopy.KeysWithSoleCopy(),
		SoleCopyDetail:     soleCopy.Detail(),
		// THE COLUMN ORDER, with both sides READ from the binary being started. Neither comes from
		// config: the coverage declaration and the erosion protection are properties of this build,
		// which is exactly why the divergence between them cannot wait for the first degraded cycle
		// to appear.
		DurableColumnsDeclared: declaradas,
		ErosionGuardedColumns:  protegidas,
		// COVERAGE PER TIMEFRAME, with both sides read before the fleet: the registry comes from
		// the configuration being started, the matrix is provider measurement declared in this
		// build. Neither depends on `A`, so the question fits in the preflight — and that is the
		// point: a timeframe without a source otherwise becomes a WARN every cycle, forever, and
		// nobody reads the thousandth.
		RegisteredTimeframes: registro,
		TimeframeSupport:     collector.TimeframeSupportRows(registro),
		// THE CAP RULE, with all three sides read from the build and the configuration being
		// started. None depends on `A` or on a row in the table: the decision is about COVERAGE
		// PER FIELD, and a decision that depended on the table filling up would turn retention on
		// and off by itself.
		CapEntriesByTimeframe: capEntries(a.cfg.Derivatives),
		DefaultSeriesCap:      int64(a.cfg.Derivatives.MaxSeriesSize),
		DurableCellCoverage:   a.durableCellCoverage(),
	})

	for _, r := range results {
		if r.Severity == config.GateFatal {
			// THE FATAL LINE IS NOT PRINTED HERE. It becomes the returned error, and an error is
			// logged OR propagated — never both. Printing and continuing was, literally, the
			// defect.
			continue
		}
		logGateVerdict(a.log, r)
	}
	return config.FatalGateError(results)
}

// capEntries returns the cap entries AS WRITTEN in the configuration.
//
// Only the entries, not the resolved cap: the gate asks who REQUESTED shortening, and a timeframe
// without an entry requested nothing — it keeps the global default. Confusing the two would make
// the gate accuse every registered timeframe.
func capEntries(cfg config.DerivativesConfig) map[string]int64 {
	if len(cfg.SeriesCapByTimeframe) == 0 {
		return nil
	}
	out := make(map[string]int64, len(cfg.SeriesCapByTimeframe))
	for tf, value := range cfg.SeriesCapByTimeframe {
		out[tf] = int64(value)
	}
	return out
}

// durableCellCoverage assembles the three sides of per-field coverage, each from its source.
//
// NONE IS TYPED HERE, and that is the only property that matters: the declaration comes from
// `storage`, the readable-column list comes from the query that actually reads, and the timeframe
// set comes from `polling`'s derivation. A fourth copy in this file would be the divergence the
// gate exists to catch, written inside the gate's caller.
func (a *App) durableCellCoverage() config.DurableCellCoverage {
	return config.DurableCellCoverage{
		FieldColumn:       redisstore.DerivativesDurableCoverage(),
		ReadableColumns:   derivatives.DurableReadColumns(),
		DurableTimeframes: a.durableTimeframes(),
	}
}

// durableOrderSets returns the two sides of the column order. See App.durableOrder.
func (a *App) durableOrderSets() (declared, guarded []string) {
	if a.durableOrder != nil {
		return a.durableOrder()
	}
	return declaredDurableColumns(), derivatives.ErosionGuardedColumns()
}

// declaredDurableColumns lists the columns the coverage declaration says the durable tier writes.
//
// It DELEGATES instead of repeating: the reduction lives next to the declaration (`storage`), and
// the CI probe consumes the SAME function. A second list here would be the divergence the gate
// exists to catch, written inside the gate itself.
func declaredDurableColumns() []string {
	return redisstore.DeclaredDurableColumns()
}

// serverFactsProbe returns the server-facts probe: the injected one when it exists, and the one
// that talks to the live client otherwise. `NewApp` injects the production one; the field exists
// because `INFO memory` has no double in miniredis and the wiring must be testable.
func (a *App) serverFactsProbe() redisstore.RedisServerProbe {
	if a.serverProbe != nil {
		return a.serverProbe
	}
	return redisstore.NewRedisServerProbe(a.redisStore.Client())
}

// logGateVerdict prints ONE verdict line at its severity.
func logGateVerdict(log *slog.Logger, r config.GateResult) {
	log = logger.OrDiscard(log)
	switch r.Severity {
	case config.GateFatal, config.GateError:
		log.Error(r.String(), "gate", r.Name)
	case config.GateWarn:
		log.Warn(r.String(), "gate", r.Name)
	default:
		log.Info(r.String(), "gate", r.Name)
	}
}
