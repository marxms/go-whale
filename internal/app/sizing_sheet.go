package app

// sizing_sheet.go — wiring of the SIZING SHEET.
//
// The sheet, the gates it reports and the disk budget live in internal/platform/ops. This file only
// reads the effective configuration, measures the server and publishes the streamer line.

import (
	"context"
	"fmt"
	"strings"
	"time"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/candle/engine"
	"github.com/marxms/go-whale/internal/platform/config"
	"github.com/marxms/go-whale/internal/platform/ops"
)

// logSizingSheet builds, prints and publishes the streamer's line. It runs AFTER the
// fleet is published, because A comes from SCARD and at boot the discovery has not
// populated active_symbols yet.
func (a *App) logSizingSheet(ctx context.Context) {
	if a.redisStore == nil {
		return
	}
	client := a.redisStore.Client()
	if client == nil {
		return
	}

	a.awaitFleet(ctx)
	if ctx.Err() != nil {
		return
	}

	facts, err := redisstore.ReadServerFacts(ctx, a.serverFactsProbe(), redisstore.DefaultCgroupRoot)
	if err != nil {
		a.logger().Warn("sizing sheet: no server fact could be read", "error", err)
	}

	timeframes := a.timeframes()
	scope := fmt.Sprintf("%s:%s", a.defaultExchange, a.defaultMarketType)
	universe, source := ops.ResolveUniverse(ctx, client, redisstore.ActiveSymbolsKey(scope))

	var derivCaps ops.DerivativesCapSource
	if a.derivStore != nil {
		derivCaps = a.derivStore
	}

	sheet := ops.BuildSheetFrom(ctx, ops.SheetInput{
		Service:           ops.ServiceGoWhale,
		Client:            client,
		UniverseCount:     universe,
		UniverseSource:    source,
		SampleBudget:      ops.CoefficientSample,
		Families:          ops.Families(ctx, client, a.cfg.CandleEngine, a.cfg.Derivatives.DeclaredSeriesCap, derivCaps, timeframes, scope),
		Facts:             facts,
		WarnRatio:         a.cfg.Retention.BudgetWarnRatio,
		NowMs:             time.Now().UnixMilli(),
		DurableTimeframes: a.durableTimeframes(),
		DerivativesWindow: a.retainedDerivativesWindow(timeframes),
	})

	ops.LogSheet(a.log, sheet)

	a.reportStartupGates(ctx, client, sheet, facts, timeframes, scope)

	if err := ops.PublishSheet(ctx, client, sheet); err != nil {
		a.logger().Warn("sizing sheet: could not publish the streamer line", "error", err)
		return
	}
	if total, parts, err := ops.AggregateMiB(ctx, client, sheet.PublishedAtMs); err == nil && len(parts) > 0 {
		a.logger().Info("sizing sheet: total across services",
			"total_mib", total, "parts", strings.Join(parts, " · "))
	}
}

// reportStartupGates runs the gate table with what THIS service can read, and it is a REPORT —
// the startup refusal does not live here (see preflightGates).
func (a *App) reportStartupGates(
	ctx context.Context,
	client ops.Redis,
	sheet ops.Sheet,
	facts redisstore.ServerFacts,
	timeframes []string,
	scope string,
) {
	declaredDerivCaps := make(map[string]int64, len(timeframes))
	effectiveDerivCaps := make(map[string]int64, len(timeframes))
	if a.derivStore != nil {
		for _, tf := range timeframes {
			sec := candle.ParseTimeframeToSeconds(tf)
			if sec <= 0 {
				continue
			}
			declaredDerivCaps[tf] = int64(a.cfg.Derivatives.DeclaredSeriesCap(tf))
			// THE EFFECTIVE VALUE IS THE FLOOR when the floor raised it. The store is the floor's
			// authority.
			effectiveDerivCaps[tf] = a.derivStore.SeriesHorizonMs(tf) / (sec * 1000)
		}
	}

	// THE SOLE-COPY GUARD runs BEFORE the gates because it produces the two numbers the gate
	// compares.
	soleCopy := a.probeSoleCopy(ctx)

	results := config.EvaluateGates(config.GateInputs{
		// NOT READ here, and declared as such: MAX_INDICATOR_VALUES is the analyzer's env var.
		IndicatorCap:             0,
		ZSetMaxListpackEntries:   facts.ZSetMaxListpackEntries,
		DeclaredCapByTimeframe:   declaredDerivCaps,
		BaselineFloorByTimeframe: effectiveDerivCaps,
		RegisteredTimeframes:     timeframes,
		CapEntriesByTimeframe:    ops.CandleCapEntries(a.cfg.CandleEngine),
		CapConfigKey:             ops.CandleCapConfigKey,
		MaxMemoryPolicy:          facts.MaxMemoryPolicy,
		ImmortalFamilies:         ops.SampleImmortalFamilies(ctx, client, scope),
		// ON. It does not refuse the current state — under `allkeys-lru` the gate's condition
		// does not even fire. It is a ratchet against returning `volatile-ttl` to the compose.
		PolicyIsFatal:        true,
		SoleCopyEvaluated:    soleCopy.Evaluated,
		SoleCopyKeysAtRisk:   soleCopy.KeysWithSoleCopy(),
		SoleCopyDetail:       soleCopy.Detail(),
		ProjectedMiB:         sheet.TotalMiB,
		MaxMemoryBytes:       facts.MaxMemoryBytes,
		UsedMemoryBytes:      facts.UsedMemoryBytes,
		WarnRatio:            a.cfg.Retention.BudgetWarnRatio,
		ErrorRatio:           a.cfg.Retention.BudgetErrorRatio,
		MeasuredStartupRatio: a.cfg.Retention.MeasuredStartupWarnRatio,
	})

	for _, r := range results {
		logGateVerdict(a.log, r)
	}
	// A FATAL LINE HERE MEANS CHANGE IN FLIGHT. The `Run` preflight already approved this same
	// comparison before the fleet started; if it refuses now, the server state changed after the
	// start. Refusing from here is not an option — the process is already serving.
	if err := config.FatalGateError(results); err != nil {
		a.logger().Error("fatal gate verdict after boot; the preflight had approved", "error", err)
	}
}

// probeSoleCopy measures the sole copy for the gate.
//
// A missing durable tier does NOT return "clean": it returns NOT EVALUATED, because without a
// durable tier every hot candle is a sole copy.
func (a *App) probeSoleCopy(ctx context.Context) engine.SoleCopyRisk {
	if a.redisStore == nil {
		return engine.SoleCopyRisk{Reason: "hot tier unavailable"}
	}
	if a.qdbStore == nil {
		return engine.SoleCopyRisk{Reason: "NO durable tier configured: every hot candle is a sole copy"}
	}
	return engine.NewSoleCopyGuard(a.redisStore, a.qdbStore).Probe(ctx, a.soleCopyCellsFromConfig())
}
