package app

import (
	"context"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/questdb"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives/collector"
	"github.com/marxms/go-whale/internal/platform/config"
	"github.com/marxms/go-whale/internal/retention"
)

// THE RETENTION OF `derivatives`, AND THE ORDER IT OWES THE GATES.
//
// THE POLICY lives in internal/retention: which table receives a window, which is the deliberate
// boundary, and which is the effective retained window. Here there are only the conversion of
// configuration into typed values and the license the startup grants.

// retentionPoliciesFromConfig declares what retention reaches, and what it must not reach.
func retentionPoliciesFromConfig(cfg config.Config, timeframes []string) []retention.Policy {
	var out []retention.Policy

	reaches := timeframeReaches(cfg.CandleEngine, timeframes)
	if policy, ok := retention.CandlesPolicy(cfg.QuestDB.Table, cfg.QuestDB.RetentionDays,
		retention.CandlesDeliberateHorizon(reaches)); ok {
		out = append(out, policy)
	}

	horizon := collector.DurableSeedHorizon(timeframes)
	if policy, ok := retention.DerivativesPolicy(questdb.DerivativesTableName(),
		cfg.QuestDB.DerivativesRetentionDays, horizon); ok {
		out = append(out, policy)
	}
	return out
}

// candlesPolicyFromConfig is the conversion from configuration to the `candles` policy.
func candlesPolicyFromConfig(cfg config.Config) (retention.Policy, bool) {
	reaches := timeframeReaches(cfg.CandleEngine, cfg.CandleEngine.Timeframes)
	return retention.CandlesPolicy(cfg.QuestDB.Table, cfg.QuestDB.RetentionDays,
		retention.CandlesDeliberateHorizon(reaches))
}

// timeframeReaches maps the candle-engine configuration onto the retention package's
// typed reach values. The MAX between inherited depth and declared cap — and the
// derivation of reach from bucket and members — lives in candle.ResidentReach.
func timeframeReaches(ce config.CandleEngineConfig, timeframes []string) []retention.TimeframeReach {
	inherited := config.DefaultResidentDepth
	if v, ok := ce.BufferSizes["default"]; ok && v > 0 {
		inherited = v
	}

	out := make([]retention.TimeframeReach, 0, len(timeframes))
	for _, timeframe := range timeframes {
		reach, measurable := candle.ResidentReach(timeframe, inherited, ce.RedisCapFor(timeframe))
		out = append(out, retention.TimeframeReach{
			Timeframe:  timeframe,
			Reach:      reach,
			Measurable: measurable,
		})
	}
	return out
}

// soleCopyCellsFromConfig returns the cells whose hot ZSET reaches further back than
// the durable window. The decision lives in retention.
func (a *App) soleCopyCellsFromConfig() []string {
	reaches := timeframeReaches(a.cfg.CandleEngine, a.cfg.CandleEngine.Timeframes)
	durableWindow := time.Duration(a.cfg.QuestDB.RetentionDays) * 24 * time.Hour
	return retention.SoleCopyGuardTimeframes(reaches, durableWindow)
}

// retainedDerivativesWindow returns, for the sizing sheet, the SAME window the retention policy
// of this table will apply.
func (a *App) retainedDerivativesWindow(timeframes []string) time.Duration {
	horizon := collector.DurableSeedHorizon(timeframes)
	days := retention.DerivativesRetentionDays(a.cfg.QuestDB.DerivativesRetentionDays, horizon)
	return retention.RetainedWindow(days, horizon)
}

// releaseRetention grants the license and starts the loop. Called in `Run`, AFTER the gates.
func (a *App) releaseRetention(ctx context.Context) {
	if a.retentionMgr == nil {
		return
	}
	a.gatesApproved.Store(true)
	a.retentionMgr.Start(ctx)
}
