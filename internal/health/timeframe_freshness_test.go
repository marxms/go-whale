package health

import (
	"testing"
	"time"
)

// A frozen derived timeframe is invisible to candle_freshness, which aggregates by SCOPE: 15m–1w
// could be frozen while the 5m kept the scope green. A frozen timeframe must at least degrade the verdict, and it must
// never turn the probe into 503 — restarting would destroy the rings without repairing the fold.
func TestFrozenTimeframeDegradesWithoutGoing503(t *testing.T) {
	agg := NewAggregator(Probes{
		StartedAt: func() time.Time { return time.Now().Add(-time.Hour) },
		Connections: func() []Connection {
			return []Connection{{Exchange: "binance", MarketType: "future", Connected: true, Healthy: true, Symbols: 1}}
		},
		TimeframeFreshness: func() ([]TimeframeFreshness, bool) {
			return []TimeframeFreshness{
				{Scope: "binance:future", Timeframe: "5m", NewestAgeMs: 30_000, HaveData: true, StaleAfterMs: 1_200_000},
				{Scope: "binance:future", Timeframe: "15m", NewestAgeMs: 15 * 60_000, HaveData: true, StaleAfterMs: 10 * 60_000},
				{Scope: "binance:future", Timeframe: "30m", NewestAgeMs: 60_000, HaveData: true, StaleAfterMs: 7_200_000},
				{Scope: "binance:future", Timeframe: "1h", StaleAfterMs: 14_400_000},
			}, true
		},
	})

	report := agg.Evaluate()

	if len(report.TimeframeFreshness) != 4 {
		t.Fatalf("block rows = %d, want 4", len(report.TimeframeFreshness))
	}
	stale := 0
	for _, row := range report.TimeframeFreshness {
		if row.Stale {
			stale++
			if row.Timeframe != "15m" {
				t.Fatalf("stale row = %+v, want only the 15m", row)
			}
		}
	}
	if stale != 1 {
		t.Fatalf("stale rows = %d, want 1", stale)
	}
	if report.Status != statusDegraded {
		t.Fatalf("status = %q, want %q — a frozen timeframe must degrade", report.Status, statusDegraded)
	}
	if report.HTTPStatus == 503 {
		t.Fatal("a frozen timeframe must NOT answer 503: a restart does not repair the fold")
	}
	found := false
	for _, component := range report.Components {
		if component.Name == componentTimeframeFreshness {
			found = true
			if component.State != ComponentDegraded {
				t.Fatalf("component state = %v, want degraded", component.State)
			}
		}
	}
	if !found {
		t.Fatal("component timeframe_freshness is missing from the verdict")
	}
}

// The probe lists the timeframes the engine SERVES. After the startup grace, one that never
// produced a bucket is a silent freeze, not "absent" — the old contract reported it healthy and
// made a frozen derived timeframe indistinguishable from a fresh one.
func TestTimeframeWithoutDataAfterGraceDegrades(t *testing.T) {
	agg := NewAggregator(Probes{
		StartedAt: func() time.Time { return time.Now().Add(-time.Hour) },
		Connections: func() []Connection {
			return []Connection{{Exchange: "binance", MarketType: "future", Connected: true, Healthy: true, Symbols: 1}}
		},
		TimeframeFreshness: func() ([]TimeframeFreshness, bool) {
			return []TimeframeFreshness{
				{Scope: "binance:future", Timeframe: "1w", StaleAfterMs: 28 * 24 * 3_600_000, HaveData: false},
			}, true
		},
	})

	report := agg.Evaluate()
	if report.Status != statusDegraded {
		t.Fatalf("status = %q, want degraded: a served timeframe with no data after the grace", report.Status)
	}
	found := false
	for _, detail := range report.TimeframeFreshness {
		if detail.Timeframe == "1w" {
			found = true
			if detail.HaveData {
				t.Fatal("have_data = true for a timeframe with no data")
			}
		}
	}
	if !found {
		t.Fatal("timeframe 1w missing from the detail")
	}
}

// Within the startup grace the same row is not a verdict: the first bucket may still be forming.
func TestTimeframeWithoutDataDuringGraceIsLenient(t *testing.T) {
	agg := NewAggregator(Probes{
		StartedAt: func() time.Time { return time.Now().Add(-5 * time.Second) },
		Connections: func() []Connection {
			return []Connection{{Exchange: "binance", MarketType: "future", Connected: true, Healthy: true, Symbols: 1}}
		},
		TimeframeFreshness: func() ([]TimeframeFreshness, bool) {
			return []TimeframeFreshness{
				{Scope: "binance:future", Timeframe: "1w", StaleAfterMs: 28 * 24 * 3_600_000},
			}, true
		},
	})

	report := agg.Evaluate()
	if report.Status == statusDegraded || report.Status == statusUnhealthy {
		t.Fatalf("status = %q, want lenient during the startup grace", report.Status)
	}
}

// A timeframe the producer could not parse has no threshold. Zero must mean "no verdict", not
// "instantly frozen": a configuration typo would otherwise alarm on every poll.
func TestTimeframeWithoutAThresholdIsNeverStale(t *testing.T) {
	agg := NewAggregator(Probes{
		StartedAt: func() time.Time { return time.Now().Add(-time.Hour) },
		Connections: func() []Connection {
			return []Connection{{Exchange: "binance", MarketType: "future", Connected: true, Healthy: true, Symbols: 1}}
		},
		TimeframeFreshness: func() ([]TimeframeFreshness, bool) {
			return []TimeframeFreshness{
				{Scope: "binance:future", Timeframe: "5x", NewestAgeMs: 60_000, HaveData: true},
			}, true
		},
	})

	report := agg.Evaluate()
	if report.Status != statusHealthy {
		t.Fatalf("status = %q, want healthy: a missing threshold is not a freeze", report.Status)
	}
	if len(report.TimeframeFreshness) != 1 || report.TimeframeFreshness[0].Stale {
		t.Fatalf("row = %+v, want not stale", report.TimeframeFreshness)
	}
}
