package derivatives

import (
	"context"
	"errors"
	"testing"
	"time"
)

type coarseColdProbe struct {
	scope    DerivativesScope
	from, to time.Time
	points   []*DerivativesSnapshot
	err      error
	calls    int
}

func (p *coarseColdProbe) QueryDerivativesRange(_ context.Context, scope DerivativesScope, from, to time.Time) ([]*DerivativesSnapshot, error) {
	p.scope, p.from, p.to = scope, from, to
	p.calls++
	return p.points, p.err
}

func coarsePoint(day string, oi *float64) *DerivativesSnapshot {
	ts, _ := time.Parse(time.RFC3339, day+"T00:00:00Z")
	return &DerivativesSnapshot{Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT", Source: SourceCoinalyze, Timestamp: ts.UnixMilli(), OpenInterestBase: oi}
}

func TestCoarseHistoryDailyPreservesScopeAbsenceAndFunding(t *testing.T) {
	value, normalized := 10.0, 0.1
	points := []*DerivativesSnapshot{coarsePoint("2026-01-12", &value), coarsePoint("2026-01-13", nil), coarsePoint("2026-01-14", &value)}
	points[2].FundingRate8h = &normalized
	points[2].NStep = NInheritedOutsideWindow
	probe := &coarseColdProbe{points: points}
	reader := NewCoarseDerivativesHistory(probe)
	reader.now = func() time.Time { return time.Date(2026, 1, 14, 13, 0, 0, 0, time.FixedZone("offset", 3*3600)) }
	got, err := reader.Series(context.Background(), "binance", "future", "BTC/USDT", "1d", 3)
	if err != nil || len(got) != 3 {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	if probe.scope.Source != SourceCoinalyze || probe.scope.Timeframe != "1d" {
		t.Fatalf("scope=%+v", probe.scope)
	}
	if probe.to.UTC() != time.Date(2026, 1, 14, 0, 0, 0, 0, time.UTC) {
		t.Fatalf("query end=%s", probe.to)
	}
	if got[0].Timestamp != points[2].Timestamp || got[2].Timestamp != points[0].Timestamp || got[1].OpenInterestBase != nil {
		t.Fatalf("order/absence changed: %+v", got)
	}
	if got[0].FundingRate8h != nil || got[0].NStep != NInheritedOutsideWindow || points[2].FundingRate8h == nil {
		t.Fatal("funding evidence changed or input mutated")
	}
}

func TestCoarseHistoryWeeklyUsesSundayAndLatestOpenWeek(t *testing.T) {
	sunday, saturday, current, older := 10.0, 999.0, 20.0, 5.0
	probe := &coarseColdProbe{points: []*DerivativesSnapshot{
		coarsePoint("2026-01-03", &saturday), // Saturday cannot fill a missing Sunday.
		coarsePoint("2026-01-10", &saturday), coarsePoint("2026-01-11", &sunday),
		coarsePoint("2026-01-14", &current), coarsePoint("2026-01-12", &older),
	}}
	reader := NewCoarseDerivativesHistory(probe)
	reader.now = func() time.Time { return time.Date(2026, 1, 14, 12, 0, 0, 0, time.UTC) }
	got, err := reader.Series(context.Background(), "binance", "future", "BTC/USDT", "1w", 3)
	if err != nil || len(got) != 2 {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	for i, want := range []struct {
		day string
		oi  float64
	}{{"2026-01-12", 20}, {"2026-01-05", 10}} {
		if got[i].Timestamp != coarsePoint(want.day, nil).Timestamp || *got[i].OpenInterestBase != want.oi || got[i].Source != SourceCoinalyze {
			t.Fatalf("week%d=%+v", i, got[i])
		}
	}
	if probe.points[2].Timestamp != coarsePoint("2026-01-11", nil).Timestamp {
		t.Fatal("reader mutated original Sunday label")
	}
}

func TestCoarseHistoryRefusesUnavailableOrIncoherentHistory(t *testing.T) {
	failure := errors.New("cold failed")
	reader := NewCoarseDerivativesHistory(&coarseColdProbe{err: failure})
	if _, err := reader.Series(context.Background(), "binance", "future", "BTC/USDT", "1w", 3); !errors.Is(err, failure) {
		t.Fatalf("cold error lost: %v", err)
	}
	if _, err := NewCoarseDerivativesHistory(nil).Series(context.Background(), "binance", "future", "BTC/USDT", "1d", 3); err == nil {
		t.Fatal("missing cold tier must fail, not use venue")
	}
	if _, err := reader.Series(context.Background(), "binance", "future", "BTC/USDT", "4h", 3); err == nil {
		t.Fatal("intraday must remain on its existing reader")
	}
	point := coarsePoint(time.Now().UTC().Format("2006-01-02"), nil)
	point.Source = SourceVenue
	if _, err := NewCoarseDerivativesHistory(&coarseColdProbe{points: []*DerivativesSnapshot{point}}).Series(context.Background(), "binance", "future", "BTC/USDT", "1d", 3); err == nil {
		t.Fatal("native source accepted by canonical reader")
	}
}

func TestCoarseHistoryBoundsWindowAndResult(t *testing.T) {
	value := 10.0
	probe := &coarseColdProbe{points: []*DerivativesSnapshot{coarsePoint("2026-01-10", &value), coarsePoint("2026-01-11", &value), coarsePoint("2026-01-12", &value)}}
	reader := NewCoarseDerivativesHistory(probe)
	reader.now = func() time.Time { return time.Date(2026, 1, 12, 20, 0, 0, 0, time.UTC) }
	got, err := reader.Series(context.Background(), "binance", "future", "BTC/USDT", "1d", 2)
	if err != nil || len(got) != 2 || got[1].Timestamp != coarsePoint("2026-01-11", nil).Timestamp {
		t.Fatalf("limit/order got=%+v err=%v", got, err)
	}
	if _, err := reader.Series(context.Background(), "binance", "future", "BTC/USDT", "1w", int(^uint(0)>>1)); err != nil {
		t.Fatal(err)
	}
	if probe.to.Sub(probe.from) > DurableMaxLookback {
		t.Fatalf("unbounded window: %s", probe.to.Sub(probe.from))
	}
}

func TestCoarseHistoryOpenWeekPreservesLatestNull(t *testing.T) {
	value := 10.0
	probe := &coarseColdProbe{points: []*DerivativesSnapshot{coarsePoint("2026-01-12", &value), coarsePoint("2026-01-14", nil)}}
	reader := NewCoarseDerivativesHistory(probe)
	reader.now = func() time.Time { return time.Date(2026, 1, 14, 12, 0, 0, 0, time.UTC) }
	got, err := reader.Series(context.Background(), "binance", "future", "BTC/USDT", "1w", 3)
	if err != nil || len(got) != 1 || got[0].OpenInterestBase != nil || got[0].Timestamp != coarsePoint("2026-01-12", nil).Timestamp {
		t.Fatalf("latest absence filled: %+v %v", got, err)
	}
}

func TestCoarseHistoryPreservesLongShortRatioValuesAndNull(t *testing.T) {
	for _, tc := range []struct {
		timeframe string
		days      [3]string
		labels    [3]string
	}{
		{"1d", [3]string{"2026-01-12", "2026-01-13", "2026-01-14"}, [3]string{"2026-01-14", "2026-01-13", "2026-01-12"}},
		{"1w", [3]string{"2026-01-04", "2026-01-11", "2026-01-14"}, [3]string{"2026-01-12", "2026-01-05", "2025-12-29"}},
	} {
		t.Run(tc.timeframe, func(t *testing.T) {
			oi, older, recent := 10.0, 1.1, 1.3
			points := []*DerivativesSnapshot{
				coarsePoint(tc.days[0], &oi), coarsePoint(tc.days[1], &oi), coarsePoint(tc.days[2], &oi),
			}
			points[0].LongShortRatio, points[1].LongShortRatio = &older, &recent
			reader := NewCoarseDerivativesHistory(&coarseColdProbe{points: points})
			reader.now = func() time.Time { return time.Date(2026, 1, 14, 12, 0, 0, 0, time.UTC) }
			got, err := reader.Series(context.Background(), "binance", "future", "BTC/USDT", tc.timeframe, 3)
			if err != nil || len(got) != 3 {
				t.Fatalf("got=%+v err=%v", got, err)
			}
			for i, want := range []*float64{nil, &recent, &older} {
				if got[i].Timestamp != coarsePoint(tc.labels[i], nil).Timestamp {
					t.Fatalf("point%d timestamp=%d", i, got[i].Timestamp)
				}
				actual := got[i].LongShortRatio
				if (actual == nil) != (want == nil) || (want != nil && *actual != *want) {
					t.Fatalf("point%d LSR=%v want=%v", i, actual, want)
				}
			}
			if points[0].Timestamp != coarsePoint(tc.days[0], nil).Timestamp || *points[0].LongShortRatio != older || *points[1].LongShortRatio != recent || points[2].LongShortRatio != nil {
				t.Fatal("reader mutated original LSR observations")
			}
		})
	}
}
