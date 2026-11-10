package retention

import (
	"testing"
	"time"
)

func reach(tf string, d time.Duration) TimeframeReach {
	return TimeframeReach{Timeframe: tf, Reach: d, Measurable: true}
}

func TestCandlesDeliberateHorizonIsTheDeepestResidentReach(t *testing.T) {
	reaches := []TimeframeReach{
		reach("5m", 800*5*time.Minute),
		reach("1h", 800*time.Hour),
		reach("1d", 800*24*time.Hour),
		reach("1w", 800*7*24*time.Hour),
	}
	if got, want := CandlesDeliberateHorizon(reaches), 800*7*24*time.Hour; got != want {
		t.Fatalf("horizon = %v, want %v (800 members of 1w)", got, want)
	}
}

func TestCandlesDeliberateHorizonIgnoresUnmeasurable(t *testing.T) {
	reaches := []TimeframeReach{
		{Timeframe: "banana", Measurable: false},
		reach("1h", time.Hour),
	}
	if got, want := CandlesDeliberateHorizon(reaches), time.Hour; got != want {
		t.Fatalf("horizon = %v, want %v", got, want)
	}
	if got := CandlesDeliberateHorizon([]TimeframeReach{{Timeframe: "banana"}}); got != 0 {
		t.Fatalf("horizon without a measurable timeframe = %v, want 0", got)
	}
}

func TestSoleCopyGuardTimeframes(t *testing.T) {
	const day = 24 * time.Hour
	reaches := []TimeframeReach{
		reach("5m", 30*day),
		reach("1d", 800*day),
		reach("1w", 5600*day),
		{Timeframe: "banana", Measurable: false},
	}

	got := SoleCopyGuardTimeframes(reaches, 365*day)
	want := []string{"1d", "1w", "banana"}
	if len(got) != len(want) {
		t.Fatalf("guard = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("guard = %v, want %v", got, want)
		}
	}

	// Without a declared window, EVERY cell is in the guard.
	if got := SoleCopyGuardTimeframes(reaches, 0); len(got) != len(reaches) {
		t.Fatalf("without a durable window the guard = %v, want every cell", got)
	}
}

func TestCandlesPolicy(t *testing.T) {
	p, ok := CandlesPolicy("candles", 365, 5600*24*time.Hour)
	if !ok {
		t.Fatal("the candles policy disappeared")
	}
	if p.RetentionDays != 365 || p.DeliberateHorizon != 5600*24*time.Hour || p.HorizonReason == "" {
		t.Fatalf("policy = %+v", p)
	}

	if _, ok := CandlesPolicy("", 365, 0); ok {
		t.Fatal("policy without a table")
	}
	if _, ok := CandlesPolicy("candles", 0, 0); ok {
		t.Fatal("policy without a declared window")
	}
}

func TestDerivativesRetentionDays(t *testing.T) {
	horizon := 2000 * 24 * time.Hour
	if got := DerivativesRetentionDays(0, horizon); got != 2000 {
		t.Errorf("derived days = %d, want 2000", got)
	}
	if got := DerivativesRetentionDays(45, horizon); got != 45 {
		t.Errorf("declared days = %d, want 45", got)
	}
	if got := DerivativesRetentionDays(0, 0); got != 0 {
		t.Errorf("days without coverage = %d, want 0", got)
	}
}

func TestDerivativesPolicyKeepsTheFloorWhenTheWindowIsDeclared(t *testing.T) {
	horizon := 2000 * 24 * time.Hour
	p, ok := DerivativesPolicy("derivatives", 45, horizon)
	if !ok {
		t.Fatal("the derivatives policy disappeared")
	}
	if p.RetentionDays != 45 {
		t.Errorf("window = %d, want 45", p.RetentionDays)
	}
	if p.DeliberateHorizon != horizon {
		t.Errorf("the boundary gave way with the window: %v", p.DeliberateHorizon)
	}

	if _, ok := DerivativesPolicy("derivatives", 0, 0); ok {
		t.Fatal("a window was derived without durable coverage")
	}
}

func TestRetainedWindow(t *testing.T) {
	tests := []struct {
		name     string
		days     int
		horizon  time.Duration
		wantDays float64
	}{
		{name: "the declared window decides, with the grace day", days: 30, horizon: 0, wantDays: 31},
		{name: "the deliberate boundary pushes the cutoff back when deeper", days: 30, horizon: 2000 * 24 * time.Hour, wantDays: 2000},
		{name: "the window wins when it already covers the boundary", days: 2000, horizon: 2000 * 24 * time.Hour, wantDays: 2001},
		{name: "no window and no boundary means no declared ceiling", days: 0, horizon: 0, wantDays: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RetainedWindow(tt.days, tt.horizon)
			if want := time.Duration(tt.wantDays) * 24 * time.Hour; got != want {
				t.Fatalf("retained window = %v, want %v", got, want)
			}
		})
	}
}
