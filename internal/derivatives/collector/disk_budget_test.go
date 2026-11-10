package collector

import (
	"testing"
	"time"
)

// derivDurableTFs is the durable coverage of the seven served timeframes.
func derivDurableTFs() []string {
	return []string{"5m", "15m", "30m", "1h", "2h", "4h", "1d"}
}

// derivBudgetWindow is the retained window the default configuration resolves,
// assembled from the same sources the retention policy uses.
func derivBudgetWindow() time.Duration {
	return time.Duration(2000) * 24 * time.Hour
}

func TestDerivativesDiskBudget_IsAFunctionOfAssetsAndNotAConstant(t *testing.T) {
	b500 := NewDerivativesDiskBudget(500, derivDurableTFs(), derivBudgetWindow())
	b600 := NewDerivativesDiskBudget(600, derivDurableTFs(), derivBudgetWindow())
	b800 := NewDerivativesDiskBudget(800, derivDurableTFs(), derivBudgetWindow())

	if b500.Bytes() == b800.Bytes() || b500.Bytes() == b600.Bytes() || b600.Bytes() == b800.Bytes() {
		t.Fatalf("the cost does not vary with A: 500=%d 600=%d 800=%d", b500.Bytes(), b600.Bytes(), b800.Bytes())
	}
	if got, want := float64(b800.Rows()), float64(b500.Rows())*1.6; got != want {
		t.Errorf("rows(800) = %.0f, want %.0f", got, want)
	}

	three := NewDerivativesDiskBudget(500, []string{"1h", "4h", "1d"}, derivBudgetWindow())
	if three.Rows() >= b500.Rows() {
		t.Errorf("rows with 3 timeframes = %d, with 7 = %d", three.Rows(), b500.Rows())
	}
}

func TestDerivativesDiskBudget_TheDepthIsPerTimeframeAndNotOneNumberForSeven(t *testing.T) {
	b := NewDerivativesDiskBudget(1, derivDurableTFs(), derivBudgetWindow())

	buckets := map[string]int64{}
	for _, s := range b.Series {
		buckets[s.Timeframe] = s.Buckets
	}
	if len(buckets) != len(derivDurableTFs()) {
		t.Fatalf("%d timeframe(s) in the account, wanted %d", len(buckets), len(derivDurableTFs()))
	}

	days := int64(derivBudgetWindow() / (24 * time.Hour))
	for timeframe, perDay := range map[string]int64{
		"5m": 288, "15m": 96, "30m": 48, "1h": 24, "2h": 12, "4h": 6, "1d": 1,
	} {
		if got, want := buckets[timeframe], days*perDay; got != want {
			t.Errorf("%s = %d buckets per series, want %d", timeframe, got, want)
		}
	}

	if got := b.BucketsPerAsset(); got <= 7*2000 {
		t.Fatalf("Σ_tf = %d buckets per asset — the sum became the footprint of one seeding run", got)
	}
	if buckets["5m"] <= buckets["1d"] {
		t.Errorf("5m=%d and 1d=%d — the finest timeframe must cost more rows", buckets["5m"], buckets["1d"])
	}
}

func TestDerivativesDiskBudget_FactorsComeFromTheirOwnSource(t *testing.T) {
	b := NewDerivativesDiskBudget(500, []string{"1h", "4h", "1d"}, derivBudgetWindow())

	if len(b.Series) != 3 {
		t.Errorf("timeframes = %d, want 3", len(b.Series))
	}
	if b.Window != derivBudgetWindow() {
		t.Errorf("window = %v, want %v", b.Window, derivBudgetWindow())
	}
	if b.BytesPerRow <= 0 {
		t.Error("the bytes-per-row coefficient was not declared")
	}

	bWithJunk := NewDerivativesDiskBudget(500, []string{"1d", "banana"}, derivBudgetWindow())
	if len(bWithJunk.Series) != 1 || len(bWithJunk.Unparsed) != 1 || bWithJunk.Unparsed[0] != "banana" {
		t.Errorf("series=%d unparsed=%v", len(bWithJunk.Series), bWithJunk.Unparsed)
	}
}

func TestDerivativesDiskBudget_TheEightHundredAssetFleetIsPublishedAtItsRealRegime(t *testing.T) {
	b := NewDerivativesDiskBudget(800, derivDurableTFs(), derivBudgetWindow())

	const gib = float64(1 << 30)
	got := float64(b.Bytes()) / gib
	if got < 400 || got > 450 {
		t.Fatalf("A=800 → %.1f GiB — outside the range (~424 GiB)", got)
	}
	if got < 100 {
		t.Fatalf("A=800 → %.1f GiB — the order of magnitude is that of a seeding run", got)
	}
}
