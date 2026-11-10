package questdb

import (
	"math"
	"testing"
)

// TestBuildColdCandle_MeasuredPairTakenVerbatim covers rows written after
// taker_sell_volume existed: both sides were measured, so both are taken as stored —
// INCLUDING a legitimate zero. Reconstructing over a measured zero would replace a fact
// with an inference.
func TestBuildColdCandle_MeasuredPairTakenVerbatim(t *testing.T) {
	const eps = 1e-9
	ts := int64(1_700_000_000_000)
	buy, sell, vol := 60.0, 25.0, 100.0

	c := buildColdCandle(ts, 1, 2, 0.5, 1.5, vol, &buy, &sell)

	if c.Timestamp != ts {
		t.Fatalf("timestamp: want %d got %d", ts, c.Timestamp)
	}
	if math.Abs(c.TakerBuyVolume-buy) > eps || math.Abs(c.TakerSellVolume-sell) > eps {
		t.Fatalf("pair altered: got %v/%v want %v/%v",
			c.TakerBuyVolume, c.TakerSellVolume, buy, sell)
	}
	// buy+sell < volume here on purpose: 15 of the 100 had no determinable side. That is a
	// legal state, and the read must NOT "fix" it up to volume.
	if math.Abs(c.TakerBuyVolume+c.TakerSellVolume-85) > eps {
		t.Fatalf("classified total was normalised: got %v want 85",
			c.TakerBuyVolume+c.TakerSellVolume)
	}
}

// TestBuildColdCandle_MeasuredZeroSideSurvives is the case the column was added for. A
// fully buy-side bar stores sell=0; before the column existed that zero was indistinguishable
// from "not stored", and the shim would have re-derived sell = volume − buy = 0 by luck. Here
// it must be taken as the measurement it is.
func TestBuildColdCandle_MeasuredZeroSideSurvives(t *testing.T) {
	buy, sell := 100.0, 0.0
	c := buildColdCandle(1_700_000_000_000, 1, 2, 0.5, 1.5, 100, &buy, &sell)

	if c.TakerSellVolume != 0 || c.TakerBuyVolume != 100 {
		t.Fatalf("one-sided bar altered: got %v/%v want 100/0", c.TakerBuyVolume, c.TakerSellVolume)
	}
	if c.TakerBuyVolume+c.TakerSellVolume <= 0 {
		t.Fatal("a fully buy-side bar must still read as MEASURED (buy+sell > 0)")
	}
}

// TestBuildColdCandle_LegacyRowDerivesSell covers rows written BEFORE the column existed:
// taker_sell_volume is NULL, so the missing side is reconstructed from the identity a
// fully-classified kline guarantees. NULL is an ERA discriminator, not a measurement one.
func TestBuildColdCandle_LegacyRowDerivesSell(t *testing.T) {
	const eps = 1e-9
	buy, vol := 60.0, 100.0

	c := buildColdCandle(1_700_000_000_000, 1, 2, 0.5, 1.5, vol, &buy, nil)

	if math.Abs(c.TakerSellVolume-(vol-buy)) > eps {
		t.Fatalf("legacy sell not reconstructed: want %v got %v", vol-buy, c.TakerSellVolume)
	}
}

// TestBuildColdCandle_LegacyOHLCVOnlyStaysFlowVoid is the guard that keeps the ILP
// zero-sentinel honest. ILP has no NULL, so a bar with no measured flow is stored with
// taker_buy = 0. Reconstructing would make it sell = volume — a fleet-wide claim of total
// sell aggression that no measurement supports.
func TestBuildColdCandle_LegacyOHLCVOnlyStaysFlowVoid(t *testing.T) {
	buy := 0.0
	c := buildColdCandle(1_700_000_000_000, 1, 2, 0.5, 1.5, 100, &buy, nil)

	if c.TakerSellVolume != 0 {
		t.Fatalf("OHLCV-only legacy row was recorded as fully sell-aggressive: sell=%v",
			c.TakerSellVolume)
	}
	if c.TakerBuyVolume+c.TakerSellVolume > 0 {
		t.Fatal("a row with no measured flow must read as UNMEASURED (buy+sell == 0)")
	}
}

// TestBuildColdCandle_NoTakerColumnsAtAll proves a row with neither side present does not
// panic and stays flow-void.
func TestBuildColdCandle_NoTakerColumnsAtAll(t *testing.T) {
	c := buildColdCandle(1_700_000_000_000, 1, 1, 1, 1, 0, nil, nil)
	if c.TakerBuyVolume != 0 || c.TakerSellVolume != 0 {
		t.Fatalf("expected flow-void, got %v/%v", c.TakerBuyVolume, c.TakerSellVolume)
	}
}
