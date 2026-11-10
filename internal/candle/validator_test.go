package candle

import (
	"testing"
)

func TestValidateCandleAggregates_LegacyCandleSkipped(t *testing.T) {
	c := Candle{
		Timestamp: 1,
		Open:      1, High: 1, Low: 1, Close: 1,
		Volume: 1,
	}
	ok, msg := ValidateCandleAggregates(c)
	if !ok {
		t.Fatalf("legacy candle should pass aggregate validation: %s", msg)
	}
}

func TestValidateCandleAggregates_Valid(t *testing.T) {
	c := Candle{
		Volume:           10,
		TakerBuyVolume:   6,
		TakerSellVolume:  4,
		SideUnknownCount: 0,
		LateTradeCount:   0,
	}
	if ok, msg := ValidateCandleAggregates(c); !ok {
		t.Fatalf("expected valid, got: %s", msg)
	}
}

func TestValidateCandleAggregates_NegativeCounters(t *testing.T) {
	cases := []struct {
		name string
		c    Candle
	}{
		{"negative takerBuy", Candle{TakerBuyVolume: -1, TakerSellVolume: 1}},
		{"negative takerSell", Candle{TakerBuyVolume: 1, TakerSellVolume: -1}},
		{"negative sideUnknownCount", Candle{TakerBuyVolume: 1, SideUnknownCount: -1}},
		{"negative lateTradeCount", Candle{TakerBuyVolume: 1, LateTradeCount: -1}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if ok, _ := ValidateCandleAggregates(tc.c); ok {
				t.Fatalf("expected invalid for %s", tc.name)
			}
		})
	}
}

// TestValidateCandleAggregates_ClassifiedNeverExceedsTotal pins the one-sided invariant.
//
// Requiring "buy + sell == volume" treats ANY shortfall as corruption, which fires on honest
// data: the aggregator routes a trade whose side it cannot determine to SideUnknownCount instead
// of pushing it onto a side, so a bucket with unclassified flow has buy + sell < volume BY
// DESIGN. The invariant that actually holds is one-sided — the classified part cannot exceed the
// whole.
func TestValidateCandleAggregates_ClassifiedNeverExceedsTotal(t *testing.T) {
	// Fully classified.
	if ok, msg := ValidateCandleAggregates(Candle{
		Volume: 10, TakerBuyVolume: 6, TakerSellVolume: 4,
	}); !ok {
		t.Fatalf("expected valid when buy+sell == volume: %s", msg)
	}
	// PARTIALLY classified — honest data that an equality check would wrongly reject.
	if ok, msg := ValidateCandleAggregates(Candle{
		Volume: 10, TakerBuyVolume: 6, TakerSellVolume: 2, SideUnknownCount: 3,
	}); !ok {
		t.Fatalf("expected valid when part of the volume had no determinable side: %s", msg)
	}
	// Impossible: the classified part exceeds the whole.
	if ok, _ := ValidateCandleAggregates(Candle{
		Volume: 10, TakerBuyVolume: 8, TakerSellVolume: 4,
	}); ok {
		t.Fatalf("expected invalid: taker sum 12 exceeds volume 10")
	}
}

// BCHUSDT 1w 2020-01-13 as returned by fapi.binance.com: high below open and close.
func TestNormalizeOHLC_ExtendsBoundsToContainOpenAndClose(t *testing.T) {
	c := Candle{
		Timestamp: 1578873600000,
		Open:      339.65,
		High:      215.69,
		Low:       182.65,
		Close:     339.62,
		Volume:    975831.371,
	}
	got := NormalizeOHLC(c)
	if got.High != 339.65 {
		t.Fatalf("high = %v, want 339.65 (the larger of open and close)", got.High)
	}
	if got.Low != 182.65 {
		t.Fatalf("low = %v, want 182.65 (already below open and close)", got.Low)
	}
	if got.Open != c.Open || got.Close != c.Close || got.Volume != c.Volume {
		t.Fatalf("normalization changed a stated field: %+v", got)
	}
}

func TestNormalizeOHLC_LeavesValidCandleUntouched(t *testing.T) {
	c := Candle{Timestamp: 1, Open: 100, High: 110, Low: 90, Close: 105, Volume: 1}
	if got := NormalizeOHLC(c); got != c {
		t.Fatalf("valid candle changed: got %+v, want %+v", got, c)
	}
}
