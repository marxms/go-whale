package candle

import (
	"math"
	"testing"
)

func TestBuildOHLCVC_TakerAggregates(t *testing.T) {
	tests := []struct {
		name             string
		trades           []Trade
		wantTakerBuy     float64
		wantTakerSell    float64
		wantUnknownCount int64
	}{
		{
			name: "buy only",
			trades: []Trade{
				{Timestamp: 60_000, Price: 100, Amount: 1, Side: "buy"},
				{Timestamp: 70_000, Price: 101, Amount: 2, Side: "BUY"},
			},
			wantTakerBuy:  3,
			wantTakerSell: 0,
		},
		{
			name: "sell only",
			trades: []Trade{
				{Timestamp: 60_000, Price: 100, Amount: 1.5, Side: "sell"},
				{Timestamp: 70_000, Price: 99, Amount: 0.5, Side: "Sell"},
			},
			wantTakerSell: 2,
		},
		{
			name: "mixed buys and sells",
			trades: []Trade{
				{Timestamp: 60_000, Price: 100, Amount: 4, Side: "buy"},
				{Timestamp: 65_000, Price: 100, Amount: 1, Side: "sell"},
				{Timestamp: 70_000, Price: 101, Amount: 1, Side: "sell"},
			},
			wantTakerBuy:  4,
			wantTakerSell: 2,
		},
		{
			name: "unknown side trade",
			trades: []Trade{
				{Timestamp: 60_000, Price: 100, Amount: 1, Side: "buy"},
				{Timestamp: 65_000, Price: 100, Amount: 1, Side: ""},
				{Timestamp: 70_000, Price: 100, Amount: 1, Side: "weird"},
			},
			wantTakerBuy:     1,
			wantUnknownCount: 2,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			out := BuildOHLCVC(tc.trades, "1m")
			if len(out) != 1 {
				t.Fatalf("expected 1 bucket, got %d", len(out))
			}
			var got BucketedCandle
			for _, c := range out {
				got = c
			}

			if got.TakerBuyVolume != tc.wantTakerBuy {
				t.Errorf("takerBuy: want %v got %v", tc.wantTakerBuy, got.TakerBuyVolume)
			}
			if got.TakerSellVolume != tc.wantTakerSell {
				t.Errorf("takerSell: want %v got %v", tc.wantTakerSell, got.TakerSellVolume)
			}
			if got.SideUnknownCount != tc.wantUnknownCount {
				t.Errorf("sideUnknownCount: want %d got %d", tc.wantUnknownCount, got.SideUnknownCount)
			}

			// OHLCV invariants must still hold.
			if got.Volume <= 0 {
				t.Errorf("volume must be > 0, got %v", got.Volume)
			}
			if got.High < got.Low {
				t.Errorf("high < low: %v < %v", got.High, got.Low)
			}
		})
	}
}

func TestBuildOHLCVC_ZeroTrades(t *testing.T) {
	out := BuildOHLCVC(nil, "1m")
	if len(out) != 0 {
		t.Fatalf("expected empty map, got %d entries", len(out))
	}
}

func TestBuildOHLCVC_PreservesLegacyOHLCV(t *testing.T) {
	trades := []Trade{
		{Timestamp: 60_000, Price: 100, Amount: 1, Side: "buy"},
		{Timestamp: 65_000, Price: 110, Amount: 2, Side: "sell"},
		{Timestamp: 70_000, Price: 95, Amount: 1, Side: "buy"},
		{Timestamp: 75_000, Price: 105, Amount: 0.5, Side: "sell"},
	}
	out := BuildOHLCVC(trades, "1m")
	if len(out) != 1 {
		t.Fatalf("expected 1 bucket, got %d", len(out))
	}
	var got BucketedCandle
	for _, c := range out {
		got = c
	}

	if got.Open != 100 {
		t.Errorf("open: want 100 got %v", got.Open)
	}
	if got.Close != 105 {
		t.Errorf("close: want 105 got %v", got.Close)
	}
	if got.High != 110 {
		t.Errorf("high: want 110 got %v", got.High)
	}
	if got.Low != 95 {
		t.Errorf("low: want 95 got %v", got.Low)
	}
	if math.Abs(got.Volume-4.5) > 1e-9 {
		t.Errorf("volume: want 4.5 got %v", got.Volume)
	}
}
