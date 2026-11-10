package candle

import (
	"math"
	"testing"
)

// TestDeriveTakerSell governs the COMPATIBILITY SHIM — the only place in this service that
// produces an order-flow value instead of measuring one. See
// docs/decisions/0010-taker-sell-legacy-reconstruction.md.
//
// It runs for two producers that cannot deliver the pair: legacy cold rows, and REST/SDK
// klines, whose payload carries a taker-buy base volume and no sell. Live data never reaches it.
//
// The no-op cases are the ones that matter. ILP has no NULL, so a bar with no measured flow
// is stored with taker_buy = 0 — indistinguishable from a genuine 100%-sell bar.
// Reconstructing anyway would turn every OHLCV-only row into sell = volume: a fleet-wide
// claim of total sell aggression that no measurement supports. Declining costs a real 100%-sell
// bar written before the column existed, which stays flow-void forever; that trade is
// deliberate, and it does not apply to new data.
func TestDeriveTakerSell(t *testing.T) {
	const eps = 1e-9
	tests := []struct {
		name     string
		buy, vol float64
		wantSell float64
		wantNoop bool
	}{
		{name: "normal 60/100", buy: 60, vol: 100, wantSell: 40},
		{name: "all buy", buy: 100, vol: 100, wantSell: 0},
		{name: "clamp buy>vol — the part cannot exceed the whole", buy: 120, vol: 100, wantSell: 0},
		{name: "no taker-buy is a NO-OP, not a 100%-sell claim", buy: 0, vol: 100, wantNoop: true},
		{name: "zero volume is a no-op", buy: 60, vol: 0, wantNoop: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Candle{Volume: tt.vol, TakerBuyVolume: tt.buy}
			DeriveTakerSell(&c)
			if tt.wantNoop {
				if c.TakerSellVolume != 0 {
					t.Fatalf("expected no-op, got sell=%v — a bar with no measured buy must stay "+
						"flow-void, not be recorded as fully sell-aggressive", c.TakerSellVolume)
				}
				return
			}
			if math.Abs(c.TakerSellVolume-tt.wantSell) > eps {
				t.Errorf("sell = %v, want %v", c.TakerSellVolume, tt.wantSell)
			}
			// The reconstruction must leave the bar internally consistent: what it claims was
			// classified cannot exceed what actually traded.
			if c.TakerBuyVolume+c.TakerSellVolume > c.Volume+eps {
				t.Errorf("buy+sell (%v) exceeds volume (%v)",
					c.TakerBuyVolume+c.TakerSellVolume, c.Volume)
			}
		})
	}
}
